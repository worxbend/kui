package kui.alerts.infrastructure

import java.time.Instant

import scala.concurrent.duration.FiniteDuration

import cats.effect.kernel.{Async, Ref, Resource}
import cats.syntax.all.*
import fs2.Stream
import fs2.concurrent.Topic

import kui.alerts.application.{AlertFeed, AlertStore}
import kui.alerts.domain.*
import kui.cache.{BoundedCache, CacheMetrics}
import kui.kernel.ClusterId
import kui.kernel.error.{ApplicationError, KuiError}
import kui.security.Principal
import kui.security.audit.AuditPrincipal

/** One cluster's events, its rule state and when it was last evaluated. */
final private case class ClusterAlerts(
    events: Vector[AlertEvent],
    ruleState: AlertRuleState,
    evaluatedAt: Option[Instant],
    reports: List[RuleReport]
)

private object ClusterAlerts {
  val empty: ClusterAlerts = ClusterAlerts(Vector.empty, AlertRuleState.empty, None, Nil)
}

/** The event store, in this process's memory.
  *
  * ==Two bounds, and neither of them is optional==
  *
  * A feed with no upper bound is a slow leak with a calendar. `retention` is the operator's —
  * `kui.alerts.retention`, seven days by default, refused below an hour and above ninety days by the loader —
  * and [[InMemoryAlertStore.MaxEventsPerCluster]] is this implementation's own floor under it, because
  * retention is a promise about *time* and a cluster that opens and clears a thousand events an hour would
  * hold a million of them inside a week. Both are applied on every pass; the ceiling wins when they disagree,
  * and the events it drops are the oldest.
  *
  * ==Fallback semantics==
  *
  * This adapter is selected only when no Kafka metadata store is configured. A restart then loses the open
  * events and read markers, and the next pass re-opens whatever is still true with a fresh `openedAt`. The
  * composition root logs that degradation at startup; a configured store selects [[DurableAlertStore]].
  *
  * ==The read markers are a bounded cache and not a map==
  *
  * One entry per principal per cluster, and the number of principals is not something this process controls.
  * `libs/cache`'s `BoundedCache` is the bound, with no TTL: a marker that expired would make a bell somebody
  * cleared last week light up again, which is worse than forgetting the least recently used one.
  */
final class InMemoryAlertStore[F[_]: Async] private (
    state: Ref[F, Map[ClusterId, ClusterAlerts]],
    markers: BoundedCache[F, String, Instant],
    updates: Topic[F, ClusterId],
    retention: FiniteDuration
) extends AlertStore[F] {

  import InMemoryAlertStore.*

  def feed(
      cluster: ClusterId,
      principal: Principal,
      limit: Int,
      markRead: Option[Instant]
  ): F[AlertFeed] = {
    val key = markerKey(cluster, principal)

    for {
      held <- state.get.map(_.getOrElse(cluster, ClusterAlerts.empty))
      marker <- markers.get(key)
      ordered = held.events.sorted
      _ <- markRead.traverse_(at => markers.put(key, at))
      effectiveMarker = markRead.orElse(marker)
      // `markRead` is a state-changing read whose response becomes the browser's new source of truth. It
      // must describe the state after the write; returning the previous count leaves the bell lit until an
      // unrelated later fetch even though the marker has already moved.
      unread = ordered.count(event => effectiveMarker.forall(event.openedAt.isAfter))
    } yield AlertFeed(
      events = ordered.take(limit).toList,
      total = ordered.size,
      openCount = ordered.count(_.isOpen),
      // Counted over the whole store rather than over the page, for the reason `openCount` is: a rule's
      // own row saying `2 open` must not change when a caller asks for fewer rows.
      openByRule = ordered.filter(_.isOpen).groupBy(_.key.rule).view.mapValues(_.size).toMap,
      unreadCount = unread,
      lastReadAt = effectiveMarker,
      evaluatedAt = held.evaluatedAt,
      reports = held.reports
    )
  }

  def acknowledge(
      cluster: ClusterId,
      event: AlertEventId,
      at: Instant,
      by: String
  ): F[Either[KuiError, AlertEvent]] =
    state
      .modify { clusters =>
        val held = clusters.getOrElse(cluster, ClusterAlerts.empty)

        held.events.indexWhere(_.id == event) match {
          case -1 => (clusters, notOpen(event).asLeft[AlertEvent])
          case index if !held.events(index).isOpen =>
            (clusters, alreadyClosed(event).asLeft[AlertEvent])
          case index =>
            val closed = held.events(index).resolvedBy(at, AlertResolutionKind.Acknowledged, Some(by))

            (
              clusters.updated(cluster, held.copy(events = held.events.updated(index, closed))),
              closed.asRight[KuiError]
            )
        }
      }
      .flatTap(_.traverse_(_ => updates.publish1(cluster).void))

  def record(cluster: ClusterId, evaluation: Evaluation, at: Instant): F[Unit] = {
    val resolved = evaluation.resolved.toSet
    val refreshed = evaluation.refreshed.map(event => event.id -> event).toMap

    state
      .update { clusters =>
        val held = clusters.getOrElse(cluster, ClusterAlerts.empty)

        val updated = held.events.map { event =>
          if !event.isOpen then event
          else if resolved.contains(event.id) then event.resolvedBy(at, AlertResolutionKind.Cleared, None)
          else
            refreshed
              .get(event.id)
              .fold(event)(latest =>
                event.copy(
                  severity = latest.severity,
                  title = latest.title,
                  detail = latest.detail,
                  lastSeenAt = at
                )
              )
        }

        clusters.updated(
          cluster,
          ClusterAlerts(
            events = bounded(updated ++ evaluation.opened, at),
            ruleState = evaluation.state,
            evaluatedAt = Some(at),
            reports = evaluation.reports
          )
        )
      }
      .flatTap(_ =>
        updates
          .publish1(cluster)
          .void
          .whenA(evaluation.opened.nonEmpty || evaluation.resolved.nonEmpty || evaluation.refreshed.nonEmpty)
      )
  }

  def ruleState(cluster: ClusterId): F[AlertRuleState] =
    state.get.map(_.get(cluster).fold(AlertRuleState.empty)(_.ruleState))

  def changes: Stream[F, ClusterId] = updates.subscribeUnbounded

  /** Retention first, then the ceiling.
    *
    * An event is kept for `retention` **after it opened**, resolved or not, which is the window
    * `AlertsConfig.retention` documents. Not after it resolved: an incident review reads a week of events,
    * and a store that kept a resolved one for a further seven days would be answering a different question
    * from the one the operator configured.
    */
  private def bounded(events: Vector[AlertEvent], at: Instant): Vector[AlertEvent] = {
    val cutoff = at.minusMillis(retention.toMillis)

    events.filterNot(_.openedAt.isBefore(cutoff)).sorted.take(MaxEventsPerCluster)
  }
}

object InMemoryAlertStore {

  /** The most events one cluster's feed holds, whatever the retention says.
    *
    * Five hundred. The feed's own reading is an open count and this week's rows, and a cluster with five
    * hundred events inside its retention window has a problem no list will help with. It is a floor under a
    * memory bound rather than a product decision, which is why it is not configurable: an operator raising it
    * would be choosing how much of this process's heap the alerts feed may take, with no screen telling them
    * what they had chosen.
    */
  val MaxEventsPerCluster: Int = 500

  /** How many read markers this process remembers, across every cluster.
    *
    * Two thousand: a marker is an instant and a short key, so two thousand of them are tens of kilobytes, and
    * a deployment with more than two thousand distinct principals watching alert feeds is one where the least
    * recently used marker being forgotten is the smallest of anybody's problems.
    */
  val MaxReadMarkers: Long = 2000L

  /** The cache's `cache` metric attribute. One short stable string per *kind* of cache. */
  val MarkerCacheName: String = "alerts.readMarkers"

  /** The cluster label the marker cache's metrics carry.
    *
    * The cache is keyed by principal *and* cluster and holds every cluster's markers, so there is no single
    * real cluster to name. `CacheMetrics` wants one, and a made-up slug that says what it is beats picking an
    * arbitrary configured cluster and mislabelling every other cluster's hits as belonging to it — the
    * consumer service's assignment cache makes the same choice for the same reason.
    */
  val MarkerCacheScope: ClusterId = ClusterId.unsafe("all-clusters")

  def resource[F[_]: Async](
      retention: FiniteDuration,
      metrics: CacheMetrics[F]
  ): Resource[F, InMemoryAlertStore[F]] =
    for {
      state <- Resource.eval(Ref.of[F, Map[ClusterId, ClusterAlerts]](Map.empty))
      markers <- BoundedCache.make[F, String, Instant](
        MarkerCacheName,
        MarkerCacheScope,
        MaxReadMarkers,
        // No TTL. A marker that expired would relight a bell somebody cleared last week.
        None,
        metrics
      )
      updates <- Resource.eval(Topic[F, ClusterId])
    } yield new InMemoryAlertStore[F](state, markers, updates, retention)

  /** One principal's marker for one cluster.
    *
    * The principal is rendered the way the audit trail renders one, so that "who cleared the bell" and "who
    * acknowledged the event" are one string, and the kind is part of the key because an anonymous caller and
    * a signed-in account that happens to be called `anonymous` are not the same reader.
    */
  private[infrastructure] def markerKey(cluster: ClusterId, principal: Principal): String =
    s"${cluster.value} ${AuditPrincipal.kindOf(principal)} ${AuditPrincipal.render(principal)}"

  /** Two refusals, one code, and the reason they share it.
    *
    * `ErrorCode` has no `KUI-ALERT-NOT-FOUND` and house rule 3 forbids adding one, so an id naming no event
    * answers the same 409 as an event that is already closed. That is not a workaround: from the caller's
    * side the two are one fact — *the event you are looking at is not open* — and an acknowledgement is a
    * button on a row drawn from a feed the caller has already read. The messages tell them apart, and neither
    * says whether an id ever existed.
    */
  private[infrastructure] def notOpen(event: AlertEventId): KuiError =
    ApplicationError.Conflict(
      s"no open alert event '${event.value}' on this cluster; it may have resolved and aged out of the " +
        "feed's retention window, or the feed may have been re-read since"
    )

  private[infrastructure] def alreadyClosed(event: AlertEventId): KuiError =
    ApplicationError.Conflict(
      s"alert event '${event.value}' is already closed, so there is nothing to acknowledge"
    )
}
