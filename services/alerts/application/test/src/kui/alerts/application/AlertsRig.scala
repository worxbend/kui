package kui.alerts.application

import java.time.Instant

import scala.concurrent.duration.DurationInt

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import fs2.Stream

import kui.alerts.domain.*
import kui.kernel.ClusterId
import kui.kernel.error.{ApplicationError, ErrorCode, KuiError}
import kui.security.Principal

/** The fakes the application suites build on.
  *
  * The store is a real implementation of the port over a `Ref` rather than a stub that answers constants,
  * because two of the required cases are about what the store was **asked to do** — "refused before the store
  * is written" and "the open count is the count of open events" — and a stub cannot tell a caller that was
  * refused from one that was never made.
  */
object AlertsRig {

  val cluster: ClusterId = ClusterId.unsafe("prod-eu")
  val readOnlyCluster: ClusterId = ClusterId.unsafe("prod-us")

  /** The instant every seeded event opens at.
    *
    * Deliberately in the past rather than "now": the use cases read a real clock, and the read marker they
    * set is that clock's instant. An event stamped in the future would still count as unread after being
    * marked read, which would look like a bug in the marker instead of a bug in the fixture.
    */
  val at: Instant = Instant.parse("2020-01-01T00:00:00Z")

  val caller: Principal = Principal.Anonymous

  val limits: AlertLimits = AlertLimits(1, 1, 5.minutes, 80, 90)

  def event(rule: AlertRule, subject: String, openedAt: Instant = at): AlertEvent =
    AlertEvent.open(AlertKey(rule, subject), AlertSeverity.Warning, openedAt, s"${rule.wire} on $subject", "")

  /** A profile source over two clusters, one of them read-only. */
  final class Profiles extends ClusterProfileSource[IO] {

    private val views = List(
      ClusterProfileView(cluster, "Production EU", readOnly = false),
      ClusterProfileView(readOnlyCluster, "Production US", readOnly = true)
    )

    def profileOf(id: ClusterId): IO[Either[KuiError, ClusterProfileView]] =
      IO.pure(
        views
          .find(_.cluster == id)
          .toRight(ApplicationError.NotFound("cluster", id.value, ErrorCode.ClusterNotFound): KuiError)
      )

    def all: IO[List[ClusterProfileView]] = IO.pure(views)
  }

  /** Every acknowledgement record the guard wrote, in order. */
  final class RecordingSink(val written: Ref[IO, List[AcknowledgementRecord]])
      extends AcknowledgementSink[IO] {

    def record(entry: AcknowledgementRecord): IO[Unit] = written.update(_ :+ entry)
  }

  object RecordingSink {
    def create: IO[RecordingSink] = Ref.of[IO, List[AcknowledgementRecord]](Nil).map(new RecordingSink(_))
  }

  /** A store over a `Ref`, counting the writes it was asked to make. */
  final class FakeStore(
      state: Ref[IO, Map[ClusterId, List[AlertEvent]]],
      markers: Ref[IO, Map[String, Instant]],
      val writes: Ref[IO, Int]
  ) extends AlertStore[IO] {

    def feed(
        id: ClusterId,
        principal: Principal,
        limit: Int,
        markRead: Option[Instant]
    ): IO[AlertFeed] =
      for {
        events <- state.get.map(_.getOrElse(id, Nil).sorted)
        marker <- markers.get.map(_.get(key(id, principal)))
        _ <- markRead.traverse_(when => markers.update(_.updated(key(id, principal), when)))
        effectiveMarker = markRead.orElse(marker)
        unread = events.count(event => effectiveMarker.forall(event.openedAt.isAfter))
      } yield AlertFeed(
        events = events.take(limit),
        total = events.size,
        openCount = events.count(_.isOpen),
        openByRule = events.filter(_.isOpen).groupBy(_.key.rule).view.mapValues(_.size).toMap,
        unreadCount = unread,
        lastReadAt = effectiveMarker,
        evaluatedAt = Some(at),
        reports = AlertRule.All.map(RuleReport(_, RuleOutcome.evaluated))
      )

    def acknowledge(
        id: ClusterId,
        event: AlertEventId,
        when: Instant,
        by: String
    ): IO[Either[KuiError, AcknowledgedAlert]] =
      writes.update(_ + 1) >> state.modify { clusters =>
        val held = clusters.getOrElse(id, Nil)

        held.indexWhere(_.id == event) match {
          case -1 =>
            (
              clusters,
              ApplicationError.Conflict(s"no open alert event '${event.value}'").asLeft[AcknowledgedAlert]
            )
          case index if !held(index).isOpen =>
            (
              clusters,
              ApplicationError.Conflict(s"'${event.value}' is already closed").asLeft[AcknowledgedAlert]
            )
          case index =>
            val closed = held(index).resolvedBy(when, AlertResolutionKind.Acknowledged, Some(by))
            val events = held.updated(index, closed)
            (
              clusters.updated(id, events),
              AcknowledgedAlert(closed, events.count(_.isOpen)).asRight[KuiError]
            )
        }
      }

    def record(id: ClusterId, evaluation: Evaluation, when: Instant): IO[Unit] =
      state.update(clusters => clusters.updated(id, clusters.getOrElse(id, Nil) ++ evaluation.opened))

    def ruleState(id: ClusterId): IO[AlertRuleState] = IO.pure(AlertRuleState.empty)

    def changes: Stream[IO, ClusterId] = Stream.empty

    def seed(id: ClusterId, events: List[AlertEvent]): IO[Unit] =
      state.update(clusters => clusters.updated(id, clusters.getOrElse(id, Nil) ++ events))

    private def key(id: ClusterId, principal: Principal): String = s"${id.value}|${principal.name.value}"
  }

  object FakeStore {
    def create: IO[FakeStore] =
      for {
        state <- Ref.of[IO, Map[ClusterId, List[AlertEvent]]](Map.empty)
        markers <- Ref.of[IO, Map[String, Instant]](Map.empty)
        writes <- Ref.of[IO, Int](0)
      } yield new FakeStore(state, markers, writes)
  }
}
