package kui.alerts.application

import java.time.Instant

import fs2.Stream

import kui.alerts.domain.*
import kui.kernel.ClusterId
import kui.kernel.error.KuiError
import kui.security.Principal

/** One cluster's feed, as one read answers it.
  *
  * ==Why the counts are fields rather than something the caller derives==
  *
  * `openCount` is the number in the danger pill (`SCREENS-V4.md` §3.8) and `unreadCount` is the bell's dot
  * (§3.9). Both are counted **over the whole store**, not over `events`, which is a page. A browser that
  * derived either from the rows it received would show `2 open` on a feed whose second page holds a third,
  * and wave 6's own rule says an alert count recomputed from a page is not the open count.
  *
  * @param events
  *   at most the requested page size, newest first, resolved rows included. The design's card draws five rows
  *   of which two are resolved, so a feed that returned only open events could not draw it.
  * @param openByRule
  *   how many of the open events each rule opened, over the whole store for the same reason `openCount` is.
  *   It is what lets a rule's own row say `2 open` beside the section that says the rule ran, so that a
  *   reader can tell "this rule found two things" from "this rule could not look".
  * @param lastReadAt
  *   when this principal last asked to be marked up to date, if ever. `None` means they never have, which is
  *   why every event is unread for them — not a bug, and not a reason to invent a marker at first sight.
  * @param evaluatedAt
  *   when the rules last ran for this cluster. `None` before the first pass, and the feed renders that as
  *   "KUI has not evaluated this cluster yet" rather than as a clean bill of health.
  */
final case class AlertFeed(
    events: List[AlertEvent],
    total: Int,
    openCount: Int,
    openByRule: Map[AlertRule, Int],
    unreadCount: Int,
    lastReadAt: Option[Instant],
    evaluatedAt: Option[Instant],
    reports: List[RuleReport]
)

object AlertFeed {
  given CanEqual[AlertFeed, AlertFeed] = CanEqual.derived
}

/** Where events live between passes.
  *
  * ==Why this service has a store at all==
  *
  * Every other read in KUI is a snapshot of a cluster as it is now, and `libs/cache`'s `SnapshotCell` is the
  * right shape for that. An alert is not: "a log directory crossed 80% an hour ago and is still above it" is
  * a fact with a beginning, and a cell that holds the current answer can only ever say that it is above the
  * threshold *now*. The age in every row of `M01` is the thing a snapshot cannot produce.
  *
  * ==Durability belongs to the adapter==
  *
  * Deployments with a Kafka metadata store use a compacted durable projection for events, rule state and
  * per-principal read markers. Deployments without one use the bounded in-memory adapter and deliberately
  * lose that state on restart. Keeping the choice outside this port lets the application retain one set of
  * event semantics while the composition root makes the operational trade-off explicit in its logs.
  */
trait AlertStore[F[_]] {

  /** One page of one cluster's feed, for one principal.
    *
    * @param markRead
    *   when set, the principal's read marker moves to this instant and the returned feed describes the new
    *   state (`unreadCount = 0`, `lastReadAt = markRead`). It is a parameter of the read rather than a second
    *   endpoint because there is no second RBAC action for it and `ALERTS:ACKNOWLEDGE` is the wrong one — see
    *   ADR-053 §6.
    */
  def feed(
      cluster: ClusterId,
      principal: Principal,
      limit: Int,
      markRead: Option[Instant]
  ): F[AlertFeed]

  /** Closes one event as acknowledged, or says why it cannot be.
    *
    * The refusals are the store's and not the route's, because "this event is already closed" is a fact only
    * the store holds, and a route that checked it first would be checking it against a read that another
    * request can invalidate between the two calls.
    *
    * The open count is taken from the projection this write just committed. A later `feed` would also decode
    * the caller's user state, and a failure there would report an acknowledgement that took effect as failed.
    */
  def acknowledge(
      cluster: ClusterId,
      event: AlertEventId,
      at: Instant,
      by: String
  ): F[Either[KuiError, AcknowledgedAlert]]

  /** Applies one pass: opens what fired, refreshes what is still firing, closes what cleared, and keeps the
    * rule state for the next pass.
    */
  def record(cluster: ClusterId, evaluation: Evaluation, at: Instant): F[Unit]

  /** What the rules remembered last pass. Empty for a cluster that has never been evaluated. */
  def ruleState(cluster: ClusterId): F[AlertRuleState]

  /** A cluster id whenever its feed changed — an event opened, closed or acknowledged.
    *
    * It is what makes the card and the bell agree: both read this one stream through the same SSE endpoint,
    * so neither can hold a count the other does not (ADR-035). A frame carries the id and nothing else, for
    * `ClusterStreamEndpoint`'s reason: a subscriber that sees its cluster re-reads the feed it is already
    * authorized for, which keeps one principal's unread count off every other subscriber's socket.
    */
  def changes: Stream[F, ClusterId]
}
