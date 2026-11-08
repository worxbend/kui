package kui.alerts.application

import cats.Monad
import cats.effect.kernel.Clock
import cats.syntax.all.*

import kui.alerts.domain.{AlertLimits, AlertRules, ClusterFactsPort, Evaluation}
import kui.kernel.ClusterId
import kui.security.Principal

/** One evaluation pass, for one cluster.
  *
  * It is a use case rather than a method on the loop so that "what one pass does" can be asserted without a
  * fibre, a clock or a `Resource` anywhere near it — the loop is then only responsible for the cadence, and
  * `AlertEvaluationLoopSuite` only has to assert the cadence.
  *
  * The pass reads the facts once, folds them with the rules over the events already open, and hands the whole
  * decision to the store in one call. Nothing between the read and the write consults a clock a second time:
  * one pass is one instant, so an event's `openedAt` and the `lastSeenAt` of the events beside it agree.
  */
trait EvaluateAlerts[F[_]] {

  /** @return
    *   what the pass decided, so the loop can log it and a suite can assert it. The store has already been
    *   told; this is a report and not an instruction.
    */
  def pass(cluster: ClusterId): F[Evaluation]
}

object EvaluateAlerts {

  def make[F[_]: {Monad, Clock}](
      facts: ClusterFactsPort[F],
      store: AlertStore[F],
      limits: AlertLimits
  ): EvaluateAlerts[F] =
    new EvaluateAlerts[F] {

      def pass(cluster: ClusterId): F[Evaluation] =
        for {
          now <- Clock[F].realTimeInstant
          read <- facts.read(cluster)
          state <- store.ruleState(cluster)
          // The whole feed rather than the open events alone. An acknowledgement stays on its row, and a
          // pass that cannot see that row opens a second event for a condition that is still firing.
          // `limit` of zero is the store's own "counts only" read, so this asks for what it needs and the
          // page size stays the browser's business.
          current <- store.feed(cluster, EvaluatingPrincipal, OpenEventsPage, None)
          evaluation = AlertRules.evaluate(now, limits, read, current.events, state)
          _ <- store.record(cluster, evaluation, now)
        } yield evaluation
    }

  /** How many open events one pass considers.
    *
    * It is the store's own ceiling rather than a page size: the fold has to see every open event or it would
    * re-open one it could not see, and a duplicate event is the failure this number exists to prevent. It is
    * a constant here and a bound there, and `InMemoryAlertStoreSuite` is what holds the two together.
    */
  val OpenEventsPage: Int = 1000

  /** Who the pass reads the feed as.
    *
    * A pass is not a person, and the read marker it would otherwise move belongs to whoever is watching. It
    * asks with `markRead = None`, so no marker moves whatever principal is named — this value only ever
    * reaches the unread *count*, which the pass discards.
    */
  private val EvaluatingPrincipal: Principal = Principal.Anonymous
}
