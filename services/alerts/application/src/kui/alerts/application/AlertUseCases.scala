package kui.alerts.application

import cats.Monad
import cats.effect.kernel.Clock
import cats.syntax.all.*

import kui.alerts.domain.{AlertEvent, AlertEventId}
import kui.kernel.ClusterId
import kui.kernel.error.KuiError
import kui.security.Principal
import kui.security.audit.AuditPrincipal

/** What an acknowledgement answers.
  *
  * The new `openCount` travels with the event so that the pill and the bell move on the response the caller
  * already has. A browser that re-derived the count by removing one row from the page it was holding would be
  * counting a page, which is the mistake this wave has a rule about.
  */
final case class AcknowledgedAlert(event: AlertEvent, openCount: Int)

object AcknowledgedAlert {
  given CanEqual[AcknowledgedAlert, AcknowledgedAlert] = CanEqual.derived
}

/** The two things a caller can do to this service.
  *
  * ==Why the feed is not `Section`-shaped here==
  *
  * Rule A3 keeps the wire out of this layer, so a `Section` cannot appear in a signature. What crosses is an
  * [[AlertFeed]] with the per-rule [[kui.alerts.domain.RuleReport]]s inside it, and `AlertsMapping` turns
  * each report into its own `Section` — which is what "`Section`-wrapped per rule" means and why one dead
  * rule costs one row rather than the document (ADR-053 §7).
  *
  * ==`Left` is only ever a wrong request==
  *
  * A cluster KUI has never heard of is a 404, because the caller followed a link to something that does not
  * exist. Everything else answers 200 with a feed that says what it knows, exactly as the metrics service
  * does: the alerts card sits on a dashboard beside cards that work, and a 4xx would make a feed behaving
  * correctly indistinguishable from a broken one.
  */
trait AlertUseCases[F[_]] {

  def feed(
      principal: Principal,
      cluster: ClusterId,
      limit: Int,
      markRead: Boolean
  ): F[Either[KuiError, AlertFeed]]

  def acknowledge(
      principal: Principal,
      cluster: ClusterId,
      event: AlertEventId
  ): F[Either[KuiError, AcknowledgedAlert]]
}

object AlertUseCases {

  def make[F[_]: {Monad, Clock}](
      profiles: ClusterProfileSource[F],
      store: AlertStore[F],
      guard: MutationGuard[F]
  ): AlertUseCases[F] =
    new AlertUseCases[F] {

      def feed(
          principal: Principal,
          cluster: ClusterId,
          limit: Int,
          markRead: Boolean
      ): F[Either[KuiError, AlertFeed]] =
        profiles.profileOf(cluster).flatMap {
          case Left(error) => error.asLeft[AlertFeed].pure[F]
          case Right(_) =>
            for {
              now <- Clock[F].realTimeInstant
              answer <- store.feed(cluster, principal, limit, Option.when(markRead)(now))
            } yield answer.asRight[KuiError]
        }

      /** The write, and every part of it that is not the store's is the guard's.
        *
        * The permission check is not here and must not be: `SecuredRoutes` runs `RbacGuard` before this
        * method is called at all, so a principal without `ALERTS:ACKNOWLEDGE` never reaches the store. Adding
        * a second check here would be a second place for the rule to be written and the one that can disagree
        * — and it would move the refusal to *after* the route had already decided to run the logic, which is
        * exactly the property `AlertsRoutesSuite` asserts.
        */
      def acknowledge(
          principal: Principal,
          cluster: ClusterId,
          event: AlertEventId
      ): F[Either[KuiError, AcknowledgedAlert]] =
        guard.guard(principal, cluster, event.value) {
          for {
            now <- Clock[F].realTimeInstant
            // The count is the one in the projection this write committed, not a second read of the feed.
            // That read also decodes the caller's user state, and raising from it recorded a committed
            // acknowledgement as Failed.
            closed <- store.acknowledge(cluster, event, now, AuditPrincipal.render(principal))
          } yield closed
        }
    }
}
