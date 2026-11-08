package kui.gateway.application.session

import scala.concurrent.duration.FiniteDuration

import cats.effect.kernel.{Async, Clock, Resource}
import cats.effect.syntax.all.*
import cats.syntax.all.*

/** Runs [[SessionStore.sweep]] on [[SessionConfig.sweepInterval]].
  *
  * The store does not start this itself. A fiber that runs for the life of the process is a composition-root
  * concern, and [[SessionConfig.sweepInterval]] is the interval `GatewayWiring` is expected to use. Without
  * it, an expired session stays in its pool until a lookup happens to drop that one id, and a pool that has
  * ever reached [[SessionConfig.maxSessions]] stays there.
  */
object SessionSweep {

  def resource[F[_]: Async](store: SessionStore[F], every: FiniteDuration): Resource[F, Unit] =
    (Async[F].sleep(every) *> Clock[F].realTimeInstant.flatMap(now =>
      store.sweep(now).void
    )).foreverM.background.void
}
