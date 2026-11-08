package kui.config.store

import scala.concurrent.duration.FiniteDuration

import cats.effect.syntax.all.*
import cats.effect.{Async, Deferred, Ref}
import cats.syntax.all.*

/** Parks a writer until the tail follower has applied a given offset.
  *
  * The store's whole write contract is "produce the record, then wait until it comes back around the log".
  * That is what makes a successful write mean the writer has read its own record from the log rather than
  * hopefully mutated a local map — and it needs one small piece of machinery, which is this.
  *
  * It lives in its own file so that the timing rule is specified rather than hoped for: every case below is
  * tested with `TestControl` and no broker, including the one that would otherwise show up as an intermittent
  * timeout in production once a month.
  */
final class WriteWaiter[F[_]: Async] private (state: Ref[F, WriteWaiter.State[F]]) {

  /** Completes once the follower has applied `offset` or later.
    *
    * The already-past case is checked under the same `modify` that registers the waiter, so the follower
    * cannot advance in the window between the two and leave a writer parked for a record that has already
    * gone by.
    */
  def await(offset: Long, timeout: FiniteDuration): F[Either[StoreError, Unit]] =
    Deferred[F, Either[StoreError, Unit]].flatMap { gate =>
      state
        .modify { current =>
          current.failure match {
            case Some(reason) => (current, Left(reason).some)
            case None if current.applied >= offset => (current, Right(()).some)
            case None => (current.copy(waiting = current.waiting :+ (offset -> gate)), none)
          }
        }
        .flatMap {
          case Some(immediate) => Async[F].pure(immediate)
          case None =>
            gate.get
              .timeoutTo(
                timeout,
                Async[F].pure(Left(StoreError.WriteTimeout(offset, timeout.toMillis)))
              )
              // A waiter left in the map after its fiber is gone is a slow leak that only appears under
              // load, which is the worst time to find one. Cancellation and timeout both clean up.
              .guarantee(state.update(_.remove(gate)))
        }
    }

  /** Called by the tail follower for every applied record, in order.
    *
    * Monotonic and idempotent: a repeated or out-of-order offset never moves the mark backwards and never
    * resurrects a waiter that has already been completed.
    */
  def advance(offset: Long): F[Unit] =
    state
      .modify { current =>
        val reached = math.max(current.applied, offset)
        val (ready, still) = current.waiting.partition((wanted, _) => wanted <= reached)
        (current.copy(applied = reached, waiting = still), ready.map(_._2))
      }
      .flatMap(_.traverse_(_.complete(Right(())).void))

  /** Called when the follower dies, so writers fail fast instead of waiting out their timeout.
    *
    * The failure is sticky: a writer arriving after the follower is gone is told immediately rather than
    * parked against a log nobody is reading any more. [[recover]] clears it once a later attempt is reading
    * the log again — a retry that comes back must not keep refusing writes with the reason of an outage that
    * is over.
    */
  def fail(reason: StoreError): F[Unit] =
    state
      .modify(current => (current.copy(failure = Some(reason), waiting = Nil), current.waiting.map(_._2)))
      .flatMap(_.traverse_(_.complete(Left(reason)).void))

  /** Lets writers park again after the follower has resumed. */
  def recover: F[Unit] =
    state.update(_.copy(failure = None))

  /** How many writers are parked. For tests and for the health surface, never for a decision. */
  def pending: F[Int] = state.get.map(_.waiting.size)
}

object WriteWaiter {

  final private[store] case class State[F[_]](
      applied: Long,
      waiting: List[(Long, Deferred[F, Either[StoreError, Unit]])],
      failure: Option[StoreError]
  ) {
    def remove(gate: Deferred[F, Either[StoreError, Unit]]): State[F] =
      copy(waiting = waiting.filterNot((_, pending) => pending eq gate))
  }

  def create[F[_]: Async]: F[WriteWaiter[F]] =
    Ref.of[F, State[F]](State(-1L, Nil, None)).map(new WriteWaiter(_))
}
