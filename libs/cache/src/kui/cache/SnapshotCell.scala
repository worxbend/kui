package kui.cache

import java.time.Instant

import scala.concurrent.duration.FiniteDuration

import cats.effect.kernel.Outcome
import cats.effect.std.{Mutex, Supervisor}
import cats.effect.syntax.all.*
import cats.effect.{Deferred, Ref, Resource, Temporal}
import cats.syntax.all.*
import fs2.Stream
import fs2.concurrent.Topic
import org.typelevel.log4cats.Logger

import kui.kernel.ClusterId
import kui.kernel.error.{InfrastructureError, KuiError}

/** One value, kept fresh in the background, always readable.
  *
  * The contract, in four sentences, because every screen in KUI depends on it:
  *
  *   - `get` never blocks on the upstream and never fails.
  *   - A failed refresh leaves the previous value in place and changes only the status.
  *   - `scrapedAt` advances only on success.
  *   - Replacement is atomic: a reader sees the old value or the new one, never a mixture and never an empty
  *     gap during a refresh.
  *
  * This is the milestone's most visible promise. When a cluster stops answering, the page keeps showing what
  * KUI last saw, greyed out and stamped with the time it was seen, instead of going blank or hanging.
  */
trait SnapshotCell[F[_], A] {

  /** The current snapshot. A pure read of a `Ref`: no I/O, no waiting, no failure. */
  def get: F[Snapshot[A]]

  /** Forces a refresh and returns the snapshot that results.
    *
    * Idempotent under concurrency: a caller arriving while a refresh is in flight joins that refresh instead
    * of starting another, and receives the *new* value rather than the one from before its own call. Five
    * presses of the refresh button are one request to the broker, which is what stops that button from being
    * an outage tool.
    */
  def refresh: F[Snapshot[A]]

  /** Drops the value, returns to `Initializing`, and reloads.
    *
    * For a profile change, where the previous value describes a cluster that is no longer the one configured.
    * Showing it greyed out would be showing another cluster's data.
    */
  def invalidate: F[Snapshot[A]]

  /** Successful snapshots, for a caller that would rather react than poll.
    *
    * Only successes: a failure is visible through `get`, and publishing failures here would make every
    * subscriber implement the same filter.
    */
  def updates: Stream[F, Snapshot[A]]
}

/** A `KuiError` raised through an effect that can only carry a `Throwable`.
  *
  * `SnapshotStatus.Offline` holds a `KuiError`, and `KuiError` is deliberately not a `Throwable` — it is a
  * value in business code. A `load` that already knows which error it hit wraps it in this and the cell
  * unwraps it; anything else becomes an `InfrastructureError.Upstream`, because a cell that could not
  * represent an arbitrary failure would have to either throw or lose it.
  */
final case class SnapshotLoadFailure(error: KuiError)
    extends RuntimeException(error.message)
    with scala.util.control.NoStackTrace

object SnapshotCell {

  /** Creates a cell and starts its refresh loop under a `Supervisor`.
    *
    * The supervisor is what makes cancellation correct rather than hopeful: releasing the `Resource` cancels
    * an in-flight refresh instead of leaking a fiber that outlives the component it belonged to — and in this
    * codebase that fiber would be holding a Kafka admin client.
    *
    * @param name
    *   the `cache` metric attribute and the log context: one short stable string per *kind* of snapshot, such
    *   as `cluster.topology`. Never a per-cluster value — the cluster has its own attribute, and a metric
    *   label whose cardinality grows with the number of clusters is how a metrics backend runs out of memory.
    * @param interval
    *   the background cadence: 30 seconds for the cluster snapshot, an hour for capabilities
    *   (`ARCHITECTURE.md` §9). There is no TTL and no expiry. A value older than `interval` is shown and
    *   marked stale, never withheld.
    * @param load
    *   the refresh. A failure is caught, mapped and recorded, and never reaches a reader.
    */
  def resource[F[_]: Temporal, A](
      name: String,
      cluster: ClusterId,
      interval: FiniteDuration,
      metrics: CacheMetrics[F],
      log: Option[Logger[F]] = None
  )(load: F[A]): Resource[F, SnapshotCell[F, A]] =
    for {
      state <- Resource.eval(Ref.of[F, Snapshot[A]](Snapshot.initializing[A]))
      inFlight <- Resource.eval(Ref.of[F, Option[Deferred[F, Option[Snapshot[A]]]]](None))
      lock <- Resource.eval(Mutex[F])
      topic <- Resource.eval(Topic[F, Snapshot[A]])
      supervisor <- Supervisor[F]
      cell = new Impl[F, A](name, cluster, state, inFlight, topic, metrics, log, load, lock)
      // Sleep *after* the refresh, so a cell has data as soon as it can rather than one interval
      // later. Supervised, so the fiber dies with the resource.
      _ <- Resource.eval(
        supervisor.supervise((cell.refresh >> Temporal[F].sleep(interval)).foreverM)
      )
    } yield cell

  /** A cell that never refreshes, for a value that is genuinely constant and for a test that needs a
    * `SnapshotCell` without a background loop.
    */
  def constant[F[_]: Temporal, A](value: A, at: Instant): SnapshotCell[F, A] =
    new SnapshotCell[F, A] {
      private val snapshot = Snapshot.online(value, at)

      def get: F[Snapshot[A]] = snapshot.pure[F]
      def refresh: F[Snapshot[A]] = snapshot.pure[F]
      def invalidate: F[Snapshot[A]] = snapshot.pure[F]
      def updates: Stream[F, Snapshot[A]] = Stream.emit(snapshot)
    }

  final private class Impl[F[_]: Temporal, A](
      name: String,
      cluster: ClusterId,
      state: Ref[F, Snapshot[A]],
      inFlight: Ref[F, Option[Deferred[F, Option[Snapshot[A]]]]],
      topic: Topic[F, Snapshot[A]],
      metrics: CacheMetrics[F],
      log: Option[Logger[F]],
      load: F[A],
      lock: Mutex[F]
  ) extends SnapshotCell[F, A] {

    def get: F[Snapshot[A]] =
      state.get.flatTap { snapshot =>
        if snapshot.value.isEmpty then metrics.miss(name, cluster)
        else if snapshot.isStale then
          // A stale read is a hit *and* a stale read: it did serve data, and whether the data was
          // old is a separate question an operator asks separately.
          metrics.hit(name, cluster) >> metrics.staleRead(name, cluster)
        else metrics.hit(name, cluster)
      }

    /** One refresh at a time, and everybody waiting gets the result of the refresh that was running when they
      * arrived.
      *
      * The `Deferred` is what makes "forced refresh" honest. Without it a second caller would receive the
      * value that was current *before* its own call, and the refresh button would appear to do nothing when
      * pressed twice.
      */
    def refresh: F[Snapshot[A]] =
      Temporal[F].uncancelable { poll =>
        Deferred[F, Option[Snapshot[A]]].flatMap { gate =>
          lock.lock
            .surround(
              inFlight
                .modify {
                  case Some(running) => (Some(running), Left(running))
                  case None => (Some(gate), Right(gate))
                }
            )
            .flatMap {
              // Somebody else is already loading: wait for *their* result. `poll` makes the wait
              // itself cancellable, so a caller that gives up does not pin its fiber.
              case Left(running) => poll(running.get).flatMap(_.fold(poll(refresh))(_.pure[F]))
              case Right(mine) =>
                poll(runLoad(mine))
                  .guaranteeCase { outcome =>
                    // The slot is released and the waiters are woken on every path, cancellation
                    // included. Without this, a cancelled refresh leaves every later caller blocked
                    // on a `Deferred` nobody will ever complete — a screen that never loads again,
                    // which is the exact failure this type exists to prevent.
                    lock.lock.surround {
                      inFlight.get.flatMap { current =>
                        if current.contains(mine) then
                          inFlight.set(None) >> (outcome match {
                            case Outcome.Succeeded(loaded) => loaded.flatMap(mine.complete).void
                            case Outcome.Errored(_) | Outcome.Canceled() =>
                              state.get.flatMap(snapshot => mine.complete(Some(snapshot))).void
                          })
                        else mine.complete(None).void
                      }
                    }
                  }
                  .flatMap(_ => mine.get.flatMap(_.fold(poll(refresh))(_.pure[F])))
            }
        }
      }

    def invalidate: F[Snapshot[A]] =
      Temporal[F].uncancelable { poll =>
        lock.lock.surround {
          inFlight.getAndSet(None).flatMap { old =>
            state.set(Snapshot.initializing[A]) >> old.traverse_(_.complete(None).void)
          }
        } >> poll(refresh)
      }

    def updates: Stream[F, Snapshot[A]] = topic.subscribeUnbounded

    private def runLoad(generation: Deferred[F, Option[Snapshot[A]]]): F[Option[Snapshot[A]]] =
      load.attempt.flatMap { result =>
        lock.lock.surround {
          inFlight.get.flatMap { current =>
            if !current.contains(generation) then none[Snapshot[A]].pure[F]
            else
              (result match {
                case Right(value) => recordSuccess(value)
                case Left(failure) => recordFailure(failure)
              }).map(_.some)
          }
        }
      }

    private def recordSuccess(value: A): F[Snapshot[A]] =
      for {
        now <- nowInstant
        // One atomic write, so a reader sees the old snapshot or the new one and never a
        // half-updated pair of value and timestamp.
        updated = Snapshot.online(value, now)
        previous <- state.getAndSet(updated)
        _ <- previous.status match {
          case SnapshotStatus.Offline(_, since) =>
            logged(
              _.info(
                s"$name for cluster ${cluster.value} recovered after " +
                  s"${java.time.Duration.between(since, now).toSeconds}s offline"
              )
            )
          // Nothing at all on a success after a success. A per-cluster INFO line every thirty
          // seconds is a log file nobody can read. Slow to complain, quick to say it is better.
          case SnapshotStatus.Online | SnapshotStatus.Initializing => Temporal[F].unit
        }
        _ <- topic.publish1(updated).void
      } yield updated

    private def recordFailure(failure: Throwable): F[Snapshot[A]] = {
      val error = asKuiError(failure)

      for {
        now <- nowInstant
        change <- state.modify { previous =>
          val since = previous.status match {
            // Sticky: the question about a greyed-out row is "how long has this been down", and a
            // `since` that resets on every retry answers "thirty seconds" for ever.
            case SnapshotStatus.Offline(_, first) => first
            case SnapshotStatus.Online | SnapshotStatus.Initializing => now
          }

          // The value and its `scrapedAt` are carried over untouched. Advancing the timestamp on a
          // failed refresh would have the UI say "as of just now" about data that did not change.
          val updated = previous.copy(status = SnapshotStatus.Offline(error, since))

          (updated, (previous.status.isOffline, updated))
        }
        (wasAlreadyOffline, updated) = change
        _ <- metrics.refreshFailed(name, cluster)
        _ <-
          if wasAlreadyOffline then Temporal[F].unit
          else logged(_.warn(s"$name for cluster ${cluster.value} is offline: ${error.code.wire}"))
      } yield updated
    }

    private def nowInstant: F[Instant] =
      Temporal[F].realTime.map(elapsed => Instant.ofEpochMilli(elapsed.toMillis))

    private def asKuiError(failure: Throwable): KuiError = failure match {
      case SnapshotLoadFailure(error) => error
      case _ => InfrastructureError.Upstream("snapshot", 502)
    }

    private def logged(write: Logger[F] => F[Unit]): F[Unit] =
      log.fold(Temporal[F].unit)(write)
  }
}
