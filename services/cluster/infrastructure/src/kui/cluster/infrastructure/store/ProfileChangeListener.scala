package kui.cluster.infrastructure.store

import scala.concurrent.duration.*

import cats.effect.kernel.{Async, Ref, Resource}
import cats.effect.std.{Random, Semaphore}
import cats.syntax.all.*
import fs2.Stream
import fs2.concurrent.Topic
import org.typelevel.log4cats.StructuredLogger

import kui.cluster.domain.{ClusterConfigStore, ClusterProfile, ProfileVersion, StoreHealth}
import kui.kernel.ClusterId

/** Turns "the metadata store changed" into "this cluster is now at this version".
  *
  * A cluster registered or edited on one replica has to become visible on every replica without a restart,
  * and the version that the other services subscribe to has to go up so that their Kafka clients are rebuilt
  * (ADR-036). This component is the piece of that which sits between the store and the registry.
  *
  * ==Why it is idempotent rather than filtered==
  *
  * The store's change feed carries this process's *own* writes back to it. That is not a nuisance to filter
  * out — it is the read-your-writes mechanism (ADR-042 §3) and the reason a write can be confirmed at all. So
  * the listener is idempotent instead: a cluster whose id and version it already holds is a no-op, with no
  * reconcile, no version bump and no event.
  *
  * Suppressing self-writes by writer identity instead would break the multi-replica case, which is the case
  * the whole design exists for. Replica B's write looks exactly like a foreign write to replica A and exactly
  * like a self-write to replica B, and only one of those two may be ignored.
  *
  * ==Why versions only ever go up==
  *
  * A store reconnect replays records this process has already seen. If a version could go backwards, every
  * downstream service's version comparison would be wrong in the silent direction: they would conclude
  * nothing had changed and keep talking to the old brokers with the old credentials. The recorded version for
  * a cluster is therefore the larger of what is held and what arrived.
  */
trait ProfileChangeListener[F[_]] {

  /** One element per genuine change, in the order the store delivered them. */
  def changes: Stream[F, ProfileChanged]

  /** The versions this listener currently believes each cluster is at. For tests and diagnostics. */
  def known: F[Map[ClusterId, ProfileVersion]]
}

object ProfileChangeListener {

  /** The backoff for a reconcile that failed: one second, doubling, capped, jittered.
    *
    * Capped because an uncapped doubling reaches half an hour after a dozen failures and the store may have
    * come back thirty minutes earlier. Jittered because every replica sees the same store outage at the same
    * moment, and an unjittered backoff would have all of them retry in the same millisecond for as long as
    * the outage lasts.
    */
  val InitialBackoff: FiniteDuration = 1.second
  val MaxBackoff: FiniteDuration = 30.seconds

  /** Subscribes to the store and publishes what genuinely changed.
    *
    * @param reconcile
    *   what to do with a new full list — in the running service, the registry's reload. It is a function
    *   rather than the registry itself because the registry lives in the application layer, which an adapter
    *   must not see; the composition root supplies it.
    */
  def resource[F[_]: Async](
      store: ClusterConfigStore[F],
      reconcile: List[ClusterProfile] => F[Unit],
      logger: StructuredLogger[F]
  ): Resource[F, ProfileChangeListener[F]] =
    for {
      topic <- Resource.eval(Topic[F, ProfileChanged])
      versions <- Resource.eval(Ref.of[F, Map[ClusterId, ProfileVersion]](Map.empty))
      degraded <- Resource.eval(Ref.of[F, Boolean](false))
      random <- Resource.eval(Random.scalaUtilRandom[F])
      // One admission at a time. The change feed and the registration re-read below both call `accept`,
      // and a stale list applied after a newer one would drop the newer cluster from `versions`.
      gate <- Resource.eval(Semaphore[F](1))
      listener = new Impl[F](store, reconcile, logger, topic, versions, degraded, random, gate)
      // Deregistration runs on release, including on the cancellation path: a handler left registered on a
      // store that outlives this resource would keep calling a reconcile whose owner is gone.
      _ <- Resource.make(store.onChange(listener.apply))(deregister => deregister)
      // The feed does not replay. A write that landed after the registry's first load and before the
      // handler above was registered was delivered to an empty map. Read the list once, now that a
      // handler exists, so that write is not stuck until some later cluster change.
      _ <- Resource.eval(listener.catchUp)
      // Closing the topic wakes every subscriber with an end of stream rather than leaving it parked on a
      // topic nobody will publish to again.
      _ <- Resource.onFinalize(topic.close.void)
    } yield listener

  /** The changes one emitted list implies, given what is already known.
    *
    * Pure, and separate from the effectful part, because this is where every interesting decision is: what
    * counts as new, what counts as a removal, and what a replayed record does. A removal is a cluster that
    * was known and is absent from a *complete* list — which is safe only because the store emits whole lists
    * and never deltas.
    */
  def diff(
      known: Map[ClusterId, ProfileVersion],
      incoming: List[ClusterProfile],
      at: java.time.Instant
  ): (Map[ClusterId, ProfileVersion], List[ProfileChanged]) = {
    val arrived = incoming.map(profile => profile.id -> profile.version).toMap

    val upserts = incoming.flatMap { profile =>
      known.get(profile.id) match {
        case None => Some(ProfileChanged(profile.id, profile.version, ProfileChanged.Kind.Added, at))
        case Some(seen) if profile.version.value > seen.value =>
          Some(ProfileChanged(profile.id, profile.version, ProfileChanged.Kind.Updated, at))
        // Same version, or an older one replayed after a reconnect. Nothing happened.
        case Some(_) => None
      }
    }

    val removals = (known.keySet -- arrived.keySet).toList.sortBy(_.value).map { id =>
      ProfileChanged(id, known.getOrElse(id, ProfileVersion.Static), ProfileChanged.Kind.Removed, at)
    }

    // `max` per cluster, so a replayed older record can never walk a version backwards.
    val merged = (known.keySet ++ arrived.keySet).view
      .filter(arrived.contains)
      .map { id =>
        val seen = known.get(id).map(_.value).getOrElse(0L)
        val now = arrived.get(id).map(_.value).getOrElse(0L)
        id -> ProfileVersion.unsafe(math.max(seen, now))
      }
      .toMap

    (merged, upserts ++ removals)
  }

  /** The larger version per cluster, keyed by what the newer map holds.
    *
    * Used when committing a reconcile that may have been retried for a while: a cluster the newer map has
    * dropped is dropped, and one both hold keeps whichever version is higher.
    */
  def merge(
      current: Map[ClusterId, ProfileVersion],
      merged: Map[ClusterId, ProfileVersion]
  ): Map[ClusterId, ProfileVersion] =
    merged.map { (id, version) =>
      val seen = current.get(id).map(_.value).getOrElse(0L)
      id -> ProfileVersion.unsafe(math.max(seen, version.value))
    }

  final private class Impl[F[_]: Async](
      store: ClusterConfigStore[F],
      reconcile: List[ClusterProfile] => F[Unit],
      logger: StructuredLogger[F],
      topic: Topic[F, ProfileChanged],
      versions: Ref[F, Map[ClusterId, ProfileVersion]],
      degraded: Ref[F, Boolean],
      random: Random[F],
      gate: Semaphore[F]
  ) extends ProfileChangeListener[F] {

    def changes: Stream[F, ProfileChanged] = topic.subscribeUnbounded

    def known: F[Map[ClusterId, ProfileVersion]] = versions.get

    /** One emission. Applied one at a time, in the order the store delivered them: `__kui_config` has a
      * single partition precisely so that a total order is available, and processing it concurrently would
      * throw that away for no gain, since metadata writes are rare.
      */
    def apply(profiles: List[ClusterProfile]): F[Unit] =
      gate.permit.use(_ => accept(profiles, publish = true))

    /** The list as it stands when the handler is registered.
      *
      * Not published. Every stored cluster would otherwise be logged as added on every start, and nothing
      * subscribed yet could hear it. `reconcile` re-reads, which is what replaces a registry snapshot taken
      * before this handler existed. Versions are recorded so the next identical emission is a no-op.
      */
    def catchUp: F[Unit] =
      gate.permit.use { _ =>
        store.list.flatMap {
          case Left(error) =>
            logger.warn(
              s"could not read the cluster list when subscribing to store changes: ${error.message}"
            )
          case Right(profiles) => accept(profiles, publish = false)
        }
      }

    private def accept(profiles: List[ClusterProfile], publish: Boolean): F[Unit] =
      for {
        now <- Async[F].realTimeInstant
        current <- versions.get
        (merged, events) = ProfileChangeListener.diff(current, profiles, now)
        _ <- noteHealth
        _ <-
          if events.isEmpty then Async[F].unit
          else if publish then applyChange(profiles, merged, events)
          else catchUpChange(profiles, merged)
      } yield ()

    /** One attempt, not the feed's retry loop.
      *
      * This runs while the process is still starting. Retrying with backoff here would hold startup, and the
      * gate, until the registry accepted the list. A failed attempt leaves the versions unset, so the next
      * store change reconciles it on the feed, which does retry.
      */
    private def catchUpChange(
        profiles: List[ClusterProfile],
        merged: Map[ClusterId, ProfileVersion]
    ): F[Unit] =
      reconcile(profiles).attempt.flatMap {
        case Left(failure) =>
          logger.warn(
            s"reconciling the cluster registry at subscribe failed (${failure.getClass.getName}); " +
              "the next store change will retry"
          )
        case Right(_) => versions.update(current => ProfileChangeListener.merge(current, merged))
      }

    /** Reconcile first, record afterwards.
      *
      * The order matters and it is the opposite of the obvious one. Recording the new versions before the
      * reconcile succeeded would make a failed reconcile permanent: the next identical emission would look
      * like a record already applied, and this replica would keep serving the old profiles for ever with no
      * error after the first line.
      */
    private def applyChange(
        profiles: List[ClusterProfile],
        merged: Map[ClusterId, ProfileVersion],
        events: List[ProfileChanged]
    ): F[Unit] =
      withBackoff(reconcile(profiles), ProfileChangeListener.InitialBackoff) >>
        // `max` again at the commit, not a blind overwrite: the retry may have taken long enough for a later
        // emission to have moved a version on, and a plain `set` would walk it back.
        versions.update(current => ProfileChangeListener.merge(current, merged)) >>
        events.traverse_ { event =>
          logger.info(
            s"cluster ${event.clusterId.value} ${event.kind.toString.toLowerCase(java.util.Locale.ROOT)} " +
              s"at profile version ${event.version.value}"
          ) >> topic.publish1(event).void
        }

    /** Retries the reconcile rather than letting one failure lose the emission.
      *
      * A store outage must degrade and never kill: a listener that gave up on the first failure would leave
      * this replica permanently behind, with no error anywhere after the first line.
      */
    private def withBackoff(action: F[Unit], delay: FiniteDuration): F[Unit] =
      action.handleErrorWith { failure =>
        for {
          _ <- logger.warn(
            s"reconciling the cluster registry failed (${failure.getClass.getName}); " +
              s"retrying in ${delay.toSeconds}s"
          )
          jitter <- random.betweenLong(0L, delay.toMillis / 2 + 1L)
          _ <- Async[F].sleep(delay + jitter.millis)
          _ <- withBackoff(action, (delay * 2).min(ProfileChangeListener.MaxBackoff))
        } yield ()
      }

    /** Logs the store going away and coming back exactly once in each direction.
      *
      * Once, because this runs on every store change: a line per emission would bury the two transitions an
      * operator is actually looking for.
      */
    private def noteHealth: F[Unit] =
      store.health.flatMap { health =>
        val nowDegraded = health.isDegraded

        degraded.getAndSet(nowDegraded).flatMap {
          case was if was == nowDegraded => Async[F].unit
          case _ =>
            health match {
              case StoreHealth.Degraded(reason, since) =>
                logger.warn(s"the metadata store has been degraded since $since: $reason")
              case StoreHealth.Online =>
                logger.info("the metadata store is available again")
              case StoreHealth.NotConfigured => Async[F].unit
            }
        }
      }
  }
}
