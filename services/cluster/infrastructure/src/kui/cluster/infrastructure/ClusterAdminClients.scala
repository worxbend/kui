package kui.cluster.infrastructure

import cats.effect.kernel.{Async, Resource}
import cats.effect.std.Semaphore
import cats.syntax.all.*
import org.typelevel.log4cats.StructuredLogger

import kui.cluster.domain.ClusterProfile
import kui.kafka.AdminClientPool
import kui.kernel.ClusterId
import kui.kernel.cluster.ClusterConnection

/** Keeps each cluster's admin client in step with the profile it was built from.
  *
  * The *client* lifecycle — one client per cluster, created on first use under a per-cluster gate, shared by
  * every caller, closed and rebuilt when the connection breaks, and closed for all when the process shuts
  * down — already lives in `libs/kafka`'s `AdminClientPool` (KAFKA-004). Building a second registry here
  * would be two pools racing to open clients against the same brokers, which is precisely the failure the
  * pool's per-cluster gate exists to prevent.
  *
  * What the pool cannot know is the one thing this module does know: that a `ClusterProfile` has a
  * `ProfileVersion`, and that a profile edited in the metadata store is a *different connection* wearing the
  * same cluster id. The pool keys by `ClusterId`, so without this component a cluster whose bootstrap list or
  * credentials changed would keep being served by the client built from the old ones — talking to the old
  * brokers with the old password, silently, until the process restarted.
  *
  * So this is a registry of versions, not of clients: it remembers which `ProfileVersion` each cluster's
  * client was built from and evicts the client when it sees a newer one.
  */
trait ClusterAdminClients[F[_]] {

  /** Resolve/register settings. Admin operations must use `withConnection` to hold the generation through the
    * eventual pool acquisition, rather than retain this returned value.
    */
  def connectionFor(profile: ClusterProfile): F[ClusterConnection]

  /** Hold the profile generation through pool acquisition and use.
    *
    * Calls on the same generation run together. A different generation waits until they have finished and
    * only then evicts, so an older call cannot rebuild the client that eviction just closed.
    */
  def withConnection[A](profile: ClusterProfile)(call: ClusterConnection => F[A]): F[A]

  /** Closes and forgets this cluster's client. The next call builds a new one.
    *
    * Called by the adapter after a reconnect-class failure (see `ReconnectPolicy`), and idempotent so that
    * ten calls failing at once on one dead client cost one reconnect rather than ten.
    */
  def invalidate(id: ClusterId): F[Unit]

  /** How many clusters this registry currently vouches for.
    *
    * An upper bound on open clients rather than a count of them: the pool creates a client lazily on the
    * first call, so a cluster can be registered here with nothing open yet. It is exposed for tests and for
    * the readiness endpoint, neither of which needs more precision than that.
    */
  def openClients: F[Int]
}

object ClusterAdminClients {

  /** How many calls on one cluster may hold a lease at once.
    *
    * A generation change acquires every permit, so this is also the overlap bound. The snapshot refresh fans
    * out to three reads and a page talks to one cluster at a time; past this a call waits instead of the
    * semaphore overflowing.
    */
  private val ShareCapacity = 1024L

  /** `shares` is the lease taken for the whole of a call. `decision` is held only while `known` is read and
    * updated, never across the call.
    */
  final private case class Gate[F[_]](shares: Semaphore[F], decision: Semaphore[F])

  /** Registered clusters are evicted from the pool when the resource closes.
    *
    * The pool closes its own clients when *it* closes, so this matters only when the registry is scoped more
    * narrowly than the pool — which is exactly the shape a test has, and the shape a future per-tenant scope
    * would have. Releasing a narrower scope must not leave a client behind in a wider one.
    */
  def resource[F[_]: Async](
      pool: AdminClientPool[F],
      logger: StructuredLogger[F],
      resolve: Option[ClusterId => F[Option[ClusterProfile]]] = None
  ): Resource[F, ClusterAdminClients[F]] =
    Resource.make(
      for {
        known <- cats.effect.Ref.of[F, Map[ClusterId, ClusterProfile]](Map.empty)
        gates <- cats.effect.Ref.of[F, Map[ClusterId, Gate[F]]](Map.empty)
        closed <- cats.effect.Ref.of[F, Boolean](false)
      } yield new Impl[F](pool, logger, known, gates, closed, resolve)
    )(_.releaseAll)

  final private class Impl[F[_]: Async](
      pool: AdminClientPool[F],
      logger: StructuredLogger[F],
      known: cats.effect.Ref[F, Map[ClusterId, ClusterProfile]],
      gates: cats.effect.Ref[F, Map[ClusterId, Gate[F]]],
      closed: cats.effect.Ref[F, Boolean],
      resolve: Option[ClusterId => F[Option[ClusterProfile]]]
  ) extends ClusterAdminClients[F] {

    private def gateFor(id: ClusterId): F[Gate[F]] =
      gates.get.flatMap {
        case current if current.contains(id) => current(id).pure[F]
        case _ =>
          for {
            shares <- Semaphore[F](ShareCapacity)
            decision <- Semaphore[F](1L)
            stored <- gates.modify { current =>
              val gate = current.getOrElse(id, Gate(shares, decision))
              (current.updated(id, gate), gate)
            }
          } yield stored
      }

    def connectionFor(profile: ClusterProfile): F[ClusterConnection] =
      withConnection(profile)(_.pure[F])

    def withConnection[A](profile: ClusterProfile)(call: ClusterConnection => F[A]): F[A] =
      gateFor(profile.id).flatMap { gate =>
        // One share for the whole call when the generation does not have to move. `None` means it
        // does: the share is released before the change is taken, or the call would wait for itself.
        gate.shares.permit
          .use(_ =>
            prepared(gate, profile).flatMap {
              case Some(connection) => call(connection).map(_.asRight[Unit])
              case None => ().asLeft[A].pure[F]
            }
          )
          .flatMap {
            case Right(value) => value.pure[F]
            case Left(_) => changed(gate, profile)(call)
          }
      }

    /** `Some` when this generation can be used now. `None` when a different profile has to be published
      * first, which takes every share and so cannot be done while this one is held.
      */
    private def prepared(gate: Gate[F], profile: ClusterProfile): F[Option[ClusterConnection]] =
      ensureOpen >> resolveProfile(profile).flatMap {
        case None => removed
        case Some((latest, authoritative)) =>
          gate.decision.permit.use { _ =>
            known.get.flatMap { current =>
              if classify(current, latest, authoritative)._2 then Option.empty[ClusterConnection].pure[F]
              else select(latest, authoritative).map(Some(_))
            }
          }
      }

    /** Publish a different generation, then keep a single share for the call so the calls that were waiting
      * on this change enter on the new profile instead of queueing behind this one.
      */
    private def changed[A](gate: Gate[F], profile: ClusterProfile)(
        call: ClusterConnection => F[A]
    ): F[A] =
      cats.effect.Ref.of[F, Boolean](false).flatMap { downgraded =>
        // Which permits are still held depends on whether the downgrade below has run. The finaliser
        // is installed only after `acquireN` succeeds: cancelling a waiter that is still queued must
        // not release permits it never took, and must not go on to evict.
        def releaseHeld: F[Unit] =
          downgraded.get.flatMap {
            case true => gate.shares.releaseN(1L)
            case false => gate.shares.releaseN(ShareCapacity)
          }

        // `bracketFull` so a waiter cancelled while queued leaves the queue instead of acquiring
        // every share later and evicting. The release runs only after the acquire has succeeded.
        Async[F].bracketFull[Unit, A](poll => poll(gate.shares.acquireN(ShareCapacity)))(_ =>
          ensureOpen >> resolveProfile(profile).flatMap {
            case None => removed
            case Some((latest, authoritative)) =>
              // Every share is held, so no other call is inside `select`. Drop back to one before
              // the call so the waiters enter on the new profile instead of queueing behind it.
              select(latest, authoritative).flatMap { connection =>
                Async[F].uncancelable(_ =>
                  gate.shares.releaseN(ShareCapacity - 1L) >> downgraded.set(true)
                ) >> call(connection)
              }
          }
        )((_, _) => releaseHeld)
      }

    private def ensureOpen: F[Unit] =
      closed.get.flatMap {
        case true => Async[F].raiseError(new IllegalStateException("cluster admin clients are closed"))
        case false => Async[F].unit
      }

    /** `None` when the authoritative registry no longer has this id. The boolean is `authoritative`. */
    private def resolveProfile(profile: ClusterProfile): F[Option[(ClusterProfile, Boolean)]] =
      resolve match {
        case None => Some((profile, false)).pure[F]
        case Some(current) => current(profile.id).map(_.map(latest => (latest, true)))
      }

    private def removed[A]: F[A] =
      Async[F].raiseError(new IllegalStateException("cluster profile was removed"))

    private def classify(
        current: Map[ClusterId, ClusterProfile],
        profile: ClusterProfile,
        authoritative: Boolean
    ): (ClusterProfile, Boolean) =
      current.get(profile.id) match {
        case Some(seen)
            if (!authoritative && seen.version.value > profile.version.value) || seen == profile =>
          (seen, false)
        case Some(_) => (profile, true)
        case None => (profile, false)
      }

    private def select(profile: ClusterProfile, authoritative: Boolean): F[ClusterConnection] =
      // Evict before publishing the generation, and do both uncancellably. A cancellation between
      // them would leave `known` on the new profile while the pool still held the old client. The
      // live registry also handles removal and recreation, whose store version can restart.
      Async[F].uncancelable { _ =>
        known.get.flatMap { current =>
          val (selected, stale) = classify(current, profile, authoritative)
          (if stale then
             logger.info(
               s"cluster ${profile.id.value} moved to profile version ${profile.version.value}; " +
                 "its admin client will be rebuilt"
             ) >> pool.evict(profile.id)
           else Async[F].unit) >> known
            .update(_.updated(profile.id, selected))
            .as(ClusterProfileConnection.of(selected))
        }
      }

    def invalidate(id: ClusterId): F[Unit] =
      Async[F].uncancelable(_ => pool.invalidate(id))

    def openClients: F[Int] = known.get.map(_.size)

    /** Evicts every cluster this registry registered, whether or not the caller was cancelled. */
    def releaseAll: F[Unit] =
      Async[F].uncancelable { _ =>
        closed.set(true) >> gates.get.flatMap(_.toList.traverse_ { (id, gate) =>
          Resource
            .make(gate.shares.acquireN(ShareCapacity))(_ => gate.shares.releaseN(ShareCapacity))
            .use(_ => pool.evict(id))
        }) >> known.set(Map.empty)
      }
  }
}
