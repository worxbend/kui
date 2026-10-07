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

  /** Hold the profile generation through pool acquisition and use. */
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
        gates <- cats.effect.Ref.of[F, Map[ClusterId, Semaphore[F]]](Map.empty)
        closed <- cats.effect.Ref.of[F, Boolean](false)
      } yield new Impl[F](pool, logger, known, gates, closed, resolve)
    )(_.releaseAll)

  final private class Impl[F[_]: Async](
      pool: AdminClientPool[F],
      logger: StructuredLogger[F],
      known: cats.effect.Ref[F, Map[ClusterId, ClusterProfile]],
      gates: cats.effect.Ref[F, Map[ClusterId, Semaphore[F]]],
      closed: cats.effect.Ref[F, Boolean],
      resolve: Option[ClusterId => F[Option[ClusterProfile]]]
  ) extends ClusterAdminClients[F] {

    private def gateFor(id: ClusterId): F[Semaphore[F]] =
      Semaphore[F](1L).flatMap(fresh =>
        gates.modify(current =>
          (current.updated(id, current.getOrElse(id, fresh)), current.getOrElse(id, fresh))
        )
      )

    def connectionFor(profile: ClusterProfile): F[ClusterConnection] =
      withConnection(profile)(_.pure[F])

    def withConnection[A](profile: ClusterProfile)(call: ClusterConnection => F[A]): F[A] =
      gateFor(profile.id).flatMap(
        _.permit.use(_ =>
          closed.get.flatMap {
            case true => Async[F].raiseError(new IllegalStateException("cluster admin clients are closed"))
            case false =>
              resolve match {
                case None => select(profile, authoritative = false).flatMap(call)
                case Some(current) =>
                  current(profile.id).flatMap {
                    case Some(latest) => select(latest, authoritative = true).flatMap(call)
                    case None => Async[F].raiseError(new IllegalStateException("cluster profile was removed"))
                  }
              }
          }
        )
      )

    private def select(profile: ClusterProfile, authoritative: Boolean): F[ClusterConnection] =
      // The gate spans selection, eviction and use. Evict before publishing the generation;
      // a failed eviction must be retried. The live registry also handles removal/recreation,
      // whose store version can restart rather than being globally monotonic.
      Async[F]
        .uncancelable { _ =>
          known.get.flatMap { current =>
            val (selected, stale) = current.get(profile.id) match {
              case Some(seen)
                  if (!authoritative && seen.version.value > profile.version.value) || seen == profile =>
                (seen, false)
              case Some(_) => (profile, true)
              case None => (profile, false)
            }
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
          gate.permit.use(_ => pool.evict(id))
        }) >> known.set(Map.empty)
      }
  }
}
