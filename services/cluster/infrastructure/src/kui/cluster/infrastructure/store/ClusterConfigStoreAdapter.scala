package kui.cluster.infrastructure.store

import cats.effect.kernel.{Async, Ref, Resource}
import cats.effect.std.Supervisor
import cats.syntax.all.*
import org.typelevel.log4cats.StructuredLogger

import kui.cluster.domain.{ClusterConfigStore, ClusterProfile, ProfileOrigin, ProfileVersion, StoreHealth}
import kui.config.store.{ConfigStore, StoreChange, StoreKey, StoreRecord, StoreSection}
import kui.kernel.ClusterId
import kui.kernel.error.KuiError

/** The cluster context's view of the metadata store: profiles under `cluster/<id>`, and nothing else.
  *
  * The generic store speaks keys and versioned JSON envelopes. This adapter is the only place in KUI that
  * knows a cluster profile is what one of them contains, and it is deliberately narrow: it owns the
  * `cluster/` prefix, so a bug here cannot corrupt the settings, role or masking sections. That is ADR-036's
  * single-writer-per-section rule, enforced by nobody writing outside their own prefix.
  *
  * ==One malformed record is not an outage==
  *
  * `list` skips a record it cannot decode, logs it once with the key and the reason, and returns the other
  * nine clusters. A startup that dies on one bad row is an outage caused by a typo, and the operator who made
  * the typo is then locked out of the screen that would let them fix it.
  */
final class ClusterConfigStoreAdapter[F[_]: Async] private (
    store: ConfigStore[F],
    logger: StructuredLogger[F],
    supervisor: Supervisor[F],
    handlers: Ref[F, Map[Long, List[ClusterProfile] => F[Unit]]],
    nextHandlerId: Ref[F, Long]
) extends ClusterConfigStore[F] {

  def list: F[Either[KuiError, List[ClusterProfile]]] =
    store.health.flatMap { health =>
      val hidden = health.unreadableKeys.filter(_.section == StoreSection.Cluster).toSet
      store.list(StoreSection.Cluster).flatMap { records =>
        // One unreadable key is not an outage: the other clusters still load. The key itself is not
        // returned as a profile, because the payload could not be read and the previous one is stale.
        val (blocked, readable) = records.partition(record => hidden.contains(record.key))
        val absent = (hidden -- records.iterator.map(_.key).toSet).toList
        (blocked.map(_.key) ++ absent).distinct
          .traverse_(key =>
            logger.error(s"skipping the stored cluster '${key.render}': the stored record could not be read")
          ) *> readable
          .traverse(decoded)
          // An empty store is `Right(Nil)`: a normal first start, and not something to report as a failure.
          .map(profiles => Right(profiles.flatten))
      }
    }

  def get(id: ClusterId): F[Either[KuiError, Option[ClusterProfile]]] =
    ClusterConfigStoreAdapter.keyFor(id) match {
      case Left(error) => Async[F].pure(Left(error))
      case Right(key) =>
        store.health.flatMap { health =>
          if health.unreadableKeys.contains(key) then
            // The record may also be absent from `get`: a decrypt failure hides it. Answering "there is
            // no such cluster" would send the caller to create a duplicate over a key the log still holds.
            Async[F].pure(
              Left(StoreErrorMapping.undecodable(key.render, "the stored record could not be read"))
            )
          else
            store.get(key).map {
              case None => Right(None)
              // A single `get` names one record the caller asked for by id, so a decode failure is a `Left`
              // here even though it is a skip inside `list`. The caller asked about *this* record, and
              // answering "there is no such cluster" would send them to create a duplicate.
              case Some(record) =>
                decodeRecord(record).bimap(
                  why => StoreErrorMapping.undecodable(record.key.render, why),
                  Some.apply
                )
            }
        }
    }

  def put(profile: ClusterProfile, expected: ProfileVersion): F[Either[KuiError, ClusterProfile]] =
    ClusterConfigStoreAdapter.keyFor(profile.id) match {
      case Left(error) => Async[F].pure(Left(error))
      case Right(key) =>
        store
          .put(
            key = key,
            payload = ClusterRecordCodec.encode(profile),
            // `ProfileVersion.Static` is zero, which means "this profile has never been stored". The store
            // spells that as `None` — "this key must not exist yet" — and mapping it to `Some(0)` would ask
            // the store to match a version no record ever has, so every create would look like a conflict.
            baseVersion = Option.when(!expected.isStatic)(expected.value),
            updatedBy = ClusterConfigStoreAdapter.WrittenBy
          )
          // The store returns only once the write has been read back from the log (ADR-042 §3), so there is
          // no second read-back loop here. Two waiters for one write is two timeouts to tune.
          .map(_.map(record => profile.at(ProfileVersion.unsafe(record.version), ProfileOrigin.Stored)))
    }

  def delete(id: ClusterId, expected: ProfileVersion): F[Either[KuiError, Unit]] =
    ClusterConfigStoreAdapter.keyFor(id) match {
      case Left(error) => Async[F].pure(Left(error))
      case Right(key) =>
        // A create-versioned delete makes no sense — there is nothing to remove at version zero — so the
        // static version is refused here rather than being handed to the store as a base version no record
        // can have.
        if expected.isStatic then Async[F].pure(Right(()))
        else
          store
            .delete(key = key, baseVersion = expected.value, updatedBy = ClusterConfigStoreAdapter.WrittenBy)
    }

  /** Registers a handler called with the whole resolved profile list on every store change.
    *
    * Whole lists rather than deltas, because a subscriber then never has to reconcile against a separate
    * `list` call and cannot race with it. It also makes recovery free: after a dropped subscription the next
    * emission is complete, so a change missed during an outage — including a removal, which a delta protocol
    * would have lost on that replica for ever — is picked up by the next one.
    */
  def onChange(handler: List[ClusterProfile] => F[Unit]): F[F[Unit]] =
    for {
      id <- nextHandlerId.getAndUpdate(_ + 1L)
      _ <- handlers.update(_ + (id -> handler))
      // Deregistration has to be idempotent: releasing a resource twice is ordinary, and a second
      // deregistration that failed would turn a clean shutdown into a crash.
    } yield handlers.update(_ - id)

  def health: F[StoreHealth] = store.health.map(StoreErrorMapping.health)

  /** The single subscription every handler is fed from.
    *
    * One subscription and not one per handler: `ConfigStore.changes` is a hot stream whose slow consumers are
    * dropped, and N subscribers each independently re-listing the section on every change is N times the work
    * for one answer they all share.
    */
  private[store] def follow: F[Unit] =
    supervisor
      .supervise(
        store.changes
          .evalMap(change => notify(change))
          .compile
          .drain
          .handleErrorWith(failure =>
            logger.error(
              s"the cluster profile change feed stopped: ${failure.getClass.getName}"
            )
          )
      )
      .void

  private def notify(change: StoreChange): F[Unit] = {
    val concerns = change match {
      // A `settings/` or `rbac/` write is not this adapter's business, and waking every subscriber for one
      // would make an unrelated section's write rate the cluster registry's problem.
      case StoreChange.Upserted(record) => record.key.section == StoreSection.Cluster
      case StoreChange.Deleted(key, _, _) => key.section == StoreSection.Cluster
      // "You fell behind and lost changes" always concerns us: the view may be missing a removal, and only a
      // full re-read can tell.
      case StoreChange.Desynchronized(_) => true
    }

    if concerns then
      list.flatMap {
        case Left(error) =>
          logger.error(s"could not rebuild the cluster list after a store change: ${error.code.wire}")
        case Right(profiles) =>
          handlers.get.flatMap(_.values.toList.traverse_(handler => invoke(handler, profiles)))
      }
    else Async[F].unit
  }

  /** A handler that throws must not take the change feed down with it. */
  private def invoke(handler: List[ClusterProfile] => F[Unit], profiles: List[ClusterProfile]): F[Unit] =
    handler(profiles).handleErrorWith(failure =>
      logger.error(s"a cluster profile change handler failed: ${failure.getClass.getName}")
    )

  private def decoded(record: StoreRecord): F[Option[ClusterProfile]] =
    decodeRecord(record) match {
      case Right(profile) => Async[F].pure(Some(profile))
      case Left(why) =>
        logger
          .error(s"skipping the stored cluster '${record.key.render}': $why")
          .as(None)
    }

  private def decodeRecord(record: StoreRecord): Either[String, ClusterProfile] =
    for {
      id <- ClusterId.from(record.key.id).leftMap(_.message)
      profile <- ClusterRecordCodec.decode(
        id,
        ProfileVersion.unsafe(record.version),
        ProfileOrigin.Stored,
        record.payload
      )
    } yield profile
}

object ClusterConfigStoreAdapter {

  /** The `updatedBy` every write from this service carries.
    *
    * A name, not a principal: M1 has no authentication, and a field that will later hold a user must not be
    * left holding something that looks like one now.
    */
  val WrittenBy: String = "kui-cluster"

  /** The adapter and its change-feed subscription, as one resource.
    *
    * The subscription is supervised, so the fiber that reads the store's change stream is owned by something
    * that will cancel it — a background fiber that is not supervised is a fiber that dies unobserved, or
    * outlives the process that needed it.
    */
  def resource[F[_]: Async](
      store: ConfigStore[F],
      logger: StructuredLogger[F]
  ): Resource[F, ClusterConfigStore[F]] =
    for {
      supervisor <- Supervisor[F]
      handlers <- Resource.eval(Ref.of[F, Map[Long, List[ClusterProfile] => F[Unit]]](Map.empty))
      ids <- Resource.eval(Ref.of[F, Long](0L))
      adapter = new ClusterConfigStoreAdapter[F](store, logger, supervisor, handlers, ids)
      _ <- Resource.eval(adapter.follow)
    } yield adapter

  /** The store key for a cluster: `cluster/<clusterId>`.
    *
    * `ClusterId` is already a slug (ADR-031) and `StoreKey`'s id rule is the same slug rule, so no escaping
    * is needed and none is invented. The `Left` is unreachable in practice and is still a value: an id that
    * somehow got past both rules must produce a named failure rather than a key that addresses the wrong
    * record.
    */
  def keyFor(id: ClusterId): Either[KuiError, StoreKey] =
    StoreKey.cluster(id.value).leftMap(error => StoreErrorMapping.undecodable(id.value, error.message))

  /** The inverse, for reading a listing back. `None` for a key outside this prefix — which is how the
    * settings, role and masking records are filtered out of everything here.
    */
  def clusterIdOf(key: StoreKey): Option[ClusterId] =
    Option.when(key.section == StoreSection.Cluster)(key.id).flatMap(ClusterId.from(_).toOption)
}
