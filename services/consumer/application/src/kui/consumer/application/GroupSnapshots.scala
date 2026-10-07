package kui.consumer.application

import java.time.Instant

import scala.concurrent.duration.FiniteDuration

import cats.effect.kernel.{Ref, Resource, Temporal}
import cats.effect.std.{Semaphore, Supervisor}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import org.typelevel.log4cats.StructuredLogger

import kui.cache.{CacheMetrics, SnapshotCell, SnapshotLoadFailure}
import kui.consumer.domain.*
import kui.kernel.error.KuiError
import kui.kernel.search.NameIndex
import kui.kernel.{ClusterId, GroupId}

/** One cluster's consumer groups, as of one refresh pass.
  *
  * `version` counts successful passes. It is what the lag delta's token carries, and it is a counter rather
  * than a timestamp for the reason DEVPLAN §10 D9 gives: a clock is not a version, and skew between a browser
  * and a server silently drops or replays updates in a way nobody can see.
  */
final case class GroupSnapshot(
    version: Long,
    summaries: Vector[GroupSummary],
    /** Kept so that the detail page renders immediately and the topic tab is answered without describing
      * again.
      */
    groups: Map[GroupId, ConsumerGroup],
    paceSamples: Map[GroupId, LagMath.PaceSample],
    /** How many coordinators did not answer this pass. The list page says the listing may be incomplete
      * rather than silently showing fewer groups.
      */
    incompleteCoordinators: Int,
    takenAt: Instant
) {

  /** Built once per pass, because the search box queries it on every keystroke and rebuilding a ten-thousand
    * name index per request is the difference between a list that responds and one that does not.
    */
  lazy val index: NameIndex = NameIndex.of(summaries.map(_.groupId.value).toList)

  def groupsOf(topic: kui.kernel.TopicName): List[ConsumerGroup] =
    groups.values.filter(_.subscriptions.exists(_.topic == topic)).toList.sortBy(_.groupId.value)
}

object GroupSnapshot {

  def empty(at: Instant): GroupSnapshot = GroupSnapshot(0L, Vector.empty, Map.empty, Map.empty, 0, at)
}

/** The per-cluster group snapshot cells, kept in step with the profiles this service is serving. */
trait GroupSnapshots[F[_]] {

  def of(cluster: ClusterId): F[Option[SnapshotCell[F, GroupSnapshot]]]

  def all: F[List[(ClusterId, SnapshotCell[F, GroupSnapshot])]]

  /** The pass before the current one, when there has been one.
    *
    * Exactly one previous pass is kept — one extra reference per cluster, bounded by construction — so that a
    * lag poll whose token is one version old can be answered as a delta. A ring of older versions, to serve a
    * client that has been away for ten minutes, would be a second cache with a staleness contract nobody
    * asked for; such a client is answered in full instead.
    */
  def previousOf(cluster: ClusterId): F[Option[GroupSnapshot]]

  /** Starts a refresh and returns as soon as it has been *requested*.
    *
    * Never awaits completion: a forced refresh against a dead cluster would block for the whole admin
    * timeout, and the button that asked for it would hang — the exact failure the snapshot design exists to
    * avoid. Deduplication is the cell's, so twenty presses are one admin call.
    */
  def requestRefresh(cluster: ClusterId): F[Boolean]

  /** After a reset or a delete the snapshot describes a group that no longer exists in that shape.
    *
    * Invalidating and re-requesting in one step is what stops the next read from serving the pre-mutation
    * state and making an operator think their reset did nothing.
    */
  def invalidate(cluster: ClusterId, reason: String): F[Unit]
}

object GroupSnapshots {

  val CacheName: String = "consumer.groups"

  def resource[F[_]: Temporal](
      profiles: ClusterProfileSource[F],
      admin: ClusterId => GroupAdminPort[F],
      refreshInterval: FiniteDuration,
      metrics: CacheMetrics[F],
      logger: StructuredLogger[F]
  ): Resource[F, GroupSnapshots[F]] =
    for {
      cells <- Resource.eval(Ref.of[F, Map[ClusterId, Entry[F]]](Map.empty))
      gate <- Resource.eval(Semaphore[F](1L))
      // Finalizers run in reverse: stop reconciliation, cancel forced refreshes, then drain cells.
      _ <- Resource.onFinalize(
        gate.permit.use(_ => cells.getAndSet(Map.empty).flatMap(_.values.toList.traverse_(_.release)))
      )
      supervisor <- Supervisor[F]
      impl = new Impl[F](admin, refreshInterval, metrics, logger, supervisor, cells, gate)
      _ <- Resource.eval(profiles.all.flatMap(impl.sync))
      _ <- profiles.changes.evalMap(_ => profiles.all.flatMap(impl.sync)).compile.drain.background
    } yield impl

  /** The refresh pass: the ordered set of port calls that produce one `GroupSnapshot`.
    *
    * **Only the listing is required.** A cluster that lists its groups has something to render, and a list
    * with member counts and no lag is far more useful than an unavailable panel — it is exactly what a
    * cluster where KUI holds `DESCRIBE` on groups but not `READ` on their topics looks like. The describe
    * degrades to a summary built from the listing alone, with `GroupCompleteness` recording what is missing.
    *
    * Optional failures log once at DEBUG, not WARN: on a partly-authorised cluster they fire every thirty
    * seconds for ever, and a warning that always fires teaches an operator to filter the log.
    */
  def refreshOne[F[_]: Temporal](
      port: GroupAdminPort[F],
      previous: Option[GroupSnapshot],
      logger: StructuredLogger[F],
      cluster: ClusterId
  ): F[Either[KuiError, GroupSnapshot]] = {
    val context = Map(
      "service.name" -> ConsumerService.Id.value,
      "cluster.id" -> cluster.value
    )

    port.list(Set.empty).flatMap {
      case Left(error) => error.asLeft[GroupSnapshot].pure[F]
      case Right(listing) =>
        val ids = listing.groups.map(_.groupId)

        port
          .describe(ids)
          .flatMap {
            case Right(described) => described.pure[F]
            case Left(error) =>
              logger
                .debug(context ++ Map("error.code" -> error.code.wire))(
                  s"consumer groups on ${cluster.value} could not be described: ${error.message}"
                )
                .as(Map.empty[GroupId, ConsumerGroup])
          }
          .flatMap { described =>
            Temporal[F].realTimeInstant.map { now =>
              val summaries = listing.groups.map { row =>
                described.get(row.groupId) match {
                  // The described group knows its members, its lag and its topics; the listing row
                  // knows only that the group exists. Falling back to the row rather than dropping
                  // it is what keeps a group KUI may not describe on the screen, with a
                  // completeness record saying which half is missing.
                  case Some(group) =>
                    val sample = paceSampleOf(group, now)
                    group.summary
                      .copy(pace = LagMath.pace(previous.flatMap(_.paceSamples.get(group.groupId)), sample))
                  case None =>
                    row.copy(completeness = row.completeness.withoutMembers.withoutCommittedOffsets)
                }
              }

              GroupSnapshot(
                version = previous.fold(1L)(_.version + 1L),
                summaries = summaries.toVector,
                groups = described,
                paceSamples = described.map((id, group) => id -> paceSampleOf(group, now)),
                incompleteCoordinators = listing.incompleteCoordinators,
                takenAt = now
              ).asRight[KuiError]
            }
          }
    }
  }

  /** Where this group's commits were, as one number, for the next pass to compare against. */
  private def paceSampleOf(group: ConsumerGroup, at: Instant): LagMath.PaceSample = {
    val committed = group.partitions.flatMap(_.committed.map(_.value))

    LagMath.PaceSample(
      at = at,
      committedTotal = Option.when(committed.nonEmpty)(committed.sum),
      partitions = group.subscriptions
        .flatMap(subscription =>
          subscription.partitions.map(state => kui.kernel.TopicPartition(subscription.topic, state.partition))
        )
        .toSet
    )
  }

  final private case class Entry[F[_]](
      cell: SnapshotCell[F, GroupSnapshot],
      previous: Ref[F, Option[GroupSnapshot]],
      release: F[Unit]
  )

  final private class Impl[F[_]: Temporal](
      admin: ClusterId => GroupAdminPort[F],
      refreshInterval: FiniteDuration,
      metrics: CacheMetrics[F],
      logger: StructuredLogger[F],
      supervisor: Supervisor[F],
      cells: Ref[F, Map[ClusterId, Entry[F]]],
      gate: Semaphore[F]
  ) extends GroupSnapshots[F] {

    def of(cluster: ClusterId): F[Option[SnapshotCell[F, GroupSnapshot]]] =
      cells.get.map(_.get(cluster).map(_.cell))

    def all: F[List[(ClusterId, SnapshotCell[F, GroupSnapshot])]] =
      cells.get.map(_.toList.sortBy(_._1.value).map((id, entry) => id -> entry.cell))

    def previousOf(cluster: ClusterId): F[Option[GroupSnapshot]] =
      cells.get.flatMap(_.get(cluster).fold(Option.empty[GroupSnapshot].pure[F])(_.previous.get))

    def requestRefresh(cluster: ClusterId): F[Boolean] =
      of(cluster).flatMap {
        case None => false.pure[F]
        case Some(cell) => supervisor.supervise(cell.refresh).as(true)
      }

    def invalidate(cluster: ClusterId, reason: String): F[Unit] =
      of(cluster).flatMap {
        case None => Temporal[F].unit
        case Some(cell) =>
          logger.debug(Map("cluster.id" -> cluster.value))(
            s"dropping the consumer-group snapshot for ${cluster.value}: $reason"
          ) >> cell.invalidate.void
      }

    /** Starts cells for clusters that appeared and cancels cells for clusters that went away. */
    def sync(wanted: List[ClusterProfileView]): F[Unit] =
      gate.permit.use { _ =>
        for {
          held <- cells.get
          ids = wanted.map(_.cluster).toSet
          obsolete = held.view.filterKeys(!ids.contains(_)).toMap
          _ <- Temporal[F].uncancelable(_ =>
            cells.update(_ -- obsolete.keySet) >> obsolete.values.toList.traverse_(_.release)
          )
          _ <- wanted
            .filterNot(profile => held.contains(profile.cluster))
            .traverse_(profile => start(profile.cluster))
        } yield ()
      }

    private def start(cluster: ClusterId): F[Unit] = {
      val resource = for {
        current <- Resource.eval(Ref.of[F, Option[GroupSnapshot]](None))
        previous <- Resource.eval(Ref.of[F, Option[GroupSnapshot]](None))
        cell <- SnapshotCell.resource[F, GroupSnapshot](CacheName, cluster, refreshInterval, metrics)(
          load(cluster, current, previous)
        )
      } yield (cell, previous)

      // No cancellation gap may exist between allocation and the owning map receiving the finalizer.
      Temporal[F].uncancelable { _ =>
        resource.allocated.flatMap { case ((cell, previous), release) =>
          cells.update(_.updated(cluster, Entry(cell, previous, release))).onError { case _ => release }
        }
      }
    }

    /** The cell's `load`. It raises rather than returns the failure, because `SnapshotCell` unwraps a
      * `SnapshotLoadFailure` back into the `KuiError` the adapter already classified.
      *
      * The previous snapshot is held in its own `Ref` rather than read back from the cell: pace is computed
      * between two *successful* passes, and a failed pass must not become one of the two.
      */
    private def load(
        cluster: ClusterId,
        current: Ref[F, Option[GroupSnapshot]],
        previous: Ref[F, Option[GroupSnapshot]]
    ): F[GroupSnapshot] =
      for {
        last <- current.get
        result <- refreshOne(admin(cluster), last, logger, cluster)
        snapshot <- result match {
          case Left(error) => Temporal[F].raiseError[GroupSnapshot](SnapshotLoadFailure(error))
          // The pass that was current becomes the previous one only when a new pass succeeded. A
          // failed refresh must not shift the pair along, or a lag delta would be computed against
          // a snapshot two passes old and report movement that never happened.
          case Right(value) => previous.set(last) >> current.set(Some(value)).as(value)
        }
      } yield snapshot

  }
}
