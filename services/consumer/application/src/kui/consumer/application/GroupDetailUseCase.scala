package kui.consumer.application

import java.time.Instant

import scala.concurrent.duration.FiniteDuration

import cats.effect.kernel.{Async, Deferred, Outcome, Ref, Resource}
import cats.syntax.all.*
import org.typelevel.log4cats.StructuredLogger

import kui.cache.{BoundedCache, CacheMetrics}
import kui.consumer.domain.*
import kui.kernel.error.{ApplicationError, ErrorCode, InfrastructureError, KuiError}
import kui.kernel.{ClusterId, GroupId}

/** Whether the assignments on screen are the ones the coordinator holds right now. */
enum AssignmentFreshness {

  case Current

  /** The group is rebalancing, and these are the assignments last seen, at this time.
    *
    * The screen greys them and shows a badge (DC-H10). The alternative — an empty table — is the failure this
    * exists to prevent: an operator watching a rebalance sees the page they were reading go blank, exactly
    * when they are trying to work out what is happening.
    */
  case LastSeen(at: Instant)

  /** Rebalancing, and nothing was ever seen. The table is genuinely empty and says so. */
  case Unknown
}

object AssignmentFreshness {
  given CanEqual[AssignmentFreshness, AssignmentFreshness] = CanEqual.derived
}

final case class GroupDetailView(
    group: ConsumerGroup,
    topics: List[TopicSubscription],
    total: LagMath.LagTotal,
    assignments: AssignmentFreshness,
    freshness: SnapshotFreshness,
    computedAt: Instant
)

/** The assignments a group last had while it was settled.
  *
  * Bounded and per cluster: a group's members are a few hundred bytes, two thousand of them are a megabyte,
  * and a cluster with more groups than that is one where an operator opening a detail page during a rebalance
  * is rarer than the memory is expensive.
  */
trait LastSeenAssignments[F[_]] {
  def put(cluster: ClusterId, group: GroupId, members: List[GroupMember], at: Instant): F[Unit]
  def get(cluster: ClusterId, group: GroupId): F[Option[(List[GroupMember], Instant)]]
  def invalidate(cluster: ClusterId, group: GroupId): F[Unit]
}

object LastSeenAssignments {

  val CacheName: String = "consumer.assignments"

  def resource[F[_]: Async](
      cluster: ClusterId,
      maxSize: Long,
      ttl: FiniteDuration,
      metrics: CacheMetrics[F]
  ): Resource[F, LastSeenAssignments[F]] =
    BoundedCache
      .make[F, String, (List[GroupMember], Instant)](CacheName, cluster, maxSize, Some(ttl), metrics)
      .map { cache =>
        new LastSeenAssignments[F] {
          private def key(cluster: ClusterId, group: GroupId): String = s"${cluster.value}/${group.value}"

          def put(cluster: ClusterId, group: GroupId, members: List[GroupMember], at: Instant): F[Unit] =
            cache.put(key(cluster, group), (members, at))

          def get(cluster: ClusterId, group: GroupId): F[Option[(List[GroupMember], Instant)]] =
            cache.get(key(cluster, group))

          def invalidate(cluster: ClusterId, group: GroupId): F[Unit] =
            cache.invalidate(key(cluster, group))
        }
      }
}

trait GroupDetailUseCase[F[_]] {

  /** Computed live, because a detail page an operator opened deliberately should not be thirty seconds old.
    *
    * `Left(NotFound)` with `KUI-CLUSTER-NOT-FOUND` for a cluster this service is not serving. An unknown
    * *group* is not a 404: the port answers with a fabricated dead group, and this returns it, so a stale
    * bookmark lands on an empty group page rather than an error. Where existence really matters — the
    * mutations — it is confirmed by listing.
    */
  def detail(cluster: ClusterId, group: GroupId): F[Either[KuiError, GroupDetailView]]
}

object GroupDetailUseCase {

  val Operation: String = "kui.consumer.detail"

  def make[F[_]: Async](
      snapshots: GroupSnapshots[F],
      admin: ClusterId => GroupAdminPort[F],
      lastSeen: LastSeenAssignments[F],
      logger: StructuredLogger[F]
  ): GroupDetailUseCase[F] =
    new GroupDetailUseCase[F] {

      /** In-flight describes, keyed by (cluster, group), so several operators on the same detail page — or
        * one polling it — join the live coordinator round trip already running instead of each starting their
        * own.
        *
        * `None` in the gate means the starter was cancelled before the describe answered. Joiners retry.
        * Completing it with an `Upstream` status would tell them Kafka answered 502 when Kafka was never
        * asked.
        */
      private type Described = Either[KuiError, Map[GroupId, ConsumerGroup]]

      private val inFlight: Ref[F, Map[(ClusterId, GroupId), Deferred[F, Option[Described]]]] =
        Ref.unsafe(Map.empty)

      def detail(cluster: ClusterId, group: GroupId): F[Either[KuiError, GroupDetailView]] =
        snapshots.of(cluster).flatMap {
          case None =>
            ApplicationError
              .NotFound("cluster", cluster.value, ErrorCode.ClusterNotFound)
              .asLeft[GroupDetailView]
              .pure[F]

          case Some(cell) =>
            describeCoalesced(cluster, group).flatMap {
              case Left(error) => error.asLeft[GroupDetailView].pure[F]
              case Right(described) =>
                for {
                  now <- Async[F].realTimeInstant
                  snapshot <- cell.get
                  // A group the describe did not answer for is a group that is not there. The port
                  // fabricates a dead one, so this fallback is only for an empty map, and it holds
                  // the same shape rather than an error.
                  live = described.getOrElse(group, emptyGroup(group, now))
                  view <- withAssignments(cluster, live, now).map { (shown, freshness) =>
                    GroupDetailView(
                      group = shown,
                      topics = shown.subscriptions.sortBy(_.topic.value),
                      total = shown.lagTotal,
                      assignments = freshness,
                      freshness = SnapshotFreshness.of(
                        snapshot,
                        kui.kernel.error.InfrastructureError
                          .Unreachable("kafka", "the group has not been scraped yet")
                      ),
                      computedAt = now
                    )
                  }
                } yield view.asRight[KuiError]
            }
        }

      /** One live describe per (cluster, group) at a time; a caller arriving while one is in flight joins it
        * rather than issuing its own describeGroups/committedOffsets/endOffsets/beginningOffsets round trip —
        * the same coalescing `SnapshotCell.refresh` gives the list page, mirrored here per group.
        */
      private def describeCoalesced(
          cluster: ClusterId,
          group: GroupId
      ): F[Either[KuiError, Map[GroupId, ConsumerGroup]]] =
        Async[F].uncancelable { poll =>
          Deferred[F, Option[Described]].flatMap { gate =>
            val key = (cluster, group)
            inFlight
              .modify { running =>
                running.get(key) match {
                  case Some(existing) => (running, Left(existing))
                  case None => (running.updated(key, gate), Right(gate))
                }
              }
              .flatMap {
                // Somebody else is already describing this group: wait for *their* result. `poll` keeps
                // the wait itself cancellable, so a caller that gives up does not pin its fiber.
                // `None` means that starter was cancelled; retry, the way `SnapshotCell.refresh`
                // does, instead of inheriting an error Kafka did not return.
                case Left(existing) =>
                  poll(existing.get).flatMap {
                    case Some(result) => result.pure[F]
                    case None => poll(describeCoalesced(cluster, group))
                  }
                case Right(mine) =>
                  Async[F]
                    .guaranteeCase(poll(admin(cluster).describe(List(group)))) { outcome =>
                      // The slot is released before anyone is woken, so a joiner told to retry does
                      // not join a gate that has already been completed.
                      inFlight.update(_ - key) >> (outcome match {
                        case Outcome.Succeeded(result) =>
                          result.flatMap(value => mine.complete(value.some)).void
                        case Outcome.Canceled() => mine.complete(None).void
                        case Outcome.Errored(failure) =>
                          logger.warn(
                            Map("cluster.id" -> cluster.value, "group.id" -> group.value),
                            failure
                          )(
                            "a group describe failed before Kafka answered"
                          ) >> mine.complete(describeFailure(failure).asLeft.some).void
                      })
                    }
                    // The starter sees the same mapped failure the joiners were given. A raw raise
                    // here would be a 500 for the tab that opened the page and a different error
                    // for the tab that joined it.
                    .handleErrorWith(failure => describeFailure(failure).asLeft.pure[F])
              }
          }
        }

      /** A describe that raised, rather than one Kafka answered.
        *
        * `Upstream` would say "kafka answered with status 502". Nothing answered. `Unreachable` is the error
        * whose contract is that, and its message does not repeat the client's text.
        */
      private def describeFailure(failure: Throwable): KuiError =
        InfrastructureError.Unreachable(
          "kafka",
          Option(failure.getMessage).filter(_.nonEmpty).getOrElse(failure.getClass.getName)
        )

      /** The rebalance rule (DC-H10).
        *
        * A rebalancing group whose members hold nothing is rendered from the last settled assignment, greyed
        * and stamped. A settled group with members *writes* that record. Nothing is ever written during a
        * rebalance: that would overwrite the good data with the empty data the badge exists to hide.
        */
      private def withAssignments(
          cluster: ClusterId,
          group: ConsumerGroup,
          now: Instant
      ): F[(ConsumerGroup, AssignmentFreshness)] = {
        val holdsNothing = group.members.forall(_.partitions.isEmpty)

        if group.isRebalancing && holdsNothing then
          lastSeen.get(cluster, group.groupId).flatMap {
            case Some((members, at)) =>
              logger
                .debug(Map("cluster.id" -> cluster.value, "group.id" -> group.groupId.value))(
                  "the group is rebalancing; rendering the assignments last seen"
                )
                .as((group.copy(members = members), AssignmentFreshness.LastSeen(at)))
            case None => (group, AssignmentFreshness.Unknown: AssignmentFreshness).pure[F]
          }
        else if group.members.nonEmpty && !group.isRebalancing then
          lastSeen
            .put(cluster, group.groupId, group.members, now)
            .as((group, AssignmentFreshness.Current: AssignmentFreshness))
        else (group, AssignmentFreshness.Current: AssignmentFreshness).pure[F]
      }
    }

  /** The shape a group that is not there has: dead, no members, nothing assigned. Same as the port's
    * fabricated one, so a caller cannot tell which produced it.
    */
  private def emptyGroup(group: GroupId, now: Instant): ConsumerGroup =
    ConsumerGroup(
      groupId = group,
      state = kui.kernel.group.GroupState.Dead,
      protocol = kui.kernel.group.GroupProtocol.Unknown,
      isSimple = false,
      partitionAssignor = "",
      members = Nil,
      coordinator = None,
      subscriptions = Nil,
      completeness = GroupCompleteness.Complete,
      observedAt = now
    )
}
