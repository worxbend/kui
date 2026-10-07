package kui.consumer.application

import cats.effect.kernel.Temporal
import cats.syntax.all.*
import org.typelevel.log4cats.StructuredLogger

import kui.consumer.domain.*
import kui.kernel.error.{ApplicationError, ErrorCode, KuiError}
import kui.kernel.{ClusterId, GroupId, Offset, TopicName, TopicPartition}
import kui.security.Principal
import kui.security.audit.MutationKind

/** What a delete-offsets actually removed. */
final case class DeletedOffsets(topic: TopicName, partitions: Set[TopicPartition])

trait DeleteGroupUseCase[F[_]] {

  /** Removes the group entirely.
    *
    * `KUI-GROUP-NOT-FOUND` for a group that is not there — confirmed by listing, never by describing.
    * `KUI-GROUP-NOT-EMPTY` for one that still has members. `KUI-READ-ONLY` on a read-only cluster, refused
    * before any Kafka client is touched.
    */
  /** @param principal
    *   who is deleting the group, so the audit record names them. Verified by the route.
    */
  def delete(principal: Principal, cluster: ClusterId, group: GroupId): F[Either[KuiError, Unit]]
}

trait DeleteOffsetsUseCase[F[_]] {

  /** Removes the group's committed offsets for one topic, so it starts again from its `auto.offset.reset`.
    *
    * The partition set comes from the group's **own commits**, not from the topic's partition list: deleting
    * an offset the group never committed is a no-op Kafka answers with an error, and asking the topic service
    * for a partition count would put a second service on this path and make this operation fail whenever that
    * one is down.
    */
  /** @param principal
    *   who is deleting the offsets, so the audit record names them. Verified by the route.
    */
  def delete(
      principal: Principal,
      cluster: ClusterId,
      group: GroupId,
      topic: TopicName
  ): F[Either[KuiError, DeletedOffsets]]
}

object DeleteGroupUseCase {

  val Operation: String = "kui.consumer.deleteGroup"

  def make[F[_]: Temporal](
      admin: ClusterId => GroupAdminPort[F],
      guard: MutationGuard[F],
      logger: StructuredLogger[F]
  ): DeleteGroupUseCase[F] =
    new DeleteGroupUseCase[F] {

      def delete(principal: Principal, cluster: ClusterId, group: GroupId): F[Either[KuiError, Unit]] = {
        val port = admin(cluster)

        GroupPreconditions.existsAndEmpty(port, group).flatMap {
          case Left(error) => error.asLeft[Unit].pure[F]
          case Right(found) =>
            val before = GroupPreconditions.committedOf(found)
            for {
              _ <- logger.info(
                Map("cluster.id" -> cluster.value, "group.id" -> group.value, "operation" -> Operation)
              )(s"deleting consumer group ${group.value}")
              result <- guard.guard(
                principal,
                cluster,
                MutationKind.DeleteGroup,
                group.value,
                AuditOffsets.of(before),
                Map.empty
              )(port.deleteGroup(group))
            } yield result
        }
      }
    }
}

object DeleteOffsetsUseCase {

  val Operation: String = "kui.consumer.deleteOffsets"

  def make[F[_]: Temporal](
      admin: ClusterId => GroupAdminPort[F],
      guard: MutationGuard[F],
      logger: StructuredLogger[F]
  ): DeleteOffsetsUseCase[F] =
    new DeleteOffsetsUseCase[F] {

      def delete(
          principal: Principal,
          cluster: ClusterId,
          group: GroupId,
          topic: TopicName
      ): F[Either[KuiError, DeletedOffsets]] = {
        val port = admin(cluster)

        GroupPreconditions.existsAndEmpty(port, group).flatMap {
          case Left(error) => error.asLeft[DeletedOffsets].pure[F]
          case Right(found) =>
            val committed = GroupPreconditions.committedOf(found)
            val partitions = committed.keySet.filter(_.topic == topic)

            if partitions.isEmpty then
              // Nothing to delete is not a failure and not a lie: the group holds no committed
              // offsets for this topic, which is the state the caller asked for.
              DeletedOffsets(topic, Set.empty).asRight[KuiError].pure[F]
            else
              logger.info(
                Map("cluster.id" -> cluster.value, "group.id" -> group.value, "operation" -> Operation)
              )(s"deleting ${partitions.size} committed offset(s) of ${group.value} for ${topic.value}") >>
                guard
                  .guard(
                    principal,
                    cluster,
                    MutationKind.DeleteOffsets,
                    s"${group.value}/${topic.value}",
                    AuditOffsets.of(committed.view.filterKeys(partitions.contains).toMap),
                    Map.empty
                  )(port.deleteOffsets(group, partitions))
                  .map(_.map(_ => DeletedOffsets(topic, partitions)))
        }
      }
    }
}

/** The two checks every consumer-group mutation makes, in one place so they cannot drift apart. */
private object GroupPreconditions {

  /** The group is really there, and it is empty in both senses (DEVPLAN §10 D4).
    *
    * Existence by listing, never by describing: a describe of an absent group answers with a fabricated dead
    * group, so it cannot tell these two apart.
    */
  def existsAndEmpty[F[_]: Temporal](
      port: GroupAdminPort[F],
      group: GroupId
  ): F[Either[KuiError, ConsumerGroup]] =
    port.exists(group).flatMap {
      case Left(error) => error.asLeft[ConsumerGroup].pure[F]
      case Right(false) =>
        ApplicationError
          .NotFound("consumer group", group.value, ErrorCode.GroupNotFound)
          .asLeft[ConsumerGroup]
          .pure[F]
      case Right(true) =>
        port.describe(List(group)).map {
          case Left(error) => error.asLeft[ConsumerGroup]
          case Right(described) =>
            described.get(group) match {
              case None =>
                ApplicationError
                  .InvalidState(s"consumer group ${group.value} could not be described")
                  .asLeft[ConsumerGroup]
              case Some(found)
                  if !found.completeness.membersKnown || !found.completeness.committedOffsetsKnown =>
                ApplicationError
                  .InvalidState(s"consumer group ${group.value} members or committed offsets are unknown")
                  .asLeft[ConsumerGroup]
              case Some(found) =>
                found.offsetChangeRefusal match {
                  case None => found.asRight[KuiError]
                  case Some(reason) if found.members.nonEmpty =>
                    ApplicationError.Refused(ErrorCode.GroupNotEmpty, reason).asLeft[ConsumerGroup]
                  case Some(reason) => ApplicationError.InvalidState(reason).asLeft[ConsumerGroup]
                }
            }
        }
    }

  /** Where the group's offsets are, for the audit record, read from a `ConsumerGroup` already fetched by
    * `existsAndEmpty` rather than describing the group again.
    */
  def committedOf(found: ConsumerGroup): Map[TopicPartition, Offset] =
    found.subscriptions.flatMap { subscription =>
      subscription.partitions.flatMap { state =>
        state.committed.map(offset => TopicPartition(subscription.topic, state.partition) -> offset)
      }
    }.toMap
}
