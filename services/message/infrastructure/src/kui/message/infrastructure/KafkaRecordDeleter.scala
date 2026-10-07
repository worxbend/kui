package kui.message.infrastructure

import scala.jdk.CollectionConverters.*

import cats.effect.kernel.Async
import cats.syntax.all.*
import org.apache.kafka.clients.admin.{DescribeConfigsOptions, OffsetSpec, RecordsToDelete}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.typelevel.log4cats.StructuredLogger

import kui.kafka.{AdminClientPool, KafkaErrorMapper, KafkaFutures}
import kui.kernel.cluster.ClusterConnection
import kui.kernel.error.{ApplicationError, ErrorCode, KuiError}
import kui.kernel.{ClusterId, Offset, PartitionId, TopicName}
import kui.message.domain.ports.RecordDeleter
import kui.message.domain.{PlannedPurge, PurgeResult}

/** The message domain's `RecordDeleter` port, over the raw Kafka `Admin` client (`MS-008`).
  *
  * ==What `deleteRecords` actually does==
  *
  * It does not delete records one at a time. Each partition is given an offset, and the broker moves that
  * partition's **low watermark** up to it: every offset below becomes unreadable immediately and the segments
  * holding them are removed when the broker next gets to it. There is no undo and no tombstone; the records
  * are gone. That is why the operation upstream of this class is two-phase.
  *
  * ==Why the result has two halves==
  *
  * `deleteRecords` answers per partition, and it can succeed on some and fail on others: a partition whose
  * leader has just moved, a compacted topic that a broker policy refuses. Reporting only the first failure
  * would hide the fact that seven of eight partitions *were* emptied, and reporting only success would hide
  * the one that was not. Every partition asked for appears in exactly one of the two maps — `libs/kafka`'s
  * `BatchResult` rule, carried into this port.
  */
final class KafkaRecordDeleter[F[_]: Async](
    pool: AdminClientPool[F],
    connections: ClusterId => Option[ClusterConnection],
    logger: StructuredLogger[F]
) extends RecordDeleter[F] {

  import KafkaRecordDeleter.*

  def topicId(cluster: ClusterId, topic: TopicName): F[Either[KuiError, String]] =
    connections(cluster) match {
      case None => clusterNotFound(cluster).asLeft[String].pure[F]
      case Some(connection) =>
        pool
          .run(connection, "describeTopics.purgeIdentity") { admin =>
            KafkaFutures
              .fromFuture(
                Async[F].delay(
                  admin
                    .describeTopics(
                      org.apache.kafka.common.TopicCollection.ofTopicNames(List(topic.value).asJava)
                    )
                    .topicNameValues
                    .get(topic.value)
                )
              )
              .map { described =>
                Option(described.topicId)
                  .filterNot(_ == Uuid.ZERO_UUID)
                  .map(_.toString)
                  .toRight(
                    ApplicationError.Refused(
                      ErrorCode.InvalidState,
                      "the broker did not provide a topic UUID; a purge cannot be approved safely"
                    ): KuiError
                  )
              }
          }
          .attempt
          .map(_.leftMap(failure => KafkaErrorMapper.map("describeTopics", failure, ApiTimeoutMs)).flatten)
    }

  def watermarks(cluster: ClusterId, topic: TopicName): F[Either[KuiError, List[PlannedPurge]]] =
    connections(cluster) match {
      case None => clusterNotFound(cluster).asLeft[List[PlannedPurge]].pure[F]
      case Some(connection) =>
        pool
          .run(connection, "listOffsets.purgePlan") { admin =>
            for {
              described <- KafkaFutures.fromFuture(
                Async[F].delay(
                  admin
                    .describeTopics(
                      org.apache.kafka.common.TopicCollection.ofTopicNames(List(topic.value).asJava)
                    )
                    .topicNameValues
                    .get(topic.value)
                )
              )
              // Only the partitions that have a leader. A leaderless partition cannot answer a
              // `listOffsets`, and a pair of offsets nobody could have measured must never reach a
              // screen an operator is about to confirm a deletion on.
              leaders = described.partitions.asScala.toList
                .filter(info => Option(info.leader).exists(_.id >= 0))
                .map(info => new TopicPartition(topic.value, info.partition))
              complete = leaders.size == described.partitions.size
              earliest <-
                if complete then bound(admin, leaders, OffsetSpec.earliest())
                else Map.empty[TopicPartition, Long].pure[F]
              latest <-
                if complete then bound(admin, leaders, OffsetSpec.latest())
                else Map.empty[TopicPartition, Long].pure[F]
            } yield {
              val incomplete: KuiError = ApplicationError.Refused(
                ErrorCode.InvalidState,
                "not every partition has a leader and valid watermarks; no complete purge plan is available"
              )
              if !complete then Left(incomplete)
              else
                leaders.traverse { partition =>
                  (for {
                    low <- earliest.get(partition)
                    high <- latest.get(partition).filter(_ >= low)
                    id <- PartitionId.from(partition.partition).toOption
                    from <- Offset.from(low).toOption
                    to <- Offset.from(high).toOption
                  } yield PlannedPurge(id, from, to)).toRight(incomplete)
                }
            }
          }
          .attempt
          .map(_.leftMap(failure => KafkaErrorMapper.map("listOffsets", failure, ApiTimeoutMs)).flatten)
    }

  def cleanupPolicy(cluster: ClusterId, topic: TopicName): F[Option[String]] =
    connections(cluster) match {
      case None => Option.empty[String].pure[F]
      case Some(connection) =>
        pool
          .run(connection, "describeConfigs.cleanupPolicy") { admin =>
            val resource = new ConfigResource(ConfigResource.Type.TOPIC, topic.value)

            KafkaFutures
              .fromFuture(
                Async[F]
                  .delay(admin.describeConfigs(List(resource).asJava, new DescribeConfigsOptions()).all())
              )
              .map(
                _.asScala
                  .get(resource)
                  .flatMap(config => Option(config.get(CleanupPolicy)))
                  .flatMap(entry => Option(entry.value))
              )
          }
          // A failure here costs the *warning* and never the plan. A purge an operator is entitled to
          // make must not be refused because KUI could not read a setting it only wanted in order to
          // say something useful about it.
          .handleErrorWith(failure =>
            logger
              .debug(failure)(
                s"cluster ${cluster.value} did not answer describeConfigs for $CleanupPolicy on " +
                  s"'${topic.value}'; the purge plan will not say whether the topic is compacted"
              )
              .as(Option.empty[String])
          )
    }

  def deleteBefore(
      cluster: ClusterId,
      topic: TopicName,
      offsets: Map[PartitionId, Offset]
  ): F[Either[KuiError, PurgeResult]] =
    connections(cluster) match {
      case None => clusterNotFound(cluster).asLeft[PurgeResult].pure[F]
      case Some(connection) =>
        pool
          .run(connection, "deleteRecords") { admin =>
            val requested = offsets.map((partition, offset) =>
              new TopicPartition(topic.value, partition.value) -> RecordsToDelete.beforeOffset(offset.value)
            )

            // `lowWatermarks` and not `all`: `all` is one future that fails if any partition fails,
            // which would turn "seven of eight were emptied" into an error naming none of them.
            val result = admin.deleteRecords(requested.asJava)

            result.lowWatermarks.asScala.toList
              .traverse { case (partition, future) =>
                KafkaFutures
                  .fromFuture(Async[F].delay(future))
                  .map(deleted =>
                    PartitionId
                      .from(partition.partition)
                      .toOption
                      .flatMap(id => Offset.from(deleted.lowWatermark).toOption.map(id -> _))
                      .toLeft(partition.partition -> "the broker reported an impossible low watermark")
                  )
                  .handleError(failure => Right(partition.partition -> describe(failure)))
              }
              .map(collect)
          }
          .attempt
          .map(_.leftMap(failure => KafkaErrorMapper.map("deleteRecords", failure, ApiTimeoutMs)))
    }

  private def bound(
      admin: org.apache.kafka.clients.admin.Admin,
      partitions: List[TopicPartition],
      spec: OffsetSpec
  ): F[Map[TopicPartition, Long]] =
    if partitions.isEmpty then Map.empty[TopicPartition, Long].pure[F]
    else
      KafkaFutures
        .fromFuture(Async[F].delay(admin.listOffsets(partitions.map(_ -> spec).toMap.asJava).all()))
        .map(_.asScala.toList.map((partition, info) => partition -> info.offset).toMap)

  private def clusterNotFound(cluster: ClusterId): KuiError =
    kui.kernel.error.ApplicationError
      .NotFound("cluster", cluster.value, kui.kernel.error.ErrorCode.ClusterNotFound)
}

object KafkaRecordDeleter {

  private val CleanupPolicy: String = "cleanup.policy"

  /** What the error mapper reports as the call's budget when a call times out. The pool owns the real
    * timeout; this is the number that reaches the message an operator reads.
    */
  private val ApiTimeoutMs: Long = 30_000L

  /** Splits the per-partition answers into the two halves `PurgeResult` promises: purged with its new low
    * watermark, or skipped with the broker's reason.
    */
  private[infrastructure] def collect(
      answers: List[Either[(PartitionId, Offset), (Int, String)]]
  ): PurgeResult = {
    val purged = answers.collect { case Left(entry) => entry }.toMap
    val skipped = answers.collect { case Right((partition, reason)) =>
      PartitionId.from(partition).toOption.map(_ -> reason)
    }.flatten

    PurgeResult(purged, skipped.toMap)
  }

  /** KUI's words for a failure, from the exception's class and never its message: a Kafka exception's message
    * routinely carries the bootstrap string and, on some SASL paths, the principal.
    */
  private def describe(failure: Throwable): String = {
    val name = KafkaFutures.unwrap(failure).getClass.getSimpleName
    if name.endsWith("Exception") then name.dropRight("Exception".length) else name
  }
}
