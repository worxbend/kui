package kui.message.infrastructure

import java.time.Instant
import java.{lang as jl, util as ju}

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.*

import cats.effect.kernel.{Async, Resource}
import cats.syntax.all.*
import fs2.io.file.Files
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.utils.Utils
import org.typelevel.log4cats.StructuredLogger

import kui.kafka.{ConsumerFactory, KafkaErrorMapper}
import kui.kernel.browse.IsolationLevel
import kui.kernel.cluster.ClusterConnection
import kui.kernel.error.{ApplicationError, ErrorCode, KuiError}
import kui.kernel.{ClusterId, Offset, PartitionId, TopicName}
import kui.message.application.{RawHeader, RawRecord}
import kui.message.domain.TimestampType

/** [[BrowseConsumer]] over a real Kafka consumer.
  *
  * ==Everything Kafka-shaped stops here==
  *
  * A `ConsumerRecord`, a `TopicPartition` and a `KafkaException` all exist inside this file and nowhere above
  * it. That is the whole reason the port exists: the arithmetic of seeking and window-walking is tested
  * against an in-memory log, and this file is the thin, boring half that a Testcontainers suite covers.
  *
  * ==Why the raw client and not fs2-kafka==
  *
  * fs2-kafka's consumer stream is built around subscription and rebalance. A browse does neither: it assigns
  * explicit partitions, seeks each to a computed offset, and reads a bounded window. Doing that through a
  * stream that expects to own the assignment is more code, not less, and it hides the one call whose timing
  * this milestone cares about — `poll`. The *settings* still come from [[kui.kafka.ConsumerFactory]], so the
  * security handling, the client id and the four KUI defaults (`enable.auto.commit=false` above all) are the
  * shared ones and cannot drift.
  *
  * ==Threading and cancellation==
  *
  * Every call runs on the blocking pool, because every one of them is a blocking network call. The consumer
  * is not thread-safe and is used from one fiber at a time by construction: it is created per browse, inside
  * that browse's `Resource`, and closed when the browse ends or is cancelled. Closing it is what actually
  * releases the broker connection when a browser tab goes away.
  */
final class KafkaBrowseConsumer[F[_]: Async] private (
    consumer: KafkaConsumer[Array[Byte], Array[Byte]]
) extends BrowseConsumer[F] {

  import KafkaBrowseConsumer.*

  def partitions(topic: TopicName): F[Either[KuiError, List[PartitionId]]] =
    attempt("partitionsFor") {
      Option(consumer.partitionsFor(topic.value)).toList
        .flatMap(_.asScala.toList)
        .map(info => PartitionId.unsafe(info.partition))
    }.map {
      // An empty partition list means the broker has no such topic. It is a 404 and never an empty
      // page: "this topic does not exist" and "this topic has no records" send a user to different
      // places, and only one of them is their own typo.
      case Right(Nil) => ApplicationError.NotFound("topic", topic.value, ErrorCode.TopicNotFound).asLeft
      case other => other
    }

  def beginningOffsets(
      topic: TopicName,
      partitions: List[PartitionId]
  ): F[Either[KuiError, Map[PartitionId, Long]]] =
    attempt("beginningOffsets")(
      offsetsOf(consumer.beginningOffsets(topicPartitions(topic, partitions)))
    )

  def endOffsets(
      topic: TopicName,
      partitions: List[PartitionId]
  ): F[Either[KuiError, Map[PartitionId, Long]]] =
    attempt("endOffsets")(offsetsOf(consumer.endOffsets(topicPartitions(topic, partitions))))

  def offsetsForTimes(
      topic: TopicName,
      partitions: List[PartitionId],
      millis: Long
  ): F[Either[KuiError, Map[PartitionId, Option[Long]]]] =
    attempt("offsetsForTimes") {
      val query: ju.Map[TopicPartition, jl.Long] =
        partitions
          .map(partition => new TopicPartition(topic.value, partition.value) -> jl.Long.valueOf(millis))
          .toMap
          .asJava

      consumer
        .offsetsForTimes(query)
        .asScala
        .map((partition, found) =>
          // `null` here is Kafka saying "nothing was written to this partition at or after that
          // moment". It is a real answer and it is not offset zero; the planner turns it into "start
          // at the end", so the partition contributes nothing rather than contributing everything.
          PartitionId.unsafe(partition.partition) -> Option(found).map(_.offset)
        )
        .toMap
    }

  def assign(topic: TopicName, partitions: List[PartitionId]): F[Either[KuiError, Unit]] =
    attempt("assign")(consumer.assign(topicPartitions(topic, partitions)))

  def seek(topic: TopicName, partition: PartitionId, offset: Long): F[Either[KuiError, Unit]] =
    attempt("seek")(consumer.seek(new TopicPartition(topic.value, partition.value), offset))

  def poll(timeout: FiniteDuration): F[Either[KuiError, List[RawRecord]]] =
    attempt("poll") {
      consumer
        .poll(java.time.Duration.ofMillis(timeout.toMillis))
        .asScala
        .toList
        .map(rawRecordOf)
    }

  def positions: F[Either[KuiError, Map[PartitionId, Long]]] =
    attempt("position") {
      consumer
        .assignment()
        .asScala
        .toList
        .map(partition => PartitionId.unsafe(partition.partition) -> consumer.position(partition))
        .toMap
    }

  private def attempt[A](operation: String)(thunk: => A): F[Either[KuiError, A]] =
    Async[F]
      .blocking(thunk)
      .map(_.asRight[KuiError])
      .handleError(t => KafkaErrorMapper.map(operation, t).asLeft[A])
}

object KafkaBrowseConsumer {

  /** A consumer for one browse of one cluster, closed when that browse ends or is cancelled.
    *
    * The `Either` is part of the result rather than a raised failure because the two things that can go wrong
    * here — a cluster this deployment does not have, and connection material the client refuses — are both
    * answers a browse reports as its terminal `error` event.
    *
    * @param connections
    *   the configured clusters. `None` is `KUI-CLUSTER-NOT-FOUND`, which is a 404 at the edge.
    */
  def resource[F[_]: {Async, Files}](
      connections: ClusterId => Option[ClusterConnection],
      logger: StructuredLogger[F]
  )(cluster: ClusterId, isolation: IsolationLevel): Resource[F, Either[KuiError, BrowseConsumer[F]]] =
    connections(cluster) match {
      case None =>
        Resource.pure(
          ApplicationError.NotFound("cluster", cluster.value, ErrorCode.ClusterNotFound).asLeft
        )

      case Some(connection) =>
        ConsumerFactory.settings[F](connection, None, Some(logger)).flatMap {
          case Left(error) => Resource.pure[F, Either[KuiError, BrowseConsumer[F]]](error.asLeft)
          case Right(settings) =>
            // The constructor is inside the mapper too, and that is not belt-and-braces.
            // `new KafkaConsumer(...)` resolves `bootstrap.servers` eagerly and throws a `ConfigException`
            // when no address resolves — which is what a stopped broker looks like from inside a container.
            // Uncaught, that escaped this `Resource` as a raised exception and the browse ended with
            // `KUI-INTERNAL` / "Internal error", while every other screen in the product said
            // `KUI-UPSTREAM-UNAVAILABLE` about the very same broker. `KafkaErrorMapper` already has a case
            // for it; the failure simply never reached the mapper.
            Resource
              .make(create[F](settings.properties, isolation).attempt)(closeQuietly[F](_, logger))
              .map(
                _.bimap(
                  KafkaErrorMapper.map("openBrowseConsumer", _),
                  consumer => new KafkaBrowseConsumer[F](consumer)
                )
              )
        }
    }

  /** The one Kafka property this file sets that the shared factory does not.
    *
    * Isolation is per browse and not per cluster — a user asking to see uncommitted records is asking about
    * this read, not reconfiguring their deployment — so it is applied here, where the browse's own choice is
    * in hand.
    */
  val IsolationKey: String = "isolation.level"

  private def create[F[_]: Async](
      properties: Map[String, String],
      isolation: IsolationLevel
  ): F[KafkaConsumer[Array[Byte], Array[Byte]]] =
    Async[F].blocking {
      val configured: ju.Map[String, AnyRef] =
        properties
          .updated(IsolationKey, isolation.kafkaConfigValue)
          .map((key, value) => key -> (value: AnyRef))
          .asJava

      new KafkaConsumer[Array[Byte], Array[Byte]](
        configured,
        new ByteArrayDeserializer,
        new ByteArrayDeserializer
      )
    }

  /** Closing must not fail the browse it is finishing.
    *
    * A consumer whose broker has already gone away throws on close, and a `Resource` finaliser that raised
    * would replace the answer the user was about to receive with an exception about tidying up. The failure
    * is logged, because a consumer that cannot be closed is worth knowing about, and swallowed, because it is
    * not the caller's problem.
    */
  private def closeQuietly[F[_]: Async](
      consumer: Either[Throwable, KafkaConsumer[Array[Byte], Array[Byte]]],
      logger: StructuredLogger[F]
  ): F[Unit] =
    consumer.fold(
      // Nothing was constructed, so there is nothing to close. The failure is already the browse's
      // terminal `error` event; logging it again here would double every such line.
      _ => Async[F].unit,
      open =>
        Async[F]
          .blocking(open.close())
          .handleErrorWith(t =>
            logger.warn(t)("closing the browse consumer failed; the browse itself finished")
          )
    )

  private def topicPartitions(topic: TopicName, partitions: List[PartitionId]): ju.List[TopicPartition] =
    partitions.map(partition => new TopicPartition(topic.value, partition.value)).asJava

  private def offsetsOf(raw: ju.Map[TopicPartition, jl.Long]): Map[PartitionId, Long] =
    raw.asScala.map((partition, offset) => PartitionId.unsafe(partition.partition) -> offset.longValue).toMap

  /** A Kafka record, as the layers above it are allowed to see it.
    *
    * `serializedKeySize` is `-1` for an absent key, which is Kafka's way of saying "there was none". It is
    * turned into zero here rather than carried up, because a size of minus one on a screen is a number a user
    * has to be taught to read, and the absence is already visible in the payload itself.
    */
  private[infrastructure] def rawRecordOf(record: ConsumerRecord[Array[Byte], Array[Byte]]): RawRecord = {
    val headers = record
      .headers()
      .asScala
      .toList
      .map(header => RawHeader(header.key, Option(header.value)))

    RawRecord(
      partition = PartitionId.unsafe(record.partition),
      offset = Offset.unsafe(record.offset),
      timestamp = Instant.ofEpochMilli(record.timestamp),
      timestampType = timestampTypeOf(record),
      key = Option(record.key),
      value = Option(record.value),
      headers = headers,
      keySize = math.max(0, record.serializedKeySize),
      valueSize = math.max(0, record.serializedValueSize),
      headersSize = headers.map(header => Utils.utf8Length(header.key) + header.value.fold(0)(_.length)).sum
    )
  }

  private def timestampTypeOf(record: ConsumerRecord[Array[Byte], Array[Byte]]): TimestampType =
    record.timestampType match {
      case org.apache.kafka.common.record.TimestampType.CREATE_TIME => TimestampType.CreateTime
      case org.apache.kafka.common.record.TimestampType.LOG_APPEND_TIME => TimestampType.LogAppendTime
      case _ => TimestampType.NoTimestamp
    }
}
