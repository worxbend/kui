package kui.message.infrastructure

import java.time.Instant

import cats.Parallel
import cats.effect.kernel.{Async, Resource}
import cats.syntax.all.*
import fs2.Chunk
import fs2.io.file.Files
import fs2.kafka.{Header, HeaderSerializer, Headers, KafkaProducer, ProducerRecord}
import org.apache.kafka.clients.producer.RecordMetadata
import org.typelevel.log4cats.StructuredLogger

import kui.kafka.{KafkaErrorMapper, ProducerFactory}
import kui.kernel.cluster.ClusterConnection
import kui.kernel.error.{ApplicationError, ErrorCode, KuiError}
import kui.kernel.{ClusterId, Offset, PartitionId, TopicName}
import kui.message.application.produce.{RawProducerRecord, RecordProducer, RecordProducers}
import kui.message.domain.ProducedAt

/** [[RecordProducer]] over a real Kafka producer.
  *
  * ==Everything Kafka-shaped stops here==
  *
  * A `ProducerRecord`, a `RecordMetadata` and a `KafkaException` exist in this file and nowhere above it,
  * which is the same rule [[KafkaBrowseConsumer]] keeps for the read path and for the same reason: the layers
  * above are testable without a broker precisely because they cannot name a Kafka type.
  *
  * ==Why fs2-kafka here and the raw client for browsing==
  *
  * They are genuinely different problems. A browse assigns explicit partitions, seeks each to a computed
  * offset and cares about the timing of a single `poll`, which fs2-kafka's subscription-shaped consumer hides
  * rather than helps. A produce is a fire-and-await with a callback, which is exactly what fs2-kafka's
  * `produce` already is — it hands back a nested effect: the outer one completes when the record is
  * *buffered* and the inner one when the broker has *acknowledged* it. That shape is what makes the batch
  * below one round trip's worth of latency rather than n.
  *
  * ==How a batch reports per record==
  *
  * Every record is dispatched first, then every acknowledgement is awaited. Dispatching all of them into one
  * producer's buffer before awaiting any is what lets the client batch them; awaiting them one at a time
  * afterwards is what lets a failure be attributed to the record that caused it. Awaiting each dispatch
  * before making the next would be correct, sequential and roughly a hundred times slower on a batch of a
  * hundred.
  *
  * There is no rollback and there is no transaction. A record the broker has accepted is written, and a batch
  * that fails halfway says which half — see [[RecordProducer.send]].
  */
final class KafkaRecordProducer[F[_]: Async] private (
    producer: KafkaProducer[F, Array[Byte], Array[Byte]]
) extends RecordProducer[F] {

  def partitionCount(topic: TopicName): F[Either[KuiError, Int]] =
    producer
      .partitionsFor(topic.value)
      .map(_.size)
      .attempt
      .map {
        case Left(failure) => KafkaErrorMapper.map("partitionsFor", failure).asLeft[Int]
        // An empty partition list means the broker has no such topic, which is a 404 rather than a
        // produce that silently goes nowhere.
        //
        // It is worth being honest about what this does *not* protect against. On a broker configured
        // with `auto.create.topics.enable=true` — the quickstart's, and many a development cluster's —
        // the metadata request itself creates the topic, so this check finds the partitions of a topic
        // that did not exist a moment ago and the produce succeeds. No Kafka client can avoid that: any
        // metadata lookup has the same effect, so there is nothing to ask that does not create it. The
        // remedy is the broker setting, and KUI never creates a topic of its own accord.
        case Right(0) =>
          ApplicationError.NotFound("topic", topic.value, ErrorCode.TopicNotFound).asLeft[Int]
        case Right(count) => count.asRight[KuiError]
      }

  def send(
      records: List[RawProducerRecord]
  ): F[Either[KuiError, List[Either[KuiError, ProducedAt]]]] =
    KafkaRecordProducer
      .dispatch(records)(record => producer.produce(Chunk.singleton(kafkaRecordOf(record))))
      .map(_.map {
        case Left(failure) => KafkaErrorMapper.map("produce", failure).asLeft[ProducedAt]
        case Right(acknowledged) => producedAt(acknowledged)
      }.asRight[KuiError])

  /** The broker's acknowledgement, as the domain's own type.
    *
    * fs2-kafka answers with the records it sent paired with their metadata; a chunk of one produces one pair.
    * An empty chunk cannot happen — every dispatch above carries exactly one record — and is reported rather
    * than assumed away, because "assumed away" is how an empty acknowledgement becomes an exception in a
    * `head` call three releases later.
    */
  private def producedAt(
      acknowledged: Chunk[(ProducerRecord[Array[Byte], Array[Byte]], RecordMetadata)]
  ): Either[KuiError, ProducedAt] =
    acknowledged.head match {
      case None =>
        ApplicationError
          .Unsupported("the broker acknowledged a produce without saying where the record landed")
          .asLeft[ProducedAt]

      case Some((_, metadata)) =>
        ProducedAt(
          partition = PartitionId.unsafe(metadata.partition),
          offset = Offset.unsafe(metadata.offset),
          timestamp = Instant.ofEpochMilli(metadata.timestamp)
        ).asRight[KuiError]
    }

  /** A header's value, serialised as Kafka wants it: `None` becomes a `null` array, not an empty one, so a
    * header explicitly carrying no value stays distinguishable from one whose value merely happens to be
    * empty — the same distinction `key` and `value` get below, and the one Spring's dead-letter machinery
    * reads by.
    */
  private given HeaderSerializer[Option[Array[Byte]]] = HeaderSerializer.identity.option

  /** One record, as Kafka wants it.
    *
    * `key` and `value` are handed through as `null` when absent, because that is what Kafka's own record
    * takes and what the two absences *mean*: no key at all, and — for the value — a tombstone. Substituting
    * an empty array for either would produce an ordinary record with an empty payload, which is a different
    * record and breaks compaction for whoever relies on it.
    */
  private def kafkaRecordOf(record: RawProducerRecord): ProducerRecord[Array[Byte], Array[Byte]] = {
    val base = ProducerRecord(record.topic.value, record.key.orNull, record.value.orNull)
      .withHeaders(
        Headers.fromSeq(record.headers.map(header => Header(header.key, header.value)))
      )

    val timestamped =
      record.timestamp.fold(base)(timestamp => base.withTimestamp(timestamp.toEpochMilli))

    record.partition.fold(timestamped)(partition => timestamped.withPartition(partition.value))
  }
}

object KafkaRecordProducer {

  /** Dispatch errors belong to that record, not to the entire already-dispatched batch. */
  private[infrastructure] def dispatch[F[_]: Async, A, B](records: List[A])(
      send: A => F[F[B]]
  ): F[List[Either[Throwable, B]]] =
    records
      .traverse(record => Async[F].defer(send(record)).attempt)
      .flatMap(
        _.traverse {
          case Left(error) => error.asLeft[B].pure[F]
          case Right(pending) => pending.attempt
        }
      )

  /** A producer per cluster, opened for one request and closed when it ends or is cancelled.
    *
    * Per request rather than pooled, and that is worth defending because a producer is the one Kafka client
    * it is normally right to keep. KUI's produce traffic is a person pressing a button: a pool would hold a
    * connection and a sender thread per configured cluster forever, in a process whose ordinary state is
    * "nobody is publishing anything". The cost of opening one is a connection and a metadata fetch, paid on
    * an operation whose latency is already dominated by `acks=all`.
    *
    * Cancelling releases the producer. It does not un-write a record the broker has already accepted, and
    * nothing above this line claims otherwise.
    */
  def resource[F[_]: {Async, Parallel, Files}](
      connections: ClusterId => Option[ClusterConnection],
      logger: StructuredLogger[F]
  ): RecordProducers[F] =
    new RecordProducers[F] {

      def forCluster(cluster: ClusterId): Resource[F, Either[KuiError, RecordProducer[F]]] =
        connections(cluster) match {
          case None =>
            Resource.pure(
              ApplicationError.NotFound("cluster", cluster.value, ErrorCode.ClusterNotFound).asLeft
            )

          case Some(connection) =>
            ProducerFactory.settings[F](connection, Some(logger)).flatMap {
              case Left(error) => Resource.pure[F, Either[KuiError, RecordProducer[F]]](error.asLeft)
              case Right(settings) =>
                // The constructor is inside the mapper for the same reason the browse consumer's is:
                // building a client resolves `bootstrap.servers` eagerly and throws when no address
                // resolves, which is what a stopped broker looks like from inside a container. Uncaught,
                // that escapes as `KUI-INTERNAL` while every other screen says the broker is unavailable.
                KafkaProducer
                  .resource(settings)
                  .attempt
                  .map(
                    _.bimap(
                      KafkaErrorMapper.map("openProducer", _),
                      producer => new KafkaRecordProducer[F](producer)
                    )
                  )
            }
        }
    }
}
