package kui.message.application.produce

import java.time.Instant

import cats.effect.IO
import cats.effect.kernel.{Ref, Resource}
import cats.syntax.all.*
import fs2.Stream

import kui.kernel.browse.PollBudget
import kui.kernel.error.{ApplicationError, ErrorCode, KuiError}
import kui.kernel.serde.{SerdeName, SerdeUse, Target}
import kui.kernel.{ClusterId, Offset, PartitionId, TopicName}
import kui.message.application.{RawRecord, RecordSource}
import kui.message.domain.ports.{BrowseCluster, ClusterProfileSource, SerdeChoice, SerdeSource}
import kui.message.domain.{BrowseRequest, Decoded, ProducedAt}
import kui.security.Principal
import kui.security.audit.{AuditSink, MutationRecord}
import kui.testkit.fakes.FakeStructuredLogger

/** The doubles both produce suites are built from.
  *
  * They are recording rather than merely stubbed, because the properties that matter here are about *what was
  * called and in what order*: a read-only cluster must be refused before a producer exists, and an audit
  * record must be written whether or not the write worked. A stub that only returned answers could not fail
  * either of those tests.
  */
object ProduceRig {

  val Cluster: ClusterId = ClusterId.unsafe("quickstart")
  val Topic: TopicName = TopicName.unsafe("orders.v1")
  val ReplayTopic: TopicName = TopicName.unsafe("orders.v1.replay")

  /** A cluster profile source with one cluster, writable or not. */
  final class Profiles(readOnly: Boolean, known: Boolean = true) extends ClusterProfileSource[IO] {
    def cluster(id: ClusterId): IO[Either[KuiError, BrowseCluster]] =
      if !known then IO.pure(ApplicationError.NotFound("cluster", id.value, ErrorCode.ClusterNotFound).asLeft)
      else
        IO.realTimeInstant.map(now =>
          BrowseCluster(id, "Quickstart (local)", readOnly, now, stale = false).asRight
        )
  }

  /** An audit sink that keeps what it was given. */
  final class RecordingAudit(val entries: Ref[IO, List[MutationRecord]]) extends AuditSink[IO] {
    def record(entry: MutationRecord): IO[Unit] = entries.update(_ :+ entry)
  }

  object RecordingAudit {
    def make: IO[RecordingAudit] = Ref.of[IO, List[MutationRecord]](Nil).map(new RecordingAudit(_))
  }

  /** A producer that remembers every record it was handed, and can be told to refuse.
    *
    * `opened` counts how many times a producer was *asked for*, which is the counter the read-only test
    * reads: ADR-047 says a refusal happens before any Kafka client is touched, and "before" is only
    * assertable if something is counting.
    */
  final class FakeProducers(
      val opened: Ref[IO, Int],
      val sent: Ref[IO, List[RawProducerRecord]],
      partitions: Either[KuiError, Int],
      failFrom: Option[Int] = None
  ) extends RecordProducers[IO] {

    def forCluster(cluster: ClusterId): Resource[IO, Either[KuiError, RecordProducer[IO]]] =
      Resource.eval(opened.update(_ + 1)).as(new FakeProducer(sent, partitions, failFrom).asRight)
  }

  object FakeProducers {

    def make(
        partitions: Either[KuiError, Int] = Right(4),
        failFrom: Option[Int] = None
    ): IO[FakeProducers] =
      for {
        opened <- Ref.of[IO, Int](0)
        sent <- Ref.of[IO, List[RawProducerRecord]](Nil)
      } yield new FakeProducers(opened, sent, partitions, failFrom)
  }

  final class FakeProducer(
      sent: Ref[IO, List[RawProducerRecord]],
      partitions: Either[KuiError, Int],
      failFrom: Option[Int]
  ) extends RecordProducer[IO] {

    def partitionCount(topic: TopicName): IO[Either[KuiError, Int]] = IO.pure(partitions)

    def send(
        records: List[RawProducerRecord]
    ): IO[Either[KuiError, List[Either[KuiError, ProducedAt]]]] =
      sent
        .update(_ ++ records)
        .as(
          records.zipWithIndex
            .map((_, index) =>
              if failFrom.exists(index >= _) then
                ApplicationError.Unsupported("the broker refused this record").asLeft[ProducedAt]
              else
                ProducedAt(
                  PartitionId.unsafe(0),
                  Offset.unsafe(100L + index),
                  Instant.parse("2026-09-04T09:00:00Z")
                ).asRight[KuiError]
            )
            .asRight
        )
  }

  /** A serde source that turns text into UTF-8 bytes, and can be told to refuse.
    *
    * `decodes` counts calls to `decode`, which is what `ResendUseCaseSuite` asserts stays at zero: a resend
    * that deserialized anything would be a resend that cannot copy a topic KUI has no serde for.
    */
  final class FakeSerdes(
      val decodes: Ref[IO, Int],
      refuse: Option[KuiError] = None
  ) extends SerdeSource[IO] {

    def decode(
        cluster: ClusterId,
        topic: TopicName,
        target: Target,
        requested: Option[SerdeName],
        bytes: Option[Array[Byte]]
    ): IO[(Decoded, Option[String])] =
      decodes.update(_ + 1).as((Decoded.absent(SerdeName.Fallback), None))

    def serialize(
        cluster: ClusterId,
        topic: TopicName,
        target: Target,
        requested: Option[SerdeName],
        properties: Map[String, String],
        text: Option[String]
    ): IO[Either[KuiError, Option[Array[Byte]]]] =
      IO.pure(refuse.toLeft(text.map(_.getBytes("UTF-8"))))

    def choices(
        cluster: ClusterId,
        topic: TopicName,
        target: Target,
        use: SerdeUse
    ): IO[Either[KuiError, List[SerdeChoice]]] = IO.pure(Nil.asRight)
  }

  object FakeSerdes {
    def make(refuse: Option[KuiError] = None): IO[FakeSerdes] =
      Ref.of[IO, Int](0).map(new FakeSerdes(_, refuse))
  }

  /** The verified principal these mutations are made by: the anonymous one a deployment without
    * authentication produces.
    */
  val Caller: Principal = Principal.Anonymous

  /** A record source that answers with a fixed log, whatever is asked of it. */
  final class FakeRecords(records: List[RawRecord]) extends RecordSource[IO] {
    override def scan(request: BrowseRequest, budget: PollBudget, upperOffsets: Map[PartitionId, Offset]) =
      kui.message.application.FiniteRecordScan(
        records.filter(record => upperOffsets.get(record.partition).forall(record.offset.value < _.value)),
        budget
      )

    def browse(request: BrowseRequest, budget: PollBudget): Stream[IO, Either[KuiError, RawRecord]] =
      Stream.emits(records.map(_.asRight[KuiError]))

    def assignedStarts(request: BrowseRequest): IO[Either[KuiError, Map[PartitionId, Offset]]] =
      IO.pure(Right(Map.empty))
  }

  def guardFor(profiles: ClusterProfileSource[IO], audit: AuditSink[IO]): IO[MutationGuard[IO]] =
    FakeStructuredLogger[IO].map(logger => MutationGuard.make[IO](profiles, audit, logger))
}
