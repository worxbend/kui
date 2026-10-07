package kui.message.infrastructure

import java.time.Instant

import scala.concurrent.duration.FiniteDuration

import cats.effect.IO
import cats.effect.kernel.{Ref, Resource}
import cats.syntax.all.*

import kui.kernel.browse.IsolationLevel
import kui.kernel.error.KuiError
import kui.kernel.{ClusterId, Offset, PartitionId, TopicName}
import kui.message.application.RawRecord
import kui.message.domain.TimestampType

/** An in-memory Kafka log, behind the browse port.
  *
  * It exists so that the two things worth testing about a browse can be tested at all: the arithmetic — which
  * offsets a seek lands on, how a backward walk moves its window, when a bounded read knows it has finished —
  * and the lifetime, which is that a cancelled browse closes its consumer. Neither needs a broker, and
  * neither is visible in a suite that has one.
  *
  * By default it polls one record at a time to exercise the loop's termination condition. Larger batches
  * exercise fetched-but-not-emitted suffixes. Retained bounds may be independent of visible records to model
  * compaction, retention and invisible control-record tails.
  */
final class FakeBrowseConsumer(
    log: Ref[IO, Map[PartitionId, Vector[RawRecord]]],
    assigned: Ref[IO, List[PartitionId]],
    currentPositions: Ref[IO, Map[PartitionId, Long]],
    assignments: Ref[IO, Int],
    polls: Ref[IO, Int],
    maxPollRecords: Int,
    retainedBounds: Map[PartitionId, (Long, Long)]
) extends BrowseConsumer[IO] {

  require(maxPollRecords > 0, "a poll batch must admit at least one record")

  private def bounds(current: Map[PartitionId, Vector[RawRecord]], partition: PartitionId): (Long, Long) =
    retainedBounds.getOrElse(
      partition, {
        val records = current.getOrElse(partition, Vector.empty)
        (records.headOption.fold(0L)(_.offset.value), records.lastOption.fold(0L)(_.offset.value + 1L))
      }
    )

  def partitions(topic: TopicName): IO[Either[KuiError, List[PartitionId]]] =
    log.get.map(current => (current.keySet ++ retainedBounds.keySet).toList.sortBy(_.value).asRight[KuiError])

  def beginningOffsets(
      topic: TopicName,
      partitions: List[PartitionId]
  ): IO[Either[KuiError, Map[PartitionId, Long]]] =
    log.get.map(current =>
      partitions
        .map(partition => partition -> bounds(current, partition)._1)
        .toMap
        .asRight[KuiError]
    )

  def endOffsets(
      topic: TopicName,
      partitions: List[PartitionId]
  ): IO[Either[KuiError, Map[PartitionId, Long]]] =
    log.get.map(current =>
      partitions
        .map(partition => partition -> bounds(current, partition)._2)
        .toMap
        .asRight[KuiError]
    )

  def offsetsForTimes(
      topic: TopicName,
      partitions: List[PartitionId],
      millis: Long
  ): IO[Either[KuiError, Map[PartitionId, Option[Long]]]] =
    log.get.map(current =>
      partitions
        .map(partition =>
          partition -> current
            .getOrElse(partition, Vector.empty)
            .find(_.timestamp.toEpochMilli >= millis)
            .map(_.offset.value)
        )
        .toMap
        .asRight[KuiError]
    )

  /** Writes one more record into the log, the way a producer would while a tail is open.
    *
    * This is what makes live tailing testable without a broker: a bounded browse reads a log that was already
    * complete when it started, and a tail is defined by the records that arrive after that moment. Nothing
    * else in this fake changes; the poll loop simply finds a record at a position that had nothing at it
    * before.
    */
  def append(record: RawRecord): IO[Unit] =
    log.update(current =>
      current.updated(record.partition, current.getOrElse(record.partition, Vector.empty) :+ record)
    )

  def assign(topic: TopicName, partitions: List[PartitionId]): IO[Either[KuiError, Unit]] =
    assignments.update(_ + 1) *> assigned.set(partitions).as(().asRight[KuiError])

  def seek(topic: TopicName, partition: PartitionId, offset: Long): IO[Either[KuiError, Unit]] =
    currentPositions.update(_.updated(partition, offset)).as(().asRight[KuiError])

  def positions: IO[Either[KuiError, Map[PartitionId, Long]]] =
    (assigned.get, currentPositions.get).mapN((ids, positions) =>
      positions.filter((partition, _) => ids.contains(partition)).asRight
    )

  /** At most maxPollRecords in assignment order, preserving offset order within each partition. Positions
    * skip holes, but never skip a visible record held back by the batch cap.
    */
  def poll(timeout: FiniteDuration): IO[Either[KuiError, List[RawRecord]]] =
    polls.update(_ + 1) *> (for {
      partitions <- assigned.get
      current <- currentPositions.get
      records <- log.get
      available = partitions.map { partition =>
        val (begin, end) = bounds(records, partition)
        val at = current.getOrElse(partition, begin).max(begin)
        partition -> records
          .getOrElse(partition, Vector.empty)
          .filter(record => record.offset.value >= at && record.offset.value < end)
      }.toMap
      next = partitions.flatMap(available).take(maxPollRecords)
      delivered = next.groupBy(_.partition)
      _ <- currentPositions.update { positions =>
        partitions.foldLeft(positions) { (updated, partition) =>
          val sent = delivered.getOrElse(partition, Nil)
          val end = bounds(records, partition)._2
          val at = current.getOrElse(partition, bounds(records, partition)._1)
          val after =
            if sent.size == available(partition).size then at.max(end)
            else sent.lastOption.fold(at)(_.offset.value + 1L)
          updated.updated(partition, after)
        }
      }
    } yield next.asRight[KuiError])

  /** How many polls this browse made, for the suite that asserts a bounded read stops. */
  val pollCount: IO[Int] = polls.get

  /** How many times the source replaced the consumer assignment. Real Kafka pays a coordination and fetch
    * setup cost for each one, so a backward page must not perform one assignment per record.
    */
  val assignmentCount: IO[Int] = assignments.get
}

object FakeBrowseConsumer {

  val Topic: TopicName = TopicName.unsafe("orders.v1")
  val Cluster: ClusterId = ClusterId.unsafe("local")

  /** A partition holding `count` records, offsets `0` upwards, one millisecond apart. */
  def partition(id: Int, count: Int, from: Long = 0L): (PartitionId, Vector[RawRecord]) =
    PartitionId.unsafe(id) -> Vector.tabulate(count)(index => record(id, from + index.toLong))

  def record(partition: Int, offset: Long): RawRecord =
    RawRecord(
      partition = PartitionId.unsafe(partition),
      offset = Offset.unsafe(offset),
      timestamp = Instant.ofEpochMilli(offset),
      timestampType = TimestampType.CreateTime,
      key = Some(s"key-$partition-$offset".getBytes("UTF-8")),
      value = Some(s"value-$partition-$offset".getBytes("UTF-8")),
      headers = Nil,
      keySize = 8,
      valueSize = 16,
      headersSize = 0
    )

  def of(
      log: Map[PartitionId, Vector[RawRecord]],
      maxPollRecords: Int = 1,
      retainedBounds: Map[PartitionId, (Long, Long)] = Map.empty
  ): IO[FakeBrowseConsumer] =
    Ref.of[IO, Map[PartitionId, Vector[RawRecord]]](log).flatMap(create(_, maxPollRecords, retainedBounds))

  def of(log: Ref[IO, Map[PartitionId, Vector[RawRecord]]]): IO[FakeBrowseConsumer] =
    create(log, 1, Map.empty)

  private def create(
      log: Ref[IO, Map[PartitionId, Vector[RawRecord]]],
      maxPollRecords: Int,
      retainedBounds: Map[PartitionId, (Long, Long)]
  ): IO[FakeBrowseConsumer] =
    (
      Ref.of[IO, List[PartitionId]](Nil),
      Ref.of[IO, Map[PartitionId, Long]](Map.empty),
      Ref.of[IO, Int](0),
      Ref.of[IO, Int](0)
    ).mapN(new FakeBrowseConsumer(log, _, _, _, _, maxPollRecords, retainedBounds))

  /** The consumer, as the `Resource` a browse opens — with a flag that records the close.
    *
    * The flag is the whole point of the cancellation suite: a `Resource` that is never released is a Kafka
    * consumer that is never closed, and the symptom in production is a broker running out of connections
    * hours after the tabs that opened them were shut.
    */
  def opening(
      log: Map[PartitionId, Vector[RawRecord]],
      closed: Ref[IO, Boolean]
  ): (ClusterId, IsolationLevel) => Resource[IO, Either[KuiError, BrowseConsumer[IO]]] =
    (_, _) =>
      Resource
        .make(of(log))(_ => closed.set(true))
        .map(consumer => (consumer: BrowseConsumer[IO]).asRight[KuiError])

  /** The consumer as it behaves in the first moments after an assignment: several polls that return nothing
    * at all, and only then the records.
    *
    * This is not a pathological case. A real consumer returns empty polls while it discovers the leaders for
    * its assignment, which is why `BrowseTuning.emptyPollsBeforeEnd` is not zero -- and why a suite whose
    * fake answers on the first poll cannot see that constant being wrong.
    */
  def openingSilentAtFirst(
      log: Map[PartitionId, Vector[RawRecord]],
      closed: Ref[IO, Boolean],
      silentPolls: Int
  ): (ClusterId, IsolationLevel) => Resource[IO, Either[KuiError, BrowseConsumer[IO]]] =
    (_, _) =>
      Resource
        .make(of(log).flatMap(consumer => Ref.of[IO, Int](0).map(silence(consumer, _, silentPolls))))(_ =>
          closed.set(true)
        )
        .map(_.asRight[KuiError])

  private def silence(
      underlying: FakeBrowseConsumer,
      polled: Ref[IO, Int],
      silentPolls: Int
  ): BrowseConsumer[IO] =
    new BrowseConsumer[IO] {
      def partitions(topic: TopicName) = underlying.partitions(topic)
      def beginningOffsets(topic: TopicName, ids: List[PartitionId]) = underlying.beginningOffsets(topic, ids)
      def endOffsets(topic: TopicName, ids: List[PartitionId]) = underlying.endOffsets(topic, ids)
      def offsetsForTimes(topic: TopicName, ids: List[PartitionId], millis: Long) =
        underlying.offsetsForTimes(topic, ids, millis)
      def assign(topic: TopicName, ids: List[PartitionId]) = underlying.assign(topic, ids)
      def seek(topic: TopicName, partition: PartitionId, offset: Long) =
        underlying.seek(topic, partition, offset)

      def poll(timeout: FiniteDuration): IO[Either[KuiError, List[RawRecord]]] =
        polled.getAndUpdate(_ + 1).flatMap { before =>
          if before < silentPolls then IO.pure(List.empty[RawRecord].asRight[KuiError])
          else underlying.poll(timeout)
        }
      def positions = underlying.positions
    }

  /** The log as it stood when the browse planned, plus one record written the moment it had.
    *
    * A bounded browse resolves each partition's window against the end of the log at the instant it plans,
    * and producers do not stop while it reads. This fake writes `appended` immediately after `endOffsets` has
    * answered, so that record sits at exactly the window's upper bound -- the one offset a half-open
    * `[low, high)` range has to exclude. Without this arrangement no fixture in the suite can put a record at
    * `high` at all: every other log here is complete before the browse starts, so `high` is one past the last
    * record that exists and the bound is never actually tested.
    */
  def openingThatGrowsAfterPlanning(
      initial: Map[PartitionId, Vector[RawRecord]],
      appended: RawRecord,
      closed: Ref[IO, Boolean]
  ): (ClusterId, IsolationLevel) => Resource[IO, Either[KuiError, BrowseConsumer[IO]]] =
    (_, _) =>
      Resource
        .make(of(initial).map(growingAfterPlanning(_, appended)))(_ => closed.set(true))
        .map(_.asRight[KuiError])

  private def growingAfterPlanning(
      underlying: FakeBrowseConsumer,
      appended: RawRecord
  ): BrowseConsumer[IO] =
    new BrowseConsumer[IO] {
      def partitions(topic: TopicName) = underlying.partitions(topic)
      def beginningOffsets(topic: TopicName, ids: List[PartitionId]) = underlying.beginningOffsets(topic, ids)

      /** The write lands here, between the plan reading the end of the log and the first poll. */
      def endOffsets(topic: TopicName, ids: List[PartitionId]) =
        underlying.endOffsets(topic, ids).flatTap(_ => underlying.append(appended))

      def offsetsForTimes(topic: TopicName, ids: List[PartitionId], millis: Long) =
        underlying.offsetsForTimes(topic, ids, millis)
      def assign(topic: TopicName, ids: List[PartitionId]) = underlying.assign(topic, ids)
      def seek(topic: TopicName, partition: PartitionId, offset: Long) =
        underlying.seek(topic, partition, offset)
      def poll(timeout: FiniteDuration): IO[Either[KuiError, List[RawRecord]]] = underlying.poll(timeout)
      def positions = underlying.positions
    }

  /** The same thing over a log the test can still write to after the browse has started.
    *
    * A live tail cannot be tested against a fixed log: the records it exists to deliver are the ones written
    * while it is open. The `Ref` is the test's handle on the log, and every consumer this opens reads through
    * it, so an append made at any point during the browse is visible to the next poll.
    */
  def openingGrowing(
      log: Ref[IO, Map[PartitionId, Vector[RawRecord]]],
      closed: Ref[IO, Boolean]
  ): (ClusterId, IsolationLevel) => Resource[IO, Either[KuiError, BrowseConsumer[IO]]] =
    (_, _) =>
      Resource
        .make(of(log))(_ => closed.set(true))
        .map(consumer => (consumer: BrowseConsumer[IO]).asRight[KuiError])
}
