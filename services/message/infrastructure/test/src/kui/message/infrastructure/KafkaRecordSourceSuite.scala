package kui.message.infrastructure

import java.time.Instant

import scala.concurrent.duration.DurationInt

import cats.effect.IO
import cats.effect.kernel.{Deferred, Ref, Resource}
import cats.syntax.all.*

import kui.kernel.browse.{Direction, PollBudget, SeekMode}
import kui.kernel.error.KuiError
import kui.kernel.{Offset, PartitionId, TopicName}
import kui.message.application.{RawRecord, ScanCompletion, ScanEvent}
import kui.message.domain.BrowseRequest
import kui.testkit.KuiIOSuite

/** The browse arithmetic, and the promise that a cancelled browse closes its consumer.
  *
  * Every test here runs against [[FakeBrowseConsumer]] rather than a broker, which is the point of the port:
  * an off-by-one at a window boundary duplicates or drops exactly one record per page, and that is a defect a
  * demo survives and a suite does not.
  */
final class KafkaRecordSourceSuite extends KuiIOSuite {

  private val budget: PollBudget = PollBudget.unsafe(10_000, 1L << 20, 30.seconds)

  private def sourceOver(
      log: Map[PartitionId, Vector[RawRecord]],
      closed: Ref[IO, Boolean]
  ): KafkaRecordSource[IO] =
    new KafkaRecordSource[IO](
      FakeBrowseConsumer.opening(log, closed),
      BrowseTuning(pollTimeout = 1.milli, emptyPollsBeforeEnd = 0)
    )

  private def growingSourceOver(
      log: Ref[IO, Map[PartitionId, Vector[RawRecord]]],
      closed: Ref[IO, Boolean],
      tuning: BrowseTuning = BrowseTuning(pollTimeout = 1.milli, emptyPollsBeforeEnd = 0)
  ): KafkaRecordSource[IO] =
    new KafkaRecordSource[IO](FakeBrowseConsumer.openingGrowing(log, closed), tuning)

  private def request(
      seek: SeekMode,
      direction: Direction,
      limit: Int,
      partitions: Option[Set[PartitionId]] = None,
      live: Boolean = false
  ): BrowseRequest =
    BrowseRequest
      .of(
        cluster = FakeBrowseConsumer.Cluster,
        topic = FakeBrowseConsumer.Topic,
        seek = seek,
        direction = Some(direction),
        partitions = partitions,
        limit = Some(limit),
        isolation = None,
        keySerde = None,
        valueSerde = None,
        stringFilter = None,
        filter = None,
        live = live
      )
      .getOrElse(fail("the request under test is not a legal browse"))

  private def browse(
      log: Map[PartitionId, Vector[RawRecord]],
      of: BrowseRequest,
      over: PollBudget = budget
  ): IO[List[Either[KuiError, RawRecord]]] =
    Ref.of[IO, Boolean](false).flatMap(closed => sourceOver(log, closed).browse(of, over).compile.toList)

  private def offsets(records: List[Either[KuiError, RawRecord]]): List[(Int, Long)] =
    records.collect { case Right(record) => (record.partition.value, record.offset.value) }

  private def observedBrowse(
      log: Map[PartitionId, Vector[RawRecord]],
      of: BrowseRequest,
      over: PollBudget
  ): IO[(List[Either[KuiError, RawRecord]], Int)] =
    for {
      consumer <- FakeBrowseConsumer.of(log)
      source = new KafkaRecordSource[IO](
        (_, _) =>
          Resource.pure[IO, Either[KuiError, BrowseConsumer[IO]]](
            (consumer: BrowseConsumer[IO]).asRight[KuiError]
          ),
        BrowseTuning(pollTimeout = 1.milli, emptyPollsBeforeEnd = 0)
      )
      records <- source.browse(of, over).compile.toList
      polls <- consumer.pollCount
    } yield (records, polls)

  // -------------------------------------------------------------------------------------- forward

  test("a sparse [0,2] log finishes forwards and backwards without an exact-offset match") {
    val log = Map(PartitionId.unsafe(0) -> Vector(0L, 2L).map(FakeBrowseConsumer.record(0, _)))
    List(Direction.Forward, Direction.Backward).traverse_ { direction =>
      for {
        closed <- Ref.of[IO, Boolean](false)
        events <- sourceOver(log, closed)
          .scan(
            request(
              if direction == Direction.Forward then SeekMode.Beginning else SeekMode.Latest,
              direction,
              10
            ),
            PollBudget.unsafe(100, 10000, 200.millis)
          )
          .compile
          .toList
      } yield {
        assertEquals(
          events.collect { case ScanEvent.Record(record) => record.offset.value },
          if direction == Direction.Forward then List(0L, 2L) else List(2L, 0L)
        )
        assertEquals(events.last, ScanEvent.Completed(ScanCompletion.End))
      }
    }
  }

  test("backward pages preserve descending offsets despite timestamp disorder") {
    val records = FakeBrowseConsumer
      .partition(0, 4)
      ._2
      .map(record =>
        record.copy(timestamp = Instant.ofEpochMilli(if record.offset.value == 1L then 1000L else 0L))
      )
    browse(Map(PartitionId.unsafe(0) -> records), request(SeekMode.Latest, Direction.Backward, 4))
      .map(result => assertEquals(offsets(result), List((0, 3L), (0, 2L), (0, 1L), (0, 0L))))
  }

  private def configuredScan(
      visible: Vector[Long],
      retained: (Long, Long),
      batchSize: Int,
      of: BrowseRequest,
      over: PollBudget = budget,
      upper: Option[Long] = None
  ): IO[List[ScanEvent]] =
    for {
      consumer <- FakeBrowseConsumer.of(
        Map(PartitionId.unsafe(0) -> visible.map(FakeBrowseConsumer.record(0, _))),
        maxPollRecords = batchSize,
        retainedBounds = Map(PartitionId.unsafe(0) -> retained)
      )
      source = new KafkaRecordSource[IO]((_, _) => Resource.pure(Right(consumer)))
      events <- source
        .scan(of, over, upper.map(end => PartitionId.unsafe(0) -> Offset.unsafe(end)).toMap)
        .compile
        .toList
    } yield events

  for {
    direction <- List(Direction.Forward, Direction.Backward)
    batchSize <- List(1, 10)
  } test(s"retention and invisible head/tail holes complete $direction with batch=$batchSize") {
    val seek = if direction == Direction.Forward then SeekMode.AtOffset(Offset.unsafe(0)) else SeekMode.Latest
    configuredScan(Vector(7L, 9L), (5L, 12L), batchSize, request(seek, direction, 10)).map { events =>
      assertEquals(
        events.collect { case ScanEvent.Record(record) => record.offset.value },
        if direction == Direction.Forward then List(7L, 9L) else List(9L, 7L)
      )
      assertEquals(events.last, ScanEvent.Completed(ScanCompletion.End))
    }
  }

  for (direction <- List(Direction.Forward, Direction.Backward))
    test(s"a wholly invisible retained log completes $direction from consumer positions") {
      val seek = if direction == Direction.Forward then SeekMode.Beginning else SeekMode.Latest
      configuredScan(Vector.empty, (5L, 12L), 10, request(seek, direction, 10)).map { events =>
        assertEquals(events, List(ScanEvent.Completed(ScanCompletion.End)))
      }
    }

  test("a batched sparse forward seek into a hole respects both bounds") {
    configuredScan(
      Vector(0L, 2L, 4L, 6L),
      (0L, 8L),
      10,
      request(SeekMode.AtOffset(Offset.unsafe(1)), Direction.Forward, 10),
      upper = Some(4L)
    ).map { events =>
      assertEquals(events.collect { case ScanEvent.Record(record) => record.offset.value }, List(2L))
      assertEquals(events.last, ScanEvent.Completed(ScanCompletion.End))
    }
  }

  test("a batched sparse backward window excludes the requested upper bound") {
    configuredScan(
      Vector(7L, 9L, 11L),
      (5L, 14L),
      10,
      request(SeekMode.AtOffset(Offset.unsafe(10)), Direction.Backward, 10),
      upper = Some(9L)
    ).map { events =>
      assertEquals(events.collect { case ScanEvent.Record(record) => record.offset.value }, List(7L))
      assertEquals(events.last, ScanEvent.Completed(ScanCompletion.End))
    }
  }

  for (direction <- List(Direction.Forward, Direction.Backward))
    test(s"a batch at broker end is not EOF when a $direction record budget leaves records undelivered") {
      val seek = if direction == Direction.Forward then SeekMode.Beginning else SeekMode.Latest
      configuredScan(
        Vector(0L, 2L, 4L),
        (0L, 5L),
        10,
        request(seek, direction, 10),
        PollBudget.unsafe(2, 10000, 30.seconds)
      ).map { events =>
        assertEquals(
          events.collect { case ScanEvent.Record(record) => record.offset.value },
          if direction == Direction.Forward then List(0L, 2L) else List(4L, 2L)
        )
        assertEquals(events.last, ScanEvent.Completed(ScanCompletion.RecordBudget))
      }
    }

  for (direction <- List(Direction.Forward, Direction.Backward))
    test(s"a batch at broker end is not EOF when a $direction byte budget leaves records undelivered") {
      val seek = if direction == Direction.Forward then SeekMode.Beginning else SeekMode.Latest
      configuredScan(
        Vector(0L, 2L, 4L),
        (0L, 5L),
        10,
        request(seek, direction, 10),
        PollBudget.unsafe(100, 48L, 30.seconds)
      ).map { events =>
        assertEquals(
          events.collect { case ScanEvent.Record(record) => record.offset.value },
          if direction == Direction.Forward then List(0L, 2L) else List(4L, 2L)
        )
        assertEquals(events.last, ScanEvent.Completed(ScanCompletion.ByteBudget))
      }
    }

  test("an exact batched record budget reports EOF only when the whole bounded input was delivered") {
    configuredScan(
      Vector(0L, 2L, 4L),
      (0L, 6L),
      10,
      request(SeekMode.Beginning, Direction.Forward, 10),
      PollBudget.unsafe(2, 10000, 30.seconds),
      upper = Some(4L)
    ).map { events =>
      assertEquals(events.collect { case ScanEvent.Record(record) => record.offset.value }, List(0L, 2L))
      assertEquals(events.last, ScanEvent.Completed(ScanCompletion.End))
    }
  }

  test("consumer positions prove EOF across control records even when every poll is empty") {
    for {
      underlying <- FakeBrowseConsumer.of(Map(FakeBrowseConsumer.partition(0, 2)))
      consumer = new BrowseConsumer[IO] {
        def partitions(topic: TopicName) = underlying.partitions(topic)
        def beginningOffsets(topic: TopicName, ids: List[PartitionId]) =
          underlying.beginningOffsets(topic, ids)
        def endOffsets(topic: TopicName, ids: List[PartitionId]) = underlying.endOffsets(topic, ids)
        def offsetsForTimes(topic: TopicName, ids: List[PartitionId], millis: Long) =
          underlying.offsetsForTimes(topic, ids, millis)
        def assign(topic: TopicName, ids: List[PartitionId]) = underlying.assign(topic, ids)
        def seek(topic: TopicName, partition: PartitionId, offset: Long) =
          underlying.seek(topic, partition, offset)
        def positions = underlying.positions
        def poll(timeout: scala.concurrent.duration.FiniteDuration) =
          underlying.poll(timeout).map(_.map(_ => Nil))
      }
      source = new KafkaRecordSource[IO]((_, _) => Resource.pure(Right(consumer)))
      result <- source.scan(request(SeekMode.Beginning, Direction.Forward, 10), budget).compile.toList
      polls <- underlying.pollCount
    } yield {
      assertEquals(result, List(ScanEvent.Completed(ScanCompletion.End)))
      assertEquals(polls, 2)
    }
  }

  test("scan completion distinguishes record budget from EOF at the same record count") {
    for {
      closed <- Ref.of[IO, Boolean](false)
      source = sourceOver(Map(FakeBrowseConsumer.partition(0, 3)), closed)
      partial <- source
        .scan(request(SeekMode.Beginning, Direction.Forward, 10), PollBudget.unsafe(1, 10000, 30.seconds))
        .compile
        .toList
      complete <- source
        .scan(
          request(SeekMode.Beginning, Direction.Forward, 10),
          PollBudget.unsafe(1, 10000, 30.seconds),
          Map(PartitionId.unsafe(0) -> Offset.unsafe(1))
        )
        .compile
        .toList
    } yield {
      assertEquals(partial.last, ScanEvent.Completed(ScanCompletion.RecordBudget))
      assertEquals(complete.last, ScanEvent.Completed(ScanCompletion.End))
      assertEquals(complete.collect { case ScanEvent.Record(record) => record.offset.value }, List(0L))
    }
  }

  test("scan reports byte exhaustion and respects both explicit window bounds") {
    for {
      closed <- Ref.of[IO, Boolean](false)
      source = sourceOver(Map(FakeBrowseConsumer.partition(0, 5)), closed)
      partial <- source
        .scan(
          request(SeekMode.AtOffset(Offset.unsafe(1)), Direction.Forward, 10),
          PollBudget.unsafe(100, 1, 30.seconds),
          Map(PartitionId.unsafe(0) -> Offset.unsafe(3))
        )
        .compile
        .toList
      complete <- source
        .scan(
          request(SeekMode.AtOffset(Offset.unsafe(1)), Direction.Forward, 10),
          budget,
          Map(PartitionId.unsafe(0) -> Offset.unsafe(3))
        )
        .compile
        .toList
    } yield {
      assertEquals(partial.last, ScanEvent.Completed(ScanCompletion.ByteBudget))
      assertEquals(complete.collect { case ScanEvent.Record(record) => record.offset.value }, List(1L, 2L))
      assertEquals(complete.last, ScanEvent.Completed(ScanCompletion.End))
    }
  }

  test("a silent consumer ends at the deadline as incomplete, never EOF") {
    for {
      closed <- Ref.of[IO, Boolean](false)
      source = new KafkaRecordSource[IO](
        FakeBrowseConsumer
          .openingSilentAtFirst(Map(FakeBrowseConsumer.partition(0, 2)), closed, Int.MaxValue),
        BrowseTuning(pollTimeout = 1.milli, emptyPollsBeforeEnd = 0)
      )
      result <- source
        .scan(request(SeekMode.Beginning, Direction.Forward, 10), PollBudget.unsafe(100, 10000, 20.millis))
        .compile
        .toList
    } yield assertEquals(result.last, ScanEvent.Completed(ScanCompletion.Deadline))
  }

  test("transient empty polls never prove the captured range empty") {
    for {
      closed <- Ref.of[IO, Boolean](false)
      source = new KafkaRecordSource[IO](
        FakeBrowseConsumer.openingSilentAtFirst(Map(FakeBrowseConsumer.partition(0, 2)), closed, 12),
        BrowseTuning(pollTimeout = 1.milli, emptyPollsBeforeEnd = 0)
      )
      result <- source.browse(request(SeekMode.Beginning, Direction.Forward, 10), budget).compile.toList
    } yield assertEquals(offsets(result), List((0, 0L), (0, 1L)))
  }

  test("a forward browse from the beginning reads a partition in order and stops at its end") {
    val log = Map(FakeBrowseConsumer.partition(0, 5))

    browse(log, request(SeekMode.Beginning, Direction.Forward, limit = 50)).map(records =>
      // Fifty were asked for and five exist. The browse ends because the log ended, not because it was
      // still waiting: a bounded read that could not tell the two apart would hang on every small topic.
      assertEquals(offsets(records), List((0, 0L), (0, 1L), (0, 2L), (0, 3L), (0, 4L)))
    )
  }

  test("a forward source scans past the delivered limit up to the raw record budget") {
    val log = Map(FakeBrowseConsumer.partition(0, 20))
    val rawBudget = PollBudget.unsafe(maxRecords = 5, maxBytes = 1L << 20, deadline = 30.seconds)

    browse(log, request(SeekMode.Beginning, Direction.Forward, limit = 2), rawBudget).map(records =>
      assertEquals(offsets(records), List((0, 0L), (0, 1L), (0, 2L), (0, 3L), (0, 4L)))
    )
  }

  test("a forward source does not emit records beyond the raw byte budget") {
    // Fake records account for 8 key bytes and 16 value bytes. Forty-eight bytes therefore admit exactly
    // two records; the request limit is deliberately one so it cannot accidentally be the scan bound.
    val log = Map(FakeBrowseConsumer.partition(0, 20))
    val rawBudget = PollBudget.unsafe(maxRecords = 20, maxBytes = 48L, deadline = 30.seconds)

    browse(log, request(SeekMode.Beginning, Direction.Forward, limit = 1), rawBudget).map(records =>
      assertEquals(offsets(records), List((0, 0L), (0, 1L)))
    )
  }

  test("a forward browse from an offset starts there and not at the beginning") {
    val log = Map(FakeBrowseConsumer.partition(0, 10))

    browse(log, request(SeekMode.AtOffset(Offset.unsafe(7)), Direction.Forward, limit = 10)).map(records =>
      assertEquals(offsets(records), List((0, 7L), (0, 8L), (0, 9L)))
    )
  }

  test("an offset past the end of the log is clamped rather than refused") {
    // A user can type an offset retention has since deleted, and a cursor can outlive the records it
    // names. Clamping answers the question they meant; refusing answers none of them.
    val log = Map(FakeBrowseConsumer.partition(0, 4))

    browse(log, request(SeekMode.AtOffset(Offset.unsafe(9_999)), Direction.Forward, limit = 10))
      .map(records => assertEquals(offsets(records), Nil))
  }

  test("a timestamp seek starts at the first record at or after it") {
    val log = Map(FakeBrowseConsumer.partition(0, 6))

    browse(log, request(SeekMode.AtTimestamp(3L), Direction.Forward, limit = 10)).map(records =>
      assertEquals(offsets(records), List((0, 3L), (0, 4L), (0, 5L)))
    )
  }

  test("a partition with nothing at or after the timestamp shows nothing, not its whole log") {
    /*
     * Ungated until now: `found.get(partition).flatten.getOrElse(0L)` in `startOffsets` left
     * `./mill services.message.__.test` at 204/204 green, because the case above gives every partition an
     * answer. `offsetsForTimes` returns no offset for a partition whose last record predates the moment
     * asked about, and the method's own comment says why that must become the partition's *end* rather
     * than zero: treating "no answer" as zero replays the entire partition from the beginning, which is
     * the loudest possible wrong answer to "show me what happened after 10am".
     */
    val log = Map(
      // Partition 0 stops before the timestamp; partition 1 runs past it.
      FakeBrowseConsumer.partition(0, 4),
      FakeBrowseConsumer.partition(1, 12)
    )

    browse(log, request(SeekMode.AtTimestamp(9L), Direction.Forward, limit = 50)).map { records =>
      assertEquals(offsets(records).filter((partition, _) => partition == 0), Nil)
      // And the other half, so the case cannot pass by refusing everything: the partition that does have
      // records at or after the timestamp still shows them, and only them.
      assertEquals(offsets(records).filter((partition, _) => partition == 1).map(_._2), List(9L, 10L, 11L))
    }
  }

  test("a browse rides out the empty polls that follow an assignment, at the tuning KUI ships") {
    /*
     * Ungated until now: `emptyPollsBeforeEnd = 8` -> `0` in `BrowseTuning.Default` left the suite at
     * 204/204 green, because every case in this file constructs its own tuning with `0` and no case
     * reads the shipped value. `BrowseTuning`'s comment states the rule: the first polls after an
     * assignment routinely return nothing while the consumer finds the leaders, and a browse that gave up
     * there would report an empty topic that is not empty -- on a healthy cluster, at random.
     */
    val log = Map(FakeBrowseConsumer.partition(0, 5))

    for {
      closed <- Ref.of[IO, Boolean](false)
      // The shipped tuning, not one this test chose. That is the whole point of the case.
      source = new KafkaRecordSource[IO](
        FakeBrowseConsumer.openingSilentAtFirst(log, closed, silentPolls = 5),
        BrowseTuning.Default
      )
      read = request(SeekMode.Beginning, Direction.Forward, limit = 50)
      records <- source.browse(read, budget).compile.toList
    } yield assertEquals(offsets(records), List((0, 0L), (0, 1L), (0, 2L), (0, 3L), (0, 4L)))
  }

  test("a per-partition seek browses the partitions it names and no others") {
    // `seekTo=0::1&seekTo=2::0` is a request about two partitions. Reading a third — from wherever its
    // own default happened to be — answers a question nobody asked with records the caller cannot place.
    val log = Map(
      FakeBrowseConsumer.partition(0, 3),
      FakeBrowseConsumer.partition(1, 3),
      FakeBrowseConsumer.partition(2, 3)
    )

    val seek = SeekMode.AtOffsets(
      Map(PartitionId.unsafe(0) -> Offset.unsafe(1), PartitionId.unsafe(2) -> Offset.unsafe(0))
    )

    browse(log, request(seek, Direction.Forward, limit = 10)).map { records =>
      assertEquals(offsets(records).map(_._1).distinct.sorted, List(0, 2))
      assertEquals(offsets(records).filter(_._1 == 0).map(_._2), List(1L, 2L))
    }
  }

  // ------------------------------------------------------------------------------------- backward

  test("a backward browse from the end returns the newest records, newest first") {
    val log = Map(FakeBrowseConsumer.partition(0, 10))
    val rawBudget = PollBudget.unsafe(maxRecords = 3, maxBytes = 1L << 20, deadline = 30.seconds)

    browse(log, request(SeekMode.Latest, Direction.Backward, limit = 3), rawBudget).map(records =>
      assertEquals(offsets(records), List((0, 9L), (0, 8L), (0, 7L)))
    )
  }

  test("a backward source walks past the delivered limit up to the raw record budget") {
    val log = Map(FakeBrowseConsumer.partition(0, 10))
    val rawBudget = PollBudget.unsafe(maxRecords = 5, maxBytes = 1L << 20, deadline = 30.seconds)

    browse(log, request(SeekMode.Latest, Direction.Backward, limit = 2), rawBudget).map(records =>
      assertEquals(offsets(records), List((0, 9L), (0, 8L), (0, 7L), (0, 6L), (0, 5L)))
    )
  }

  test("a backward source emits one small page without preloading the whole raw record budget") {
    val log = Map(FakeBrowseConsumer.partition(0, 100))
    val rawBudget = PollBudget.unsafe(maxRecords = 100, maxBytes = 1L << 20, deadline = 30.seconds)

    for {
      consumer <- FakeBrowseConsumer.of(log)
      source = new KafkaRecordSource[IO](
        (_, _) =>
          Resource.pure[IO, Either[KuiError, BrowseConsumer[IO]]](
            (consumer: BrowseConsumer[IO]).asRight[KuiError]
          ),
        BrowseTuning(pollTimeout = 1.milli, emptyPollsBeforeEnd = 0)
      )
      records <- source
        .browse(request(SeekMode.Latest, Direction.Backward, limit = 2), rawBudget)
        .take(2L)
        .compile
        .toList
      polls <- consumer.pollCount
    } yield {
      assertEquals(offsets(records), List((0, 99L), (0, 98L)))
      assertEquals(polls, 2)
    }
  }

  test("a backward page assigns its candidate window once before the first record") {
    val log = Map(FakeBrowseConsumer.partition(0, 200))
    val rawBudget = PollBudget.unsafe(maxRecords = 100, maxBytes = 1L << 20, deadline = 30.seconds)

    for {
      consumer <- FakeBrowseConsumer.of(log)
      source = new KafkaRecordSource[IO](
        (_, _) =>
          Resource.pure[IO, Either[KuiError, BrowseConsumer[IO]]](
            (consumer: BrowseConsumer[IO]).asRight[KuiError]
          ),
        BrowseTuning(pollTimeout = 1.milli, emptyPollsBeforeEnd = 0)
      )
      first <- source
        .browse(request(SeekMode.Latest, Direction.Backward, limit = 100), rawBudget)
        .take(1L)
        .compile
        .lastOrError
      assignments <- consumer.assignmentCount
    } yield {
      assertEquals(offsets(List(first)), List((0, 199L)))
      assertEquals(assignments, 1)
    }
  }

  test("a backward candidate batch falls back when actual payload bytes exceed its allowance") {
    val oversized = FakeBrowseConsumer.partition(0, 200)._2.map(record => record.copy(valueSize = 16 * 1024))
    val log = Map(PartitionId.unsafe(0) -> oversized)
    val rawBudget = PollBudget.unsafe(maxRecords = 100, maxBytes = 1L << 20, deadline = 30.seconds)

    for {
      consumer <- FakeBrowseConsumer.of(log)
      source = new KafkaRecordSource[IO](
        (_, _) =>
          Resource.pure[IO, Either[KuiError, BrowseConsumer[IO]]](
            (consumer: BrowseConsumer[IO]).asRight[KuiError]
          ),
        BrowseTuning(pollTimeout = 1.milli, emptyPollsBeforeEnd = 0)
      )
      records <- source
        .browse(request(SeekMode.Latest, Direction.Backward, limit = 100), rawBudget)
        .compile
        .toList
      assignments <- consumer.assignmentCount
    } yield {
      assertEquals(records.size, 64)
      assertEquals(offsets(records).headOption, Some((0, 199L)))
      assert(assignments > 1, clues(assignments))
    }
  }

  test("a backward source does not emit records beyond the raw byte budget") {
    val log = Map(FakeBrowseConsumer.partition(0, 10))
    val rawBudget = PollBudget.unsafe(maxRecords = 20, maxBytes = 48L, deadline = 30.seconds)

    browse(log, request(SeekMode.Latest, Direction.Backward, limit = 1), rawBudget).map(records =>
      assertEquals(offsets(records), List((0, 9L), (0, 8L)))
    )
  }

  test("a backward source applies one raw record budget across a high-partition round and its continuation") {
    val partitionCount = 64
    val log = (0 until partitionCount).map(id => FakeBrowseConsumer.partition(id, count = 1)).toMap
    val rawBudget = PollBudget.unsafe(maxRecords = 9, maxBytes = 1L << 20, deadline = 30.seconds)

    for {
      first <- observedBrowse(log, request(SeekMode.Latest, Direction.Backward, limit = 100), rawBudget)
      firstOffsets = offsets(first._1)
      seen = firstOffsets.iterator.map(_._1).toSet
      continuation = (0 until partitionCount).map { id =>
        val next = if seen.contains(id) then 0L else 1L
        PartitionId.unsafe(id) -> Offset.unsafe(next)
      }.toMap
      second <- observedBrowse(
        log,
        request(SeekMode.AtOffsets(continuation), Direction.Backward, limit = 100),
        rawBudget
      )
    } yield {
      assertEquals(first._2, 9)
      assertEquals(firstOffsets, (8 to 0 by -1).map(id => (id, 0L)).toList)
      assertEquals(second._2, 9)
      assertEquals(offsets(second._1), (17 to 9 by -1).map(id => (id, 0L)).toList)
    }
  }

  test("backward byte exhaustion does not report EOF for unvisited partitions") {
    for {
      closed <- Ref.of[IO, Boolean](false)
      source = sourceOver((0 until 4).map(id => FakeBrowseConsumer.partition(id, count = 1)).toMap, closed)
      events <- source
        .scan(
          request(SeekMode.Latest, Direction.Backward, limit = 100),
          PollBudget.unsafe(100, 24L, 30.seconds)
        )
        .compile
        .toList
    } yield {
      assertEquals(events.collect { case ScanEvent.Record(record) => record.partition.value }, List(0))
      assertEquals(events.last, ScanEvent.Completed(ScanCompletion.ByteBudget))
    }
  }

  test("a backward source stops draining partitions when the aggregate raw byte budget is spent") {
    val log = (0 until 64).map(id => FakeBrowseConsumer.partition(id, count = 1)).toMap
    // Each fake record is 24 bytes, so the aggregate allowance admits two records across the whole round.
    val rawBudget = PollBudget.unsafe(maxRecords = 64, maxBytes = 48L, deadline = 30.seconds)

    observedBrowse(log, request(SeekMode.Latest, Direction.Backward, limit = 100), rawBudget).map {
      case (records, polls) =>
        assertEquals(polls, 2)
        assertEquals(offsets(records), List((1, 0L), (0, 0L)))
    }
  }

  test("a backward browse walks down in windows and never reads below the oldest record") {
    // Twelve records, four at a time: three rounds and then the walk ends. The assertion that matters is
    // that every offset appears exactly once — a window boundary that overlapped by one would show a
    // duplicate row on every page, and one that gapped by one would hide a record for ever.
    val log = Map(FakeBrowseConsumer.partition(0, 12))

    browse(log, request(SeekMode.Latest, Direction.Backward, limit = 12)).map { records =>
      assertEquals(offsets(records).map(_._2), (11L to 0L by -1L).toList)
      assertEquals(offsets(records).distinct.size, 12)
    }
  }

  test("a backward browse from an offset stops just below it") {
    // The range is half-open in the same direction as the forward case, which is what lets a cursor
    // minted by one be read by the other with no record shown twice.
    val log = Map(FakeBrowseConsumer.partition(0, 10))
    val rawBudget = PollBudget.unsafe(maxRecords = 3, maxBytes = 1L << 20, deadline = 30.seconds)

    browse(
      log,
      request(SeekMode.AtOffset(Offset.unsafe(5)), Direction.Backward, limit = 3),
      rawBudget
    ).map(records => assertEquals(offsets(records), List((0, 4L), (0, 3L), (0, 2L))))
  }

  test("a backward browse merges partitions newest first") {
    val log = Map(FakeBrowseConsumer.partition(0, 4), FakeBrowseConsumer.partition(1, 4))
    val rawBudget = PollBudget.unsafe(maxRecords = 4, maxBytes = 1L << 20, deadline = 30.seconds)

    browse(log, request(SeekMode.Latest, Direction.Backward, limit = 4), rawBudget).map { records =>
      // Timestamps are the offsets here, so the newest four records are offsets 3 and 2 of both
      // partitions — in that order, with the partition number breaking the tie.
      assertEquals(offsets(records).map(_._2), List(3L, 3L, 2L, 2L))
    }
  }

  test("a backward page fetches every partition under one candidate-window assignment") {
    val log = Map(FakeBrowseConsumer.partition(0, 4), FakeBrowseConsumer.partition(1, 4))
    val rawBudget = PollBudget.unsafe(maxRecords = 4, maxBytes = 1L << 20, deadline = 30.seconds)

    for {
      consumer <- FakeBrowseConsumer.of(log)
      source = new KafkaRecordSource[IO](
        (_, _) =>
          Resource.pure[IO, Either[KuiError, BrowseConsumer[IO]]](
            (consumer: BrowseConsumer[IO]).asRight[KuiError]
          ),
        BrowseTuning(pollTimeout = 1.milli, emptyPollsBeforeEnd = 0)
      )
      records <- source
        .browse(request(SeekMode.Latest, Direction.Backward, limit = 2), rawBudget)
        .take(2L)
        .compile
        .toList
      assignments <- consumer.assignmentCount
    } yield {
      assertEquals(offsets(records), List((1, 3L), (0, 3L)))
      assertEquals(assignments, 1)
    }
  }

  test("a backward merge breaks a timestamp tie the same way twice") {
    /*
     * Ungated until now: dropping the partition and offset from `Newest` -- leaving
     * `Ordering.by[RawRecord, Long](_.timestamp.toEpochMilli).reverse` -- left
     * `./mill services.message.__.test` at 1442/1442 green. The case above reads only
     * `offsets(records).map(_._2)`, and both orderings give it `List(3, 3, 2, 2)`; which *partition* the
     * newest record came from was asserted by nothing.
     *
     * Records sharing a millisecond are ordinary -- one producer batch is written inside one -- and a sort
     * on the timestamp alone falls back to the order the round happened to accumulate in, which is the
     * order the partitions were assigned. Two replicas that assigned them differently would then hand the
     * same page back in two different orders, and the cursor minted from it names a record that is no
     * longer where the next page expects it.
     */
    val log = Map(FakeBrowseConsumer.partition(0, 4), FakeBrowseConsumer.partition(1, 4))
    val rawBudget = PollBudget.unsafe(maxRecords = 4, maxBytes = 1L << 20, deadline = 30.seconds)

    browse(log, request(SeekMode.Latest, Direction.Backward, limit = 4), rawBudget).map { records =>
      // Timestamps here are the offsets, so 3 and 3 tie and 2 and 2 tie. The tie is broken by the
      // partition, descending, which is what `Newest` says and what nothing has been reading.
      assertEquals(offsets(records), List((1, 3L), (0, 3L), (1, 2L), (0, 2L)))
    }
  }

  test("a backward chunk keeps enough candidates per partition to preserve newest-first timestamp order") {
    val fast = FakeBrowseConsumer.partition(0, 4)._2.map { record =>
      val timestamp =
        if record.offset.value == 3L then 100L else if record.offset.value == 2L then 99L else 98L
      record.copy(timestamp = Instant.ofEpochMilli(timestamp))
    }
    val slow = FakeBrowseConsumer
      .partition(1, 4)
      ._2
      .map(record => record.copy(timestamp = Instant.ofEpochMilli(record.offset.value - 3L)))
    val log = Map(PartitionId.unsafe(0) -> fast, PartitionId.unsafe(1) -> slow)
    val rawBudget = PollBudget.unsafe(maxRecords = 100, maxBytes = 1L << 20, deadline = 30.seconds)

    for {
      consumer <- FakeBrowseConsumer.of(log)
      source = new KafkaRecordSource[IO](
        (_, _) =>
          Resource.pure[IO, Either[KuiError, BrowseConsumer[IO]]](
            (consumer: BrowseConsumer[IO]).asRight[KuiError]
          ),
        BrowseTuning(pollTimeout = 1.milli, emptyPollsBeforeEnd = 0)
      )
      records <- source
        .browse(request(SeekMode.Latest, Direction.Backward, limit = 2), rawBudget)
        .take(2L)
        .compile
        .toList
      polls <- consumer.pollCount
    } yield {
      assertEquals(offsets(records), List((0, 3L), (0, 2L)))
      // Two candidates from each partition preserve the ordering without scanning the 100-record budget.
      assertEquals(polls, 4)
    }
  }

  test("a backward chunk does not spend an exhausted partition's candidate allowance on another partition") {
    val log = Map(FakeBrowseConsumer.partition(0, 1), FakeBrowseConsumer.partition(1, 100))
    val rawBudget = PollBudget.unsafe(maxRecords = 100, maxBytes = 1L << 20, deadline = 30.seconds)

    for {
      consumer <- FakeBrowseConsumer.of(log)
      source = new KafkaRecordSource[IO](
        (_, _) =>
          Resource.pure[IO, Either[KuiError, BrowseConsumer[IO]]](
            (consumer: BrowseConsumer[IO]).asRight[KuiError]
          ),
        BrowseTuning(pollTimeout = 1.milli, emptyPollsBeforeEnd = 0)
      )
      records <- source
        .browse(request(SeekMode.Latest, Direction.Backward, limit = 2), rawBudget)
        .take(2L)
        .compile
        .toList
      polls <- consumer.pollCount
    } yield {
      assertEquals(offsets(records), List((1, 99L), (1, 98L)))
      assertEquals(polls, 3)
    }
  }

  test("a bounded browse stops at the end of the log as it stood when it planned") {
    /*
     * Ungated until now: `record.offset.value < _` -> `<=` in `inside` left
     * `./mill services.message.__.test` at 1442/1442 green, because every log in this suite is complete
     * before the browse starts -- so a window's `high` is one past the last record that exists and no
     * fixture could put a record *at* the bound. `FakeBrowseConsumer.openingThatGrowsAfterPlanning` writes
     * one between the plan and the first poll, which is what a producer does.
     *
     * The bound is half-open for the reason the backward walk is: a page shows the records the plan
     * resolved, and the cursor minted from it starts the next page at `high`. A page that also showed the
     * record at `high` would show it twice, once at the bottom of this page and once at the top of the
     * next.
     *
     * The second partition is load-bearing. `reachedEnd` ends the loop as soon as *every* partition has
     * reached its window, so on a one-partition log the record at `high` is never even polled and `inside`
     * decides nothing. With a longer partition still being read, the short one keeps polling — and the
     * bound is the only thing standing between the caller and a record written after the plan.
     */
    val initial = Map(FakeBrowseConsumer.partition(0, 3), FakeBrowseConsumer.partition(1, 8))

    for {
      closed <- Ref.of[IO, Boolean](false)
      source = new KafkaRecordSource[IO](
        FakeBrowseConsumer.openingThatGrowsAfterPlanning(initial, FakeBrowseConsumer.record(0, 3L), closed),
        BrowseTuning(pollTimeout = 1.milli, emptyPollsBeforeEnd = 0)
      )
      records <- source
        .browse(request(SeekMode.Beginning, Direction.Forward, limit = 50), budget)
        .compile
        .toList
    } yield assertEquals(
      offsets(records),
      List((0, 0L), (0, 1L), (0, 2L)) ++ (0L until 8L).map(offset => (1, offset)).toList
    )
  }

  test("a browse of a partition subset reads only those partitions") {
    val log = Map(
      FakeBrowseConsumer.partition(0, 3),
      FakeBrowseConsumer.partition(1, 3),
      FakeBrowseConsumer.partition(2, 3)
    )

    browse(
      log,
      request(SeekMode.Beginning, Direction.Forward, limit = 20, Some(Set(PartitionId.unsafe(1))))
    ).map(records => assertEquals(offsets(records).map(_._1).distinct, List(1)))
  }

  test("an empty topic is a finished browse with no records, not a failure") {
    browse(Map(FakeBrowseConsumer.partition(0, 0)), request(SeekMode.Latest, Direction.Backward, 10))
      .map(records => assertEquals(records, Nil))
  }

  // ----------------------------------------------------------------------------------------- live

  /** A tail over a log the test writes to while it is open.
    *
    * `expected` records are taken and the stream is then let go, because a tail does not end by itself — that
    * is the property under test, and a `compile.toList` over one would never return. The writes are made from
    * a second fiber after a short pause so that they land *after* the browse has planned its windows, which
    * is the only arrangement that distinguishes a tail from an ordinary forward read of a log that already
    * contained them.
    */
  private def tail(
      initial: Map[PartitionId, Vector[RawRecord]],
      writes: List[RawRecord],
      of: BrowseRequest,
      expected: Int,
      over: PollBudget = budget
  ): IO[List[(Int, Long)]] =
    for {
      log <- Ref.of[IO, Map[PartitionId, Vector[RawRecord]]](initial)
      closed <- Ref.of[IO, Boolean](false)
      producing = IO.sleep(50.millis) *> writes.traverse_(record =>
        log.update(current =>
          current.updated(record.partition, current.getOrElse(record.partition, Vector.empty) :+ record)
        )
      )
      reading = growingSourceOver(log, closed).browse(of, over).take(expected.toLong).compile.toList
      records <- IO.both(reading, producing).map(_._1)
    } yield offsets(records)

  test("a live browse stays open past the end of the log and delivers what is written next") {
    // The defect this stands for: a tail that plans its window against the end of the log as it stood
    // when the browse started reads an empty range, delivers nothing and stops — which is exactly what
    // the Follow live control did. Two records are written 50 ms after the browse begins.
    val initial = Map(FakeBrowseConsumer.partition(0, 3))
    val later = List(FakeBrowseConsumer.record(0, 3L), FakeBrowseConsumer.record(0, 4L))

    tail(initial, later, request(SeekMode.Latest, Direction.Forward, limit = 10, live = true), expected = 2)
      .map(seen => assertEquals(seen, List((0, 3L), (0, 4L))))
  }

  test("a live browse does not stop at the caller's limit") {
    // `limit` is a page size, and a tail has no pages. A tail that honoured it would deliver one record
    // and close, which on screen is a Follow control that works once.
    val initial = Map(FakeBrowseConsumer.partition(0, 1))
    val later = List(1L, 2L, 3L).map(FakeBrowseConsumer.record(0, _))

    tail(initial, later, request(SeekMode.Latest, Direction.Forward, limit = 1, live = true), expected = 3)
      .map(seen => assertEquals(seen, List((0, 1L), (0, 2L), (0, 3L))))
  }

  test("a live browse does not end when several polls in a row come back empty") {
    // `emptyPollsBeforeEnd` is 0 in this suite, so a bounded read gives up on the very first empty poll.
    // A quiet topic is the ordinary state of a tail rather than the end of one, and the 50 ms before the
    // first write is many empty polls at a 1 ms poll timeout.
    val initial = Map(FakeBrowseConsumer.partition(0, 0))

    tail(
      initial,
      List(FakeBrowseConsumer.record(0, 0L)),
      request(SeekMode.Beginning, Direction.Forward, limit = 10, live = true),
      expected = 1
    ).map(seen => assertEquals(seen, List((0, 0L))))
  }

  test("a live browse outlives the budget's deadline") {
    // The deadline is the last line of defence for a read that scans without matching. A tail is the one
    // read whose purpose is to still be open later, and closing it after a minute would end the stream
    // for a reason nothing on the screen could explain.
    val brief = PollBudget.unsafe(10_000, 1L << 20, 20.millis)
    val initial = Map(FakeBrowseConsumer.partition(0, 1))

    tail(
      initial,
      List(FakeBrowseConsumer.record(0, 1L)),
      request(SeekMode.Latest, Direction.Forward, limit = 10, live = true),
      expected = 1,
      over = brief
    ).map(seen => assertEquals(seen, List((0, 1L))))
  }

  test("cancelling a live browse closes the Kafka consumer") {
    // The one way a tail ends. Everything else about it is arranged so that it does not stop, which makes
    // this the only release path its consumer has.
    for {
      log <- Ref.of[IO, Map[PartitionId, Vector[RawRecord]]](Map(FakeBrowseConsumer.partition(0, 1)))
      closed <- Ref.of[IO, Boolean](false)
      started <- Deferred[IO, Unit]
      fiber <- growingSourceOver(log, closed)
        .browse(request(SeekMode.Beginning, Direction.Forward, limit = 10, live = true), budget)
        .evalTap(_ => started.complete(()).void)
        .compile
        .drain
        .start
      _ <- started.get
      _ <- fiber.cancel
      wasClosed <- closed.get
    } yield assert(wasClosed, "the consumer was still open after the tail was cancelled")
  }

  // --------------------------------------------------------------------------------- cancellation

  test("cancelling a browse closes the Kafka consumer") {
    // The chain this stands in for is: browser tab closed, gateway stream cancelled, service fiber
    // cancelled, consumer closed. This is its last link, and it is the one that leaks: a `Resource`
    // that is never released is a broker connection nobody notices until it runs out of them.
    val log = Map(FakeBrowseConsumer.partition(0, 100_000))

    for {
      closed <- Ref.of[IO, Boolean](false)
      reading <- Deferred[IO, Unit]
      fiber <- sourceOver(log, closed)
        .browse(request(SeekMode.Beginning, Direction.Forward, limit = 100_000), budget)
        .evalTap(_ => reading.complete(()).void)
        .compile
        .drain
        .start
      _ <- reading.get
      _ <- fiber.cancel
      wasClosed <- closed.get
    } yield assert(wasClosed, "the consumer was still open after the browse was cancelled")
  }

  test("a finished browse closes the Kafka consumer too") {
    val log = Map(FakeBrowseConsumer.partition(0, 3))

    for {
      closed <- Ref.of[IO, Boolean](false)
      _ <- sourceOver(log, closed)
        .browse(request(SeekMode.Beginning, Direction.Forward, limit = 3), budget)
        .compile
        .drain
      wasClosed <- closed.get
    } yield assert(wasClosed, "the consumer was left open by a browse that ended normally")
  }
}
