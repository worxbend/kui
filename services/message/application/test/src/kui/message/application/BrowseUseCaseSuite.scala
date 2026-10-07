package kui.message.application

import java.time.Instant
import java.util.Locale

import scala.concurrent.duration.{DurationInt, FiniteDuration}

import cats.Applicative
import cats.effect.kernel.{Clock, Concurrent}
import cats.effect.{IO, Ref}
import cats.syntax.all.*
import fs2.Stream

import kui.kernel.browse.{Direction, PollBudget, SeekMode}
import kui.kernel.error.{ApplicationError, ErrorCode, KuiError}
import kui.kernel.serde.{PayloadKind, SerdeName, SerdeUse, Target}
import kui.kernel.{ClusterId, Offset, PartitionId, Secret, TopicName}
import kui.message.application.cursor.{BrowseCursor, CursorCodec}
import kui.message.domain.ports.{
  BrowseCluster,
  ClusterProfileSource,
  CompiledFilter,
  FilterSample,
  FilterSource,
  FilterVerdict,
  SerdeChoice,
  SerdeSource
}
import kui.message.domain.{BrowseLimits, BrowseRequest, Decoded, DecodedRecord, FilterRef, TimestampType}
import kui.testkit.KuiIOSuite

/** The browse use case's promises: a record that cannot be decoded is still delivered, a browse accounts for
  * what it did, and the cursor it hands back resumes exactly where it stopped.
  *
  * The first is the milestone's central rule and the reason the quickstart seeds a topic of deliberately
  * unparseable payloads. A stream that ended on the first bad record would hide every good record after it,
  * and the bad record is usually the one the screen was opened to find.
  */
final class BrowseUseCaseSuite extends KuiIOSuite {

  private val cluster = ClusterId.unsafe("local")
  private val topic = TopicName.unsafe("audit.log.raw")
  private val budget = PollBudget.unsafe(1000, 1L << 20, 30.seconds)
  private val key = Secret("test-key".getBytes("UTF-8"))

  final private class CountingClock(calls: Ref[IO, Int]) extends Clock[IO] {
    def applicative: Applicative[IO] = Applicative[IO]

    def monotonic: IO[FiniteDuration] = calls.getAndUpdate(_ + 1).map(call => (100000 + call).seconds)

    def realTime: IO[FiniteDuration] = IO.pure(0.seconds)
  }

  private val clusters: ClusterProfileSource[IO] = (id: ClusterId) =>
    IO.pure(
      if id == cluster then Right(BrowseCluster(cluster, "Local", readOnly = false, Instant.EPOCH, false))
      else Left(ApplicationError.NotFound("cluster", id.value, ErrorCode.ClusterNotFound))
    )

  /** A serde that reads text and refuses one particular payload.
    *
    * That is what a real serde failure looks like: not a broken cluster, but one record whose producer wrote
    * something the configured decoder does not accept.
    */
  private def serdes(failsOn: String): SerdeSource[IO] = new SerdeSource[IO] {

    def decode(
        cluster: ClusterId,
        topic: TopicName,
        target: Target,
        requested: Option[SerdeName],
        bytes: Option[Array[Byte]]
    ): IO[(Decoded, Option[String])] = IO.pure {
      val text = bytes.fold("")(new String(_, "UTF-8"))

      // The fallback's answer, with the reason the intended serde could not produce one. The record is
      // delivered either way; this is the difference between a row a user can act on and a blank screen.
      if text == failsOn then
        (Decoded(text, PayloadKind.Text, SerdeName.Fallback, Map.empty), Some("not a valid document"))
      else (Decoded(text, PayloadKind.Text, SerdeName.String, Map.empty), None)
    }

    def serialize(
        cluster: ClusterId,
        topic: TopicName,
        target: Target,
        requested: Option[SerdeName],
        properties: Map[String, String],
        text: Option[String]
    ): IO[Either[KuiError, Option[Array[Byte]]]] = IO.pure(Right(None))

    def choices(
        cluster: ClusterId,
        topic: TopicName,
        target: Target,
        use: SerdeUse
    ): IO[Either[KuiError, List[SerdeChoice]]] = IO.pure(Right(Nil))
  }

  private def source(records: List[Either[KuiError, RawRecord]]): RecordSource[IO] =
    new RecordSource[IO] {
      def browse(request: BrowseRequest, budget: PollBudget): Stream[IO, Either[KuiError, RawRecord]] =
        Stream.emits(records)

      def assignedStarts(request: BrowseRequest): IO[Either[KuiError, Map[PartitionId, Offset]]] =
        IO.pure(Right(Map.empty))
    }

  private def raw(offset: Long, value: String): RawRecord =
    RawRecord(
      partition = PartitionId.unsafe(0),
      offset = Offset.unsafe(offset),
      timestamp = Instant.ofEpochMilli(offset),
      timestampType = TimestampType.CreateTime,
      key = None,
      value = Some(value.getBytes("UTF-8")),
      headers = Nil,
      keySize = 0,
      valueSize = value.length,
      headersSize = 0
    )

  private def request(
      limit: Int,
      filter: Option[String] = None,
      of: ClusterId = cluster,
      live: Boolean = false
  ): BrowseRequest =
    BrowseRequest
      .of(
        cluster = of,
        topic = topic,
        seek = SeekMode.Beginning,
        direction = Some(Direction.Forward),
        partitions = None,
        limit = Some(limit),
        isolation = None,
        keySerde = None,
        valueSerde = None,
        stringFilter = filter,
        filter = None,
        live = live
      )
      .getOrElse(fail("the request under test is not a legal browse"))

  private def useCase(
      records: List[Either[KuiError, RawRecord]],
      failsOn: String = "<nothing fails>",
      filters: FilterSource[IO] = FilterSource.unsupported[IO],
      masking: RecordMasking[IO] = RecordMasking.none[IO]
  ): BrowseUseCase[IO] =
    BrowseUseCase.make[IO](
      clusters,
      serdes(failsOn),
      source(records),
      CursorCodec.hmacSha256[IO](key),
      filters,
      masking
    )

  /** A smart filter that answers by looking at the value's text, so that a test can say which records it
    * keeps without writing an expression the CEL engine would have to compile.
    */
  private def filterOver(verdict: DecodedRecord => FilterVerdict): FilterSource[IO] =
    new FilterSource[IO] {
      def compile(cluster: ClusterId, filter: FilterRef): IO[Either[KuiError, CompiledFilter[IO]]] =
        IO.pure(
          Right(new CompiledFilter[IO] {
            def test(record: DecodedRecord): IO[FilterVerdict] = IO.pure(verdict(record))
          })
        )

      def register(cluster: ClusterId, source: String): IO[Either[KuiError, String]] =
        IO.pure(Right("0123456789abcdef"))

      def check(
          cluster: ClusterId,
          source: String,
          record: FilterSample
      ): IO[Either[KuiError, FilterVerdict]] =
        IO.pure(Right(FilterVerdict.Matched))
    }

  /** A browse that names a smart filter. The id is well-formed because `FilterRef` refuses anything else. */
  private def filtered(limit: Int): BrowseRequest =
    BrowseRequest
      .of(
        cluster = cluster,
        topic = topic,
        seek = SeekMode.Beginning,
        direction = Some(Direction.Forward),
        partitions = None,
        limit = Some(limit),
        isolation = None,
        keySerde = None,
        valueSerde = None,
        stringFilter = None,
        filter = FilterRef.of("0123456789abcdef", Some("record.value.status == 'PAID'")).toOption,
        live = false
      )
      .getOrElse(fail("the request under test is not a legal browse"))

  private def events(browse: BrowseUseCase[IO], of: BrowseRequest): IO[List[BrowseEvent]] =
    browse.browse(of, budget).compile.toList

  private def delivered(events: List[BrowseEvent]): List[String] =
    events.collect { case BrowseEvent.Record(record) => record.value.text }

  // ------------------------------------------------------------------------------ resuming a browse

  private def cursorFor(
      offsets: Map[PartitionId, Offset],
      direction: Direction = Direction.Forward,
      limit: Int = 50
  ): IO[String] =
    CursorCodec
      .hmacSha256[IO](key)
      .encode(
        BrowseCursor(
          v = BrowseCursor.Version,
          cluster = cluster,
          topic = topic,
          direction = direction,
          perPartitionNext = offsets,
          filterId = None,
          keySerde = Some(SerdeName.String),
          valueSerde = None,
          limit = limit,
          isolation = kui.kernel.browse.IsolationLevel.Default,
          expiresAt = Instant.now().plusSeconds(600)
        )
      )
      .map(_.getOrElse(fail("the cursor under test could not be signed")))

  test("a cursor resumes every partition at its own offset") {
    // The reason the seek grammar keeps a per-partition form: a continuation that could only express one
    // offset for every partition could not express what a cursor already means.
    val offsets =
      Map(PartitionId.unsafe(0) -> Offset.unsafe(100L), PartitionId.unsafe(3) -> Offset.unsafe(250L))

    for {
      cursor <- cursorFor(offsets)
      resumed <- useCase(Nil).resume(cluster, topic, cursor, None, BrowseLimits.Default)
    } yield {
      val request = resumed.getOrElse(fail(s"the cursor did not resume: $resumed"))
      assertEquals(request.seek, SeekMode.AtOffsets(offsets))
      // The subset is the cursor's own keys and not "all of them": a partition added to the topic since the
      // first page has no start position of its own and would arrive from wherever the consumer landed.
      assertEquals(request.partitions.map(_.toSortedSet.toSet), Some(offsets.keySet))
      assertEquals(request.live, false)
    }
  }

  test("the cursor carries the page size, the direction and the serdes so the next page matches the last") {
    val offsets = Map(PartitionId.unsafe(0) -> Offset.unsafe(7L))

    for {
      cursor <- cursorFor(offsets, direction = Direction.Backward, limit = 17)
      resumed <- useCase(Nil).resume(cluster, topic, cursor, None, BrowseLimits.Default)
    } yield {
      val request = resumed.getOrElse(fail(s"the cursor did not resume: $resumed"))
      assertEquals(request.direction, Direction.Backward)
      assertEquals(request.limit, 17)
      assertEquals(request.keySerde, Some(SerdeName.String))
    }
  }

  test("a plain substring filter may change between pages, because it is applied after decoding") {
    val offsets = Map(PartitionId.unsafe(0) -> Offset.unsafe(7L))

    for {
      cursor <- cursorFor(offsets)
      resumed <- useCase(Nil).resume(cluster, topic, cursor, Some("order-42"), BrowseLimits.Default)
    } yield assertEquals(resumed.map(_.stringFilter), Right(Some("order-42")))
  }

  test("a cursor minted for another topic does not resume here") {
    for {
      cursor <- cursorFor(Map(PartitionId.unsafe(0) -> Offset.unsafe(1L)))
      resumed <- useCase(Nil)
        .resume(cluster, TopicName.unsafe("somewhere.else"), cursor, None, BrowseLimits.Default)
    } yield assert(resumed.isLeft, "a cursor for another topic was accepted")
  }

  test("a tampered cursor is refused rather than read as if the change were absent") {
    for {
      cursor <- cursorFor(Map(PartitionId.unsafe(0) -> Offset.unsafe(1L)))
      resumed <- useCase(Nil).resume(cluster, topic, cursor.dropRight(3) + "aaa", None, BrowseLimits.Default)
    } yield assert(resumed.isLeft, "a tampered cursor was accepted")
  }

  // -------------------------------------------------------------------------- decoding never fails

  test("a record that cannot be decoded is delivered anyway, and the stream carries on") {
    val records = List(raw(0, "good"), raw(1, "broken"), raw(2, "also good")).map(_.asRight[KuiError])

    events(useCase(records, failsOn = "broken"), request(10)).map { produced =>
      assertEquals(delivered(produced), List("good", "broken", "also good"))

      val failed = produced.collectFirst { case BrowseEvent.Record(r) if r.decodeErrors.nonEmpty => r }
      assertEquals(failed.map(_.offset.value), Some(1L))
      assertEquals(failed.toList.flatMap(_.decodeErrors).map(_.cause), List("not a valid document"))
      // The serde named on the payload is the one that actually produced the text, the fallback, while
      // the failure names the one the user configured. They answer different questions.
      assertEquals(failed.map(_.value.serde), Some(SerdeName.Fallback))
    }
  }

  test("a stream that fails halfway keeps the records it already delivered") {
    val boom: KuiError = ApplicationError.NotFound("topic", "gone", ErrorCode.TopicNotFound)
    val records = List(raw(0, "first").asRight[KuiError], boom.asLeft[RawRecord], raw(1, "never").asRight)

    events(useCase(records), request(10)).map { produced =>
      assertEquals(delivered(produced), List("first"))
      assertEquals(produced.lastOption, Some(BrowseEvent.Failed(boom)))
      // A failed browse ends with the failure and nothing else: two terminal events would leave a client
      // with no rule about which one it is meant to believe.
      assert(produced.forall {
        case BrowseEvent.Finished(_, _) => false
        case _ => true
      })
    }
  }

  // ------------------------------------------------------------------------------------ the ending

  // ------------------------------------------------------------------------------- the smart filter

  test("a smart filter that will not compile ends the browse before a single record is read") {
    // Before, and not during: an expression with a typo in it must not open a Kafka consumer and read a
    // million records in order to tell the user that nothing matched.
    val refusing = new FilterSource[IO] {
      def compile(cluster: ClusterId, filter: FilterRef): IO[Either[KuiError, CompiledFilter[IO]]] =
        IO.pure(Left(ApplicationError.Invalid("line 1, column 8: undeclared reference to 'staus'", Nil)))

      def register(cluster: ClusterId, source: String): IO[Either[KuiError, String]] =
        IO.pure(Left(ApplicationError.Invalid("nope", Nil)))

      def check(
          cluster: ClusterId,
          source: String,
          record: FilterSample
      ): IO[Either[KuiError, FilterVerdict]] =
        IO.pure(Left(ApplicationError.Invalid("nope", Nil)))
    }

    val records = List(raw(0, "one"), raw(1, "two")).map(_.asRight[KuiError])

    events(useCase(records, filters = refusing), filtered(10)).map { produced =>
      assert(delivered(produced).isEmpty, "records were read despite the filter not compiling")
      assert(produced.exists {
        case BrowseEvent.Failed(_) => true
        case _ => false
      })
    }
  }

  test("records the smart filter rejects are read and not delivered") {
    val records = List(raw(0, "keep"), raw(1, "drop"), raw(2, "keep")).map(_.asRight[KuiError])

    val keeping = filterOver(record =>
      if record.value.text == "keep" then FilterVerdict.Matched else FilterVerdict.DidNotMatch
    )

    events(useCase(records, filters = keeping), filtered(10)).map { produced =>
      assertEquals(delivered(produced), List("keep", "keep"))

      // Three read, two delivered. The gap is what tells a user the filter is doing something, and
      // without it a narrow filter and an empty topic are the same screen.
      val consumed = produced.collect { case event: BrowseEvent.Consumed => event }.last
      assertEquals((consumed.read, consumed.delivered), (3L, 2L))
    }
  }

  test("a record the filter threw on is excluded and counted rather than delivered or fatal") {
    // ADR-017's rule, and the reason `FilterVerdict` has three cases. A filter that errors on every
    // record would otherwise be indistinguishable from a filter that matches nothing, and the user would
    // conclude their data is missing rather than their expression is wrong.
    val records = List(raw(0, "good"), raw(1, "broken"), raw(2, "good")).map(_.asRight[KuiError])

    val throwing = filterOver(record =>
      if record.value.text == "broken" then FilterVerdict.Failed("no such field 'status'")
      else FilterVerdict.Matched
    )

    events(useCase(records, filters = throwing), filtered(10)).map { produced =>
      assertEquals(delivered(produced), List("good", "good"))

      val consumed = produced.collect { case event: BrowseEvent.Consumed => event }.last
      assertEquals(consumed.filterErrors, 1L)
    }
  }

  test("a browse that ran out of records says exhausted and offers no cursor") {
    val records = List(raw(0, "one"), raw(1, "two")).map(_.asRight[KuiError])

    events(useCase(records), request(10)).map(produced =>
      assertEquals(produced.lastOption, Some(BrowseEvent.Finished(BrowseEnd.Exhausted, None)))
    )
  }

  test("a selective browse that spends its raw budget returns a cursor to later matches") {
    val partition = PartitionId.unsafe(0)
    val scanBudget = PollBudget.unsafe(2, 1L << 20, 30.seconds)
    val available = List(raw(0, "skip"), raw(1, "skip"), raw(2, "match"))
    val pagedSource: RecordSource[IO] = new RecordSource[IO] {
      private def startOf(request: BrowseRequest): Long = request.seek match {
        case SeekMode.Beginning => 0L
        case SeekMode.AtOffsets(offsets) => offsets(partition).value
        case other => fail(s"expected a beginning or resumed seek, got $other")
      }

      def browse(request: BrowseRequest, budget: PollBudget): Stream[IO, Either[KuiError, RawRecord]] =
        Stream
          .emits(available.filter(_.offset.value >= startOf(request)).map(_.asRight[KuiError]))
          .take(budget.recordsLeft.toLong)

      def assignedStarts(request: BrowseRequest): IO[Either[KuiError, Map[PartitionId, Offset]]] =
        IO.pure(Right(Map(partition -> Offset.unsafe(startOf(request)))))
    }
    val browse = BrowseUseCase.make[IO](
      clusters,
      serdes("<nothing fails>"),
      pagedSource,
      CursorCodec.hmacSha256[IO](key),
      FilterSource.unsupported[IO],
      RecordMasking.none[IO]
    )
    val firstRequest = request(limit = 1, filter = Some("match"))

    for {
      first <- browse.browse(firstRequest, scanBudget).compile.toList
      token = first.last match {
        case BrowseEvent.Finished(BrowseEnd.Budget, Some(cursor)) => cursor
        case other => fail(s"expected a budget ending with a cursor, got $other")
      }
      resumed <- browse.resume(cluster, topic, token, Some("match"), BrowseLimits.Default)
      secondRequest = resumed.getOrElse(fail(s"the budget cursor did not resume: $resumed"))
      second <- browse.browse(secondRequest, scanBudget).compile.toList
    } yield {
      assertEquals(delivered(first), Nil)
      assertEquals(delivered(second), List("match"))
      assertEquals(
        second.lastOption.map {
          case BrowseEvent.Finished(reason, _) => reason
          case other => fail(s"expected a finished event, got $other")
        },
        Some(BrowseEnd.Limit)
      )
    }
  }

  test("a tail delivers past its limit, because a limit is a page size and a tail has no pages") {
    // `limit` bounds a page; a tail is not paged, and the bound that keeps a tail from growing without
    // end is on the screen — the browser keeps the newest rows and drops the rest. A tail that stopped at
    // `limit` would be a Follow control that worked once.
    val records = List(raw(0, "one"), raw(1, "two"), raw(2, "three")).map(_.asRight[KuiError])

    events(useCase(records), request(limit = 1, live = true)).map(produced =>
      assertEquals(delivered(produced), List("one", "two", "three"))
    )
  }

  test("a browse that hit its limit says so and hands back a cursor") {
    val records = List(raw(0, "one"), raw(1, "two"), raw(2, "three")).map(_.asRight[KuiError])

    events(useCase(records), request(2)).map { produced =>
      assertEquals(delivered(produced), List("one", "two"))

      produced.last match {
        case BrowseEvent.Finished(BrowseEnd.Limit, Some(cursor)) => assert(cursor.nonEmpty)
        case other => fail(s"expected a limit ending with a cursor, got $other")
      }
    }
  }

  test("the cursor a forward browse hands back resumes after the last record it delivered") {
    val records = List(raw(0, "one"), raw(1, "two"), raw(2, "three")).map(_.asRight[KuiError])
    val codec = CursorCodec.hmacSha256[IO](key)

    for {
      produced <- events(useCase(records), request(2))
      token = produced.last match {
        case BrowseEvent.Finished(_, Some(cursor)) => cursor
        case other => fail(s"expected a cursor, got $other")
      }
      decoded <- codec.decode(token, (cluster, topic), Instant.EPOCH)
    } yield decoded match {
      // Offset 2 and not 1: the last record delivered was offset 1, and a cursor that resumed there would
      // show it twice. This is the one increment in paging, and BrowseCursor.afterForward owns it.
      case Right(cursor) =>
        assertEquals(cursor.perPartitionNext, Map(PartitionId.unsafe(0) -> Offset.unsafe(2)))
        assertEquals(cursor.v, BrowseCursor.Version)
      case Left(error) => fail(s"the cursor this build minted could not be read back: ${error.message}")
    }
  }

  test("a backward cursor resumes before the oldest record processed without duplicates or gaps") {
    val partition = PartitionId.unsafe(0)
    val available = List(99L, 98L, 97L, 96L, 95L).map(offset => raw(offset, offset.toString))
    val pagedSource: RecordSource[IO] = new RecordSource[IO] {
      def browse(request: BrowseRequest, budget: PollBudget): Stream[IO, Either[KuiError, RawRecord]] = {
        val high = request.seek match {
          case SeekMode.Latest => 100L
          case SeekMode.AtOffsets(offsets) => offsets(partition).value
          case other => fail(s"expected a latest or resumed backward seek, got $other")
        }
        Stream.emits(available.filter(_.offset.value < high).map(_.asRight[KuiError]))
      }

      def assignedStarts(request: BrowseRequest): IO[Either[KuiError, Map[PartitionId, Offset]]] =
        IO.pure(Right(Map(partition -> Offset.unsafe(100L))))
    }
    val browse = BrowseUseCase.make[IO](
      clusters,
      serdes("<nothing fails>"),
      pagedSource,
      CursorCodec.hmacSha256[IO](key),
      FilterSource.unsupported[IO],
      RecordMasking.none[IO]
    )
    val firstRequest = BrowseRequest
      .of(
        cluster = cluster,
        topic = topic,
        seek = SeekMode.Latest,
        direction = Some(Direction.Backward),
        partitions = Some(Set(partition)),
        limit = Some(2),
        isolation = None,
        keySerde = None,
        valueSerde = None,
        stringFilter = None,
        filter = None,
        live = false
      )
      .getOrElse(fail("the backward request under test is not legal"))

    for {
      first <- events(browse, firstRequest)
      token = first.last match {
        case BrowseEvent.Finished(BrowseEnd.Limit, Some(cursor)) => cursor
        case other => fail(s"expected the first backward page to end with a cursor, got $other")
      }
      resumed <- browse.resume(cluster, topic, token, None, BrowseLimits.Default)
      secondRequest = resumed.getOrElse(fail(s"the backward cursor did not resume: $resumed"))
      second <- events(browse, secondRequest)
    } yield {
      assertEquals(delivered(first), List("99", "98"))
      assertEquals(delivered(second), List("97", "96"))
      assertEquals((delivered(first) ++ delivered(second)).distinct, List("99", "98", "97", "96"))
    }
  }

  test(
    "a partition assigned but never seen before the limit still appears in the minted cursor, even on a " +
      "first page"
  ) {
    // The bug this guards: `request.partitions` is `None` on an ordinary first page, so before this fix
    // nothing here knew a second partition had been assigned at all, and a partition that never yielded a
    // record before `limit` was reached silently dropped out of `perPartitionNext` -- and then out of every
    // page after this one.
    val records = List(raw(0, "one"), raw(1, "two"), raw(2, "three")).map(_.asRight[KuiError])
    val starved = PartitionId.unsafe(1)

    val twoPartitionSource: RecordSource[IO] = new RecordSource[IO] {
      def browse(request: BrowseRequest, budget: PollBudget): Stream[IO, Either[KuiError, RawRecord]] =
        Stream.emits(records)

      // What a real `RecordSource` reports once it has asked the broker which partitions exist: both are
      // assigned, and both start at offset zero, but only partition 0 ever produces a record before the
      // page's limit is reached.
      def assignedStarts(request: BrowseRequest): IO[Either[KuiError, Map[PartitionId, Offset]]] =
        IO.pure(Right(Map(PartitionId.unsafe(0) -> Offset.unsafe(0L), starved -> Offset.unsafe(0L))))
    }

    val browse = BrowseUseCase.make[IO](
      clusters,
      serdes("<nothing fails>"),
      twoPartitionSource,
      CursorCodec.hmacSha256[IO](key),
      FilterSource.unsupported[IO],
      RecordMasking.none[IO]
    )
    val codec = CursorCodec.hmacSha256[IO](key)

    for {
      produced <- events(browse, request(2))
      token = produced.last match {
        case BrowseEvent.Finished(_, Some(cursor)) => cursor
        case other => fail(s"expected a cursor, got $other")
      }
      decoded <- codec.decode(token, (cluster, topic), Instant.EPOCH)
    } yield decoded match {
      case Right(cursor) =>
        // Partition 0 resumes after the last record actually delivered from it; the starved partition
        // resumes from the same offset it was never read past, rather than disappearing from the cursor.
        assertEquals(
          cursor.perPartitionNext,
          Map(PartitionId.unsafe(0) -> Offset.unsafe(2L), starved -> Offset.unsafe(0L))
        )
      case Left(error) => fail(s"the cursor this build minted could not be read back: ${error.message}")
    }
  }

  // --------------------------------------------------------------------------------- the filtering

  test("the string filter matches decoded text, and the accounting shows what it rejected") {
    val records = List(raw(0, "keep me"), raw(1, "drop"), raw(2, "keep this too")).map(_.asRight[KuiError])

    events(useCase(records), request(10, Some("KEEP"))).map { produced =>
      // Case-insensitive, and against the decoded text rather than the raw bytes: a user searching for
      // what they can see on the screen is searching the text, not the payload's encoding.
      assertEquals(delivered(produced), List("keep me", "keep this too"))

      val consumed = produced.collect { case c: BrowseEvent.Consumed => c }.last
      // Three read, two delivered. The gap is the number that tells a user their filter is working, and
      // it is the only thing on the stream that would ever say so.
      assertEquals((consumed.read, consumed.delivered), (3L, 2L))
    }
  }

  test("the string filter is case-insensitive independently of the host locale") {
    val records = List(raw(0, "invoice")).map(_.asRight[KuiError])

    IO(Locale.getDefault)
      .bracket { _ =>
        IO(Locale.setDefault(Locale.forLanguageTag("tr-TR"))) *>
          events(useCase(records), request(10, Some("I")))
      }(previous => IO(Locale.setDefault(previous)))
      .map { produced =>
        assertEquals(delivered(produced), List("invoice"))
      }
  }

  test("progress elapsed time uses the same origin as terminal elapsed time") {
    val records = List.tabulate(BrowseUseCase.ProgressEvery)(offset => raw(offset.toLong, "value"))
    for {
      calls <- Ref.of[IO, Int](0)
      clock = new CountingClock(calls)
      browse = BrowseUseCase.make[IO](
        clusters,
        serdes("<nothing fails>"),
        source(records.map(_.asRight[KuiError])),
        CursorCodec.hmacSha256[IO](key),
        FilterSource.unsupported[IO],
        RecordMasking.none[IO]
      )(using summon[Concurrent[IO]], clock)
      produced <- events(browse, request(BrowseUseCase.ProgressEvery))
    } yield assertEquals(
      produced.collect { case c: BrowseEvent.Consumed => c.elapsed },
      List(1.second, 2.seconds)
    )
  }

  test("a browse reads the monotonic clock only when it reports elapsed time") {
    val records =
      List.tabulate(BrowseUseCase.ProgressEvery - 1)(offset => raw(offset.toLong, s"value-$offset"))

    for {
      calls <- Ref.of[IO, Int](0)
      clock = new CountingClock(calls)
      browse = BrowseUseCase.make[IO](
        clusters,
        serdes("<nothing fails>"),
        source(records.map(_.asRight[KuiError])),
        CursorCodec.hmacSha256[IO](key),
        FilterSource.unsupported[IO],
        RecordMasking.none[IO]
      )(using summon[Concurrent[IO]], clock)
      _ <- events(browse, request(BrowseUseCase.ProgressEvery))
      observed <- calls.get
    } yield assertEquals(observed, 2, "only the browse start and terminal accounting need the clock")
  }

  // ---------------------------------------------------------------------------- an unknown cluster

  test("a cluster nobody configured fails before the record source is touched") {
    val untouchable: RecordSource[IO] = new RecordSource[IO] {
      def browse(request: BrowseRequest, budget: PollBudget): Stream[IO, Either[KuiError, RawRecord]] =
        Stream.raiseError[IO](new IllegalStateException("the record source must not be reached"))

      def assignedStarts(request: BrowseRequest): IO[Either[KuiError, Map[PartitionId, Offset]]] =
        IO.raiseError(new IllegalStateException("the record source must not be reached"))
    }

    val browse = BrowseUseCase.make[IO](
      clusters,
      serdes("<nothing fails>"),
      untouchable,
      CursorCodec.hmacSha256[IO](key),
      FilterSource.unsupported[IO],
      RecordMasking.none[IO]
    )

    events(browse, request(10, of = ClusterId.unsafe("nowhere"))).map { produced =>
      assert(delivered(produced).isEmpty)
      assert(produced.exists {
        case BrowseEvent.Failed(_) => true
        case _ => false
      })
    }
  }

  // ----------------------------------------------------------------------------------- the masking

  /** A `RecordMasking` that rewrites the value's text, and counts how often it was asked for a mask.
    *
    * The count is the interesting half. "Is the record masked" and "how often were the rules resolved" are
    * different questions, and only the second can distinguish a mask resolved once for the browse from one
    * resolved per record — which is the difference between `kui.masking.applied` meaning what
    * `MetricNames.MaskingApplied` says it means and it being a record counter wearing the wrong name.
    */
  private def maskingThat(
      resolutions: Ref[IO, Int],
      rewrite: String => String
  ): RecordMasking[IO] =
    (_, _) =>
      resolutions
        .update(_ + 1)
        .as(record => record.copy(value = record.value.copy(text = rewrite(record.value.text))))

  test("every delivered record carries the mask, and the browse resolves it once and not once per record") {
    // DM-001, ADR-023: masking runs after deserialization and before any DTO leaves the service. The
    // browse is where "after deserialization" happens, so this is the assertion that the product applies
    // the rule at all -- every other masking case in this repository would stay green with the mask
    // resolved and then dropped on the floor.
    val records = List(raw(0, "4111"), raw(1, "4222"), raw(2, "4333")).map(_.asRight[KuiError])

    for {
      resolutions <- Ref.of[IO, Int](0)
      produced <- events(
        useCase(records, masking = maskingThat(resolutions, _ => "****")),
        request(10)
      )
      resolved <- resolutions.get
    } yield {
      assertEquals(delivered(produced), List("****", "****", "****"))
      assertEquals(resolved, 1)
    }
  }

  test("the string filter reads the masked text, so a masked value cannot be searched for") {
    // THE ORACLE, AND IT IS THE REASON THE MASK IS APPLIED AT THE DECODE RATHER THAN AT THE API LAYER.
    // A filter running on the unmasked text answers questions about the hidden value: `4111` returning
    // one row tells the reader the card number without ever drawing it. The cost is that a masked field
    // is not searchable, which is the honest consequence of hiding it -- `MaskingRule` says masking
    // "hides a field from every reader equally", and a reader's filter is that reader.
    val records = List(raw(0, "4111"), raw(1, "9999")).map(_.asRight[KuiError])

    for {
      resolutions <- Ref.of[IO, Int](0)
      browse = useCase(records, masking = maskingThat(resolutions, _ => "****"))
      byOriginal <- events(browse, request(10, Some("4111")))
      byMask <- events(browse, request(10, Some("****")))
    } yield {
      assert(delivered(byOriginal).isEmpty, clue = delivered(byOriginal))
      assertEquals(delivered(byMask), List("****", "****"))
    }
  }

  test("the smart filter is handed the masked record too, for the same reason") {
    val records = List(raw(0, "4111"), raw(1, "9999")).map(_.asRight[KuiError])
    val keepsUnmasked = filterOver(record =>
      if record.value.text == "4111" then FilterVerdict.Matched else FilterVerdict.DidNotMatch
    )

    for {
      resolutions <- Ref.of[IO, Int](0)
      produced <- events(
        useCase(records, filters = keepsUnmasked, masking = maskingThat(resolutions, _ => "****")),
        filtered(10)
      )
    } yield assert(delivered(produced).isEmpty, clue = delivered(produced))
  }

  /** A `RecordMasking` that READS both of its arguments: it masks the one cluster and topic it was scoped to,
    * records every pair it was asked about, and leaves everything else alone.
    *
    * `maskingThat` above discards both — `(_, _) => …` — and every other masking fake in this service does
    * the same, which is why the argument hand-off was ungated: `topicKeysPattern` and `topicValuesPattern`
    * exist to decide WHICH rules reach a read, and that decision is made from exactly this pair. A fake that
    * ignores the pair cannot tell a browse that asked for its own topic from one that asked for a topic it is
    * not reading, so a browse that resolves the mask for the wrong topic keeps working and masks nothing.
    * Filed as W9-06/F1.
    */
  private def maskingScopedTo(
      onlyCluster: ClusterId,
      onlyTopic: TopicName,
      asked: Ref[IO, List[(ClusterId, TopicName)]]
  ): RecordMasking[IO] =
    (of, forTopic) =>
      asked
        .update(_ :+ (of, forTopic))
        .as(
          if of == onlyCluster && forTopic == onlyTopic then
            record => record.copy(value = record.value.copy(text = "****"))
          else RecordMask.identity
        )

  test("the mask is resolved for the cluster and topic this browse is reading, and for no other") {
    // A RULE SCOPED BY TOPIC PATTERN APPLIES TO THE TOPIC BEING READ, OR IT APPLIES TO NOTHING. Asking
    // the port for a topic other than `request.topic` is a one-word edit that turns every
    // `topicKeysPattern`/`topicValuesPattern` rule in a deployment off while the browse goes on
    // delivering records: the scope is matched against a name nobody is reading, no rule matches, and
    // the identity mask comes back. Both halves are asserted here — the pair the product asked about,
    // and the text that came out — because either alone can be satisfied by a fake that discards its
    // arguments.
    val records = List(raw(0, "4111"), raw(1, "4222")).map(_.asRight[KuiError])

    for {
      asked <- Ref.of[IO, List[(ClusterId, TopicName)]](Nil)
      produced <- events(
        useCase(records, masking = maskingScopedTo(cluster, topic, asked)),
        request(10)
      )
      pairs <- asked.get
    } yield {
      assertEquals(pairs, List((cluster, topic)))
      assertEquals(delivered(produced), List("****", "****"))
    }
  }

  test("a browse that fails on the cluster never resolves a mask") {
    // Resolving the mask is where `kui.masking.applied` is written, so resolving one for a browse that
    // could not start would publish a masking series for a read that never read anything.
    for {
      resolutions <- Ref.of[IO, Int](0)
      _ <- events(
        useCase(Nil, masking = maskingThat(resolutions, identity)),
        request(10, of = ClusterId.unsafe("nowhere"))
      )
      resolved <- resolutions.get
    } yield assertEquals(resolved, 0)
  }
}
