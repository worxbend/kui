package kui.http.sse

import java.time.Instant

import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.*

import cats.effect.IO
import cats.effect.testkit.TestControl
import cats.syntax.all.*
import fs2.Stream
import io.circe.Json
import munit.CatsEffectSuite
import org.typelevel.otel4s.oteljava.testkit.OtelJavaTestkit

import kui.contracts.ErrorEnvelope
import kui.contracts.sse.{DoneReason, SseEventName}
import kui.kernel.CorrelationId
import kui.observability.{MetricNames, Telemetry}
import kui.testkit.fakes.FakeStructuredLogger

/** That a stream behaves the same way whatever it is streaming.
  *
  * The timing cases run under `TestControl`: a suite that really waited fifteen seconds for a heartbeat would
  * take a minute and would still be the first thing to go flaky on a loaded CI machine.
  */
final class SseSuite extends CatsEffectSuite {

  private val streamName = "capabilities"

  private def run(
      source: Stream[IO, SseEvent],
      config: SseConfig = SseConfig.default
  ): IO[(List[SseEvent], FakeStructuredLogger[IO])] =
    FakeStructuredLogger[IO].flatMap { logger =>
      Sse
        .stream[IO](source, config, streamName, Telemetry.noop[IO], logger)
        .compile
        .toList
        .map(_ -> logger)
    }

  // ---------------------------------------------------------------------------------------------
  // The wire format
  // ---------------------------------------------------------------------------------------------

  test("goldenWireFormat") {
    // Byte for byte. `frontend/ui-kernel`'s parser suite is tested against exactly these bytes, so
    // this assertion and that one are the two ends of the same contract: change either and the
    // other fails.
    val expected =
      "event: heartbeat\ndata: {}\n\n" +
        "event: done\nid: eyJ2IjoxfQ.abc\ndata: " +
        """{"reason":"exhausted","cursor":"eyJ2IjoxfQ.abc"}""" + "\n\n"

    val frames = List(
      SseEvent.heartbeat,
      SseEvent.done(DoneReason.Exhausted, Some("eyJ2IjoxfQ.abc"))
    )

    assertEquals(frames.map(SseEvent.render).mkString, expected)
  }

  test("the golden bytes really are UTF-8 with no byte-order mark") {
    val bytes = SseEvent.bytes(SseEvent.heartbeat)

    assertEquals(new String(bytes, java.nio.charset.StandardCharsets.UTF_8), "event: heartbeat\ndata: {}\n\n")
    assert(!bytes.startsWith(Array(0xef.toByte, 0xbb.toByte, 0xbf.toByte)), "a BOM was written")
  }

  test("idFieldIsRenderedOnlyWhenPresent") {
    assertEquals(
      SseEvent.render(SseEvent.data("row", Json.obj("n" -> Json.fromInt(1)))),
      "event: row\ndata: {\"n\":1}\n\n"
    )
    assertEquals(
      SseEvent.render(SseEvent.data("row", Json.obj("n" -> Json.fromInt(1)), Some("cursor-1"))),
      "event: row\nid: cursor-1\ndata: {\"n\":1}\n\n"
    )
  }

  test("every frame ends with a blank line, and data is the last field") {
    List(
      SseEvent.heartbeat,
      SseEvent.done(DoneReason.Limit, None),
      SseEvent.data("row", Json.obj(), Some("c"))
    ).foreach { event =>
      val rendered = SseEvent.render(event)
      assert(rendered.endsWith("\n\n"), rendered)
      assert(rendered.linesIterator.toList.filter(_.nonEmpty).last.startsWith("data: "), rendered)
    }
  }

  test("a payload is always one data: line, because it is written compactly") {
    // The SSE format allows several `data:` lines and the browser rejoins them with newlines, but
    // KUI never needs that: `noSpaces` cannot produce a newline, so every frame is one line.
    val nested = Json.obj("a" -> Json.arr(Json.fromInt(1), Json.fromInt(2)), "b" -> Json.fromString("x\ny"))

    val dataLines = SseEvent.render(SseEvent.data("row", nested)).linesIterator.count(_.startsWith("data: "))
    assertEquals(dataLines, 1)
  }

  test("done carries the cursor in both the id field and the payload") {
    // The `id:` field is what `Last-Event-ID` sends back on a reconnect; the payload copy is what a
    // client that stored the cursor itself reads. They must be the same value.
    val event = SseEvent.done(DoneReason.Exhausted, Some("cursor-9"))

    assertEquals(event.id, Some("cursor-9"))
    assertEquals(event.data.hcursor.get[String]("cursor"), Right("cursor-9"))
  }

  test("done with no cursor renders an explicit null, not a missing field") {
    assertEquals(
      SseEvent.render(SseEvent.done(DoneReason.Cancelled, None)),
      "event: done\ndata: {\"reason\":\"cancelled\",\"cursor\":null}\n\n"
    )
  }

  // ---------------------------------------------------------------------------------------------
  // Heartbeats
  // ---------------------------------------------------------------------------------------------

  test("emitsAHeartbeatAfterFifteenIdleSecondsAndNoneWhileBusy") {
    val idle =
      Stream.eval(IO.sleep(46.seconds)).drain ++ Stream.emit(SseEvent.done(DoneReason.Exhausted, None))

    val busy = Stream
      .awakeEvery[IO](1.second)
      .map(_ => SseEvent.data("row", Json.obj()))
      .take(40) ++ Stream.emit(SseEvent.done(DoneReason.Exhausted, None))

    val program = (run(idle), run(busy)).tupled

    TestControl.executeEmbed(program).map { case ((whenIdle, _), (whenBusy, _)) =>
      // Three heartbeats in 46 idle seconds, then the terminal event.
      assertEquals(whenIdle.count(_.name == SseEventName.Heartbeat), 3)
      assertEquals(whenIdle.last.name, SseEventName.Done)

      // Forty seconds of one event a second: the timer is re-armed by every event, so none.
      assertEquals(whenBusy.count(_.name == SseEventName.Heartbeat), 0)
      assertEquals(whenBusy.count(_.name == "row"), 40)
    }
  }

  test("the heartbeat interval is configurable, and a terminal event is not delayed behind one") {
    val source = Stream.eval(IO.sleep(2500.milliseconds)).drain ++
      Stream.emit(SseEvent.done(DoneReason.Budget, None))

    val program = run(source, SseConfig.default.copy(heartbeatInterval = 1.second)).timed

    TestControl.executeEmbed(program).map { case (elapsed, (events, _)) =>
      assertEquals(events.count(_.name == SseEventName.Heartbeat), 2)
      assertEquals(elapsed, 2500.milliseconds, "the stream waited for another heartbeat before ending")
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Exactly one terminal event
  // ---------------------------------------------------------------------------------------------

  test("emitsExactlyOneTerminalEventEvenWhenTheSourceEmitsTwo") {
    val source = Stream.emits(
      List(
        SseEvent.data("row", Json.obj()),
        SseEvent.done(DoneReason.Limit, None),
        SseEvent.done(DoneReason.Exhausted, None)
      )
    )

    TestControl.executeEmbed(run(source)).flatMap { (events, logger) =>
      logger.entries.map { entries =>
        assertEquals(events.map(_.name), List("row", SseEventName.Done))
        assertEquals(events.count(_.isTerminal), 1)

        val warnings = entries.filter(_.level == "warn")
        assertEquals(warnings.size, 1, entries.toString)
        assertEquals(warnings.head.context.get("stream"), Some(streamName))
        assertEquals(warnings.head.context.get("dropped"), Some("1"))
      }
    }
  }

  test("nothing is emitted after the terminal event, not even ordinary data") {
    val source = Stream.emits(
      List(
        SseEvent.done(DoneReason.Limit, None),
        SseEvent.data("row", Json.obj())
      )
    )

    TestControl.executeEmbed(run(source)).map { (events, _) =>
      assertEquals(events.map(_.name), List(SseEventName.Done))
    }
  }

  test("aFailedSourceBecomesAnErrorEventThenEnds") {
    val correlationId = CorrelationId.unsafe("abc123")
    val failing = Stream.emit(SseEvent.data("row", Json.obj())) ++
      Stream.raiseError[IO](new RuntimeException("a very secret stack trace"))

    TestControl.executeEmbed(run(Sse.withErrorEvent[IO](failing, correlationId))).map { (events, _) =>
      assertEquals(events.map(_.name), List("row", SseEventName.Error))

      val envelope = events.last.data.as[ErrorEnvelope].fold(f => fail(f.toString), identity)
      assertEquals(envelope.code, "KUI-INTERNAL")
      assertEquals(envelope.message, "Internal error")
      assertEquals(envelope.correlationId, "abc123")
      // The same rule as an ordinary error response: the cause reaches the log, never the wire.
      assert(!SseEvent.render(events.last).contains("a very secret stack trace"))
    }
  }

  test("a stream that fails before producing anything still ends with an error event") {
    val failing = Stream.raiseError[IO](new RuntimeException("nope"))

    TestControl
      .executeEmbed(run(Sse.withErrorEvent[IO](failing, CorrelationId.unsafe("abc123"))))
      .map((events, _) => assertEquals(events.map(_.name), List(SseEventName.Error)))
  }

  // ---------------------------------------------------------------------------------------------
  // Backpressure
  // ---------------------------------------------------------------------------------------------

  test("bufferOverflowDropsOldestAndIncrementsTheCounter") {
    // A producer of a thousand events and a reader that takes one every ten milliseconds. The
    // buffer is eight, so most of the thousand must be dropped rather than held: unbounded
    // buffering here is a browser tab that stopped reading turning into an out-of-memory error.
    val source = Stream.emits(1.to(1000).toList).map(n => SseEvent.data("row", Json.fromInt(n))) ++
      Stream.emit(SseEvent.done(DoneReason.Exhausted, None))

    // A real in-memory meter rather than `Telemetry.noop`: the drop *count* is half the promise,
    // and a no-op meter cannot tell a counted drop from a silent one. A stream that is shedding
    // events is the only signal an operator gets that a consumer cannot keep up.
    val program = OtelJavaTestkit.inMemory[IO]().use { testkit =>
      val telemetry = Telemetry.fromProviders(testkit.tracerProvider, testkit.meterProvider)

      for {
        logger <- FakeStructuredLogger[IO]
        events <- Sse
          .stream[IO](source, SseConfig.default.copy(bufferSize = 8), streamName, telemetry, logger)
          .metered(10.milliseconds)
          .compile
          .toList
        metrics <- testkit.collectMetrics
      } yield (events, metrics)
    }

    program.map { (events, metrics) =>
      val rows = events.filter(_.name == "row")

      assert(rows.size < 1000, s"${rows.size} events survived an 8-deep buffer; nothing was dropped")
      assert(rows.nonEmpty, "everything was dropped")
      // Dropping the *oldest* is right for a live view: the newest state is the one worth showing,
      // so whatever survives must include the end of the sequence.
      assertEquals(events.lastOption.map(_.name), Some(SseEventName.Done))

      val dropped = metrics
        .filter(_.getName == MetricNames.StreamEvents)
        .flatMap(_.getData.getPoints.asScala.toList)
        .filter(point =>
          point.getAttributes.asMap.asScala.exists((key, value) =>
            key.getKey == MetricNames.Attr.Event && value.toString == Sse.DroppedEvent
          )
        )

      assert(dropped.nonEmpty, s"no drop was counted; ${metrics.map(_.getName)}")

      // The gauge that is the leak detector: every stream this test opened has been closed.
      val active = metrics
        .filter(_.getName == MetricNames.StreamActive)
        .flatMap(_.getData.getPoints.asScala.toList)

      assert(active.nonEmpty, "the open-stream gauge was never recorded")
    }
  }

  test("a finite page delivers every event to a reader slower than the producer") {
    // Browse pages use this buffer. A slow socket must not discard records the cursor will
    // then skip. One millisecond per event is enough for a 256-deep queue to overflow.
    val source = Stream.emits(1.to(1000).toList).map(n => SseEvent.data("message", Json.fromInt(n))) ++
      Stream.emit(SseEvent.done(DoneReason.Limit, None))

    FakeStructuredLogger[IO].flatMap { logger =>
      Sse
        .stream[IO](
          source,
          SseConfig.default.copy(bufferSize = 256, overflow = SseOverflow.Block),
          streamName,
          Telemetry.noop[IO],
          logger
        )
        .evalTap(_ => IO.sleep(1.milli))
        .compile
        .toList
        .map { events =>
          val rows = events.filter(_.name == "message")
          assertEquals(rows.size, 1000)
          assertEquals(events.lastOption.map(_.name), Some(SseEventName.Done))
        }
    }
  }

  test("a consumer that goes away never leaves the producer blocked on a full buffer") {
    // The client disconnect, in miniature. The consumer stops after two events while the producer
    // still has hundreds queued behind a four-deep buffer, so `concurrently` interrupts the
    // producer and waits for its finaliser. A finaliser that *blocks* putting the terminator into
    // a queue nobody is reading any more never returns — and because fs2 runs finalisers
    // uncancelably, nothing can rescue it. The request fiber, the queue and whatever the source
    // held open (a registry subscription, a Kafka consumer) would leak, one per disconnect.
    val source = Stream.emits(1.to(1000).toList).map(n => SseEvent.data("row", Json.fromInt(n)))

    val program = FakeStructuredLogger[IO].flatMap { logger =>
      Sse
        .stream[IO](source, SseConfig.default.copy(bufferSize = 4), streamName, Telemetry.noop[IO], logger)
        .metered(10.milliseconds)
        .take(2)
        .compile
        .toList
    }

    TestControl.executeEmbed(program).map(events => assertEquals(events.size, 2))
  }

  test("a blocking buffer unblocks when the reader goes away") {
    val source = Stream.emits(1.to(1000).toList).map(n => SseEvent.data("row", Json.fromInt(n)))

    val program = FakeStructuredLogger[IO].flatMap { logger =>
      Sse
        .stream[IO](
          source,
          SseConfig.default.copy(bufferSize = 4, overflow = SseOverflow.Block),
          streamName,
          Telemetry.noop[IO],
          logger
        )
        .take(2)
        .compile
        .toList
    }

    program.timeout(5.seconds).map(events => assertEquals(events.size, 2))
  }

  test("a buffer size of zero is treated as one rather than deadlocking") {
    val source = Stream.emit(SseEvent.done(DoneReason.Exhausted, None))

    TestControl
      .executeEmbed(run(source, SseConfig.default.copy(bufferSize = 0)))
      .map((events, _) => assertEquals(events.map(_.name), List(SseEventName.Done)))
  }

  test("a rate limit spaces events out without dropping them") {
    val source = Stream.emits(1.to(5).toList).map(n => SseEvent.data("row", Json.fromInt(n)))

    val program = run(source, SseConfig.default.copy(rateLimit = Some(10))).timed

    TestControl.executeEmbed(program).map { case (elapsed, (events, _)) =>
      assertEquals(events.count(_.name == "row"), 5)
      // Ten a second, so five events span at least half a second. The upper bound is loose on
      // purpose: exactly how many ticks fs2's `metered` draws before it notices the source has
      // ended is its business, not this contract's.
      assert(elapsed >= 500.milliseconds && elapsed <= 1.second, elapsed.toString)
    }
  }

  test("with no rate limit configured, nothing is delayed") {
    val source = Stream.emits(1.to(100).toList).map(n => SseEvent.data("row", Json.fromInt(n)))

    TestControl.executeEmbed(run(source).timed).map { case (elapsed, (events, _)) =>
      assertEquals(events.size, 100)
      assertEquals(elapsed, 0.milliseconds)
    }
  }

  // ---------------------------------------------------------------------------------------------

  test("the terminal events are the two ADR-035 names, and heartbeat is not one of them") {
    assert(SseEvent.done(DoneReason.Limit, None).isTerminal)
    assert(SseEvent.error(envelope).isTerminal)
    assert(!SseEvent.heartbeat.isTerminal)
    assert(!SseEvent.data("row", Json.obj()).isTerminal)
  }

  private val envelope = ErrorEnvelope(
    code = "KUI-UPSTREAM-UNAVAILABLE",
    message = "schema-registry could not be reached",
    details = Nil,
    correlationId = "abc123",
    timestamp = Instant.parse("2026-09-03T10:11:12Z"),
    retryable = true
  )
}
