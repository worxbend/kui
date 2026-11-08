package kui.message.api

import java.time.Instant

import scala.concurrent.duration.FiniteDuration

import io.circe.Json
import munit.FunSuite

import kui.contracts.message.{DecodeErrorDto, DecodedPayloadDto}
import kui.contracts.sse.DoneReason
import kui.http.sse.{SseConfig, SseOverflow}
import kui.kernel.browse.PollBudget
import kui.kernel.serde.{PayloadKind, SerdeName, Target}
import kui.kernel.{Offset, PartitionId}
import kui.message.application.{BrowseEnd, BrowseEvent}
import kui.message.contract.MessageDto
import kui.message.domain.{DecodeError, Decoded, DecodedRecord, RenderedHeader, TimestampType}

/** The field-level rules `MessageMapping` argues for at length, and which nothing asserted.
  *
  * `services/message/api` shipped one suite — `ServiceRbacGuardSuite` — so every rule in this file was
  * reachable only through a route that asserts a status code, which is not what a route suite looks at. Five
  * mutations were applied one at a time against `./mill services.message.__.test`, each leaving it at 183/183
  * green: making `payload`'s absent branch unreachable, dropping `deserializeErrors` to `Nil`, zeroing
  * `filterErrors`, removing the floor under a spent budget, and mapping `NoTimestamp` to `LOG_APPEND_TIME`.
  *
  * The budget and filter cases read the encoder's own output — the frame `MessageMapping.event` builds —
  * rather than a hand-written literal, because a hand-written literal on each side of a wire is what put two
  * of the metrics service's four cards on screen drawing nothing.
  */
final class MessageMappingSuite extends FunSuite {

  private val At: Instant = Instant.parse("2026-09-06T10:00:00Z")

  private val serde: SerdeName = SerdeName.unsafe("string")

  private def record(
      value: Decoded,
      valueSize: Int,
      errors: List[DecodeError] = Nil,
      stamped: TimestampType = TimestampType.CreateTime,
      headers: List[RenderedHeader] = Nil,
      keySize: Int = 0,
      headersSize: Int = 0
  ): DecodedRecord =
    DecodedRecord(
      partition = PartitionId.unsafe(0),
      offset = Offset.unsafe(41L),
      timestamp = At,
      timestampType = stamped,
      key = Decoded.absent(serde),
      value = value,
      headers = headers,
      keySize = keySize,
      valueSize = valueSize,
      headersSize = headersSize,
      decodeErrors = errors
    )

  test("a tombstone's absent value is null on the wire and an empty string is not") {
    // One byte apart on the wire and opposite in meaning: a tombstone deletes the key at compaction, and
    // an empty value is a record somebody deliberately wrote.
    val tombstone = MessageMapping.payload(Decoded.absent(serde), 0)
    val empty = MessageMapping.payload(Decoded("", PayloadKind.Text, serde, Map.empty), 4)

    assertEquals(tombstone.kind, DecodedPayloadDto.Kind.Absent)
    assertEquals(empty.kind, DecodedPayloadDto.Kind.Text)
  }

  test("a payload that decoded carries its text, its kind and the serde that read it") {
    val decoded = MessageMapping.payload(Decoded("""{"a":1}""", PayloadKind.Json, serde, Map.empty), 7)

    assertEquals(decoded.text, """{"a":1}""")
    assertEquals(decoded.kind, DecodedPayloadDto.Kind.Json)
    assertEquals(decoded.serde, serde.value)
  }

  test("a record the serde could not read reaches the wire carrying the failure") {
    val failed = DecodeError(Target.Value, serde, "not valid Avro")
    val dto = MessageMapping.message(record(Decoded.absent(serde), 12, errors = List(failed)))

    // Dropping this list turns "the producer wrote something this serde cannot read" into "this record
    // decoded perfectly", which is the sentence the field exists to prevent.
    assertEquals(dto.deserializeErrors.map(_.cause), List("not valid Avro"))
    assertEquals(dto.deserializeErrors.map(_.target), List(DecodeErrorDto.Target.Value))
  }

  test("a record that decoded cleanly carries an empty error list, which is a different document") {
    val clean = MessageMapping.message(record(Decoded("hi", PayloadKind.Text, serde, Map.empty), 2))

    assertEquals(clean.deserializeErrors, Nil)
  }

  test("a filter that threw on every record says so, and is not a topic that matched nothing") {
    val body = frameOf(
      BrowseEvent.Consumed(
        bytes = 4096L,
        read = 500L,
        delivered = 0L,
        filterErrors = 500L,
        elapsed = FiniteDuration(1200L, "millis"),
        budget = PollBudget.unsafe(1000, 1024L * 1024L, FiniteDuration(30, "seconds"))
      )
    )

    // The pair that makes the sentence: five hundred records read, none delivered, and every one a throw.
    assertEquals(body.hcursor.get[Long]("records"), Right(500L))
    assertEquals(body.hcursor.get[Long]("filterErrors"), Right(500L))
  }

  test("a budget spent past its own size is reported as nothing left, never as a negative") {
    // `recordsLeft` is drawn as remaining work. A negative one renders as a bar running backwards and as
    // "-150 records left", which is not a state a browse can be in.
    val budget = frameOf(
      BrowseEvent.Consumed(
        bytes = 2048L,
        read = 250L,
        delivered = 250L,
        filterErrors = 0L,
        elapsed = FiniteDuration(90L, "seconds"),
        budget = PollBudget.unsafe(100, 1024L, FiniteDuration(30, "seconds"))
      )
    ).hcursor.downField("budget")

    assertEquals(budget.get[Int]("recordsLeft"), Right(0))
    assertEquals(budget.get[Long]("bytesLeft"), Right(0L))
    assertEquals(budget.get[Long]("millisLeft"), Right(0L))
  }

  test("a raw scan budget ending is distinct from an exhausted topic on the wire") {
    assertEquals(MessageMapping.doneReason(BrowseEnd.Budget), DoneReason.Budget)
  }

  test("a record whose producer stamped no timestamp is reported as create-time") {
    // Kafka's `-1` has already become an instant by the time it reaches here, so a third spelling would be
    // a value every client has to learn for no gain — but it has to be create-time and not the other one,
    // because log-append time is a claim about when the broker wrote the record.
    val unstamped = record(Decoded.absent(serde), 0, stamped = TimestampType.NoTimestamp)

    assertEquals(MessageMapping.message(unstamped).timestampType, MessageDto.TimestampType.CreateTime)
  }

  test("a record's headers reach the wire, because a header is how a producer says what a record is") {
    /*
     * Ungated until now: `headers = Map.empty[String, String]` in `MessageMapping.message` left
     * `./mill services.message.__.test` at 204/204 green. Headers are how the Spring, CloudEvents and
     * dead-letter conventions carry a record's type, its origin and the reason it was rejected, so a
     * browse that quietly dropped them would show a screen of payloads nobody could place.
     */
    val dto = MessageMapping.message(
      record(
        Decoded("hi", PayloadKind.Text, serde, Map.empty),
        2,
        headers = List(RenderedHeader("__TypeId__", "com.example.Order"), RenderedHeader("attempt", "3"))
      )
    )

    assertEquals(dto.headers, Map("__TypeId__" -> "com.example.Order", "attempt" -> "3"))
  }

  test("the three sizes are the key's, the value's and the headers' own, and not each other's") {
    /*
     * Ungated until now: swapping `keySize` and `valueSize` in `MessageMapping.message` left the suite at
     * 204/204. These are the figures the size column is sorted on when an operator is looking for the
     * record that is filling a partition, and three fields of the same type are the easiest thing in this
     * file to transpose.
     */
    val dto = MessageMapping.message(
      record(Decoded("hi", PayloadKind.Text, serde, Map.empty), 2048, keySize = 16, headersSize = 64)
    )

    assertEquals(dto.keySize, 16)
    assertEquals(dto.valueSize, 2048)
    assertEquals(dto.headersSize, 64)
    // The domain's own arithmetic, which the browse's byte budget is spent in.
    assertEquals(dto.keySize.toLong + dto.valueSize.toLong + dto.headersSize.toLong, 2128L)
  }

  test("every browse frame carries its own event name, because the browser dispatches on the name") {
    /*
     * Ungated until now: rendering a `Phase` under `EventNames.Message` left the suite at 204/204, because
     * every case here read `frame.data` and none read `frame.name`. An SSE client subscribes by name, so a
     * phase announcement arriving as `event: message` is decoded as a record and the browse's progress is
     * never drawn -- with the payload perfectly valid on both sides.
     */
    val phase = frameFor(BrowseEvent.Phase("seeking"))
    val message =
      frameFor(BrowseEvent.Record(record(Decoded("hi", PayloadKind.Text, serde, Map.empty), 2)))
    val consumed = frameFor(
      BrowseEvent.Consumed(
        bytes = 1L,
        read = 1L,
        delivered = 1L,
        filterErrors = 0L,
        elapsed = FiniteDuration(1L, "millis"),
        budget = PollBudget.unsafe(10, 10L, FiniteDuration(1, "seconds"))
      )
    )

    assertEquals(phase.name, MessageMapping.EventNames.Phase)
    assertEquals(message.name, MessageMapping.EventNames.Message)
    assertEquals(consumed.name, MessageMapping.EventNames.Consumed)
    // Three frames, three names: the set is what a client's three handlers bind to.
    assertEquals(Set(phase.name, message.name, consumed.name).size, 3)
  }

  test("a bounded browse blocks on a slow reader and a live tail does not") {
    val base = SseConfig.default.copy(bufferSize = 8)
    assertEquals(MessageRoutes.streamConfig(live = false, base).overflow, SseOverflow.Block)
    assertEquals(MessageRoutes.streamConfig(live = true, base).overflow, SseOverflow.DropOldest)
    assertEquals(MessageRoutes.streamConfig(live = false, base).bufferSize, 8)
  }

  /** The frame the browser actually receives, so these assertions read the encoder's own output. */
  private def frameFor(event: BrowseEvent): kui.http.sse.SseEvent =
    MessageMapping.event(event) match {
      case Some(frame) => frame
      case None => fail("this event must render a frame")
    }

  /** The frame the browser actually receives, so these assertions read the encoder's own output. */
  private def frameOf(event: BrowseEvent): Json =
    MessageMapping.event(event) match {
      case Some(frame) => frame.data
      case None => fail("this event must render a frame")
    }
}
