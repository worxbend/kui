package kui.serde.builtin

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.UUID

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.scalacheck.Prop.forAll
import org.scalacheck.{Arbitrary, Gen}

import kui.kernel.TopicName
import kui.serde.*
import kui.testkit.KuiSuite

/** The eight built-in formats, and the rule that binds them: a serde says `Left` rather than rendering
  * something that is nearly right.
  */
final class BuiltinSerdesSuite extends KuiSuite {

  private val topic: TopicName = TopicName.unsafe("orders")

  private def decode(serde: Serde[IO], bytes: Array[Byte]): Either[DeserializeFailure, DeserializeResult] =
    serde.deserializer(topic, Target.Value).flatMap(_.deserialize(Nil, bytes)).unsafeRunSync()

  private def encode(serde: Serde[IO], text: String): Either[SerializeFailure, Array[Byte]] =
    serde.serializer(topic, Target.Value, Map.empty).flatMap(_.serialize(text, Nil)).unsafeRunSync()

  private def roundTrip(serde: Serde[IO], text: String): Either[String, String] =
    encode(serde, text).left
      .map(_.cause)
      .flatMap(bytes => decode(serde, bytes).left.map(_.cause).map(_.text))

  private def int32Bytes(value: Int): Array[Byte] = ByteBuffer.allocate(4).putInt(value).array()
  private def int64Bytes(value: Long): Array[Byte] = ByteBuffer.allocate(8).putLong(value).array()

  // ---------------------------------------------------------------- round trips

  private val cases: List[(Serde[IO], Gen[String])] = List(
    NumberSerdes.int32[IO] -> Arbitrary.arbitrary[Int].map(_.toString),
    NumberSerdes.int64[IO] -> Arbitrary.arbitrary[Long].map(_.toString),
    NumberSerdes.uint32[IO] -> Arbitrary.arbitrary[Int].map(java.lang.Integer.toUnsignedString),
    NumberSerdes.uint64[IO] -> Arbitrary.arbitrary[Long].map(java.lang.Long.toUnsignedString),
    UuidSerde[IO] -> Gen.uuid.map(_.toString),
    JsonSerde[IO] -> Gen.const("""{"a":1,"b":["x"]}"""),
    StringSerde[IO] -> Arbitrary.arbitrary[String],
    Base64Serde[IO] -> Gen
      .listOf(Arbitrary.arbitrary[Byte])
      .map(bs => java.util.Base64.getEncoder.encodeToString(bs.toArray)),
    HexSerde[IO] -> Gen.listOf(Arbitrary.arbitrary[Byte]).map(bs => HexSerde.toHex(bs.toArray))
  )

  cases.foreach { (serde, valid) =>
    property(s"${serde.name.value} round-trips every value it accepts") {
      forAll(valid)(text => roundTrip(serde, text) == Right(text))
    }
  }

  // ---------------------------------------------------------------- the numbers

  private val numbers: List[(Serde[IO], Int)] = List(
    NumberSerdes.int32[IO] -> 4,
    NumberSerdes.uint32[IO] -> 4,
    NumberSerdes.int64[IO] -> 8,
    NumberSerdes.uint64[IO] -> 8
  )

  numbers.foreach { (serde, width) =>
    property(s"${serde.name.value} refuses any length but $width rather than truncating") {
      forAll(Gen.choose(0, 32).suchThat(_ != width)) { length =>
        decode(serde, Array.fill(length)(0.toByte)).isLeft
      }
    }
  }

  test("a four-byte serde handed eight bytes fails rather than reading half a number") {
    // The failure is the feature. Reading the first four bytes would render a plausible value that is
    // simply wrong, with nothing on screen to say so.
    assertEquals(decode(NumberSerdes.int32[IO], Array.fill(8)(0.toByte)).isLeft, true)
  }

  test("uint64 renders values above Long.MaxValue as themselves, not as negative numbers") {
    val aboveSignedMax = int64Bytes(-1L) // 0xFFFF… — the largest unsigned 64-bit value
    assertEquals(decode(NumberSerdes.uint64[IO], aboveSignedMax).map(_.text), Right("18446744073709551615"))
    assertEquals(decode(NumberSerdes.int64[IO], aboveSignedMax).map(_.text), Right("-1"))
  }

  test("uint32 does the same at 32 bits") {
    assertEquals(decode(NumberSerdes.uint32[IO], int32Bytes(-1)).map(_.text), Right("4294967295"))
  }

  test("a number that does not fit its width is a serialize failure naming the width") {
    val failed = encode(NumberSerdes.int32[IO], "99999999999")
    assert(failed.swap.exists(_.cause.contains("32-bit")), failed.toString)
  }

  // ---------------------------------------------------------------- JSON

  test("the JSON serde marks its result as JSON and sends it compact") {
    val result = decode(JsonSerde[IO], """{ "a" :  1 }""".getBytes(StandardCharsets.UTF_8))
    assertEquals(result.map(_.kind), Right(PayloadKind.Json))
    // Compact, not pretty: pretty-printing on the server triples the bytes on the wire, and the browser
    // re-formats for display anyway because only the browser knows how wide the window is.
    assertEquals(result.map(_.text), Right("""{"a":1}"""))
  }

  test("the JSON serde refuses a bare number and a bare string, which are valid JSON but not documents") {
    assert(decode(JsonSerde[IO], "123".getBytes).isLeft)
    assert(decode(JsonSerde[IO], "\"hello\"".getBytes).isLeft)
    assert(decode(JsonSerde[IO], "not json at all".getBytes).isLeft)
  }

  test("the JSON serde accepts an array as well as an object") {
    assertEquals(decode(JsonSerde[IO], "[1,2]".getBytes).map(_.kind), Right(PayloadKind.Json))
  }

  test("producing a bare number to a JSON topic is refused, so KUI can always read back what it sent") {
    assert(encode(JsonSerde[IO], "123").isLeft)
  }

  // ---------------------------------------------------------------- JSON nesting depth

  /** `{"a":` `depth` times, `0`, then the braces: valid JSON, `depth` levels deep, about five bytes a level.
    */
  private def nestedObjects(depth: Int): String = ("""{"a":""" * depth) + "0" + ("}" * depth)

  private def nestedArrays(depth: Int): String = ("[" * depth) + ("]" * depth)

  /** The JSON serde over `Id`, so that decoding runs on the calling thread instead of a runtime's pool. */
  private val syncJson: Serde[cats.Id] = JsonSerde[cats.Id]

  private def decodeSync(text: String): Either[DeserializeFailure, DeserializeResult] =
    syncJson.deserializer(topic, Target.Value).deserialize(Nil, text.getBytes(StandardCharsets.UTF_8))

  /** Runs `body` on a fresh thread with a 256 KiB stack and returns whatever it threw, fatal errors included.
    *
    * The small stack is what makes the overflow deterministic. Whether printing three thousand levels fits in
    * the default 1 MiB depends on how far the JIT has compiled circe's printer — it overflows on a cold JVM
    * and fits on a warm one — and a regression test whose outcome depends on JIT state proves nothing.
    */
  private def onSmallStack[A](body: => A): Either[Throwable, A] = {
    val outcome = new java.util.concurrent.atomic.AtomicReference[Either[Throwable, A]]()
    val thread = new Thread(
      null,
      () =>
        outcome.set(try Right(body)
        catch { case thrown: Throwable => Left(thrown) }),
      "json-nesting-small-stack",
      256L * 1024L
    )
    thread.start()
    thread.join()
    outcome.get()
  }

  test("a JSON document nested thousands of levels deep is refused rather than overflowing the stack") {
    // A StackOverflowError is fatal to Cats Effect: escaping a decode it halts the whole process, and the
    // record halts it again on every later read. Review R2 finding h-serde-untrusted-decode-1.
    List(nestedObjects(3000), nestedArrays(3500)).foreach { document =>
      onSmallStack(decodeSync(document)) match {
        case Left(thrown) => fail(s"decoding threw $thrown instead of returning a Left")
        case Right(Right(result)) => fail(s"decoded ${result.text.length} characters as JSON")
        case Right(Left(failure)) => assert(failure.cause.contains("256 levels"), failure.cause)
      }
    }
  }

  test("a document deeper than the limit is not claimed by auto-detection, nor produced to a topic") {
    val deep = nestedObjects(3000)
    JsonSerde[IO] match {
      case detector: SampleDetector => assert(!detector.claims(deep.getBytes(StandardCharsets.UTF_8)))
      case other => fail(s"${other.name.value} is not a sample detector")
    }
    onSmallStack(syncJson.serializer(topic, Target.Value, Map.empty).serialize(deep, Nil)) match {
      case Left(thrown) => fail(s"encoding threw $thrown instead of returning a Left")
      case Right(written) => assert(written.left.exists(_.cause.contains("256 levels")), written)
    }
  }

  test("a document exactly at the limit still decodes, compact, even on a small stack") {
    val document = nestedObjects(kui.kernel.serde.JsonNesting.MaxDepth)
    assertEquals(onSmallStack(decodeSync(document)).map(_.map(_.text)), Right(Right(document)))
  }

  // The refusal a person reads. The publish drawer shows `cause` verbatim beside the field, and it used to
  // show circe's own words — `expected null got 'not js...' (line 1, column 1)`, which describes one branch
  // the parser tried and reads as though KUI had wanted `null`. These four cases pin the sentences instead.

  private def refusal(text: String): String =
    encode(JsonSerde[IO], text).fold(_.cause, bytes => fail(s"expected a refusal, got ${bytes.length} bytes"))

  test("plain text typed into a JSON field is refused in words, and names the serde to use instead") {
    val cause = refusal("not json at all")
    assert(cause.contains("has to start with `{` or `[`"), cause)
    assert(cause.contains("starts with `n`"), cause)
    assert(cause.contains("choose the String serde instead"), cause)
    assert(!cause.contains("expected null"), s"the parser's own wording leaked through: $cause")
  }

  test("JSON that starts right and breaks inside keeps the parser's position and suggests what to look for") {
    val cause = refusal("""{"a": 1,}""")
    assert(cause.startsWith("this starts like JSON but does not parse"), cause)
    assert(cause.contains("line 1, column"), cause)
    assert(cause.contains("comma left before"), cause)
  }

  test("an empty payload says it is empty rather than reporting a parse error at column 1") {
    assert(refusal("   ").contains("nothing here to send"), refusal("   "))
  }

  test("a bare value is refused with what this serde accepts and where else to send it") {
    val cause = refusal("123")
    assert(cause.contains("only an object"), cause)
    assert(cause.contains("choose a different serde"), cause)
  }

  // ---------------------------------------------------------------- UUID

  test("sixteen bytes with no RFC 4122 version and variant are refused") {
    val notAUuid = Array.fill(16)(0x7f.toByte)
    assert(decode(UuidSerde[IO], notAUuid).isLeft, "0x7f repeated has version 7 but variant 01, not 10")
  }

  test("the nil UUID is admitted explicitly: it is legal and fails both structural checks") {
    assertEquals(
      decode(UuidSerde[IO], Array.fill(16)(0.toByte)).map(_.text),
      Right("00000000-0000-0000-0000-000000000000")
    )
  }

  property("every random UUID decodes to its own canonical form") {
    forAll(Gen.uuid) { (uuid: UUID) =>
      val bytes = ByteBuffer
        .allocate(16)
        .putLong(uuid.getMostSignificantBits)
        .putLong(uuid.getLeastSignificantBits)
        .array()
      decode(UuidSerde[IO], bytes).map(_.text) == Right(uuid.toString)
    }
  }

  // ---------------------------------------------------------------- hex and base64

  property("hex is lowercase, unseparated and exactly two characters per byte") {
    forAll { (bytes: Array[Byte]) =>
      val text = decode(HexSerde[IO], bytes).map(_.text)
      text.exists(t =>
        t.length == bytes.length * 2 && t == t.toLowerCase && t.forall("0123456789abcdef".contains(_))
      )
    }
  }

  property("hex parses either case back to the same bytes") {
    forAll { (bytes: Array[Byte]) =>
      val hex = HexSerde.toHex(bytes)
      encode(HexSerde[IO], hex.toUpperCase).map(_.toSeq) == Right(bytes.toSeq)
    }
  }

  test("hex refuses an odd number of characters and a non-hex character") {
    assert(encode(HexSerde[IO], "abc").isLeft)
    assert(encode(HexSerde[IO], "zz").isLeft)
  }

  property("base64 is standard and padded, and parses back to the same bytes") {
    forAll { (bytes: Array[Byte]) =>
      val text = decode(Base64Serde[IO], bytes).map(_.text)
      text.exists(t => t == java.util.Base64.getEncoder.encodeToString(bytes)) &&
      text.flatMap(t => encode(Base64Serde[IO], t).left.map(_.cause)).map(_.toSeq) == Right(bytes.toSeq)
    }
  }

  // ---------------------------------------------------------------- the String serde

  test("the String serde refuses bytes that are not valid UTF-8, which is what the fallback is for") {
    assert(decode(StringSerde[IO], Array[Byte](0xc3.toByte, 0x28.toByte)).isLeft)
  }

  // ---------------------------------------------------------------- everything, together

  property("no serde throws on any input: every decode returns a decision") {
    forAll(Gen.oneOf(BuiltinSerdes.all[IO]), Arbitrary.arbitrary[Array[Byte]]) { (serde, bytes) =>
      serde
        .deserializer(topic, Target.Value)
        .flatMap(_.deserialize(Nil, bytes))
        .attempt
        .unsafeRunSync()
        .isRight
    }
  }

  test("the empty payload is handled by every serde: a decision, never a crash") {
    BuiltinSerdes.all[IO].foreach { serde =>
      val outcome = serde
        .deserializer(topic, Target.Value)
        .flatMap(_.deserialize(Nil, Array.emptyByteArray))
        .attempt
        .unsafeRunSync()
      assert(outcome.isRight, clue = serde.name.value)
    }
  }

  test("every built-in describes itself, and the list of names matches the list of serdes") {
    assertEquals(BuiltinSerdes.all[IO].map(_.name), BuiltinSerdes.names)
    BuiltinSerdes.all[IO].foreach { serde =>
      assertEquals(serde.describe.name, serde.name)
      assert(serde.describe.description.length > 40, clue = serde.name.value)
    }
  }

  test("the fallback is not one of the built-ins: it is where resolution ends, not a candidate in it") {
    assert(!BuiltinSerdes.names.contains(SerdeName.Fallback))
  }
}
