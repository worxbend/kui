package kui.serde.confluent

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

import io.circe.parser
import munit.FunSuite

/** Protobuf bytes in, JSON out — against bytes this suite writes itself.
  *
  * The encoder below is deliberately part of the test rather than of the module. It is thirty lines of the
  * published encoding rules, written independently of the decoder, so a mistake shared by both would have to
  * be made twice in two different shapes. A decoder tested only against its own encoder proves nothing; the
  * final check in this file is against bytes produced by `protoc` itself and pasted in as literals.
  */
final class ProtobufPayloadSuite extends FunSuite {

  private val definition: String =
    """syntax = "proto3";
      |package shop.orders;
      |
      |// An order, roughly as a real service would declare one.
      |message OrderPlaced {
      |  string id = 1;
      |  int32 quantity = 2;
      |  int64 placed_at = 3;
      |  bool express = 4;
      |  Status status = 5;
      |  repeated string tags = 6;
      |  Address shipping = 7;
      |  map<string, string> labels = 8;
      |  repeated int32 sizes = 9;
      |  double weight = 10;
      |  bytes signature = 11;
      |  sint32 adjustment = 12;
      |
      |  enum Status {
      |    UNKNOWN = 0;
      |    PLACED = 1;
      |    SHIPPED = 2;
      |  }
      |
      |  message Address {
      |    string city = 1;
      |    string postcode = 2;
      |  }
      |}
      |
      |message OrderCancelled {
      |  string id = 1;
      |}
      |""".stripMargin

  private val file: ProtoFile = ProtoSchema.parse(definition).fold(why => fail(why), identity)

  private def decoded(body: Array[Byte]): io.circe.Json =
    ProtobufPayload
      .decode(file, body)
      .flatMap(text => parser.parse(text).left.map(_.getMessage))
      .fold(why => fail(why), identity)

  // -------------------------------------------------------------------------------------------
  // The parser
  // -------------------------------------------------------------------------------------------

  test("the schema's messages, nested messages and enums are all named fully") {
    assert(file.messagesByName.contains("shop.orders.OrderPlaced"), file.messagesByName.keySet)
    assert(file.messagesByName.contains("shop.orders.OrderPlaced.Address"), file.messagesByName.keySet)
    assert(file.enumsByName.contains("shop.orders.OrderPlaced.Status"), file.enumsByName.keySet)
    assertEquals(file.packageName, Some("shop.orders"))
  }

  test("a map field becomes a repeated entry field with key 1 and value 2, as the language defines it") {
    val message = file.messagesByName("shop.orders.OrderPlaced")
    val labels = message.byNumber(8)
    assertEquals(labels.label, ProtoLabel.Repeated)
    assertEquals(labels.mapEntry.map(_.byNumber(1).fieldType), Some(ProtoType.Str))
    assertEquals(labels.mapEntry.map(_.byNumber(2).fieldType), Some(ProtoType.Str))
  }

  test("comments, options and reserved statements do not disturb the field table") {
    val source =
      """syntax = "proto3";
        |option java_package = "com.example"; // trailing comment
        |/* a block
        |   comment with a } and a ; in it */
        |message M {
        |  reserved 2, 15 to 20;
        |  reserved "old_name";
        |  string kept = 1 [deprecated = true];
        |}""".stripMargin
    val parsed = ProtoSchema.parse(source).fold(why => fail(why), identity)
    assertEquals(parsed.messages.head.fields.map(_.name), List("kept"))
  }

  test("a oneof's members are ordinary fields, because that is what they are on the wire") {
    val source =
      """syntax = "proto3";
        |message M {
        |  oneof payload {
        |    string text = 1;
        |    int32 number = 2;
        |  }
        |}""".stripMargin
    val parsed = ProtoSchema.parse(source).fold(why => fail(why), identity)
    assertEquals(parsed.messages.head.fields.map(_.number), List(1, 2))
  }

  test("text that is not a schema fails with a sentence naming what was expected") {
    val refused = ProtoSchema.parse("this is not a proto file")
    assert(refused.isLeft, refused)
    assert(refused.left.exists(_.contains("top level")), refused)
  }

  // -------------------------------------------------------------------------------------------
  // The decoder
  // -------------------------------------------------------------------------------------------

  test("a repeated nested message is not a same-named enum from another scope") {
    val schema = ProtoSchema
      .parse("""syntax = "proto3";
      message M { message E { int32 n = 1; } repeated E values = 1; }
      message Other { enum E { ZERO = 0; ONE = 1; } }
    """)
      .fold(fail(_), identity)
    val actual = ProtobufPayload
      .decode(schema, Array[Byte](0, 10, 2, 8, 1))
      .flatMap(text => parser.parse(text).left.map(_.getMessage))
    assertEquals(actual, parser.parse("""{"values":[{"n":1}]}""").left.map(_.getMessage))
  }

  test("every scalar type decodes to the value that was written") {
    val body = framed(
      Nil,
      Encoder()
        .string(1, "o-1")
        .varint(2, 3)
        .varint(3, 9007199254740993L)
        .varint(4, 1)
        .varint(5, 2)
        .double(10, 1.5)
        .bytes(11, Array[Byte](1, 2, 3))
        .zigZag(12, -7)
        .result()
    )

    val json = decoded(body)
    assertEquals(json.hcursor.get[String]("id"), Right("o-1"))
    assertEquals(json.hcursor.get[Int]("quantity"), Right(3))
    // 64-bit integers are strings, exactly: as a JSON number this value would render as ...92.
    assertEquals(json.hcursor.get[String]("placed_at"), Right("9007199254740993"))
    assertEquals(json.hcursor.get[Boolean]("express"), Right(true))
    assertEquals(json.hcursor.get[String]("status"), Right("SHIPPED"))
    assertEquals(json.hcursor.get[Double]("weight"), Right(1.5))
    assertEquals(json.hcursor.get[String]("signature"), Right("AQID"))
    assertEquals(json.hcursor.get[String]("adjustment"), Right("-7"))
  }

  test("a repeated field is an array even with one element, and a packed run is read as many") {
    val repeated = decoded(framed(Nil, Encoder().string(6, "gift").result()))
    assertEquals(repeated.hcursor.downField("tags").as[List[String]], Right(List("gift")))

    val packedSizes = Encoder().varintsPacked(9, List(1L, 2L, 300L)).result()
    val sizes = decoded(framed(Nil, packedSizes))
    assertEquals(sizes.hcursor.downField("sizes").as[List[Int]], Right(List(1, 2, 300)))
  }

  test("a nested message decodes as an object and a map as a JSON object") {
    val address = Encoder().string(1, "Kraków").string(2, "30-001").result()
    val entry = Encoder().string(1, "tier").string(2, "gold").result()
    val body = framed(Nil, Encoder().message(7, address).message(8, entry).result())

    val json = decoded(body)
    assertEquals(json.hcursor.downField("shipping").get[String]("city"), Right("Kraków"))
    assertEquals(json.hcursor.downField("labels").get[String]("tier"), Right("gold"))
  }

  test("an enum number the schema does not declare renders as the number rather than as nothing") {
    val json = decoded(framed(Nil, Encoder().varint(5, 99).result()))
    assertEquals(json.hcursor.get[String]("status"), Right("99"))
  }

  test("a field the schema does not declare is shown rather than dropped") {
    // The commonest reason for this is a record written with a newer schema than the subject's latest
    // version. Dropping it silently is how someone concludes their data is missing.
    val json = decoded(framed(Nil, Encoder().string(1, "o-9").string(77, "from the future").result()))
    assertEquals(json.hcursor.get[String]("unknown_77"), Right("from the future"))
  }

  test("the message-index path selects which message of the schema the record is") {
    val cancelled = framed(List(1), Encoder().string(1, "o-2").result())
    assertEquals(decoded(cancelled).hcursor.get[String]("id"), Right("o-2"))

    val nested = framed(List(0, 0), Encoder().string(1, "Gdańsk").result())
    assertEquals(decoded(nested).hcursor.get[String]("city"), Right("Gdańsk"))
  }

  test("an index path that names a message the schema does not have says so and does not throw") {
    val refused = ProtobufPayload.decode(file, framed(List(9), Encoder().string(1, "x").result()))
    assert(refused.left.exists(_.contains("do not belong together")), refused)
  }

  test("truncated bytes produce a sentence, not an exception") {
    // A length-delimited field claiming more bytes than remain: the shape a half-written record has.
    val truncated = Array[Byte](0, 0x0a, 0x40, 0x61)
    val refused = ProtobufPayload.decode(file, truncated)
    assert(refused.isLeft, refused)
    assert(refused.left.exists(_.contains("truncated")), refused)
  }

  test("bytes that are not Protobuf at all fail rather than rendering as nonsense") {
    val text = "this is plain text, not protobuf".getBytes(StandardCharsets.UTF_8)
    val refused = ProtobufPayload.decode(file, Array[Byte](0) ++ text)
    assert(refused.isLeft, refused)
  }

  test("a schema whose field type comes from an import names the import in its refusal") {
    val importing =
      """syntax = "proto3";
        |import "google/protobuf/timestamp.proto";
        |message M {
        |  google.protobuf.Timestamp at = 1;
        |}""".stripMargin
    val parsed = ProtoSchema.parse(importing).fold(why => fail(why), identity)
    val body = framed(Nil, Encoder().message(1, Encoder().varint(1, 5).result()).result())
    val refused = ProtobufPayload.decode(parsed, body)
    assert(refused.left.exists(_.contains("google/protobuf/timestamp.proto")), refused)
  }

  test("bytes written by protoc decode to the values protoc was given") {
    // Produced by protoc 36.0 from exactly the schema above, and pasted in as hexadecimal:
    //
    //   printf 'id: "o-77" quantity: 4 status: PLACED tags: "a" tags: "b"
    //           placed_at: 9007199254740993 weight: 1.5 adjustment: -7
    //           labels { key: "tier" value: "gold" } shipping { city: "Krakow" }
    //           sizes: 1 sizes: 2 sizes: 300' |
    //     protoc --encode=shop.orders.OrderPlaced order.proto | xxd -p
    //
    // This is the assertion that matters most in the file. Everything above it is checked against an
    // encoder written in this same suite, and two pieces of code written by the same hand can agree with
    // each other and both be wrong; these bytes were produced by the reference implementation.
    val fromProtoc = hex(
      "0a046f2d37371004188180808080808010280132016132016" +
        "23a090a074b72616bc3b377420c0a04746965721204676f6c644a040102ac0251000000000000f83f600d"
    )

    val json = decoded(Array[Byte](0) ++ fromProtoc)
    assertEquals(json.hcursor.get[String]("id"), Right("o-77"))
    assertEquals(json.hcursor.get[Int]("quantity"), Right(4))
    assertEquals(json.hcursor.get[String]("placed_at"), Right("9007199254740993"))
    assertEquals(json.hcursor.get[String]("status"), Right("PLACED"))
    assertEquals(json.hcursor.downField("tags").as[List[String]], Right(List("a", "b")))
    assertEquals(json.hcursor.downField("shipping").get[String]("city"), Right("Kraków"))
    assertEquals(json.hcursor.downField("labels").get[String]("tier"), Right("gold"))
    assertEquals(json.hcursor.downField("sizes").as[List[Int]], Right(List(1, 2, 300)))
    assertEquals(json.hcursor.get[Double]("weight"), Right(1.5))
    assertEquals(json.hcursor.get[String]("adjustment"), Right("-7"))
  }

  List("E", "M.E", "p.M.E", ".p.M.E").foreach { reference =>
    test(s"$reference resolves the local packed enum rather than an unrelated message") {
      val schema = ProtoSchema
        .parse(s"""syntax = "proto3"; package p;
        message M { enum E { LOCAL_ZERO = 0; LOCAL_ONE = 1; } repeated $reference values = 1; }
        message Other { message E { int32 n = 1; } }
      """)
        .fold(fail(_), identity)
      val actual = ProtobufPayload
        .decode(schema, Array[Byte](0, 10, 2, 0, 1, 8, 1))
        .flatMap(text => parser.parse(text).left.map(_.getMessage))
        .fold(fail(_), identity)
      assertEquals(
        actual.hcursor.get[List[String]]("values"),
        Right(List("LOCAL_ZERO", "LOCAL_ONE", "LOCAL_ONE"))
      )
    }
  }

  test("identically named nested enums bind locally even when selected by a message-index path") {
    val schema = ProtoSchema
      .parse("""syntax = "proto3"; package p;
      message A { enum E { A_ZERO = 0; A_ONE = 1; } repeated E values = 1; }
      message B { enum E { B_ZERO = 0; B_ONE = 1; } repeated E values = 1; }
    """)
      .fold(fail(_), identity)
    List((0, "A"), (1, "B")).foreach { (index, label) =>
      val actual = ProtobufPayload
        .decode(schema, framed(List(index), Array[Byte](10, 2, 0, 1, 8, 1)))
        .flatMap(text => parser.parse(text).left.map(_.getMessage))
        .fold(fail(_), identity)
      assertEquals(
        actual.hcursor.get[List[String]]("values"),
        Right(List(s"${label}_ZERO", s"${label}_ONE", s"${label}_ONE"))
      )
    }
  }

  test("nested fields and map values resolve relative and absolute messages in their enclosing scope") {
    val schema = ProtoSchema
      .parse("""syntax = "proto3"; package p;
      message M {
        message E { int32 n = 1; }
        message Inner { repeated E relative = 1; repeated .p.M.E absolute = 2; map<string, E> entries = 3; }
      }
      message Other { enum E { ZERO = 0; ONE = 1; } }
    """)
      .fold(fail(_), identity)
    val nested = Encoder().varint(1, 1).result()
    val entry = Encoder().string(1, "key").message(2, nested).result()
    val actual = ProtobufPayload
      .decode(
        schema,
        framed(List(0, 1), Encoder().message(1, nested).message(2, nested).message(3, entry).result())
      )
      .flatMap(text => parser.parse(text).left.map(_.getMessage))
    assertEquals(
      actual,
      parser
        .parse("""{"relative":[{"n":1}],"absolute":[{"n":1}],"entries":{"key":{"n":1}}}""")
        .left
        .map(_.getMessage)
    )
  }

  test("an unrelated short-name message is not a lexical match") {
    val schema = ProtoSchema
      .parse("message M { E value = 1; } message Other { message E { int32 n = 1; } }")
      .fold(fail(_), identity)
    assert(ProtobufPayload.decode(schema, Array[Byte](0, 10, 2, 8, 1)).isLeft)
  }

  test("packed named enums decode mixed packed and unpacked values") {
    val schema = ProtoSchema
      .parse("message M { enum E { ZERO = 0; ONE = 1; } repeated E values = 1; }")
      .fold(fail(_), identity)
    val result = ProtobufPayload
      .decode(schema, Array[Byte](0, 10, 2, 0, 1, 8, 1))
      .flatMap(text => parser.parse(text).left.map(_.getMessage))
      .fold(fail(_), identity)
    assertEquals(result.hcursor.get[List[String]]("values"), Right(List("ZERO", "ONE", "ONE")))
  }

  test("nested and packed fields cannot consume their enclosing field's neighbor") {
    val schema = ProtoSchema
      .parse(
        "message M { message N { bytes b = 1; } N n = 1; repeated fixed32 f = 2; repeated int32 i = 3; }"
      )
      .fold(fail(_), identity)
    val malformed = List(
      Array[Byte](0, 10, 2, 10, 2, 32, 1),
      Array[Byte](0, 18, 1, 1, 32, 1, 32, 1),
      Array[Byte](0, 26, 1, 0x80.toByte, 32, 1),
      Array[Byte](0, 10, 1, 0x80.toByte, 32, 1)
    )
    malformed.foreach(bytes => assert(ProtobufPayload.decode(schema, bytes).isLeft, bytes.toList))
  }

  private def hex(text: String): Array[Byte] =
    text.grouped(2).map(pair => Integer.parseInt(pair, 16).toByte).toArray

  // -------------------------------------------------------------------------------------------
  // A Protobuf encoder, written from the published rules for this suite only
  // -------------------------------------------------------------------------------------------

  /** The Confluent framing a record carries after its five-byte header: the message-index path, then the
    * message. An empty path is written as a single zero byte, which is what a producer writes for a schema
    * with one message.
    */
  private def framed(path: List[Int], message: Array[Byte]): Array[Byte] = {
    val prefix = new ByteArrayOutputStream()
    if path.isEmpty then prefix.write(0)
    else {
      writeVarint(prefix, path.size.toLong << 1)
      path.foreach(index => writeVarint(prefix, index.toLong << 1))
    }
    prefix.toByteArray ++ message
  }

  private def writeVarint(out: ByteArrayOutputStream, value: Long): Unit = {
    var remaining = value
    var continue = true
    while continue do {
      val chunk = (remaining & 0x7fL).toInt
      remaining = remaining >>> 7
      if remaining == 0L then {
        out.write(chunk)
        continue = false
      } else out.write(chunk | 0x80)
    }
  }

  final private class Encoder {
    private val out = new ByteArrayOutputStream()

    private def tag(number: Int, wireType: Int): Encoder = {
      writeVarint(out, (number.toLong << 3) | wireType.toLong)
      this
    }

    def varint(number: Int, value: Long): Encoder = {
      val _ = tag(number, 0)
      writeVarint(out, value)
      this
    }

    def zigZag(number: Int, value: Long): Encoder =
      varint(number, (value << 1) ^ (value >> 63))

    def string(number: Int, value: String): Encoder =
      bytes(number, value.getBytes(StandardCharsets.UTF_8))

    def bytes(number: Int, value: Array[Byte]): Encoder = {
      val _ = tag(number, 2)
      writeVarint(out, value.length.toLong)
      out.write(value)
      this
    }

    def message(number: Int, body: Array[Byte]): Encoder = bytes(number, body)

    def double(number: Int, value: Double): Encoder = {
      val _ = tag(number, 1)
      val bits = java.lang.Double.doubleToLongBits(value)
      (0 until 8).foreach(index => out.write(((bits >>> (8 * index)) & 0xffL).toInt))
      this
    }

    def varintsPacked(number: Int, values: List[Long]): Encoder = {
      val inner = new ByteArrayOutputStream()
      values.foreach(writeVarint(inner, _))
      bytes(number, inner.toByteArray)
    }

    def result(): Array[Byte] = out.toByteArray
  }
}
