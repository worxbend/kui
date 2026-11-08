package kui.serde.confluent

import munit.FunSuite

/** Avro binary in, Avro JSON out, and back again. */
final class AvroPayloadSuite extends FunSuite {

  private val definition: String =
    """{
      |  "type": "record",
      |  "name": "OrderPlaced",
      |  "fields": [
      |    {"name": "id", "type": "string"},
      |    {"name": "quantity", "type": "int"},
      |    {"name": "note", "type": ["null", "string"], "default": null}
      |  ]
      |}""".stripMargin

  private val schema: org.apache.avro.Schema =
    AvroPayload.parse(definition).fold(why => fail(why), identity)

  test("a record encodes and decodes back to the same JSON") {
    val json = """{"id":"o-1","quantity":3,"note":{"string":"gift"}}"""
    val bytes = AvroPayload.encode(schema, json).fold(why => fail(why), identity)
    assertEquals(AvroPayload.decode(schema, bytes), Right(json))
  }

  test("a null branch of a union round trips as null") {
    val json = """{"id":"o-2","quantity":0,"note":null}"""
    val bytes = AvroPayload.encode(schema, json).fold(why => fail(why), identity)
    assertEquals(AvroPayload.decode(schema, bytes), Right(json))
  }

  test("bytes that are not this schema's fail with a sentence rather than an exception") {
    val decoded = AvroPayload.decode(schema, Array[Byte](1, 2, 3, 4, 5, 6, 7, 8))
    assert(decoded.isLeft, decoded)
    assert(decoded.left.exists(_.contains("do not match the schema")), decoded)
  }

  test("a union written the way a person would write it is refused, and the message explains the shape") {
    // `"gift"` rather than `{"string": "gift"}`: the single most common mistake at a produce form, and the
    // one place where Avro's JSON encoding surprises everybody the first time.
    val refused = AvroPayload.encode(schema, """{"id":"o-3","quantity":1,"note":"gift"}""")
    assert(refused.left.exists(_.contains("names the branch of a union")), refused)
  }

  test("schema text that is not a schema fails at parse time, not at decode time") {
    assert(AvroPayload.parse("not a schema").left.exists(_.contains("could not be parsed")))
  }

  private val nestedDefinition: String =
    """{
      |  "type": "record",
      |  "name": "OrderPlacedWithShipping",
      |  "fields": [
      |    {"name": "id", "type": "string"},
      |    {"name": "tags", "type": {"type": "array", "items": "string"}},
      |    {
      |      "name": "shipping",
      |      "type": {
      |        "type": "record",
      |        "name": "Shipping",
      |        "fields": [
      |          {"name": "city", "type": "string"},
      |          {"name": "cost", "type": "double"}
      |        ]
      |      }
      |    }
      |  ]
      |}""".stripMargin

  private val nestedSchema: org.apache.avro.Schema =
    AvroPayload.parse(nestedDefinition).fold(why => fail(why), identity)

  test("a nested record field and an array field decode to real nested-object and array JSON shapes") {
    val json =
      """{"id":"o-4","tags":["fragile","express"],"shipping":{"city":"Prague","cost":9.5}}"""
    val bytes = AvroPayload.encode(nestedSchema, json).fold(why => fail(why), identity)
    val decoded = AvroPayload.decode(nestedSchema, bytes).fold(why => fail(why), identity)
    assertEquals(decoded, json)

    val parsed = io.circe.parser.parse(decoded).fold(why => fail(why.getMessage), identity)
    val cursor = parsed.hcursor
    assertEquals(cursor.downField("tags").focus.flatMap(_.asArray).map(_.size), Some(2))
    assertEquals(cursor.downField("tags").downArray.as[String], Right("fragile"))
    assertEquals(cursor.downField("shipping").downField("city").as[String], Right("Prague"))
    assertEquals(cursor.downField("shipping").downField("cost").as[Double], Right(9.5))
  }

  // -------------------------------------------------------------------------------------------
  // Schemas written by somebody else. Review R2 finding h-serde-untrusted-decode-1: a recursive record used to
  // end in a StackOverflowError, which `NonFatal` does not catch and which Cats Effect answers by halting
  // the process.
  // -------------------------------------------------------------------------------------------

  private def parsed(definition: String): org.apache.avro.Schema =
    AvroPayload.parse(definition).fold(why => fail(why), identity)

  /** A linked list: legal, common in tree-shaped data, and recursive. */
  private val recursive: org.apache.avro.Schema =
    parsed("""{"type":"record","name":"Node","fields":[{"name":"next","type":["null","Node"]}]}""")

  /** `levels` nodes: union branch 1 (zig-zag `0x02`) per level, then branch 0, `null`, to end the list. */
  private def linkedList(levels: Int): Array[Byte] = Array.fill(levels)(0x02.toByte) :+ 0.toByte

  test("a recursive record nested far deeper than any stack is refused instead of overflowing it") {
    Isolated.onSmallStack(AvroPayload.decode(recursive, linkedList(100000))) match {
      case Left(thrown) => fail(s"decoding threw $thrown instead of returning a Left")
      case Right(decoded) => assert(decoded.left.exists(_.contains("256 levels")), decoded)
    }
  }

  test("a recursive record within the nesting limit still decodes and encodes back") {
    val bytes = linkedList(100)
    val json = AvroPayload.decode(recursive, bytes).fold(why => fail(why), identity)
    assert(json.startsWith("""{"next":{"Node":{"next":"""), json.take(60))
    assertEquals(AvroPayload.encode(recursive, json).map(_.toList), Right(bytes.toList))
  }

  test("JSON for a recursive schema nested past the limit is refused at the produce form") {
    val deep = ("""{"next":{"Node":""" * 2000) + """{"next":null}""" + ("}}" * 2000)
    Isolated.onSmallStack(AvroPayload.encode(recursive, deep)) match {
      case Left(thrown) => fail(s"encoding threw $thrown instead of returning a Left")
      case Right(encoded) => assert(encoded.left.exists(_.contains("256 levels")), encoded.map(_.length))
    }
  }
}
