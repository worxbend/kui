package kui.serde.confluent

import munit.FunSuite

/** JSON Schema payloads: decode passes plain JSON through unjudged, encode refuses whatever the schema
  * refuses and explains every violation, not just the first.
  */
final class JsonSchemaPayloadSuite extends FunSuite {

  private val definition: String =
    """{
      |  "$schema": "https://json-schema.org/draft/2020-12/schema",
      |  "type": "object",
      |  "properties": {
      |    "id": {"type": "string"},
      |    "quantity": {"type": "integer", "minimum": 1},
      |    "items": {
      |      "type": "array",
      |      "items": {
      |        "type": "object",
      |        "properties": {
      |          "sku": {"type": "string"},
      |          "price": {"type": "number"}
      |        },
      |        "required": ["sku", "price"]
      |      }
      |    }
      |  },
      |  "required": ["id", "quantity", "items"]
      |}""".stripMargin

  private val schema = JsonSchemaPayload.parse(definition).fold(why => fail(why), identity)

  test("well-formed JSON decodes unchanged, whether or not it matches the schema") {
    val json = """{"id":"o-1","quantity":2,"items":[{"sku":"A1","price":9.99}]}"""
    assertEquals(JsonSchemaPayload.decode(json.getBytes("UTF-8")), Right(json))

    // Decoding does not validate: bytes already in the topic are shown as they are, matching schema or not.
    val notMatchingTheSchema = """{"id":42}"""
    assertEquals(
      JsonSchemaPayload.decode(notMatchingTheSchema.getBytes("UTF-8")),
      Right(notMatchingTheSchema)
    )
  }

  test("bytes that are not JSON at all are rejected with a clear message, not thrown") {
    val decoded = JsonSchemaPayload.decode("not json at all".getBytes("UTF-8"))
    assert(decoded.isLeft, decoded)
    assert(decoded.left.exists(_.contains("not valid JSON")), decoded)
  }

  test("a payload matching a nested schema is accepted, and a CEL filter could address its nested paths") {
    val json = """{"id":"o-2","quantity":3,"items":[{"sku":"A1","price":9.99},{"sku":"B2","price":4.5}]}"""
    val encoded = JsonSchemaPayload.encode(schema, json).fold(why => fail(why), identity)
    assertEquals(new String(encoded, "UTF-8"), json)

    // The same text is exactly what `record.value.items[0].sku` and `record.value.items.exists(i, i.price >
    // 5)` would see once the message service parses it for filtering: proof that a schema-validated payload
    // is field-filterable JSON, not just "some bytes that happened to pass".
    val parsed = io.circe.parser.parse(json).toOption.get
    assertEquals(parsed.hcursor.downField("items").downArray.get[String]("sku"), Right("A1"))
  }

  test("a payload violating the schema in several ways is refused, and every violation is reported") {
    val json = """{"id":"o-3","quantity":"two","items":[{"sku":"A1"}]}"""
    val refused = JsonSchemaPayload.encode(schema, json)
    assert(refused.isLeft, refused)

    val problems = refused.left.getOrElse("").split("; ").toList
    // `quantity` is a string rather than an integer, and `items[0]` is missing its required `price` — two
    // independent violations from two different parts of the schema, both of which must survive together.
    assert(problems.size >= 2, refused)
    assert(refused.left.exists(_.contains("integer expected")), refused)
    assert(refused.left.exists(_.contains("'price' not found")), refused)
  }

  test("a payload that does match the schema is accepted and round trips through decode unchanged") {
    val json = """{"id":"o-4","quantity":1,"items":[]}"""
    val encoded = JsonSchemaPayload.encode(schema, json).fold(why => fail(why), identity)
    assertEquals(JsonSchemaPayload.decode(encoded), Right(json))
  }

  test("schema text that is not a schema fails at parse time with a message naming the failure") {
    assert(JsonSchemaPayload.parse("not a schema").left.exists(_.contains("could not be parsed")))
  }

  // -------------------------------------------------------------------------------------------
  // Nesting. Review R2 finding h-serde-untrusted-decode-1: a StackOverflowError is fatal to Cats Effect, so a
  // payload or a schema that recursed without bound halted the process instead of failing one record.
  // -------------------------------------------------------------------------------------------

  private def nestedObjects(depth: Int): String = ("""{"a":""" * depth) + "0" + ("}" * depth)

  test("a schema that refers to itself on every path is refused at the produce form, not overflowed") {
    // `{"$ref":"#"}` parses and is a valid schema; validating anything against it recurses without end.
    val cyclic = JsonSchemaPayload.parse("""{"$ref":"#"}""").fold(why => fail(why), identity)
    Isolated.onSmallStack(JsonSchemaPayload.encode(cyclic, "{}")) match {
      case Left(thrown) => fail(s"encoding threw $thrown instead of returning a Left")
      case Right(encoded) => assert(encoded.left.exists(_.contains("refers to itself")), encoded)
    }
  }

  test("a payload nested past the limit is refused on read, so nothing downstream has to walk it") {
    val decoded = JsonSchemaPayload.decode(nestedObjects(3000).getBytes("UTF-8"))
    assert(decoded.left.exists(_.contains("256 levels")), decoded.map(_.length))
  }

  test("a document nested past the limit is refused at the produce form before it is validated") {
    val permissive = JsonSchemaPayload.parse("{}").fold(why => fail(why), identity)
    Isolated.onSmallStack(JsonSchemaPayload.encode(permissive, nestedObjects(3000))) match {
      case Left(thrown) => fail(s"encoding threw $thrown instead of returning a Left")
      case Right(encoded) => assert(encoded.left.exists(_.contains("256 levels")), encoded.map(_.length))
    }
    assert(JsonSchemaPayload.encode(permissive, nestedObjects(50)).isRight)
  }
}
