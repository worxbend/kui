package kui.serde.confluent

import cats.data.NonEmptyList
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*

import kui.cache.{BoundedCache, CacheMetrics}
import kui.config.SafeUrl
import kui.kernel.error.{InfrastructureError, KuiError}
import kui.kernel.{ClusterId, TopicName}
import kui.serde.{DeserializeResult, PayloadKind, Serde, SerdeAutodetect, SerdeName, Target}
import kui.testkit.KuiIOSuite

/** The serde end to end, against a registry that is a `Map` rather than a server.
  *
  * Everything below is a statement about what a *user* sees: a decoded record, a picker row that is or is not
  * offered, a sentence when something cannot work. The registry is faked at its own interface — three methods
  * — rather than at HTTP, so these tests say nothing about wire details that `SchemaRegistryHttpSuite`
  * already pins.
  */
final class SchemaRegistrySerdeSuite extends KuiIOSuite {

  private val cluster: ClusterId = ClusterId.unsafe("prod-eu")
  private val topic: TopicName = TopicName.unsafe("orders-v2")

  private val avroDefinition: String =
    """{"type":"record","name":"OrderPlaced","fields":[{"name":"id","type":"string"}]}"""

  private val avroSchema = RegistrySchema(11, SchemaType.Avro, avroDefinition)
  private val jsonSchema =
    RegistrySchema(
      12,
      SchemaType.Json,
      """{"type":"object","required":["id"],"properties":{"id":{"type":"string"}}}"""
    )
  private val protobufSchema =
    RegistrySchema(13, SchemaType.Protobuf, "syntax = \"proto3\"; message OrderPlaced { string id = 1; }")

  /** A registry that answers from two maps and counts what it was asked.
    *
    * The count is not decoration: "one registry call per distinct schema id, not one per record" is the
    * property the by-id cache exists for, and it can only be observed from the registry's side.
    */
  private def fake(
      byId: Map[Int, RegistrySchema],
      bySubject: Map[String, RegistrySchema],
      calls: Ref[IO, Int],
      failure: Option[KuiError] = None
  ): SchemaRegistry[IO] = new SchemaRegistry[IO] {
    def schemaById(id: Int): IO[Either[KuiError, RegistrySchema]] =
      calls.update(_ + 1) *> IO.pure(
        failure
          .toLeft(())
          .flatMap(_ =>
            byId.get(id).toRight(InfrastructureError.Unreachable("schema-registry", "no such id"))
          )
      )
    def latestForSubject(subject: String): IO[Either[KuiError, Option[RegistrySchema]]] =
      IO.pure(failure.toLeft(bySubject.get(subject)))
  }

  private def serde(registry: SchemaRegistry[IO]): Resource[IO, Serde[IO]] =
    BoundedCache
      .make[IO, java.lang.Integer, ParsedSchema]("test.parsed", cluster, 64L, None, CacheMetrics.noop[IO])
      .map(parsed => SchemaRegistrySerde[IO](registry, parsed))

  private def avroRecord(id: String): Array[Byte] = {
    val schema = AvroPayload.parse(avroDefinition).fold(why => fail(why), identity)
    val body = AvroPayload.encode(schema, s"""{"id":"$id"}""").fold(why => fail(why), identity)
    WireFormat.frame(avroSchema.id, body)
  }

  private def decode(registry: SchemaRegistry[IO], bytes: Array[Byte]) =
    serde(registry).use(s => s.deserializer(topic, Target.Value).flatMap(_.deserialize(Nil, bytes)))

  test("an Avro record decodes to JSON and carries its schema's type, id and subject") {
    for {
      calls <- Ref.of[IO, Int](0)
      result <- decode(fake(Map(11 -> avroSchema), Map.empty, calls), avroRecord("o-1"))
    } yield result match {
      case Right(DeserializeResult(text, kind, properties)) =>
        assertEquals(text, """{"id":"o-1"}""")
        assertEquals(kind, PayloadKind.Json)
        assertEquals(properties.get("type").flatMap(_.asString), Some("AVRO"))
        assertEquals(properties.get("id").flatMap(_.asNumber).flatMap(_.toInt), Some(11))
        assertEquals(properties.get("subject").flatMap(_.asString), Some("orders-v2-value"))
      case Left(failure) => fail(s"expected a decode, got: ${failure.cause}")
    }
  }

  test("a page of records written by one producer costs one registry call, not one per record") {
    // The property belongs to `CachingSchemaRegistry`, so the serde is built over a cached registry here
    // rather than over the bare fake. This is the single reason that cache exists: a page of five hundred
    // records carries five hundred copies of one schema id, and one registry call is the difference between
    // a screen that draws and a registry that is knocked over by somebody scrolling.
    for {
      calls <- Ref.of[IO, Int](0)
      bare = fake(Map(11 -> avroSchema), Map.empty, calls)
      seen <- CachingSchemaRegistry
        .resource[IO](
          bare,
          SchemaRegistryConfig(NonEmptyList.one(SafeUrl.unsafe("http://registry:8081"))),
          cluster,
          CacheMetrics.noop[IO]
        )
        .flatMap(cached =>
          BoundedCache
            .make[IO, java.lang.Integer, ParsedSchema](
              "test.parsed",
              cluster,
              64L,
              None,
              CacheMetrics.noop[IO]
            )
            .map(parsed => SchemaRegistrySerde[IO](cached, parsed))
        )
        .use(s =>
          s.deserializer(topic, Target.Value).flatMap { decoder =>
            List("a", "b", "c", "d", "e").traverse_(id => decoder.deserialize(Nil, avroRecord(id)))
          }
        ) *> calls.get
    } yield assertEquals(seen, 1)
  }

  test("a registry failure is never cached, so a registry that comes back is used again") {
    // The mistake this guards against is one line long and invisible: caching the `Either` rather than the
    // value turns one refused connection into an entry that keeps answering "down" long after the registry
    // is up.
    for {
      calls <- Ref.of[IO, Int](0)
      cached <- CachingSchemaRegistry
        .resource[IO](
          fake(Map.empty, Map.empty, calls),
          SchemaRegistryConfig(NonEmptyList.one(SafeUrl.unsafe("http://registry:8081"))),
          cluster,
          CacheMetrics.noop[IO]
        )
        .use(registry => registry.schemaById(11) *> registry.schemaById(11) *> calls.get)
    } yield assertEquals(cached, 2)
  }

  test("a JSON Schema record decodes to the JSON it carries, without being validated on the way in") {
    // The payload is missing the required `id`. It is in the topic; a viewer that refused to show it would
    // hide exactly the record somebody opened the screen to find.
    val record = WireFormat.frame(12, """{"unexpected":true}""".getBytes("UTF-8"))
    for {
      calls <- Ref.of[IO, Int](0)
      result <- decode(fake(Map(12 -> jsonSchema), Map.empty, calls), record)
    } yield assertEquals(result.map(_.text), Right("""{"unexpected":true}"""))
  }

  test("a Protobuf record decodes against the .proto text the registry returned") {
    // The body is Confluent's Protobuf framing: `00` for "the first message of the schema", then
    // `0a 03 6f 2d 31` — field 1, length-delimited, three bytes, "o-1".
    val body = Array[Byte](0, 0x0a, 0x03, 'o'.toByte, '-'.toByte, '1'.toByte)
    for {
      calls <- Ref.of[IO, Int](0)
      result <- decode(fake(Map(13 -> protobufSchema), Map.empty, calls), WireFormat.frame(13, body))
    } yield result match {
      case Right(decoded) =>
        assert(decoded.text.contains("\"id\""), decoded.text)
        assert(decoded.text.contains("o-1"), decoded.text)
      case Left(failure) => fail(s"expected a decoded record, got ${failure.cause}")
    }
  }

  test("producing a Protobuf record is refused before the write, with the reason") {
    // Reading Protobuf works; writing it does not, and a record KUI encoded wrongly would outlive the
    // mistake in the topic. The refusal happens before anything is sent.
    for {
      calls <- Ref.of[IO, Int](0)
      registry = fake(Map(13 -> protobufSchema), Map(s"${topic.value}-value" -> protobufSchema), calls)
      written <- serde(registry).use(
        _.serializer(topic, Target.Value, Map.empty).flatMap(_.serialize("""{"id":"o-1"}""", Nil))
      )
    } yield written match {
      case Left(failure) => assert(failure.cause.contains("cannot yet write"), failure.cause)
      case Right(_) => fail("expected the write to be refused")
    }
  }

  test("a payload with no registry header fails on the record and names the serdes to try instead") {
    for {
      calls <- Ref.of[IO, Int](0)
      result <- decode(fake(Map.empty, Map.empty, calls), """{"plain":"json"}""".getBytes("UTF-8"))
      seen <- calls.get
    } yield {
      assert(result.left.exists(_.cause.contains("String")), result)
      // Nothing was asked of the registry: the header settled it.
      assertEquals(seen, 0)
    }
  }

  test("a registry that fails mid-browse puts its sentence on the record, not on the stream") {
    for {
      calls <- Ref.of[IO, Int](0)
      registry = fake(Map.empty, Map.empty, calls, Some(InfrastructureError.AuthFailed("schema-registry")))
      result <- decode(registry, avroRecord("o-1"))
    } yield assert(result.isLeft, result)
  }

  test("decoding capability is independent of subject naming while preference is not") {
    for {
      calls <- Ref.of[IO, Int](0)
      withSubject = fake(Map.empty, Map("orders-v2-value" -> avroSchema), calls)
      without = fake(Map.empty, Map.empty, calls)
      yes <- serde(withSubject).use(_.canDeserialize(topic, Target.Value))
      no <- serde(without).use(_.preferable(topic, Target.Value))
      keyToo <- serde(withSubject).use(_.canDeserialize(topic, Target.Key))
    } yield {
      assert(yes)
      assert(!no)
      assert(keyToo)
    }
  }

  test("a registry that cannot be reached leaves the picker drawable rather than throwing") {
    for {
      calls <- Ref.of[IO, Int](0)
      registry = fake(Map.empty, Map.empty, calls, Some(InfrastructureError.AuthFailed("schema-registry")))
      offered <- serde(registry).use(_.preferable(topic, Target.Value))
    } yield assert(!offered)
  }

  test("schema-ID autodetection decodes RecordNameStrategy without a topic subject") {
    for {
      calls <- Ref.of[IO, Int](0)
      registry = fake(Map(11 -> avroSchema), Map("OrderPlaced" -> avroSchema), calls)
      ranked <- serde(registry).use(s =>
        SerdeAutodetect.rank(List(s), topic, Target.Value, Some(avroRecord("o-1")))
      )
      noSample <- serde(registry).use(s => SerdeAutodetect.rank(List(s), topic, Target.Value, None))
    } yield {
      assertEquals(ranked, List(SerdeName.SchemaRegistry))
      assertEquals(noSample, Nil)
    }
  }

  test("the schema panel shows a JSON Schema as its own produce-form projection, and an Avro one as none") {
    for {
      calls <- Ref.of[IO, Int](0)
      avro <- serde(fake(Map.empty, Map("orders-v2-value" -> avroSchema), calls))
        .use(_.schema(topic, Target.Value))
      json <- serde(fake(Map.empty, Map("orders-v2-value" -> jsonSchema), calls))
        .use(_.schema(topic, Target.Value))
    } yield {
      assertEquals(avro.map(_.schemaType), Some("AVRO"))
      assertEquals(avro.flatMap(_.jsonSchema), None)
      assertEquals(json.flatMap(_.jsonSchema), Some(jsonSchema.definition))
    }
  }

  test("producing writes the header of the subject's latest schema in front of the encoded body") {
    for {
      calls <- Ref.of[IO, Int](0)
      registry = fake(Map(11 -> avroSchema), Map("orders-v2-value" -> avroSchema), calls)
      bytes <- serde(registry).use(s =>
        s.serializer(topic, Target.Value, Map.empty).flatMap(_.serialize("""{"id":"o-9"}""", Nil))
      )
      written = bytes.fold(failure => fail(failure.cause), identity)
      decoded <- decode(registry, written)
    } yield assertEquals(decoded.map(_.text), Right("""{"id":"o-9"}"""))
  }

  test("producing to a topic whose subject is not registered says so and says what to do") {
    for {
      calls <- Ref.of[IO, Int](0)
      result <- serde(fake(Map.empty, Map.empty, calls)).use(s =>
        s.serializer(topic, Target.Value, Map.empty).flatMap(_.serialize("""{"id":"o-9"}""", Nil))
      )
    } yield assert(result.left.exists(_.cause.contains("subject")), result)
  }

  test("the subject parameter overrides the derived one, for a topic that does not follow the convention") {
    for {
      calls <- Ref.of[IO, Int](0)
      registry = fake(Map(11 -> avroSchema), Map("orders.v2.OrderPlaced" -> avroSchema), calls)
      result <- serde(registry).use(s =>
        s.serializer(
          topic,
          Target.Value,
          Map(SchemaRegistrySerde.SubjectParameter -> "orders.v2.OrderPlaced")
        ).flatMap(_.serialize("""{"id":"o-9"}""", Nil))
      )
    } yield assert(result.isRight, result)
  }

  test("the parameter the produce form renders defaults to the conventional subject") {
    for {
      calls <- Ref.of[IO, Int](0)
      params <- serde(fake(Map.empty, Map.empty, calls)).use(_.parameters(topic, Target.Value))
    } yield {
      assertEquals(params.map(_.name), List(SchemaRegistrySerde.SubjectParameter))
      assertEquals(params.headOption.flatMap(_.default), Some("orders-v2-value"))
    }
  }
}
