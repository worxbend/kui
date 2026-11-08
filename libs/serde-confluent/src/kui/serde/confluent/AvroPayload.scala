package kui.serde.confluent

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

import scala.util.control.{NoStackTrace, NonFatal}

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter}
import org.apache.avro.io.{DecoderFactory, EncoderFactory, ResolvingDecoder}

import kui.kernel.serde.JsonNesting

/** Avro binary to JSON text and back, against a schema the registry supplied.
  *
  * Pure functions over a parsed schema and some bytes. No effect, no registry, no cache: everything about
  * *which* schema applies is decided by the caller, so this file is a codec and can be tested as one.
  *
  * ==Why Apache Avro and not Confluent's serializer==
  *
  * `io.confluent:kafka-avro-serializer` would do this too, and what it adds on top of the Avro library is a
  * registry client and a schema cache. KUI has to own both of those anyway — the client so that the registry
  * gets `libs/http`'s circuit breaker and failover rather than a second, invisible retry loop, and the cache
  * so that it is one of ADR-016's two primitives and reports the same metrics as every other cache in the
  * product. What is left is `GenericDatumReader`, which is Avro's, is Apache-2.0, and is on Maven Central.
  *
  * ==On the JSON this produces==
  *
  * Avro's own JSON encoding, not a hand-rolled one. It is verbose in exactly one place: a value of a union
  * type is written as `{"string": "hello"}` rather than as `"hello"`, because a union is genuinely ambiguous
  * without the branch name and Avro will not guess. That is the same text every other Avro tool prints, which
  * matters more than brevity — an operator comparing KUI's output with `avro-tools` or with Kafbat should see
  * the same characters.
  */
object AvroPayload {

  /** Parses schema text into the form the codec needs.
    *
    * Separate from decoding because parsing is the expensive half and the result is reusable: one schema
    * covers every record a producer wrote with it, and a page of five hundred records must parse it once. The
    * caller holds the parsed schemas; this function only makes them.
    */
  def parse(definition: String): Either[String, Schema] =
    try Right(new Schema.Parser().parse(definition))
    catch {
      case NonFatal(failure) =>
        Left(s"the registry's Avro schema could not be parsed: ${message(failure)}")
    }

  /** Avro binary to Avro JSON.
    *
    * The writer's schema is used as the reader's schema as well. That is correct for the registry case and
    * only for it: the bytes carry the id of the schema they were written with, so the exact writer schema is
    * always available, and schema *resolution* — reading old bytes through a newer reader schema — is a
    * producer/consumer concern that a viewer has no reason to impose.
    */
  // scalafix:off DisableSyntax.null
  //
  // Avro's reader and writer factories take a *reuse* argument: an object to decode into, or an encoder to
  // recycle, so that a hot loop allocates nothing. `null` is how that API spells "allocate a fresh one",
  // and there is no overload that omits it. Wrapping it in an `Option` would not remove the `null` - it
  // would only move it one line further from the call that needs it - so the ban is lifted for exactly
  // these four arguments and turned back on immediately below.
  def decode(schema: Schema, body: Array[Byte]): Either[String, String] =
    try {
      val reader = new BoundedReader(schema)
      val decoder = DecoderFactory.get().binaryDecoder(body, null)
      val datum = reader.read(null, decoder)
      val out = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get().jsonEncoder(schema, out)
      val writer = new GenericDatumWriter[Any](schema)
      writer.write(datum, encoder)
      encoder.flush()
      Right(out.toString(StandardCharsets.UTF_8))
    } catch {
      case refused: Refused => Left(refused.explanation)
      // A `StackOverflowError` is fatal to Cats Effect: `NonFatal` does not match it, and a decode that lets
      // it escape halts the process instead of failing one record. The reader below refuses past
      // `JsonNesting.MaxDepth` before it gets here; this is the floor under any path that still recurses.
      case _: StackOverflowError => Left(TooDeep)
      case NonFatal(failure) =>
        Left(
          "these bytes do not match the schema the record says wrote them " +
            s"(schema id in the record, Avro's complaint: ${message(failure)})"
        )
    }

  /** Avro JSON to Avro binary, for the produce form.
    *
    * The input is Avro's JSON encoding, which is what [[decode]] emits — so "copy a record, change a field,
    * send it" works, and that round trip is the single most common thing anyone does with a produce form.
    */
  def encode(schema: Schema, json: String): Either[String, Array[Byte]] =
    try {
      val reader = new BoundedReader(schema)
      val decoder = DecoderFactory.get().jsonDecoder(schema, json)
      val datum = reader.read(null, decoder)
      val out = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get().binaryEncoder(out, null)
      val writer = new GenericDatumWriter[Any](schema)
      writer.write(datum, encoder)
      encoder.flush()
      Right(out.toByteArray)
    } catch {
      case refused: Refused => Left(refused.explanation)
      case _: StackOverflowError => Left(TooDeep)
      case NonFatal(failure) =>
        Left(
          "this does not match the topic's Avro schema. Note that Avro's JSON encoding names the branch " +
            "of a union explicitly - a nullable string is `{\"string\": \"hello\"}` or `null`, not " +
            s"`\"hello\"`. Avro's complaint: ${message(failure)}"
        )
    }

  // scalafix:on DisableSyntax.null

  /** A refusal raised from inside Avro's reader, where throwing is the only way out of a read. It never
    * leaves this object: [[decode]] and [[encode]] turn it into their `Left`.
    */
  final private class Refused(val explanation: String) extends RuntimeException(explanation) with NoStackTrace

  private val TooDeep: String =
    s"this record nests more than ${JsonNesting.MaxDepth} levels deep, and KUI reads Avro only up to that " +
      "depth: every level costs a stack frame, and a recursive schema lets a record nest as deep as its " +
      "bytes allow"

  /** Avro's generic data model with its "fast reader" switched off, so that [[BoundedReader]]'s hooks run.
    *
    * Avro 1.12 enables the fast reader by default (`org.apache.avro.fastread`): `GenericDatumReader.read`
    * then hands the whole read to a reader `FastReaderBuilder` assembled from the schema, and no protected
    * method of the `GenericDatumReader` is ever called. The generic path is a little slower per record and is
    * the only one that can be bounded from outside. Configured once and never changed afterwards.
    */
  private val Model: GenericData = new GenericData().setFastReaderEnabled(false)

  /** `GenericDatumReader`, refusing to recurse deeper than `JsonNesting.MaxDepth`.
    *
    * The schema decides how deep a read recurses, and it is somebody else's: a recursive record (`Node` with
    * a `["null", "Node"]` field) lets a producer nest a record one level per byte, and Avro recurses once per
    * level with no limit of its own. The `StackOverflowError` that ends it is a `VirtualMachineError`, which
    * the `NonFatal` handlers above do not catch and which Cats Effect answers by halting the process. Every
    * value Avro reads — record, union, array item, map value — passes through `readWithoutConversion`, so
    * counting there bounds every path, and the depth it counts is never less than the depth of the JSON
    * written from the datum.
    */
  // scalafix:off DisableSyntax.var, DisableSyntax.throw
  //
  // A Java reader is extended by overriding its protected hooks, and a hook returns a datum: throwing is the
  // only way to stop a read from inside one, and how deep the read has gone is a counter. Both are private
  // to this class, which is created per call and never shared, and `Refused` is caught by both callers.
  final private class BoundedReader(schema: Schema) extends GenericDatumReader[Any](schema, schema, Model) {

    private var depth: Int = 0

    override protected def readWithoutConversion(
        old: AnyRef,
        expected: Schema,
        in: ResolvingDecoder
    ): AnyRef = {
      depth += 1
      try
        if depth > JsonNesting.MaxDepth then throw new Refused(TooDeep)
        else super.readWithoutConversion(old, expected, in)
      finally depth -= 1
    }
  }
  // scalafix:on DisableSyntax.var, DisableSyntax.throw

  /** An exception's message, or its class when it has none.
    *
    * Never the stack trace and never the payload: `DeserializeFailure.cause` is display text and follows
    * `KuiError`'s rule, and an Avro failure's message can already be long.
    */
  private def message(failure: Throwable): String =
    Option(failure.getMessage).filter(_.nonEmpty).getOrElse(failure.getClass.getSimpleName)
}
