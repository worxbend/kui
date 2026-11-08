package kui.serde.confluent

import java.nio.charset.StandardCharsets

import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

import com.networknt.schema.{
  InputFormat,
  Schema as JsonSchemaDocument,
  SchemaRegistry as JsonSchemaRegistry,
  SpecificationVersion
}
import io.circe.parser

import kui.kernel.serde.JsonNesting

/** JSON Schema payloads: the bytes are JSON, and the schema decides whether they are *allowed*.
  *
  * The asymmetry between the two directions is deliberate and is the whole content of this file.
  *
  *   - **Reading** does not validate. A record that is already in the topic is there whether or not it
  *     matches its schema, and a viewer that refused to show it would hide the one record an operator opened
  *     the screen to investigate — which is very often the malformed one. So decoding strips the five-byte
  *     header and returns the JSON.
  *   - **Writing** validates, and refuses. Producing a record KUI could not read back would put bytes in a
  *     topic that outlive the mistake, and the schema is right there.
  *
  * ==On the two `SchemaRegistry` types==
  *
  * `com.networknt.schema.SchemaRegistry` is this library's *local* schema store — a factory that turns schema
  * text into a validator — and has nothing to do with `kui.serde.confluent.SchemaRegistry`, which is the
  * Confluent registry over HTTP. The import renames it so that no reader of this file has to hold both
  * meanings of the word at once.
  *
  * The library validates straight from a `String`, so KUI never constructs a Jackson tree and this module
  * needs no Jackson dependency of its own. Well-formedness is checked with circe, which the module already
  * has, and which produces the same judgement.
  */
object JsonSchemaPayload {

  /** Draft 2020-12 as the assumed dialect when a schema does not declare one.
    *
    * A schema that *does* declare `$schema` is validated against the dialect it names; this only settles the
    * case where the registry holds a bare schema object, and 2020-12 is the current draft.
    */
  private val registry: JsonSchemaRegistry =
    JsonSchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12)

  def parse(definition: String): Either[String, JsonSchemaDocument] =
    try Right(registry.getSchema(definition))
    catch {
      case NonFatal(failure) =>
        Left(s"the registry's JSON Schema could not be parsed: ${message(failure)}")
    }

  /** The payload, as text, with no judgement passed on it beyond "is this JSON at all" — and "is it shallow
    * enough to walk", because the text is marked JSON and masking, filtering and the browser all walk it
    * recursively afterwards (`JsonNesting`).
    */
  def decode(body: Array[Byte]): Either[String, String] = {
    val text = new String(body, StandardCharsets.UTF_8)
    parser
      .parse(text)
      .left
      .map(failure =>
        "the payload after the Schema Registry header is not valid JSON, although the record claims a " +
          s"JSON Schema: ${failure.message}"
      )
      .flatMap(_ => Either.cond(!JsonNesting.exceeds(text, JsonNesting.MaxDepth), text, tooDeep("payload")))
  }

  /** Text to bytes, refused unless the schema accepts it.
    *
    * Every violation is reported, not the first. A form that fixes one error per submit is a form that takes
    * five submits to fill in, and the validator already knows all of them.
    */
  def encode(schema: JsonSchemaDocument, json: String): Either[String, Array[Byte]] =
    if JsonNesting.exceeds(json, JsonNesting.MaxDepth) then Left(tooDeep("document"))
    else
      try {
        val problems = schema.validate(json, InputFormat.JSON).asScala.toList.map(_.getMessage)
        if problems.isEmpty then Right(json.getBytes(StandardCharsets.UTF_8))
        else Left(s"this does not match the topic's JSON Schema: ${problems.mkString("; ")}")
      } catch {
        // The validator follows `$ref`s as it meets them, so a schema that refers to itself on a path every
        // document takes (`{"$ref": "#"}` is the smallest) recurses until the stack runs out, whatever the
        // input. A `StackOverflowError` is fatal to Cats Effect and would halt the process, so it is caught
        // by name here, once the validator has unwound: validation is a pure function and leaves nothing
        // half-done behind it.
        case _: StackOverflowError =>
          Left(
            "this could not be checked against the topic's JSON Schema: validating it recursed without end, " +
              "which almost always means the schema refers to itself (a `$ref` to an enclosing schema) on a " +
              "path every document takes. Fix the schema in the registry"
          )
        case NonFatal(failure) =>
          Left(s"this could not be checked against the topic's JSON Schema: ${message(failure)}")
      }

  private def tooDeep(what: String): String =
    s"the $what nests objects and arrays more than ${JsonNesting.MaxDepth} levels deep, and KUI reads and " +
      "writes JSON only up to that depth: every level costs a stack frame wherever the document is processed"

  private def message(failure: Throwable): String =
    Option(failure.getMessage).filter(_.nonEmpty).getOrElse(failure.getClass.getSimpleName)
}
