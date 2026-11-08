package kui.serde.builtin

import java.nio.charset.StandardCharsets

import cats.Applicative
import io.circe.parser

import kui.kernel.serde.JsonNesting
import kui.serde.*

/** JSON, and only JSON that is an object or an array.
  *
  * A bare `123` and a bare `"hello"` are both valid JSON documents, and neither is what a user means when
  * they say a topic is a JSON topic. Accepting them would make this serde claim every number payload and
  * every quoted string, and — more damagingly — mark them `PayloadKind.Json`, which is what the table view
  * uses to decide it can flatten a record into columns. A table view offered a column layout for a payload of
  * `123` is a table view with one nameless column.
  *
  * The text KUI sends is **compact**, not pretty-printed. Pretty-printing on the server multiplies the bytes
  * on the wire by roughly three for no gain: the browser re-formats for display anyway (MSG-036), and it is
  * the browser that knows how wide the window is.
  */
object JsonSerde {

  private val Summary: String =
    "Parses the payload as JSON and marks it as JSON, which is what lets the table view flatten records " +
      "into columns. Only objects and arrays are accepted: a bare number or a bare quoted string is valid " +
      "JSON but is not a JSON document in the sense a topic means. Sent compact; the browser formats it."

  def apply[F[_]: Applicative]: Serde[F] = new Impl[F]

  /** Whether these bytes parse as a JSON object or array. Shared with `claims` so the two cannot disagree.
    *
    * Depth is part of "is this a document". The parse itself is safe at any depth jawn accepts, but printing
    * the result (`noSpaces`, below) and everything downstream that walks it recurse once per level, and a
    * `StackOverflowError` there is fatal to Cats Effect rather than a failure on one record. A document past
    * `JsonNesting.MaxDepth` is therefore not this serde's, in either direction.
    */
  private def parseDocument(bytes: Array[Byte]): Either[String, io.circe.Json] =
    Payloads.asUtf8(bytes) match {
      case None => Left("the payload is not valid UTF-8, so it cannot be JSON")
      case Some(text) =>
        parser.parse(text) match {
          case Left(failure) => Left(explain(text, failure))
          case Right(json) =>
            if !(json.isObject || json.isArray) then
              Left(
                "this is valid JSON but not a JSON document: it is a single value, and this serde accepts " +
                  "only an object (starting with `{`) or an array (starting with `[`). To send a bare " +
                  "number or a bare piece of text, choose a different serde - String, or one of the number " +
                  "formats"
              )
            else if JsonNesting.exceeds(text, JsonNesting.MaxDepth) then Left(TooDeep)
            else Right(json)
        }
    }

  private val TooDeep: String =
    s"this JSON nests objects and arrays more than ${JsonNesting.MaxDepth} levels deep, and KUI reads and " +
      "writes JSON documents only up to that depth: every level costs a stack frame wherever the document " +
      "is processed, so a deeper one is handled as text rather than as JSON"

  /** What to tell a person when their text is not JSON.
    *
    * The parser's own message is written for whoever is debugging the parser: `expected null got 'not js...'
    * (line 1, column 1)` describes one branch the parser tried on its way to giving up, and it reads as
    * though KUI wanted `null`. It does not. Somebody looking at a publish form needs to know what is wrong
    * with what they typed and what to do instead, so this says that in a sentence and keeps the parser's
    * *position* - the one genuinely useful thing in its message - when there is one.
    *
    * Three shapes cover essentially everything a form produces:
    *
    *   - nothing at all, usually a field left empty by accident;
    *   - text that never even starts like JSON, which is nearly always plain text typed into a JSON field -
    *     so the sentence names the serde to pick instead rather than only saying no;
    *   - text that starts correctly and breaks somewhere inside, where the actionable advice is the short
    *     list of things that are actually wrong with hand-typed JSON, plus the position to look at.
    */
  private def explain(text: String, failure: io.circe.ParsingFailure): String = {
    val trimmed = text.trim
    val at = positionIn(failure.message).fold("")(where => s" $where")

    if trimmed.isEmpty then "there is nothing here to send: a JSON document is at least `{}` or `[]`"
    else if trimmed.startsWith("{") || trimmed.startsWith("[") then
      s"this starts like JSON but does not parse$at. Check for a quote, comma or closing bracket that is " +
        "missing, a comma left before a `}` or `]`, or single quotes where JSON requires double ones"
    else
      s"this is not JSON: a JSON document has to start with `{` or `[`, and this one starts with " +
        s"${describeFirst(trimmed)}. If you meant to send it as plain text, choose the String serde instead"
  }

  /** The `(line L, column C)` circe puts at the end of its message, if it is there.
    *
    * Taken from the message rather than recomputed, because the parser is the only thing that knows where it
    * stopped. Nothing here depends on finding it: a message in some other shape simply yields no position,
    * and the sentence above is still a sentence.
    */
  private def positionIn(message: String): Option[String] =
    PositionPattern
      .findFirstMatchIn(message)
      .map(found => s"at line ${found.group(1)}, column ${found.group(2)}")

  private val PositionPattern: scala.util.matching.Regex = """\(line (\d+), column (\d+)\)""".r

  /** The offending first character, named in a way a reader can match against their own screen.
    *
    * The character itself for anything printable, and a description for the ones that would print as nothing
    * or would mislead: "starts with " followed by an invisible character is a puzzle rather than a message,
    * and a leading `<` is worth naming because it means somebody pasted XML or HTML.
    */
  private def describeFirst(trimmed: String): String =
    trimmed.head match {
      case '\'' => "a single quote"
      case '<' => "`<`, which looks like XML or HTML rather than JSON"
      case first if first.isControl => "a control character"
      case first => s"`$first`"
    }

  final private class Impl[F[_]: Applicative] extends SimpleSerde[F] {

    val name: SerdeName = SerdeName.Json
    val summary: String = Summary

    def decode(headers: List[RawHeader], bytes: Array[Byte]): Either[DeserializeFailure, DeserializeResult] =
      parseDocument(bytes) match {
        case Left(reason) => failure(reason)
        case Right(json) => Right(DeserializeResult.json(json.noSpaces))
      }

    def encode(input: String, headers: List[RawHeader]): Either[SerializeFailure, Array[Byte]] =
      // Encoding is held to the same rule as decoding on purpose. A produce that accepted a bare `123`
      // would put a record in the topic that this serde then refuses to read back, and the user would see
      // their own message rendered through the fallback a second after sending it.
      parseDocument(input.getBytes(StandardCharsets.UTF_8)) match {
        case Left(reason) => encodeFailure(reason)
        case Right(json) => Right(json.noSpaces.getBytes(StandardCharsets.UTF_8))
      }

    def claims(sample: Array[Byte]): Boolean = parseDocument(sample).isRight
  }
}
