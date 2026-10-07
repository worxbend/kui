package kui.serde.confluent

import java.nio.charset.StandardCharsets
import java.util.Base64

import scala.annotation.tailrec

import io.circe.Json

/** Protobuf binary to JSON text, against a schema the registry supplied.
  *
  * Pure functions over a parsed [[ProtoFile]] and some bytes: no effect, no registry, no cache. Which schema
  * applies and which message inside it is the caller's decision, so this file is a codec and is tested as
  * one.
  *
  * ==Why the JSON looks the way it does==
  *
  * It follows Protobuf's own canonical JSON mapping where that mapping has an opinion, because an operator
  * comparing KUI's output with `protoc --decode` or with their own service's logs should see the same values:
  *
  *   - field names are the names declared in the `.proto`, not lowerCamelCase, because that is the spelling
  *     in the schema panel next to the record;
  *   - 64-bit integers are rendered as JSON *strings*, as the canonical mapping requires — a JSON number
  *     cannot hold every `int64` exactly, and a viewer that silently rounded an order id would be worse than
  *     one that quoted it;
  *   - `bytes` fields are base64, which is the canonical mapping and the only lossless rendering;
  *   - enum values are their names, falling back to the number when the schema does not declare it, which is
  *     what a record written by a newer producer looks like;
  *   - a `map<k, v>` is a JSON object, and repeated fields are arrays.
  *
  * ==Unknown fields are shown, not dropped==
  *
  * A field number the schema does not declare is rendered as `"unknown_7"` with the best reading of its
  * bytes. Protobuf is designed so that a reader tolerates fields it does not know, and in a viewer that
  * tolerance is exactly wrong to apply silently: the commonest reason for an unknown field is that the record
  * was written with a newer schema than the subject's latest version, and an operator staring at a record
  * with a field missing has no way to discover that. Showing it answers the question.
  */
object ProtobufPayload {

  /** The wire types, as the encoding defines them. Types 3 and 4 are proto2 groups, which are deprecated and
    * which [[decode]] reports rather than guesses at.
    */
  private val VarintWire: Int = 0
  private val Fixed64Wire: Int = 1
  private val LengthDelimitedWire: Int = 2
  private val StartGroupWire: Int = 3
  private val EndGroupWire: Int = 4
  private val Fixed32Wire: Int = 5

  /** Parses the schema text a registry returned. Separate from decoding because parsing is the expensive half
    * and the result is reused for every record written with that schema id.
    */
  def parse(definition: String): Either[String, ProtoFile] = ProtoSchema.parse(definition)

  /** The message-index prefix Confluent writes between the schema id and the message itself.
    *
    * A `.proto` file can declare many messages, and the record has to say which one it is. The format is a
    * varint count followed by that many varints naming a path through the file's declaration order, with one
    * special case: a single `0` byte means "the first message", which is what a producer writes for the
    * overwhelmingly common one-message schema. Reading this wrong shifts every subsequent byte, so it is the
    * first thing that must be right about a Protobuf payload — and it is the reason Avro's five-byte header
    * is not enough for this format.
    */
  def readIndexes(body: Array[Byte]): Either[String, (List[Int], Array[Byte])] =
    if body.isEmpty then Left("the record has a Schema Registry header and no body")
    else
      readVarint(body, 0).flatMap { (rawCount, afterCount) =>
        val count = zigZag(rawCount)
        if count == 0L then Right((Nil, body.drop(afterCount)))
        else if count < 0L || count > MaxIndexDepth then
          Left(
            s"the record says its message-index path is $count levels deep, which no Protobuf schema is. " +
              "These bytes were probably not written by a Schema-Registry-aware Protobuf producer"
          )
        else
          readIndexPath(body, afterCount, count.toInt, Nil).map((path, offset) => (path, body.drop(offset)))
      }

  /** Nobody nests messages a hundred deep; a larger count means the bytes are not what they claim. */
  private val MaxIndexDepth: Long = 100L

  @tailrec
  private def readIndexPath(
      bytes: Array[Byte],
      offset: Int,
      remaining: Int,
      collected: List[Int]
  ): Either[String, (List[Int], Int)] =
    if remaining == 0 then Right((collected.reverse, offset))
    else
      readVarint(bytes, offset) match {
        case Left(problem) => Left(problem)
        case Right((rawValue, next)) =>
          val value = zigZag(rawValue)
          if value < 0L || value > Int.MaxValue then
            Left(s"the record's message-index path contains $value, which is not a message number")
          else readIndexPath(bytes, next, remaining - 1, value.toInt :: collected)
      }

  /** One Protobuf message as JSON text.
    *
    * `Left` carries display text and never an exception, because every caller is on the record-decoding path
    * where a failure belongs on the record rather than on the stream (ADR-035).
    */
  def decode(file: ProtoFile, body: Array[Byte]): Either[String, String] =
    readIndexes(body).flatMap { (path, message) =>
      val resolved = file.resolved
      resolved
        .messageAt(path)
        .flatMap(declared => decodeMessage(resolved, declared, message, 0, message.length))
        .map(_.spaces2)
    }

  /** Decodes one message body into a JSON object.
    *
    * Fields are collected in the order they appear on the wire and then emitted in the order the *schema*
    * declares them, with unknown numbers last. Wire order is an encoder's private business — two producers of
    * the same message may write the same fields in different orders — so rendering in wire order would make
    * two identical records look different on screen.
    */
  private def decodeMessage(
      file: ProtoFile,
      message: ProtoMessage,
      bytes: Array[Byte],
      from: Int,
      until: Int
  ): Either[String, Json] = {

    @tailrec
    def loop(offset: Int, collected: List[(Int, Json)]): Either[String, List[(Int, Json)]] =
      if offset >= until then Right(collected.reverse)
      else
        readVarint(bytes, offset, until) match {
          case Left(problem) => Left(problem)
          case Right((tag, afterTag)) =>
            val number = (tag >>> 3).toInt
            val wireType = (tag & 0x7L).toInt
            if number <= 0 then
              Left(s"the record contains field number $number, and Protobuf numbers start at 1")
            else
              readValue(file, message.byNumber.get(number), wireType, bytes, afterTag, until) match {
                case Left(problem) => Left(problem)
                case Right((value, next)) => loop(next, (number -> value) :: collected)
              }
        }

    loop(from, Nil).map(entries => assemble(file, message, entries))
  }

  /** Wire entries as one JSON object, grouped by field and ordered by the schema. */
  private def assemble(file: ProtoFile, message: ProtoMessage, entries: List[(Int, Json)]): Json = {
    val grouped: Map[Int, List[Json]] =
      entries.groupBy((number, _) => number).map((number, pairs) => number -> pairs.map(_._2))

    val declared = message.fields.flatMap { field =>
      grouped.get(field.number).map(values => field.name -> render(file, field, values))
    }

    val unknown = grouped.toList
      .filterNot((number, _) => message.byNumber.contains(number))
      .sortBy((number, _) => number)
      .map((number, values) =>
        s"unknown_$number" -> (if values.sizeIs == 1 then values.head else Json.arr(values*))
      )

    Json.obj((declared ++ unknown)*)
  }

  /** How a field's occurrences become one JSON value.
    *
    * A repeated field is always an array, even with one element, because a viewer that showed a
    * single-element list as a bare value would make the schema look different from what it is. A map is an
    * object built from its entry messages. A non-repeated field that somehow appeared twice keeps the last
    * occurrence, which is what every Protobuf implementation does.
    */
  private def render(file: ProtoFile, field: ProtoField, values: List[Json]): Json =
    field.mapEntry match {
      case Some(_) =>
        Json.obj(
          values.flatMap { entry =>
            val cursor = entry.hcursor
            cursor
              .get[Json]("key")
              .toOption
              .map(key => keyText(key) -> cursor.get[Json]("value").toOption.getOrElse(Json.Null))
          }*
        )
      case None =>
        field.label match {
          // A packed run arrives as one array; an unpacked one arrives as several separate values, and an
          // encoder may mix the two in a single record. Flattening is what makes those three encodings of
          // the same list render identically, which is the whole promise of a packed field.
          case ProtoLabel.Repeated if isPackable(file, field.fieldType) =>
            Json.arr(values.flatMap(value => value.asArray.map(_.toList).getOrElse(List(value)))*)
          case ProtoLabel.Repeated => Json.arr(values*)
          case _ => values.lastOption.getOrElse(Json.Null)
        }
    }

  /** A map key as an object member name. JSON objects are keyed by strings, and the canonical mapping says an
    * integer or boolean key is written as its own text.
    */
  private def keyText(key: Json): String = key.asString.getOrElse(key.noSpaces)

  /** One tag's value, and where the next tag starts.
    *
    * `declared` is `None` for a field the schema does not know. Its bytes are still read — the wire type says
    * exactly how many to consume, which is why an unknown field never derails the rest of the message — and
    * rendered as the most useful reading available.
    */
  private def readValue(
      file: ProtoFile,
      declared: Option[ProtoField],
      wireType: Int,
      bytes: Array[Byte],
      offset: Int,
      until: Int
  ): Either[String, (Json, Int)] =
    wireType match {
      case VarintWire =>
        readVarint(bytes, offset, until).map((raw, next) => (varintAs(file, declared, raw), next))

      case Fixed64Wire =>
        readFixed(bytes, offset, 8, until).map((raw, next) => (fixed64As(declared, raw), next))

      case Fixed32Wire =>
        readFixed(bytes, offset, 4, until).map((raw, next) => (fixed32As(declared, raw.toInt), next))

      case LengthDelimitedWire =>
        readVarint(bytes, offset, until).flatMap { (length, afterLength) =>
          if length < 0L || length > until - afterLength then
            Left(
              s"a length-delimited field claims $length bytes and the record has " +
                s"${until - afterLength} left. The record is truncated or is not Protobuf"
            )
          else {
            val end = afterLength + length.toInt
            lengthDelimited(file, declared, bytes, afterLength, end).map(value => (value, end))
          }
        }

      case StartGroupWire | EndGroupWire =>
        Left(
          "the record uses a proto2 group, a construct deprecated in 2015 that KUI's Protobuf reader does " +
            "not decode"
        )

      case other =>
        Left(s"the record contains wire type $other, and Protobuf defines 0, 1, 2, 3, 4 and 5")
    }

  /** A length-delimited run: a string, some bytes, a nested message, or a packed run of numbers. */
  private def lengthDelimited(
      file: ProtoFile,
      declared: Option[ProtoField],
      bytes: Array[Byte],
      from: Int,
      until: Int
  ): Either[String, Json] =
    declared match {
      case Some(field) =>
        field.fieldType match {
          case ProtoType.Str => Right(Json.fromString(text(bytes, from, until)))
          case ProtoType.Bytes => Right(Json.fromString(base64(bytes, from, until)))
          case named @ ProtoType.Named(_) if field.label == ProtoLabel.Repeated && isPackable(file, named) =>
            packed(file, field, bytes, from, until)
          case ProtoType.Named(reference) =>
            resolve(file, field, reference) match {
              case Right(nested) => decodeMessage(file, nested, bytes, from, until)
              case Left(problem) => Left(problem)
            }
          case scalar if field.label == ProtoLabel.Repeated && scalar.isPackable =>
            packed(file, field, bytes, from, until)
          case _ =>
            // A single scalar arriving length-delimited is a packed run of one, which is legal.
            packed(file, field, bytes, from, until).map(array =>
              array.asArray.flatMap(_.headOption).getOrElse(array)
            )
        }

      case None =>
        // Nothing says what these bytes are. Valid UTF-8 is overwhelmingly likely to be the string it looks
        // like; anything else is shown as base64, which at least lets someone copy it out.
        Right(Json.fromString(printable(bytes, from, until).getOrElse(base64(bytes, from, until))))
    }

  private def enumType(file: ProtoFile, reference: String): Option[ProtoEnum] =
    file.enumsByName.get(reference.stripPrefix("."))

  private def isPackable(file: ProtoFile, fieldType: ProtoType): Boolean = fieldType match {
    case ProtoType.Named(reference) => enumType(file, reference).isDefined
    case scalar => scalar.isPackable
  }

  /** A packed repeated field: values of one type, one after another, with no tags between them. */
  private def packed(
      file: ProtoFile,
      field: ProtoField,
      bytes: Array[Byte],
      from: Int,
      until: Int
  ): Either[String, Json] = {
    @tailrec
    def loop(offset: Int, collected: List[Json]): Either[String, List[Json]] =
      if offset >= until then Right(collected.reverse)
      else
        packedOne(file, field, bytes, offset, until) match {
          case Left(problem) => Left(problem)
          case Right((value, next)) => loop(next, value :: collected)
        }

    loop(from, Nil).map(values => Json.arr(values*))
  }

  private def packedOne(
      file: ProtoFile,
      field: ProtoField,
      bytes: Array[Byte],
      offset: Int,
      until: Int
  ): Either[String, (Json, Int)] =
    field.fieldType match {
      case ProtoType.Fixed32 | ProtoType.SFixed32 | ProtoType.Float =>
        readFixed(bytes, offset, 4, until).map((raw, next) => (fixed32As(Some(field), raw.toInt), next))
      case ProtoType.Fixed64 | ProtoType.SFixed64 | ProtoType.Double =>
        readFixed(bytes, offset, 8, until).map((raw, next) => (fixed64As(Some(field), raw), next))
      case _ =>
        readVarint(bytes, offset, until).map((raw, next) => (varintAs(file, Some(field), raw), next))
    }

  /** A varint's value, read as the declared type says to read it.
    *
    * The three integer families differ only here, and getting it wrong is silent: a `sint32` read as an
    * `int32` shows -1 as 4294967295, which looks like data rather than like a bug.
    */
  private def varintAs(file: ProtoFile, declared: Option[ProtoField], raw: Long): Json =
    declared.map(_.fieldType) match {
      case Some(ProtoType.Bool) => Json.fromBoolean(raw != 0L)
      case Some(ProtoType.SInt32) | Some(ProtoType.SInt64) => longJson(zigZag(raw))
      case Some(ProtoType.Int32) | Some(ProtoType.SFixed32) => Json.fromInt(raw.toInt)
      case Some(ProtoType.UInt32) => Json.fromLong(raw & 0xffffffffL)
      case Some(ProtoType.Int64) | Some(ProtoType.SFixed64) => longJson(raw)
      case Some(ProtoType.UInt64) => unsignedJson(raw)
      case Some(ProtoType.Named(reference)) =>
        // An enum, or a message field carrying a varint, which can only mean the record and the schema
        // disagree. The enum case is the real one and is named; the other renders as a number.
        enumType(file, reference)
          .flatMap(_.values.get(raw.toInt))
          .map(Json.fromString)
          .getOrElse(longJson(raw))
      case _ => longJson(raw)
    }

  private def fixed64As(declared: Option[ProtoField], raw: Long): Json =
    declared.map(_.fieldType) match {
      case Some(ProtoType.Double) => doubleJson(java.lang.Double.longBitsToDouble(raw))
      case Some(ProtoType.SFixed64) => longJson(raw)
      case Some(ProtoType.Fixed64) => unsignedJson(raw)
      case _ => unsignedJson(raw)
    }

  private def fixed32As(declared: Option[ProtoField], raw: Int): Json =
    declared.map(_.fieldType) match {
      case Some(ProtoType.Float) => doubleJson(java.lang.Float.intBitsToFloat(raw).toDouble)
      case Some(ProtoType.SFixed32) => Json.fromInt(raw)
      case Some(ProtoType.Fixed32) => Json.fromLong(raw & 0xffffffffL)
      case _ => Json.fromLong(raw & 0xffffffffL)
    }

  /** 64-bit integers as strings, which is Protobuf's canonical JSON mapping.
    *
    * A JSON number is a double to most readers, and a double cannot hold every `int64`: an order id of
    * 9007199254740993 would render as ...92, which is a wrong value shown with no warning. A quoted string is
    * exact and is what every other Protobuf tool prints.
    */
  private def longJson(value: Long): Json = Json.fromString(value.toString)

  private def unsignedJson(value: Long): Json = Json.fromString(java.lang.Long.toUnsignedString(value))

  /** A floating-point value, with the two non-finite cases spelled as the canonical mapping spells them —
    * JSON has no literal for either, and dropping them would turn a real value into an absent field.
    */
  private def doubleJson(value: Double): Json =
    if value.isNaN then Json.fromString("NaN")
    else if value.isPosInfinity then Json.fromString("Infinity")
    else if value.isNegInfinity then Json.fromString("-Infinity")
    else Json.fromDoubleOrString(value)

  /** ZigZag: how `sint32` and `sint64` encode negative numbers compactly. */
  private def zigZag(raw: Long): Long = (raw >>> 1) ^ -(raw & 1L)

  /** References have already been bound to the lexical symbol, before deciding packability. */
  private def resolve(file: ProtoFile, field: ProtoField, reference: String): Either[String, ProtoMessage] =
    field.mapEntry
      .orElse(file.messagesByName.get(reference.stripPrefix(".")))
      .toRight(unresolved(file, field, reference))

  private def unresolved(file: ProtoFile, field: ProtoField, reference: String): String =
    if file.imports.nonEmpty then
      s"field '${field.name}' has type '$reference', which this schema imports from " +
        s"${file.imports.mkString(", ")}. KUI decodes a Protobuf schema on its own and does not follow a " +
        "schema's references yet, so a message using an imported type cannot be decoded"
    else
      s"field '${field.name}' has type '$reference', and the schema does not declare it. The registry " +
        "returned a schema that is not self-contained"

  /** A varint, and the offset after it.
    *
    * Ten bytes is the maximum: a 64-bit value in seven-bit groups. An eleventh continuation bit means the
    * bytes are not a varint, and saying so is better than looping until the array ends.
    */
  private def readVarint(bytes: Array[Byte], offset: Int): Either[String, (Long, Int)] =
    readVarint(bytes, offset, bytes.length)

  private def readVarint(bytes: Array[Byte], offset: Int, until: Int): Either[String, (Long, Int)] = {
    @tailrec
    def loop(index: Int, shift: Int, acc: Long): Either[String, (Long, Int)] =
      if index >= until then
        Left("the record ends in the middle of a number; it is truncated or is not Protobuf")
      else if shift >= 70 || (shift == 63 && (bytes(index) & 0xfe) != 0) then
        Left("the record contains a ten-byte number that never ends; these bytes are not Protobuf")
      else {
        val byte = bytes(index)
        val next = acc | ((byte & 0x7fL) << shift)
        if (byte & 0x80) == 0 then Right((next, index + 1))
        else loop(index + 1, shift + 7, next)
      }

    loop(offset, 0, 0L)
  }

  /** A fixed-width little-endian value, and the offset after it. */
  private def readFixed(
      bytes: Array[Byte],
      offset: Int,
      width: Int,
      until: Int
  ): Either[String, (Long, Int)] =
    if width > until - offset then
      Left(s"the record ends in the middle of a $width-byte value; it is truncated or is not Protobuf")
    else {
      val value =
        (0 until width).foldLeft(0L)((acc, index) => acc | ((bytes(offset + index) & 0xffL) << (8 * index)))
      Right((value, offset + width))
    }

  private def text(bytes: Array[Byte], from: Int, until: Int): String =
    new String(bytes, from, until - from, StandardCharsets.UTF_8)

  private def base64(bytes: Array[Byte], from: Int, until: Int): String =
    Base64.getEncoder.encodeToString(bytes.slice(from, until))

  /** The bytes as text, when they are text: valid UTF-8 with no control characters in it.
    *
    * The control-character test is what stops a nested message from being shown as mojibake — a message's
    * encoding starts with a small tag byte, which decodes to a control character, so this rejects almost
    * every non-string run while accepting almost every real string.
    */
  private def printable(bytes: Array[Byte], from: Int, until: Int): Option[String] = {
    val candidate = text(bytes, from, until)
    val clean = candidate.forall(character =>
      !character.isControl || character == '\n' || character == '\t' || character == '\r'
    )
    Option.when(clean && !candidate.contains('�'))(candidate)
  }
}
