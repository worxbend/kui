package kui.serde.confluent

import scala.annotation.tailrec

/** A `.proto` schema, parsed far enough to decode a payload against it.
  *
  * ==Why KUI parses `.proto` itself==
  *
  * A Protobuf payload is unreadable without its schema: the bytes carry field *numbers* and wire types and no
  * names, no types and no structure. The registry hands back the schema as `.proto` source text, so something
  * has to turn that text into a field table. ADR-014 Amendment 1 recorded that the only maintained dynamic
  * parser is Confluent's `kafka-protobuf-provider`, which is published under the Confluent Community License
  * and only on Confluent's own repository — a licence KUI cannot accept on an operator's behalf and a
  * resolver the build would have to add.
  *
  * The alternative is this file: a parser for the subset of the language a *schema held in a schema registry*
  * actually uses, and `ProtobufPayload`'s decoder over it. It adds no dependency and no licence obligation.
  * What it costs is honesty about its limits, which is why the failures below are named rather than silent —
  * see [[ProtoSchema.parse]] and `ProtobufPayload.decode`.
  *
  * ==What is supported==
  *
  *   - `syntax = "proto2"` and `"proto3"`, `package`, and `option` lines (read and ignored).
  *   - `message`, arbitrarily nested, with nested `enum`s.
  *   - fields with `optional`, `required` and `repeated` labels, every scalar type, message and enum types,
  *     and `map<k, v>` (which the language itself defines as a repeated entry message, and which is decoded
  *     that way here).
  *   - `oneof` groups, whose members decode exactly like ordinary optional fields, because that is what they
  *     are on the wire.
  *   - `reserved` statements and field options such as `[packed = true]`, which are parsed and discarded:
  *     packing is detected from the wire type of the bytes actually received, which is what the encoding
  *     rules require of every reader.
  *
  * ==What is not==
  *
  *   - `import`. A schema that imports another names types this text does not contain, and a registry
  *     schema's imports are its *references*, fetched separately. Rather than decode such a message partially
  *     and silently render its imported fields as unknown numbers, [[parse]] keeps the import list so the
  *     decoder can say which file it would have needed.
  *   - `service` and `rpc` definitions, which describe calls rather than data and are skipped.
  *   - proto2 groups, a construct deprecated since 2015 that no registry schema in the wild uses.
  */
final case class ProtoFile(
    packageName: Option[String],
    messages: List[ProtoMessage],
    enums: List[ProtoEnum],
    imports: List[String]
) {

  /** Every message in the file, including nested ones, by fully qualified name (`pkg.Outer.Inner`). */
  lazy val messagesByName: Map[String, ProtoMessage] = {
    def collect(prefix: String, message: ProtoMessage): List[(String, ProtoMessage)] = {
      val qualified = if prefix.isEmpty then message.name else s"$prefix.${message.name}"
      (qualified -> message) :: message.nested.flatMap(collect(qualified, _))
    }
    messages.flatMap(collect(packageName.getOrElse(""), _)).toMap
  }

  /** Every enum in the file, including nested ones, by fully qualified name. */
  lazy val enumsByName: Map[String, ProtoEnum] = {
    def collect(prefix: String, message: ProtoMessage): List[(String, ProtoEnum)] = {
      val qualified = if prefix.isEmpty then message.name else s"$prefix.${message.name}"
      message.enums.map(one => s"$qualified.${one.name}" -> one) ++
        message.nested.flatMap(collect(qualified, _))
    }
    val root = packageName.getOrElse("")
    val topLevel =
      enums.map(one => (if root.isEmpty then one.name else s"$root.${one.name}") -> one)
    (topLevel ++ messages.flatMap(collect(root, _))).toMap
  }

  /** Bind named fields against the shared message/enum namespace before decoding wire types. A leading dot is
    * absolute; otherwise search the enclosing message and its parents, not unrelated symbols with the same
    * suffix. https://protobuf.dev/programming-guides/proto3/#name-resolution
    */
  private[confluent] lazy val resolved: ProtoFile = {
    val symbols = messagesByName.keySet ++ enumsByName.keySet

    def qualify(prefix: String, name: String): String =
      if prefix.isEmpty then name else s"$prefix.$name"

    def reference(scope: String, name: String): String = {
      @tailrec
      def search(prefix: String): String = {
        val candidate = qualify(prefix, name)
        if symbols.contains(candidate) || prefix.isEmpty then s".$candidate"
        else
          search(prefix.lastIndexOf('.') match {
            case -1 => ""
            case index => prefix.take(index)
          })
      }
      if name.startsWith(".") then name else search(scope)
    }

    def message(prefix: String, declared: ProtoMessage): ProtoMessage = {
      val scope = qualify(prefix, declared.name)
      declared.copy(
        fields = declared.fields.map(field =>
          field.copy(
            fieldType = field.fieldType match {
              case ProtoType.Named(name) => ProtoType.Named(reference(scope, name))
              case scalar => scalar
            },
            mapEntry = field.mapEntry.map(message(scope, _))
          )
        ),
        nested = declared.nested.map(message(scope, _))
      )
    }

    copy(messages = messages.map(message(packageName.getOrElse(""), _)))
  }

  /** The message a Confluent message-index path selects.
    *
    * A Protobuf payload does not name its message type; it carries a path of indexes into the file's
    * declaration order — `[0]` is the first top-level message, `[1, 0]` the first nested message of the
    * second. An empty path means the first message, which is the case a producer writes as a single zero byte
    * and is by far the commonest.
    */
  def messageAt(path: List[Int]): Either[String, ProtoMessage] = {
    @tailrec
    def walk(candidates: List[ProtoMessage], remaining: List[Int], found: Option[ProtoMessage]): Either[
      String,
      ProtoMessage
    ] =
      remaining match {
        case Nil =>
          found.orElse(candidates.headOption).toRight("the schema declares no message types")
        case index :: rest =>
          candidates.lift(index) match {
            case None =>
              Left(
                s"the record selects message number $index of its schema, and the schema has " +
                  s"${candidates.size} at that level. The record and the schema do not belong together"
              )
            case Some(message) => walk(message.nested, rest, Some(message))
          }
      }

    walk(messages, path, None)
  }
}

/** One `message`, with its fields flattened: a `oneof`'s members are ordinary fields here because they are
  * ordinary fields on the wire, distinguished only by the encoder's promise to write at most one of them.
  */
final case class ProtoMessage(
    name: String,
    fields: List[ProtoField],
    nested: List[ProtoMessage],
    enums: List[ProtoEnum]
) {
  lazy val byNumber: Map[Int, ProtoField] = fields.map(field => field.number -> field).toMap

  /** Whether this message is a synthetic map entry, which protoc generates for every `map<k, v>` field and
    * which is rendered as a JSON object rather than as a list of `{key, value}` pairs.
    */
  def isMapEntry: Boolean =
    fields.sizeIs == 2 && byNumber.contains(1) && byNumber.contains(2) && name.endsWith("Entry")
}

final case class ProtoEnum(name: String, values: Map[Int, String])

/** How often a field may appear. `Repeated` is the only one the decoder treats differently: it collects. */
enum ProtoLabel {
  case Optional, Required, Repeated
}

object ProtoLabel {
  given CanEqual[ProtoLabel, ProtoLabel] = CanEqual.derived
}

/** A field's declared type: one of the fifteen scalars, or a name to be resolved against the file. */
enum ProtoType {
  case Double, Float, Int32, Int64, UInt32, UInt64, SInt32, SInt64
  case Fixed32, Fixed64, SFixed32, SFixed64, Bool, Str, Bytes
  case Named(reference: String)

  /** Whether values of this type are packed when repeated, which decides how a length-delimited run of a
    * numeric field is read. Every scalar numeric and boolean type is packable; strings, bytes and messages
    * are not.
    */
  def isPackable: Boolean = this match {
    case Str | Bytes | Named(_) => false
    case _ => true
  }
}

object ProtoType {
  given CanEqual[ProtoType, ProtoType] = CanEqual.derived

  /** The scalar spellings, exactly as the language defines them. `string` is `Str` here only because `String`
    * is taken.
    */
  def scalar(word: String): Option[ProtoType] = word match {
    case "double" => Some(Double)
    case "float" => Some(Float)
    case "int32" => Some(Int32)
    case "int64" => Some(Int64)
    case "uint32" => Some(UInt32)
    case "uint64" => Some(UInt64)
    case "sint32" => Some(SInt32)
    case "sint64" => Some(SInt64)
    case "fixed32" => Some(Fixed32)
    case "fixed64" => Some(Fixed64)
    case "sfixed32" => Some(SFixed32)
    case "sfixed64" => Some(SFixed64)
    case "bool" => Some(Bool)
    case "string" => Some(Str)
    case "bytes" => Some(Bytes)
    case _ => None
  }
}

final case class ProtoField(
    number: Int,
    name: String,
    label: ProtoLabel,
    fieldType: ProtoType,
    /** The synthetic entry message of a `map<k, v>` field, which decides that this field renders as an
      * object. Held here rather than looked up by name because protoc's generated entry name is derived from
      * the field name and re-deriving it at decode time would be a second place to get the rule wrong.
      */
    mapEntry: Option[ProtoMessage] = None
)

object ProtoSchema {

  /** Parses `.proto` source text, or says what stopped it.
    *
    * The failure text is written for the operator looking at a record that would not decode, so it names the
    * construct and, where the construct is one KUI does not implement, says so plainly instead of reporting a
    * syntax error against a file that is perfectly valid.
    */
  def parse(source: String): Either[String, ProtoFile] =
    try Right(new Parser(Lexer.tokenize(source)).file())
    catch {
      case failure: ParseFailure => Left(failure.explanation)
    }

  final private class ParseFailure(val explanation: String) extends Exception(explanation)

  // scalafix:off DisableSyntax.throw
  //
  // A recursive-descent parser reports a failure from wherever it happens to be - twelve calls deep in a
  // nested message, halfway through a field - and the alternative to an exception is threading an `Either`
  // through every one of those returns, which triples the size of the parser and hides the grammar inside
  // the error plumbing. The exception is private to this object, is thrown only by `fail`, and is caught by
  // `parse`, which is the only entry point: no `ParseFailure` can escape into a caller, so from outside this
  // file the parser is a total function returning `Either`. The ban is lifted for that one line and turned
  // back on immediately.
  private def fail(explanation: String): Nothing = throw new ParseFailure(explanation)
  // scalafix:on DisableSyntax.throw

  /** Words, punctuation and string literals, with comments removed.
    *
    * A hand-written lexer rather than a regular expression per construct: the two things that actually matter
    * here — a `//` inside a string literal and a `/* */` spanning lines — are exactly what a per-construct
    * regular expression gets wrong, and both appear in real registry schemas.
    */
  // scalafix:off DisableSyntax.var
  //
  // A lexer and a recursive-descent parser are position machines: both are a cursor walking a sequence, and
  // the cursor moves. Written without `var` the same code becomes a fold carrying an index, a builder and a
  // lookahead in a tuple, which does not remove the state -- it only spells it out in a shape that is harder
  // to read and no easier to reason about. Every `var` below is local to one method or to this one private
  // object, none escapes, and nothing outside this file can observe any of them: from the outside
  // `ProtoSchema.parse` is a pure function from a string to an `Either`. The ban is lifted for the parser
  // and turned back on at the end of the file.
  private object Lexer {

    def tokenize(source: String): List[String] = {
      val out = List.newBuilder[String]
      var index = 0
      val length = source.length

      while index < length do {
        val current = source.charAt(index)
        if current == '/' && index + 1 < length && source.charAt(index + 1) == '/' then
          while index < length && source.charAt(index) != '\n' do index += 1
        else if current == '/' && index + 1 < length && source.charAt(index + 1) == '*' then {
          index += 2
          while index + 1 < length && !(source.charAt(index) == '*' && source.charAt(index + 1) == '/') do
            index += 1
          index = math.min(index + 2, length)
        } else if current.isWhitespace then index += 1
        else if current == '"' || current == '\'' then {
          val quote = current
          val start = index + 1
          index += 1
          while index < length && source.charAt(index) != quote do
            index += (if source.charAt(index) == '\\' then 2 else 1)
          out += "\"" + source.substring(start, math.min(index, length))
          index = math.min(index + 1, length)
        } else if isWordCharacter(current) then {
          val start = index
          while index < length && isWordCharacter(source.charAt(index)) do index += 1
          out += source.substring(start, index)
        } else {
          out += current.toString
          index += 1
        }
      }

      out.result()
    }

    private def isWordCharacter(character: Char): Boolean =
      character.isLetterOrDigit || character == '_' || character == '.' || character == '-'
  }

  /** The parser proper: one method per construct, each consuming exactly what it declares.
    *
    * It is written with a mutable cursor over the token list rather than as a combinator chain because the
    * grammar is small and flat, and because every failure has to name what was expected — which a
    * hand-written parser says in one line and a combinator library says in a paragraph nobody can read.
    */
  final private class Parser(initial: List[String]) {

    private var tokens: List[String] = initial

    private def peek: Option[String] = tokens.headOption

    private def next(): String = tokens match {
      case head :: rest =>
        tokens = rest
        head
      case Nil => fail("the schema text ended in the middle of a definition")
    }

    private def expect(token: String): Unit = {
      val found = next()
      if found != token then fail(s"expected '$token' in the schema and found '${describe(found)}'")
    }

    private def describe(token: String): String =
      if token.startsWith("\"") then token.drop(1) else token

    private def accept(token: String): Boolean =
      peek.contains(token) && { val _ = next(); true }

    def file(): ProtoFile = {
      var packageName: Option[String] = None
      val messages = List.newBuilder[ProtoMessage]
      val enums = List.newBuilder[ProtoEnum]
      val imports = List.newBuilder[String]

      while peek.isDefined do
        next() match {
          case "syntax" | "edition" =>
            expect("=")
            val _ = next()
            expect(";")
          case "package" =>
            packageName = Some(next())
            expect(";")
          case "import" =>
            // `public` and `weak` are modifiers on the path that follows.
            val first = next()
            val path = if first == "public" || first == "weak" then next() else first
            imports += describe(path)
            expect(";")
          case "option" => skipStatement()
          case "message" => messages += message()
          case "enum" => enums += enumeration()
          case "service" => skipBlock()
          case ";" => ()
          case other =>
            fail(
              s"'${describe(other)}' is not something KUI's Protobuf reader understands at the top level " +
                "of a schema; it handles syntax, package, import, option, message and enum"
            )
        }

      ProtoFile(packageName, messages.result(), enums.result(), imports.result())
    }

    /** Consumes tokens up to and including the next `;`, for statements whose content does not matter. */
    private def skipStatement(): Unit = {
      var depth = 0
      var done = false
      while !done do
        next() match {
          case "{" => depth += 1
          case "}" => depth -= 1
          case ";" if depth <= 0 => done = true
          case _ => ()
        }
    }

    /** Consumes a `name { ... }` block whose content does not matter, braces balanced. */
    private def skipBlock(): Unit = {
      while !peek.contains("{") do { val _ = next() }
      expect("{")
      var depth = 1
      while depth > 0 do
        next() match {
          case "{" => depth += 1
          case "}" => depth -= 1
          case _ => ()
        }
    }

    private def message(): ProtoMessage = {
      val name = next()
      expect("{")
      val fields = List.newBuilder[ProtoField]
      val nested = List.newBuilder[ProtoMessage]
      // The synthetic entry messages of `map` fields are collected apart from the declared nested types and
      // appended after them, because that is the order protoc puts them in the descriptor — and a record's
      // message-index path counts positions in that order. Interleaving them with the declared types by
      // where the map field happens to appear in the source would decode `[0, 1]` as the wrong message.
      val mapEntries = List.newBuilder[ProtoMessage]
      val enums = List.newBuilder[ProtoEnum]

      while !accept("}") do
        peek match {
          case Some("message") =>
            val _ = next()
            nested += message()
          case Some("enum") =>
            val _ = next()
            enums += enumeration()
          case Some("oneof") =>
            val _ = next()
            val _ = next() // the group's name, which does not survive onto the wire
            expect("{")
            while !accept("}") do
              if peek.contains("option") then { val _ = next(); skipStatement() }
              else fields += field(ProtoLabel.Optional)
          case Some("map") =>
            val _ = next()
            val declaredMap = mapField()
            fields += declaredMap._1
            mapEntries += declaredMap._2
          case Some("reserved") | Some("extensions") | Some("option") =>
            val _ = next()
            skipStatement()
          case Some("repeated") =>
            val _ = next()
            fields += field(ProtoLabel.Repeated)
          case Some("optional") =>
            val _ = next()
            fields += field(ProtoLabel.Optional)
          case Some("required") =>
            val _ = next()
            fields += field(ProtoLabel.Required)
          case Some("group") =>
            fail(
              "this schema uses a proto2 'group', a construct deprecated in 2015 that KUI's Protobuf " +
                "reader does not implement"
            )
          case Some(";") => val _ = next()
          case Some(_) => fields += field(ProtoLabel.Optional)
          case None => fail(s"the schema text ended inside message '$name'")
        }

      ProtoMessage(name, fields.result(), nested.result() ++ mapEntries.result(), enums.result())
    }

    /** `map<key, value> name = n;` as the language defines it: a repeated field of a synthetic entry message
      * with `key` at 1 and `value` at 2. Decoding it as anything else would disagree with every encoder.
      */
    private def mapField(): (ProtoField, ProtoMessage) = {
      expect("<")
      val keyType = typeOf(next())
      expect(",")
      val valueType = typeOf(next())
      expect(">")
      val name = next()
      expect("=")
      val number = fieldNumber(next(), name)
      skipOptionsAndSemicolon()

      val entryName = s"${name.split('_').map(_.capitalize).mkString}Entry"
      val entry = ProtoMessage(
        entryName,
        List(
          ProtoField(1, "key", ProtoLabel.Optional, keyType),
          ProtoField(2, "value", ProtoLabel.Optional, valueType)
        ),
        Nil,
        Nil
      )
      (ProtoField(number, name, ProtoLabel.Repeated, ProtoType.Named(entryName), Some(entry)), entry)
    }

    private def field(label: ProtoLabel): ProtoField = {
      val declared = next()
      if declared == "map" then fail("a 'map' field may not carry a label; write 'map<key, value> name = n;'")
      val fieldType = typeOf(declared)
      val name = next()
      expect("=")
      val number = fieldNumber(next(), name)
      skipOptionsAndSemicolon()
      ProtoField(number, name, label, fieldType)
    }

    private def fieldNumber(raw: String, name: String): Int =
      raw.toIntOption.filter(_ > 0).getOrElse {
        fail(s"field '$name' has the number '$raw', and a Protobuf field number is a positive whole number")
      }

    /** `[deprecated = true, packed = true]` and the closing `;`. Field options do not change how bytes are
      * read: packing is decided by the wire type actually present, which is what the encoding rules require.
      */
    private def skipOptionsAndSemicolon(): Unit = {
      if accept("[") then {
        var depth = 1
        while depth > 0 do
          next() match {
            case "[" => depth += 1
            case "]" => depth -= 1
            case _ => ()
          }
      }
      val _ = accept(";")
    }

    private def typeOf(word: String): ProtoType =
      ProtoType.scalar(word).getOrElse(ProtoType.Named(word))

    private def enumeration(): ProtoEnum = {
      val name = next()
      expect("{")
      val values = List.newBuilder[(Int, String)]
      while !accept("}") do
        peek match {
          case Some("option") | Some("reserved") =>
            val _ = next()
            skipStatement()
          case Some(";") => val _ = next()
          case Some(_) =>
            val valueName = next()
            expect("=")
            val number = next().toIntOption.getOrElse(
              fail(s"enum value '$valueName' of '$name' does not have a whole number")
            )
            skipOptionsAndSemicolon()
            // First declaration wins, which is what `allow_alias` means for a reader: two names for one
            // number, and the one written first is the canonical one.
            values += number -> valueName
          case None => fail(s"the schema text ended inside enum '$name'")
        }
      // First declaration wins over a later alias, so the list is folded rather than turned into a map
      // directly: `toMap` would keep the last entry, which is the alias.
      ProtoEnum(
        name,
        values
          .result()
          .foldLeft(Map.empty[Int, String])((seen, entry) =>
            if seen.contains(entry._1) then seen else seen + entry
          )
      )
    }
  }
}

// scalafix:on DisableSyntax.var
