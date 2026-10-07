package kui.ksql.domain

/** What a statement *is*, which decides which endpoint may answer it.
  *
  * The three cases are not stylistic: they are three different promises about when the answer ends.
  *
  *   - a [[StatementShape.PushQuery]] never ends. `SELECT … EMIT CHANGES` emits a row every time one arrives
  *     on the underlying topic and runs until the client goes away or the budget expires (ADR-035), so it can
  *     only be answered by the stream endpoint. Answering it from the JSON endpoint would mean buffering an
  *     unbounded result into one document, which is a request that never returns and a heap that never stops
  *     growing.
  *   - a [[StatementShape.PullQuery]] ends. `SELECT … FROM t WHERE k = 'x'` reads the materialised state once
  *     and answers rows.
  *   - everything else is a [[StatementShape.Statement]]: DDL and DML, which answer a *status* rather than
  *     rows. ADR-055 §4 is what puts both answers on one DTO with a discriminator rather than on two.
  */
enum StatementShape {
  case PushQuery, PullQuery, Statement

  def wire: String = this match {
    case PushQuery => "push_query"
    case PullQuery => "pull_query"
    case Statement => "statement"
  }
}

object StatementShape {

  def fromWire(raw: String): Option[StatementShape] = values.find(_.wire == raw)

  given CanEqual[StatementShape, StatementShape] = CanEqual.derived
}

/** Why a piece of text is not a statement this service will send anywhere.
  *
  * Each case is a different sentence for the person who typed it, and that is the reason there are three
  * rather than one `Invalid`: *"you typed nothing"*, *"send one statement at a time"* and *"end it with a
  * semicolon"* are three things to do next, and a single message would have to say all three every time.
  */
enum StatementProblem(val message: String) {

  case Empty
      extends StatementProblem(
        "a statement is required; the editor sent nothing but whitespace and comments"
      )

  /** More than one statement in one request.
    *
    * Refused rather than forwarded, and the reason is the whole of ADR-045's protection: this service decides
    * whether a request needs a plan token by classifying **the** statement it was given. A request carrying
    * `CREATE STREAM a AS SELECT * FROM b; DROP STREAM c DELETE TOPIC;` has two, and any rule that classified
    * "the request" would either refuse harmless batches or wave a topic deletion through behind a create. One
    * statement per request is the only shape in which the classification is sound.
    */
  case Multiple
      extends StatementProblem(
        "one statement per request; this service classifies the statement it is given in order to " +
          "decide whether it needs a confirmation, and a batch has no single classification"
      )

  case TooLong
      extends StatementProblem(
        s"a statement may be at most ${KsqlStatement.MaxLength} characters"
      )

  case UnsupportedSyntax
      extends StatementProblem(
        "unterminated or unsupported SQL quoting/comment syntax; use doubled quotes, not backslash escapes"
      )
}

object StatementProblem {
  given CanEqual[StatementProblem, StatementProblem] = CanEqual.derived
}

/** One ksqlDB statement, already classified.
  *
  * ==Why the classification is in the domain and not in the adapter==
  *
  * Two decisions this whole service is shaped around are made here: *which endpoint may answer this* and
  * *does this need a plan token*. Both are rules about what a statement means, both have to be identical
  * wherever they are asked, and a rule made inside an HTTP adapter is one a use case cannot assert. The
  * adapter's job is to send the text; this type's job is to say what the text is.
  *
  * ==What this parser does and does not understand==
  *
  * It is a **classifier, not a SQL parser**, and the line matters because the classification gates a
  * destructive operation. It strips `--` line comments and the slash-star block comments, it knows that a `;`
  * inside a single-quoted literal does not end a statement, and it then looks at the leading keyword and at
  * whether the remaining text contains `EMIT CHANGES` or `DELETE TOPIC` outside quotes. That is enough to be
  * *conservative* in the direction that matters: anything it cannot confidently call harmless it calls
  * destructive, and a statement it cannot split into exactly one is refused rather than guessed at. ADR-055
  * §6 states the limits and the one case that would change them.
  */
final case class KsqlStatement private (
    text: String,
    shape: StatementShape,
    destructive: Boolean,
    target: Option[String],
    targetKind: Option[KsqlObjectKind]
) {

  /** The text as it will be sent and as a plan token signs it: trimmed, with a single trailing `;`.
    *
    * Canonical rather than verbatim because the token binds the *statement*, and a token minted for
    * `DROP STREAM orders DELETE TOPIC;` that could not be spent on the same statement with a trailing newline
    * would make the confirm step fail for a reason nobody could see. Whitespace inside the statement is left
    * exactly as typed: ksqlDB echoes statements back in error messages, and reformatting somebody's SQL so
    * that the server's own error quotes text they did not write is worse than a long line.
    */
  def canonical: String = text

  /** Whether this statement has to be answered by the stream endpoint rather than by the JSON one. */
  def push: Boolean = shape == StatementShape.PushQuery

  /** Whether the answer is rows rather than a status. */
  def query: Boolean = shape != StatementShape.Statement
}

object KsqlStatement {

  /** The most characters a statement may have.
    *
    * A statement editor is a text box and a text box is a place people paste things. Sixteen kilobytes is far
    * past any hand-written ksqlDB statement — the longest in Confluent's own documentation is under one — and
    * it is the bound that stops a paste of a log file becoming a request this service forwards to a server
    * that then has to parse it.
    */
  val MaxLength: Int = 16384

  /** The keyword that makes a `SELECT` unbounded. Two words, matched with any run of whitespace between them,
    * because `EMIT\n  CHANGES` is what a formatted statement looks like.
    */
  private val EmitChanges = """(?is)\bEMIT\s+CHANGES\b""".r

  /** The clause that makes a `DROP` destroy data outside ksqlDB.
    *
    * `DROP STREAM orders;` removes ksqlDB's view of a topic and leaves the topic — and every record in it —
    * alone. `DROP STREAM orders DELETE TOPIC;` deletes the Kafka topic, which is the only statement in
    * ksqlDB's language that destroys records, and is therefore the one ADR-045 applies to.
    */
  private val DeleteTopic = """(?is)\bDELETE\s+TOPIC\b""".r

  private val SelectLeading = """(?is)^\s*SELECT\b.*""".r

  private val DropLeading = """(?is)^\s*DROP\b.*""".r

  /** The one way a statement is built. `Left` carries a sentence for the person who typed it.
    *
    * There is no public constructor and no `unsafe`: every `KsqlStatement` in this service has been through
    * this function, so "was this classified" is answered by the type rather than by review.
    */
  def parse(raw: String): Either[StatementProblem, KsqlStatement] =
    if raw.length > MaxLength then Left(StatementProblem.TooLong)
    else {
      StatementLexer.scan(raw).flatMap {
        case Nil => Left(StatementProblem.Empty)
        case single :: Nil => Right(classifyTokens(single))
        case _ => Left(StatementProblem.Multiple)
      }
    }

  private def classifyTokens(statement: StatementLexer.Statement): KsqlStatement = {
    val body = statement.text.trim
    val quoteless = statement.tokens.map(token => if token.keyword then token.text else "?").mkString(" ")

    val shape =
      if !SelectLeading.matches(quoteless) then StatementShape.Statement
      else if EmitChanges.findFirstIn(quoteless).isDefined then StatementShape.PushQuery
      else StatementShape.PullQuery

    KsqlStatement(
      text = if body.endsWith(";") then body else s"$body;",
      shape = shape,
      // A `DROP … DELETE TOPIC` and nothing else. The test is on the comment-stripped, literal-stripped
      // text so that neither `-- DELETE TOPIC` nor `WHERE note = 'DELETE TOPIC'` can make a harmless
      // statement ask for a confirmation nobody can give it — and, far more importantly, so that neither
      // can hide a real one. `StatementsSuite` runs both directions.
      destructive = DropLeading.matches(quoteless) && DeleteTopic.findFirstIn(quoteless).isDefined,
      target = statement.dropTarget,
      targetKind = statement.tokens.lift(1).filter(_.keyword).flatMap { token =>
        if token.text.equalsIgnoreCase("STREAM") then Some(KsqlObjectKind.Stream)
        else if token.text.equalsIgnoreCase("TABLE") then Some(KsqlObjectKind.Table)
        else None
      }
    )
  }

  given CanEqual[KsqlStatement, KsqlStatement] = CanEqual.derived
}

/** What a statement that finished produced.
  *
  * Two cases, because ksqlDB's two answers are genuinely different documents and flattening them into one
  * with everything optional would make *"no rows"* and *"this statement does not return rows"*
  * indistinguishable — which is the class of invented emptiness this product is built against.
  *
  * ADR-055 §4 is this decision, and the wire's discriminator is [[StatementOutcome.wire]].
  */
enum StatementOutcome {

  /** A pull query's answer: the column names the server declared, and the rows under them.
    *
    * `rows` being empty is a **measured** emptiness — the query ran and matched nothing — and the browser
    * must draw it as such. That is only true because this case exists: a statement that returns no rows at
    * all lands on [[Status]], not here with an empty list.
    */
  case Rows(columns: List[String], rows: List[List[Option[String]]])

  /** A DDL or DML statement's answer: the server's own sentence about what it did.
    *
    * The sentence is ksqlDB's, verbatim — *"Stream created and running"* — and never one KUI composed, for
    * `SCREENS-V4.md` §7.7's reason applied one service over: the words on the screen after a statement ran
    * are the server's report of what happened, and a sentence KUI wrote would be a guess wearing the server's
    * clothes.
    */
  case Status(message: String, entity: Option[String])

  /** Accepted or running, but not yet observed in a terminal command state. */
  case Pending(message: String, entity: Option[String])

  def wire: String = this match {
    case Rows(_, _) => "rows"
    case Status(_, _) => "status"
    case Pending(_, _) => "pending"
  }
}

object StatementOutcome {
  given CanEqual[StatementOutcome, StatementOutcome] = CanEqual.derived
}

/** One row of a push query, as it leaves the server.
  *
  * Rendered text and not a typed record, for `DecodedRecord`'s reason one service over: rule A1 keeps circe
  * out of this module, and a push query's schema is whatever the statement selected. The columns are carried
  * once, in [[QueryFrame.Header]], rather than repeated on every row — a push query emitting a thousand rows
  * would otherwise send its column names a thousand times.
  *
  * @param values
  *   one entry per column, in the header's order. `None` is a **SQL NULL**, and it is an `Option` rather than
  *   an empty or a literal `"null"` for this product's central reason: a cell with no value is not a cell
  *   whose value is the four characters `null`, and a table that drew the two the same way would be inventing
  *   a measurement. It reaches the browser as JSON `null`.
  */
final case class QueryRow(values: List[Option[String]])

object QueryRow {
  given CanEqual[QueryRow, QueryRow] = CanEqual.derived
}

/** What a push query emits, in order.
  *
  * The header arrives first and exactly once, which is what makes this stream produce a frame immediately
  * rather than whenever the next record happens to be produced. House rule 16 asks a stream for a frame that
  * can be made to arrive; ADR-055 §5 records that this one's *first* frame arrives as soon as the server
  * accepts the query, and that the row frames after it arrive when something is produced to the underlying
  * topic.
  */
enum QueryFrame {
  case Header(columns: List[String])
  case Row(row: QueryRow)

  /** The server itself finished the query.
    *
    * A push query that ends on its own has run out of something — a `LIMIT` was reached, the server was
    * shutting down — and that is a different fact from KUI's own stream budget expiring, which is what
    * happens to a push query nobody closed. ADR-035's `done` event carries the difference as `exhausted`
    * versus `budget`, and a client that was told "finished" for both would report a topic as having stopped
    * producing when in fact the tab had simply been open for five minutes.
    */
  case Ended(exhausted: Boolean)
}

object QueryFrame {
  given CanEqual[QueryFrame, QueryFrame] = CanEqual.derived
}
