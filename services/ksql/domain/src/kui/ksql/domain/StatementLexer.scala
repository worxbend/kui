package kui.ksql.domain

import java.util.Locale

import scala.annotation.tailrec

/** One lexical pass owns quoting, comments, statement boundaries and keyword visibility. */
private[domain] object StatementLexer {
  final case class Token(
      text: String,
      keyword: Boolean,
      identifier: Boolean,
      from: Int,
      until: Int,
      stringLiteral: Boolean
  )
  final case class Statement(text: String, tokens: List[Token]) {
    def dropTarget: Option[String] = {
      val words = tokens.map(t => if t.keyword then t.text.toUpperCase(Locale.ROOT) else "?")
      if words.headOption.contains("DROP") && words.lift(1).exists(w => w == "STREAM" || w == "TABLE") then {
        val index = if words.slice(2, 4) == List("IF", "EXISTS") then 4 else 2
        tokens
          .lift(index)
          .filter(_.identifier)
          .map(t => if t.keyword then t.text.toUpperCase(Locale.ROOT) else t.text)
      } else None
    }
  }

  def scan(raw: String): Either[StatementProblem, List[Statement]] = {
    def nextIs(index: Int, c: Char): Boolean = index + 1 < raw.length && raw.charAt(index + 1) == c
    def finish(text: List[String], tokens: List[Token], statements: List[Statement]): List[Statement] =
      if tokens.isEmpty then statements
      else Statement(text.reverse.mkString.trim, tokens.reverse) :: statements

    @tailrec
    def blockEnd(index: Int): Option[Int] =
      if index >= raw.length then None
      else if raw.charAt(index) == '/' && nextIs(index, '*') then None
      else if raw.charAt(index) == '*' && nextIs(index, '/') then Some(index + 2)
      else blockEnd(index + 1)

    @tailrec
    def quoted(index: Int, quote: Char, value: List[Char]): Option[(Int, String)] =
      if index >= raw.length || raw.charAt(index) == '\\' then None
      else if raw.charAt(index) != quote then quoted(index + 1, quote, raw.charAt(index) :: value)
      else if nextIs(index, quote) then quoted(index + 2, quote, quote :: value)
      else Some((index + 1, value.reverse.mkString))

    @tailrec
    def loop(
        index: Int,
        text: List[String],
        tokens: List[Token],
        statements: List[Statement]
    ): Either[StatementProblem, List[Statement]] =
      if index >= raw.length then Right(finish(text, tokens, statements).reverse)
      else {
        val c = raw.charAt(index)
        if c == '-' && nextIs(index, '-') then {
          val newline = raw.indexOf('\n', index + 2)
          loop(if newline < 0 then raw.length else newline, " " :: text, tokens, statements)
        } else if c == '/' && nextIs(index, '*') then
          blockEnd(index + 2) match {
            case None => Left(StatementProblem.UnsupportedSyntax)
            case Some(end) => loop(end, " " :: text, tokens, statements)
          }
        else if c == '\'' || c == '`' || c == '"' then
          quoted(index + 1, c, Nil) match {
            case None => Left(StatementProblem.UnsupportedSyntax)
            case Some((end, value)) =>
              loop(
                end,
                raw.substring(index, end) :: text,
                Token(
                  value,
                  keyword = false,
                  identifier = c != '\'',
                  from = index,
                  until = end,
                  stringLiteral = c == '\''
                ) :: tokens,
                statements
              )
          }
        else if c == ';' then loop(index + 1, Nil, Nil, finish(text, tokens, statements))
        else if c.isWhitespace then loop(index + 1, c.toString :: text, tokens, statements)
        else if c.isLetterOrDigit || c == '_' then {
          val boundary = raw.indexWhere(ch => !(ch.isLetterOrDigit || ch == '_' || ch == '.'), index)
          val end = if boundary < 0 then raw.length else boundary
          val word = raw.substring(index, end)
          loop(
            end,
            word :: text,
            Token(
              word,
              keyword = true,
              identifier = true,
              from = index,
              until = end,
              stringLiteral = false
            ) ::
              tokens,
            statements
          )
        } else if c == '\\' || c.isControl then Left(StatementProblem.UnsupportedSyntax)
        else
          loop(
            index + 1,
            c.toString :: text,
            Token(
              c.toString,
              keyword = false,
              identifier = false,
              from = index,
              until = index + 1,
              stringLiteral = false
            ) :: tokens,
            statements
          )
      }
    loop(0, Nil, Nil, Nil)
  }

  /** `text` with quoted literals inside `WITH (...)` and `VALUES (...)` replaced by `'***'`.
    *
    * The replacement is the whole literal, quotes included, so a password or an inserted cell cannot survive
    * by being only part of the token. Spans come from this lexer, which already knows a string from a
    * keyword, so a comment or a backtick identifier is left alone. `Left` means the text could not be
    * scanned; the caller withholds it rather than logging the raw statement.
    */
  def redactForAudit(raw: String): Either[StatementProblem, String] =
    scan(raw).map { statements =>
      val spans = secretSpans(statements.flatMap(_.tokens)).sortBy(_._1)
      val (cursor, chunks) =
        spans.foldLeft((0, List.empty[String])) { case ((at, parts), (start, end)) =>
          if start >= at && end <= raw.length && start < end then
            (end, "'***'" :: raw.substring(at, start) :: parts)
          else (at, parts)
        }
      (raw.substring(cursor) :: chunks).reverse.mkString
    }

  final private case class SpanState(
      depth: Int,
      propertyDepth: Option[Int],
      valuesDepth: Option[Int],
      armProperty: Boolean,
      armValues: Boolean,
      found: List[(Int, Int)]
  )

  private def secretSpans(tokens: List[Token]): List[(Int, Int)] = {
    val end = tokens.foldLeft(SpanState(0, None, None, false, false, Nil)) { (state, token) =>
      if token.keyword && isWord(token, "WITH") then state.copy(armProperty = true)
      else if token.keyword && isWord(token, "VALUES") then state.copy(armValues = true)
      else if token.text == "(" && !token.stringLiteral then {
        val depth = state.depth + 1
        state.copy(
          depth = depth,
          propertyDepth = if state.armProperty then Some(depth) else state.propertyDepth,
          valuesDepth = if state.armValues then Some(depth) else state.valuesDepth,
          // The parenthesis consumes a pending WITH or VALUES. A nested one is not a new clause.
          armProperty = false,
          armValues = false
        )
      } else if token.text == ")" && !token.stringLiteral then {
        val closingValues = state.valuesDepth.contains(state.depth)
        state.copy(
          depth = math.max(0, state.depth - 1),
          propertyDepth = if state.propertyDepth.contains(state.depth) then None else state.propertyDepth,
          valuesDepth = if closingValues then None else state.valuesDepth,
          // A following `(...)` in the same INSERT is another row, not a new clause.
          armValues = closingValues || state.armValues
        )
      } else if token.stringLiteral && covers(state) then
        state.copy(found = (token.from, token.until) :: state.found)
      else state
    }
    end.found.reverse
  }

  private def covers(state: SpanState): Boolean =
    state.propertyDepth.exists(opened => state.depth >= opened) ||
      state.valuesDepth.exists(opened => state.depth >= opened)

  private def isWord(token: Token, expected: String): Boolean =
    token.text.toUpperCase(Locale.ROOT) == expected
}
