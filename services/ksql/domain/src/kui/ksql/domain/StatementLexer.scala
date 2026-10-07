package kui.ksql.domain

import java.util.Locale

import scala.annotation.tailrec

/** One lexical pass owns quoting, comments, statement boundaries and keyword visibility. */
private[domain] object StatementLexer {
  final case class Token(text: String, keyword: Boolean, identifier: Boolean)
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
                Token(value, keyword = false, identifier = c != '\'') :: tokens,
                statements
              )
          }
        else if c == ';' then loop(index + 1, Nil, Nil, finish(text, tokens, statements))
        else if c.isWhitespace then loop(index + 1, c.toString :: text, tokens, statements)
        else if c.isLetterOrDigit || c == '_' then {
          val boundary = raw.indexWhere(ch => !(ch.isLetterOrDigit || ch == '_' || ch == '.'), index)
          val end = if boundary < 0 then raw.length else boundary
          val word = raw.substring(index, end)
          loop(end, word :: text, Token(word, keyword = true, identifier = true) :: tokens, statements)
        } else if c == '\\' || c.isControl then Left(StatementProblem.UnsupportedSyntax)
        else
          loop(
            index + 1,
            c.toString :: text,
            Token(c.toString, keyword = false, identifier = false) :: tokens,
            statements
          )
      }
    loop(0, Nil, Nil, Nil)
  }
}
