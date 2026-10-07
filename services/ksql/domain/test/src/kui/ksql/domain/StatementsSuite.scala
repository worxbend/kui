package kui.ksql.domain

import java.util.Locale

import munit.FunSuite

/** What a statement *is*, which is the decision the rest of this service is shaped around.
  *
  * Two of the four things this classifier decides gate something real: `push` decides which endpoint may
  * answer, and `destructive` decides whether a plan token is required before a Kafka topic is deleted. So
  * every case below is run in **both directions** — a statement that must be classified one way, and the
  * near-miss that must not be classified the same way — because a classifier that only ever sees the positive
  * half is one that could return a constant.
  */
final class StatementsSuite extends FunSuite {

  private def parsed(raw: String): KsqlStatement =
    KsqlStatement.parse(raw).fold(problem => fail(s"'$raw' did not parse: ${problem.message}"), identity)

  test("quoted identifiers cannot hide destructive clauses or split statements") {
    List("orders'archive", "orders;archive", "orders--archive", "orders/*archive*/", "orders``archive")
      .foreach { name =>
        val statement = parsed(s"DROP STREAM `$name` DELETE/* comment */ TOPIC;")
        assert(statement.destructive, clue = name)
        assertEquals(statement.target, Some(name.replace("``", "`")))
      }
    assert(!parsed("DROP STREAM `DELETE TOPIC`;").destructive)
    assert(!parsed("SELECT `EMIT CHANGES` FROM ORDERS;").push)
    assert(KsqlStatement.parse("DROP STREAM `orders'archive`; DROP STREAM X DELETE TOPIC;").isLeft)
  }

  test("unterminated quotes and comments and unsupported escapes are refused") {
    List(
      "DROP STREAM `orders DELETE TOPIC;",
      "SELECT 'unfinished",
      "SHOW STREAMS; /* open",
      "SELECT 'a\\'b' FROM X;"
    ).foreach(raw => assert(KsqlStatement.parse(raw).isLeft, clue = raw))
    assertEquals(parsed("SELECT 'it''s; -- fine' FROM X;").shape, StatementShape.PullQuery)
  }

  test("a SELECT with EMIT CHANGES is a push query and a SELECT without one is not") {
    assertEquals(parsed("SELECT * FROM ORDERS EMIT CHANGES;").shape, StatementShape.PushQuery)
    assertEquals(parsed("select * from orders emit changes;").shape, StatementShape.PushQuery)
    // Formatted SQL puts a newline between the two words, which a naive `contains("EMIT CHANGES")` misses
    // — and a push query classified as a pull query is a request that never returns.
    assertEquals(parsed("SELECT *\n  FROM ORDERS\n  EMIT\n  CHANGES;").shape, StatementShape.PushQuery)

    assertEquals(parsed("SELECT * FROM ORDERS WHERE ID = '1';").shape, StatementShape.PullQuery)
    assertEquals(parsed("CREATE STREAM A AS SELECT * FROM B EMIT CHANGES;").shape, StatementShape.Statement)
  }

  test("a statement that is not a SELECT answers a status rather than rows") {
    List(
      "CREATE STREAM ORDERS (ID STRING) WITH (KAFKA_TOPIC='orders', VALUE_FORMAT='JSON');",
      "DROP TABLE USERS;",
      "TERMINATE CSAS_ORDERS_0;",
      "INSERT INTO ORDERS VALUES ('1');",
      "SHOW STREAMS;"
    ).foreach(text => assertEquals(clue(text) -> parsed(text).shape, text -> StatementShape.Statement))

    assert(!parsed("SHOW STREAMS;").query)
    assert(parsed("SELECT * FROM ORDERS;").query)
  }

  test("DROP ... DELETE TOPIC is destructive and DROP on its own is not") {
    // The rule ADR-045 hangs off. `DROP STREAM ORDERS;` removes ksqlDB's view of a topic and leaves every
    // record in it; the same statement with `DELETE TOPIC` destroys them.
    assert(parsed("DROP STREAM ORDERS DELETE TOPIC;").destructive)
    assert(parsed("drop table users delete topic;").destructive)
    assert(parsed("DROP STREAM IF EXISTS ORDERS DELETE TOPIC;").destructive)

    assert(!parsed("DROP STREAM ORDERS;").destructive)
    assert(!parsed("DROP TABLE USERS;").destructive)
    // Not a DROP at all: the words appearing in something else must not demand a confirmation nobody can
    // give, because an editor that asks for one on every statement teaches people to click through them.
    assert(!parsed("CREATE STREAM A AS SELECT 'DELETE TOPIC' AS NOTE FROM B;").destructive)
  }

  test("a comment cannot hide a DELETE TOPIC and cannot invent one") {
    // Both directions, and the first is the one that matters: a classifier that read the comment would let
    // `-- DELETE TOPIC` make a harmless drop look dangerous, and one that stripped too much would let a
    // real clause be hidden behind one.
    assert(!parsed("DROP STREAM ORDERS; -- DELETE TOPIC").destructive)
    assert(!parsed("DROP STREAM ORDERS /* DELETE TOPIC */;").destructive)
    assert(parsed("DROP STREAM ORDERS -- a comment\n DELETE TOPIC;").destructive)
    assert(parsed("DROP STREAM /* which one */ ORDERS DELETE TOPIC;").destructive)
  }

  test("a literal cannot hide a DELETE TOPIC and cannot invent one") {
    assert(!parsed("INSERT INTO NOTES VALUES ('DROP STREAM X DELETE TOPIC');").destructive)
    assert(parsed("DROP STREAM ORDERS DELETE TOPIC;").destructive)
  }

  test("a semicolon inside a literal does not end a statement") {
    // Otherwise `INSERT INTO NOTES VALUES ('a;b');` is two statements, is refused, and the operator is
    // told to send one at a time when they already did.
    val statement = parsed("INSERT INTO NOTES VALUES ('a;b');")

    assertEquals(statement.canonical, "INSERT INTO NOTES VALUES ('a;b');")
  }

  test("more than one statement is refused, because a batch has no single classification") {
    // The property the whole plan-token flow rests on: this service decides whether a request needs a
    // confirmation by classifying the statement it was given, and a request carrying two has no such
    // classification. A batch that was forwarded could hide a topic deletion behind a create.
    assertEquals(
      KsqlStatement.parse("CREATE STREAM A AS SELECT * FROM B; DROP STREAM C DELETE TOPIC;"),
      Left(StatementProblem.Multiple)
    )
    assertEquals(KsqlStatement.parse("SHOW STREAMS; SHOW TABLES;"), Left(StatementProblem.Multiple))

    // A trailing semicolon is not a second statement, and neither is trailing whitespace.
    assert(KsqlStatement.parse("SHOW STREAMS;   \n").isRight)
  }

  test("nothing but whitespace and comments is refused with a sentence about what to do") {
    assertEquals(KsqlStatement.parse("   \n\t "), Left(StatementProblem.Empty))
    assertEquals(KsqlStatement.parse("-- just a note\n"), Left(StatementProblem.Empty))
    assertEquals(KsqlStatement.parse("/* nothing here */"), Left(StatementProblem.Empty))
    assert(clue(StatementProblem.Empty.message).nonEmpty)
  }

  test("a statement past the length bound is refused before anything parses it") {
    val huge = "SELECT * FROM ORDERS WHERE NOTE = '" + "x" * KsqlStatement.MaxLength + "';"

    assertEquals(KsqlStatement.parse(huge), Left(StatementProblem.TooLong))
    assert(clue(StatementProblem.TooLong.message).contains(KsqlStatement.MaxLength.toString))
    // And the bound is not so tight that an ordinary statement trips it.
    assert(KsqlStatement.parse("SELECT * FROM ORDERS;").isRight)
  }

  test("the canonical text is what a token signs, so a trailing newline cannot invalidate a confirmation") {
    val typed = parsed("DROP STREAM ORDERS DELETE TOPIC")
    val pasted = parsed("  DROP STREAM ORDERS DELETE TOPIC;  \n")

    assertEquals(typed.canonical, "DROP STREAM ORDERS DELETE TOPIC;")
    assertEquals(typed.canonical, pasted.canonical)
  }

  test("the statement's own whitespace is left exactly as typed") {
    // ksqlDB echoes statements back in its error messages. Reformatting somebody's SQL so that the
    // server's own error quotes text they did not write is worse than a long line.
    val text = "SELECT ID,\n       TOTAL\n  FROM ORDERS;"

    assertEquals(parsed(text).canonical, text)
  }

  test("the object a DROP names is recovered, and a statement that is not a DROP names none") {
    // The target decides nothing — `destructive` does not depend on it — and it is what lets a plan say
    // *which* Kafka topic would be deleted rather than only that one would.
    assertEquals(parsed("DROP STREAM ORDERS DELETE TOPIC;").target, Some("ORDERS"))
    assertEquals(parsed("drop stream orders delete topic;").target, Some("ORDERS"))
    assertEquals(parsed("DROP TABLE IF EXISTS users;").target, Some("USERS"))
    assertEquals(parsed("DROP STREAM `MixedCase` DELETE TOPIC;").target, Some("MixedCase"))
    assertEquals(parsed("SELECT * FROM ORDERS;").target, None)
  }

  test("an unquoted DROP target is normalized independently of the host locale") {
    val previous = Locale.getDefault
    try {
      Locale.setDefault(Locale.forLanguageTag("tr-TR"))
      assertEquals(parsed("DROP STREAM id DELETE TOPIC;").target, Some("ID"))
    } finally Locale.setDefault(previous)
  }

  test("only a DROP is destructive, however loudly something else says DELETE TOPIC") {
    // W9-A1: `destructive` is a conjunction and only the right-hand half was held. Every negative case in
    // this file gets there by having the words stripped — a comment, a literal — so neutralising
    // `DropLeading.matches` left all seventeen green. The conjunct is what stops a confirmation being
    // demanded for a statement that destroys nothing, which the header of this file and
    // `StatementProblem`'s own scaladoc both argue is how a reader is taught to click past the one that
    // does.
    // Double quotes are ksqlDB's *identifier* quotes, so `withoutLiterals` — which blanks single-quoted
    // literals and nothing else — leaves this text intact and `DELETE\\s+TOPIC` matches inside it. Being a
    // `CREATE` is the only reason it is harmless, which is precisely the half that was untested.
    val quoted = """CREATE STREAM AUDIT AS SELECT * FROM EVENTS WHERE NOTE = "DELETE TOPIC";"""
    assert(!parsed(quoted).destructive)
    assert(!parsed("""INSERT INTO AUDIT (NOTE) VALUES ("DELETE TOPIC");""").destructive)
    // And the near-miss in the other direction still is one.
    assert(parsed("DROP STREAM ORDERS DELETE TOPIC;").destructive)
  }

  test("the bound admits a statement of exactly MaxLength characters and refuses the one after it") {
    // W9-A1: `raw.length > MaxLength` had no case at its own boundary — the only long fixture is thirty-six
    // characters past it, so `>` and `>=` were indistinguishable. A cap whose edge is untested is a cap
    // whose edge moves silently, and this one is quoted verbatim in the sentence the operator is shown.
    val prefix = "SELECT 1 FROM ORDERS WHERE NOTE = '"
    val suffix = "';"
    val exact = prefix + "x" * (KsqlStatement.MaxLength - prefix.length - suffix.length) + suffix
    assertEquals(clue(exact.length), KsqlStatement.MaxLength)

    assert(KsqlStatement.parse(exact).isRight, clue = KsqlStatement.parse(exact))
    assertEquals(KsqlStatement.parse(exact + " "), Left(StatementProblem.TooLong))
  }

  test("a DROP whose target cannot be read is still destructive") {
    // The conservative direction. A statement this classifier cannot pick a name out of must not become a
    // statement it treats as harmless, because the harmless branch is the one with no confirmation.
    val odd = parsed("DROP  /* odd */ STREAM  ORDERS  DELETE  TOPIC ;")

    assert(odd.destructive)
  }

  test("the shape's wire spelling is the one the browser switches on") {
    assertEquals(StatementShape.values.map(_.wire).toList, List("push_query", "pull_query", "statement"))
    StatementShape.values.foreach(shape => assertEquals(StatementShape.fromWire(shape.wire), Some(shape)))
    assertEquals(StatementShape.fromWire("PUSH_QUERY"), None)
  }

  test("an outcome's wire spelling is the discriminator a browser reads, and there are exactly two") {
    assertEquals(StatementOutcome.Rows(Nil, Nil).wire, "rows")
    assertEquals(StatementOutcome.Status("done", None).wire, "status")
  }

  test("a query frame's end reason distinguishes the server finishing from KUI's budget expiring") {
    assertEquals(QueryFrame.Ended(exhausted = true), QueryFrame.Ended(true))
    assertNotEquals(QueryFrame.Ended(exhausted = true), QueryFrame.Ended(false))
  }

  test("a row's null cell is None, and None is not the string null") {
    val row = QueryRow(List(Some("17"), None))

    assertEquals(row.values.last, None)
    assertNotEquals(row.values.last, Some("null"))
  }
}
