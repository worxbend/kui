package kui.ksql.api

import java.time.Instant

import munit.FunSuite

import kui.contracts.Section
import kui.kernel.error.{ApplicationError, InfrastructureError}
import kui.ksql.application.{ExecutedStatement, KsqlListing, StatementPlan}
import kui.ksql.domain.*

/** Application types to wire types, asserted against the functions rather than against a copy of them.
  *
  * Everything here is a decision about honesty rather than about shape, which is why each case names the
  * thing a screen would otherwise say wrongly.
  */
final class KsqlMappingSuite extends FunSuite {

  private val at: Instant = Instant.parse("2026-09-03T10:11:12Z")

  private val statement: KsqlStatement =
    KsqlStatement.parse("DROP STREAM ORDERS DELETE TOPIC;").getOrElse(fail("the fixture did not parse"))

  test("a cluster with no ksqlDB is not_configured and carries no data") {
    val response = KsqlMapping.objects(KsqlListing.NotConfigured, at)

    assertEquals(response.objects.status, "not_configured")
    assertEquals(response.objects.toOption, None)
  }

  test("a server that answered is ok, timestamped with the read") {
    val answered = KsqlListing.Answered(Right(KsqlObjects.of(List(KsqlTestServer.orders), Nil)))
    val response = KsqlMapping.objects(answered, at)

    assertEquals(response.objects.status, "ok")
    assertEquals(response.objects.toOption.map(_.items.map(_.name)), Some(List("ORDERS")))
    response.objects match {
      case Section.Ok(_, fetchedAt) => assertEquals(fetchedAt, at)
      case other => fail(s"expected an ok section, got $other")
    }
  }

  test("a server that did not answer goes through the shared fold, so every screen says it the same way") {
    val down = KsqlListing.Answered(Left(InfrastructureError.Unreachable("ksqldb", "refused")))

    assertEquals(KsqlMapping.objects(down, at).objects.status, "unavailable")
    assertEquals(
      KsqlMapping.section(InfrastructureError.Unreachable("ksqldb", "refused"), at),
      Section.fromEither(Left(InfrastructureError.Unreachable("ksqldb", "refused")), at)
    )
  }

  test("a refusal is forbidden and a deployment gap is not_configured, through the same fold") {
    // Both come off `Section.fromEither`, which is the fold every other section in the product uses, so
    // a browser renders them with what it already has. `Unsupported` landing on `not_configured` rather
    // than on `unavailable` is the shared vocabulary's decision and not this service's: "this deployment
    // has no such thing" is a different sentence from "it is not answering", and the browser hides one
    // and explains the other.
    assertEquals(KsqlMapping.section(ApplicationError.Forbidden("no"), at).status, "forbidden")
    assertEquals(KsqlMapping.section(ApplicationError.Unsupported("none here"), at).status, "not_configured")
    assertEquals(KsqlMapping.section(ApplicationError.Unsupported("none here"), at), Section.NotConfigured)
  }

  test("each kind flattens to the fields that kind has, and to no others") {
    // `KsqlObjectDto`'s header states which fields belong to which kind; this is the function that has to
    // agree with it. A mapping that put a topic on a query would be a silent change to what a screen can
    // draw, and there is nothing else in the tree that would notice.
    val stream = KsqlMapping.objectDto(KsqlObject.Stream("ORDERS", "orders", Some("JSON")))
    val table = KsqlMapping.objectDto(KsqlObject.Table("USERS", "users", None, windowed = true))
    val query = KsqlMapping.objectDto(KsqlObject.Query("CSAS_0", List("X"), "SELECT 1;"))
    val topic = KsqlMapping.objectDto(KsqlObject.Topic("orders", 3, 2))

    assertEquals(
      (stream.kind, stream.topic, stream.format, stream.windowed, stream.statement),
      ("stream", Some("orders"), Some("JSON"), None, None)
    )
    assertEquals(
      (table.kind, table.topic, table.format, table.windowed),
      ("table", Some("users"), None, Some(true))
    )
    assertEquals(
      (query.kind, query.name, query.topic, query.sinks, query.statement),
      ("query", "CSAS_0", None, List("X"), Some("SELECT 1;"))
    )
    assertEquals(
      (topic.kind, topic.topic, topic.partitions, topic.replication),
      ("topic", None, Some(3), Some(2))
    )
  }

  test("a rows answer carries no message and a status answer carries no rows") {
    val rows = KsqlMapping.result(
      ExecutedStatement(
        KsqlStatement.parse("SELECT * FROM ORDERS;").getOrElse(fail("bad fixture")),
        StatementOutcome.Rows(List("ID"), List(List(Some("17")), List(None))),
        at
      )
    )
    val status = KsqlMapping.result(
      ExecutedStatement(statement, StatementOutcome.Status("Stream dropped", Some("stream/ORDERS/drop")), at)
    )

    assertEquals((rows.outcome, rows.message, rows.columns), ("rows", None, List("ID")))
    // A NULL survives the mapping as a NULL: a cell with no value and a cell holding the word "null" are
    // different facts, and the mapping is where one could quietly become the other.
    assertEquals(rows.rows, List(List(Some("17")), List(None)))

    assertEquals((status.outcome, status.columns, status.rows), ("status", Nil, Nil))
    assertEquals(status.message, Some("Stream dropped"))
    assertEquals(status.entity, Some("stream/ORDERS/drop"))
  }

  test("pending commands retain their discriminator and command ID at the API boundary") {
    val pending = KsqlMapping.result(
      ExecutedStatement(statement, StatementOutcome.Pending("queued", Some("stream/ORDERS/drop")), at)
    )
    assertEquals(pending.outcome, "pending")
    assertEquals(pending.entity, Some("stream/ORDERS/drop"))
    assertEquals(pending.message, Some("queued"))
    assertEquals(pending.rows, Nil)
  }

  test("a rows answer that matched nothing is still a rows answer, which is a measured emptiness") {
    val empty = KsqlMapping.result(
      ExecutedStatement(
        KsqlStatement.parse("SELECT * FROM ORDERS;").getOrElse(fail("bad fixture")),
        StatementOutcome.Rows(List("ID"), Nil),
        at
      )
    )

    assertEquals(empty.outcome, "rows")
    assertEquals(empty.rows, Nil)
    assertEquals(empty.columns, List("ID"))
  }

  test("a plan carries both booleans, so a browser draws the warning from the right one") {
    val plan = KsqlMapping.plan(StatementPlan(statement, List("a warning"), Some("token"), Some(at), at))

    assertEquals((plan.destructive, plan.deletesTopic), (true, true))
    assertEquals(plan.token, Some("token"))
    assertEquals(plan.warnings, List("a warning"))
    assertEquals(plan.statement, "DROP STREAM ORDERS DELETE TOPIC;")
  }

  test("a harmless plan carries no token and no expiry") {
    val harmless =
      KsqlStatement.parse("CREATE STREAM A AS SELECT * FROM B;").getOrElse(fail("bad fixture"))
    val plan = KsqlMapping.plan(StatementPlan(harmless, Nil, None, None, at))

    assertEquals((plan.destructive, plan.deletesTopic), (false, false))
    assertEquals((plan.token, plan.expiresAt), (None, None))
  }

  test("the truncation count survives the mapping, so a cut answer says how much it cut") {
    val many =
      (1 to KsqlObjects.MaxObjects + 3).toList.map(index => KsqlObject.Stream(f"S$index%05d", "t", None))
    val dto = KsqlMapping.objectsDto(KsqlObjects.of(many, List("BROKEN")))

    assertEquals(dto.truncated, 3)
    assertEquals(dto.unreadable, List("BROKEN"))
    assertEquals(dto.items.size, KsqlObjects.MaxObjects)
  }

  test("a frame maps to the DTO the golden document was cut from") {
    assertEquals(KsqlMapping.header(List("ID", "TOTAL")).columns, List("ID", "TOTAL"))
    assertEquals(KsqlMapping.row(QueryRow(List(Some("17"), None))).values, List(Some("17"), None))
  }
}
