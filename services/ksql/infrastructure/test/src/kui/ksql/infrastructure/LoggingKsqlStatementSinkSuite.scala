package kui.ksql.infrastructure

import java.time.Instant

import cats.effect.IO
import munit.CatsEffectSuite

import kui.kernel.{ClusterId, UserName}
import kui.ksql.application.KsqlStatementRecord
import kui.ksql.domain.StatementShape
import kui.observability.audit.LoggingAuditSink
import kui.security.audit.MutationOutcome
import kui.security.{Principal, PrincipalKind}
import kui.testkit.fakes.FakeStructuredLogger

/** The audit line is the statement, minus the literals that are somebody's secret or somebody's row.
  *
  * `CREATE SOURCE CONNECTOR ... WITH ('connection.password'='...')` and `INSERT ... VALUES (...)` both carry
  * data the rest of the product refuses to put in a log. The line still has to name the connector and the
  * target, which is what an incident review follows.
  */
final class LoggingKsqlStatementSinkSuite extends CatsEffectSuite {

  private val at: Instant = Instant.parse("2026-02-01T10:00:00Z")

  private val connector: String =
    "CREATE SOURCE CONNECTOR jdbc WITH (" +
      "'connector.class'='io.confluent.connect.jdbc.JdbcSourceConnector'," +
      "'connection.url'='jdbc:postgresql://db/app'," +
      "'connection.password'='hunter2');"

  private val insert: String =
    "INSERT INTO profiles (name, email) VALUES ('Ada', 'ada@example.com');"

  private def entry(statement: String): KsqlStatementRecord =
    KsqlStatementRecord(
      at = at,
      principal = Principal(UserName.unsafe("alice"), Set.empty, PrincipalKind.Session),
      cluster = ClusterId.unsafe("prod"),
      statement = statement,
      shape = StatementShape.Statement,
      destructive = false,
      operation = "ksql.statement",
      outcome = MutationOutcome.Succeeded,
      detail = Map.empty
    )

  test("a connector property and an INSERT payload are not written to the audit line") {
    FakeStructuredLogger[IO].flatMap { logger =>
      val sink = LoggingKsqlStatementSink.make[IO](logger)
      for {
        _ <- sink.record(entry(connector))
        _ <- sink.record(entry(insert))
        entries <- logger.entries
      } yield {
        val contexts = entries.map(_.context)
        val rendered = contexts.flatMap(_.values) ++ entries.map(_.message)
        assert(!rendered.exists(_.contains("hunter2")), clue = rendered)
        assert(!rendered.exists(_.contains("ada@example.com")), clue = rendered)
        val statements =
          contexts.map(_.get(s"${LoggingAuditSink.DetailPrefix}${LoggingKsqlStatementSink.StatementDetail}"))
        assert(clue(statements.head).exists(_.contains("jdbc")))
        assert(clue(statements(1)).exists(_.contains("profiles")))
        assertEquals(
          contexts.map(_.get(s"${LoggingAuditSink.DetailPrefix}${LoggingKsqlStatementSink.ShapeDetail}")),
          List(Some("statement"), Some("statement"))
        )
        assertEquals(
          contexts.map(
            _.get(s"${LoggingAuditSink.DetailPrefix}${LoggingKsqlStatementSink.DestructiveDetail}")
          ),
          List(Some("false"), Some("false"))
        )
      }
    }
  }
}
