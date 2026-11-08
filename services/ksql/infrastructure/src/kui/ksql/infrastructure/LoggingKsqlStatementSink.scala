package kui.ksql.infrastructure

import cats.effect.kernel.Sync
import cats.syntax.all.*
import org.typelevel.log4cats.StructuredLogger

import kui.ksql.application.{KsqlStatementRecord, KsqlStatementSink}
import kui.ksql.domain.KsqlStatement
import kui.observability.audit.LoggingAuditSink
import kui.security.audit.AuditPrincipal

/** One structured log line per ksqlDB statement, in the same trail every other mutation writes to.
  *
  * ==The field names are imported, not retyped==
  *
  * `LoggingAuditSink.Field` is the vocabulary of KUI's audit trail — `audit.operation`, `audit.cluster`,
  * `audit.resource`, `audit.principal`, `audit.principal_kind`, `audit.outcome` — and this sink writes those
  * exact keys by referring to that object rather than by spelling the strings again. That is the whole
  * defence against the drift ADR-051 §5 refuses to create: a ksqlDB statement is a second record *type*, and
  * a second record type that wrote `ksql.who` instead of `audit.principal` would be a second audit trail,
  * which is the thing nobody can query.
  *
  * ==The statement is a detail field, and it is the point of the line==
  *
  * `audit.detail.statement` carries the statement with quoted literals inside `WITH (...)` and `VALUES (...)`
  * replaced by `'***'` (`KsqlStatement.forAudit`). "A ksqlDB statement was run on production" names nothing
  * an incident review can follow; the statement names the stream, the table and — for the one statement in
  * ksqlDB's language that destroys records — the topic. It does not name a connector password or the cells of
  * an `INSERT`. `audit.detail.destructive` carries whether it was that one destructive statement, so the
  * trail can be filtered without re-parsing every line's SQL.
  *
  * `audit.before` and `audit.after` are absent, and their absence is a fact rather than an omission. The two
  * fields hold a scalar the operation moved, and a ksqlDB statement moves no scalar: a `CREATE STREAM`
  * creates an object, and writing a stream's name into a field a reader diffs would put a value there that
  * cannot be compared to anything.
  *
  * ==It never fails the operation it is recording==
  *
  * Every failure inside it is caught and dropped, for `LoggingAuditSink`'s stated reason: a sink that could
  * refuse would one day stop an operator creating a stream because a log disk was full.
  */
object LoggingKsqlStatementSink {

  /** The detail keys this sink adds, named once so that a case can assert them rather than a copy of them. */
  val StatementDetail: String = "statement"
  val DestructiveDetail: String = "destructive"
  val ShapeDetail: String = "shape"

  def make[F[_]: Sync](logger: StructuredLogger[F]): KsqlStatementSink[F] =
    new KsqlStatementSink[F] {

      def record(entry: KsqlStatementRecord): F[Unit] = {
        val details =
          Map(
            StatementDetail -> KsqlStatement.forAudit(entry.statement),
            ShapeDetail -> entry.shape.wire,
            DestructiveDetail -> entry.destructive.toString
          ) ++ entry.detail

        val fields =
          Map(
            LoggingAuditSink.Field.Operation -> entry.operation,
            LoggingAuditSink.Field.Cluster -> entry.cluster.value,
            LoggingAuditSink.Field.Resource -> entry.resource,
            LoggingAuditSink.Field.Principal -> AuditPrincipal.render(entry.principal),
            LoggingAuditSink.Field.PrincipalKind -> AuditPrincipal.kindOf(entry.principal),
            LoggingAuditSink.Field.Outcome -> entry.outcome.label
          ) ++ details.map((key, value) => s"${LoggingAuditSink.DetailPrefix}$key" -> value)

        logger
          .info(fields)(s"${entry.operation} on ${entry.resource}: ${entry.outcome.label}")
          .handleError(_ => ())
      }
    }
}
