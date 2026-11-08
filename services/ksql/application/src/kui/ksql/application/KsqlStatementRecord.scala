package kui.ksql.application

import java.time.Instant

import cats.Applicative

import kui.kernel.ClusterId
import kui.ksql.domain.StatementShape
import kui.security.Principal
import kui.security.audit.MutationOutcome

/** One statement, attempted or run.
  *
  * ==Why this is not a `MutationRecord`==
  *
  * ADR-047 §3 requires one audit record per mutation and `kui.security.audit.MutationRecord` is that record.
  * Its `kind` is a `MutationKind`, a sealed enum in `libs/security-core` with twelve cases and none for a
  * ksqlDB statement. Wave 8's partition gives `libs/security-core` to an adversarial packet, not to this one,
  * so this packet cannot add the thirteenth, and ADR-051 §5 already settled what a service does in that
  * position: it does not invent a second vocabulary locally, and it does not reuse the nearest case, because
  * either produces an audit trail whose answer to "what changed today" depends on which service did it.
  *
  * What it does instead is `ConnectorOperationRecord`'s answer, one service over, which is itself
  * `AcknowledgementRecord`'s: a **second record type** that shares the vocabulary rather than forking it.
  * This record shares `MutationOutcome`, so "how it ended" is one enum across the whole trail; it renders its
  * principal through `AuditPrincipal`, so "who" has one spelling; and the sink that writes it writes
  * `LoggingAuditSink`'s own field names, imported from that object rather than retyped.
  *
  * The line that would remove this type is asserted rather than described: `MutationKindGapSuite` fails the
  * day `MutationKind` grows a case whose operation starts with `ksql.`, which is the day this record
  * collapses into `MutationRecord` and this file is deleted.
  *
  * ==What the record carries, and what it deliberately does not==
  *
  * It carries the **statement**, because that is the only thing an incident review can act on: "a ksqlDB
  * statement was run on production" names nothing, and the statement names the stream, the table and the
  * topic. It does not carry the rows a query returned — an audit trail is a record of what was done, not a
  * copy of the data, and a pull query's result in a log line is a copy of somebody's records in a place
  * nobody expects to find them. The same rule applies to literals inside the statement: the logging sink
  * writes [[kui.ksql.domain.KsqlStatement.forAudit]] rather than this field verbatim, so a `WITH` property or
  * a `VALUES` cell is not copied into the log.
  */
final case class KsqlStatementRecord(
    at: Instant,
    principal: Principal,
    cluster: ClusterId,
    statement: String,
    shape: StatementShape,
    destructive: Boolean,
    operation: String,
    outcome: MutationOutcome,
    detail: Map[String, String]
) {

  /** What the audit trail's `resource` column holds.
    *
    * `ksql` and the cluster, because `Resource.Ksql` is unnamed: there is at most one ksqlDB per cluster, so
    * the cluster *is* the identity of the thing that was changed. A statement's own text is a field of its
    * own rather than the resource, because a resource has to be a value somebody can group by.
    */
  def resource: String = KsqlStatementRecord.resourceOf(cluster)
}

object KsqlStatementRecord {

  def resourceOf(cluster: ClusterId): String = s"ksql/${cluster.value}"

  given CanEqual[KsqlStatementRecord, KsqlStatementRecord] = CanEqual.derived
}

/** Where ksqlDB statement records go.
  *
  * The port has `AuditSink`'s contract, which is the part that matters: `record` must not fail the operation
  * it is describing. A sink that could refuse would make the audit trail an availability dependency of
  * running a statement, and an operator who cannot create a stream because a log disk is full is worse off
  * than one whose bookkeeping is a minute late.
  */
trait KsqlStatementSink[F[_]] {
  def record(entry: KsqlStatementRecord): F[Unit]
}

object KsqlStatementSink {

  /** For tests and for a deployment that has deliberately turned auditing off. Never the default. */
  def noop[F[_]: Applicative]: KsqlStatementSink[F] = new KsqlStatementSink[F] {
    def record(entry: KsqlStatementRecord): F[Unit] = Applicative[F].unit
  }
}
