package kui.security.audit

import java.time.Instant

import cats.Applicative

import kui.kernel.ClusterId
import kui.security.{Principal, PrincipalKind}

/** What kind of change an operation makes to a cluster.
  *
  * The marker ADR-047 requires. It exists from KUI's very first mutation rather than from the milestone that
  * first writes it down, because M5's read-only policy and M6's RBAC both key on this classification, and
  * both are far cheaper as a policy over an existing marker than as a hunt through endpoints that have
  * already shipped.
  *
  * Each service's contract suite enumerates its own endpoints and asserts that every one of them is
  * classified — as a mutation, or explicitly as a read. An unclassified endpoint fails the build.
  */
enum MutationKind(val operation: String) {

  /** A record written to a topic (MP-001). */
  case Produce extends MutationKind("produce")

  /** A range of records copied into another topic (MP-003). Two clusters' worth of consequences and one
    * operation, which is why it is not simply "produce, several times".
    */
  case Resend extends MutationKind("resend")

  /** Records deleted below a new low watermark (MS-008). **Irreversible**: the records below the watermark
    * are gone, and no amount of KUI can bring them back. It is the operation ADR-047 exists for.
    */
  case Purge extends MutationKind("purge")

  /** A topic created, with its partitions, replication factor and configuration (`MT-001`). */
  case CreateTopic extends MutationKind("topic.create")

  /** A topic's dynamic configuration set or reset (`MT-002`). */
  case AlterTopicConfig extends MutationKind("topic.config.alter")

  /** A topic's partition count raised (`MT-003`). **Irreversible**: Kafka has no way to remove a partition,
    * and every future record with an existing key may land somewhere other than the records already written
    * under that key.
    */
  case IncreasePartitions extends MutationKind("topic.partitions.increase")

  /** A topic and every record in it (`MT-004`). **Irreversible.** */
  case DeleteTopic extends MutationKind("topic.delete")

  /** A consumer group's committed offsets moved (`CG-006`). The group reads from somewhere else next time it
    * polls, which is either exactly what the operator wanted or a replay of a week of traffic.
    */
  case ResetOffsets extends MutationKind("consumer.group.offsets.reset")

  /** A consumer group's committed offsets removed for one topic (`CG-008`). */
  case DeleteOffsets extends MutationKind("consumer.group.offsets.delete")

  /** A consumer group removed outright (`CG-007`). */
  case DeleteGroup extends MutationKind("consumer.group.delete")

  /** The Schema Registry's registry-wide compatibility level changed (`SR-005`).
    *
    * It writes nothing to Kafka and deletes nothing, and it is still one of the more consequential things KUI
    * can do: lowering the global level to `NONE` removes the check that stops a producer from publishing a
    * schema no existing consumer can read, for every subject that has not overridden it.
    */
  case SetGlobalCompatibility extends MutationKind("schema.compatibility.global.set")

  /** One subject's compatibility level set, overriding the global one from now on (`SR-005`). */
  case SetSubjectCompatibility extends MutationKind("schema.compatibility.subject.set")

  /** A schema version registered under a subject. */
  case RegisterSchema extends MutationKind("schema.subject.version.register")
}

object MutationKind {
  given CanEqual[MutationKind, MutationKind] = CanEqual.derived
}

/** How a mutation ended. A failed mutation is recorded too — a record of what someone *tried* to do to a
  * production cluster is often the more interesting one.
  */
enum MutationOutcome {
  case Succeeded, Failed, Refused

  /** The operation was cancelled, or timed out, after the request had already gone to the broker.
    *
    * Kafka gives no guarantee that it was *not* applied, so a record claiming either would be a lie. An
    * operator who reads this knows to go and look, which is the only honest thing this case can offer.
    */
  case Unknown

  def label: String = this match {
    case Succeeded => "succeeded"
    case Failed => "failed"
    case Refused => "refused"
    case Unknown => "unknown"
  }
}

object MutationOutcome {
  given CanEqual[MutationOutcome, MutationOutcome] = CanEqual.derived
}

/** One thing that was done, or attempted, to a cluster.
  *
  * @param principal
  *   who did it, as the gateway signed it and this service verified it (ADR-020). A `Principal` and not a
  *   string, because the two facts an audit reader needs — the name, and whether anybody was actually signed
  *   in — are exactly the two the type carries, and a string that flattened them would have to spell the
  *   second one out in prose that every sink then renders slightly differently. Until authentication exists
  *   (M6) every request arrives as [[kui.security.Principal.Anonymous]], and that is an honest record of a
  *   deployment with no login rather than a placeholder somebody invented.
  * @param resource
  *   what was operated on, in the shape an operator recognises: a topic name, a group id, `orders:3`.
  * @param before
  *   the value that was there, where KUI knew it. `None` for an operation that has no before — a produce.
  * @param after
  *   the value that resulted, where KUI knows it.
  * @param detail
  *   anything else worth recording, as short strings. **Never a credential and never a payload**: an audit
  *   log is routinely more widely readable than the data it describes, which is exactly why it must not
  *   contain the data.
  */
final case class MutationRecord(
    at: Instant,
    principal: Principal,
    cluster: ClusterId,
    kind: MutationKind,
    resource: String,
    before: Option[String],
    after: Option[String],
    outcome: MutationOutcome,
    detail: Map[String, String]
)

object MutationRecord {

  given CanEqual[MutationRecord, MutationRecord] = CanEqual.derived
}

/** How a principal is written into an audit trail, in one place.
  *
  * It exists because three services write the same trail. Two of them used to spell "nobody was signed in"
  * differently — one said `system (authentication is not enabled)`, the other `anonymous (authentication is
  * not enabled)` — and a trail that answers "who changed this cluster today" with two names for one absence
  * is a trail nobody can query. This object is where the one spelling lives.
  */
object AuditPrincipal {

  /** The sentence a log line or a viewer shows.
    *
    * An anonymous principal is rendered with the reason attached, because a bare `anonymous` in an audit
    * record reads like a bug in the audit trail rather than like a fact about the deployment. Every other
    * kind renders as the name alone: once there is a login, the name is the answer.
    */
  def render(principal: Principal): String = principal.kind match {
    case PrincipalKind.Anonymous => s"${principal.name.value} (authentication is not enabled)"
    case _ => principal.name.value
  }

  /** The machine-readable half: which way KUI came to believe this identity. Kept beside [[render]] so a sink
    * writes both or neither.
    */
  def kindOf(principal: Principal): String = principal.kind.wire
}

/** Where mutation records go.
  *
  * One port, declared once in `libs/security-core` because three services will write through it and ADR-023
  * already places the audit model here. M3 ships this and a structured-log sink; M5 adds the `__kui_audit`
  * Kafka topic behind the same interface and the viewer that reads it; M6 fills in the principal.
  *
  * `record` must not fail the operation it is describing. A sink that could refuse would make the audit trail
  * a availability dependency of every mutation, which inverts the point: a cluster you cannot change because
  * the audit log is down is worse than a change you find out about a minute late.
  */
trait AuditSink[F[_]] {
  def record(entry: MutationRecord): F[Unit]
}

object AuditSink {

  /** For tests and for a deployment that has deliberately turned auditing off. Never the default. */
  def noop[F[_]: Applicative]: AuditSink[F] = new AuditSink[F] {
    def record(entry: MutationRecord): F[Unit] = Applicative[F].unit
  }
}
