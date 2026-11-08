package kui.topic.application

import cats.effect.kernel.{Outcome, Temporal}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import org.typelevel.log4cats.StructuredLogger

import kui.kernel.ClusterId
import kui.kernel.error.{ApplicationError, ErrorCode, KuiError}
import kui.security.Principal
import kui.security.audit.{AuditSink, MutationKind, MutationOutcome, MutationRecord}
import kui.topic.domain.{ClusterProfiles, TopicError, TopicMutation}

/** The only way this service changes a cluster (ADR-047).
  *
  * M5 ships KUI's first *topic* mutations — create, configure, add partitions, delete — into a product that
  * has no authentication and no global read-only mode. ADR-047 says they may not ship bare, and names the
  * three things that arrive with them. All three are here:
  *
  *   1. the per-cluster `readOnly` refusal — `KUI-READ-ONLY`, decided **before any Kafka client is touched**,
  *      which on this path means before the admin pool is asked for a client, before a `NewTopic` is built
  *      and before a single byte reaches a broker;
  *   2. the audit record — exactly one per attempt, successful or not, through `AuditSink[F]`;
  *   3. the [[kui.topic.domain.TopicMutation]] marker, which is a parameter of [[guard]] rather than a
  *      convention, so a mutation added without a classification does not compile.
  *
  * The message and consumer services carry the same guard over the same port, and the three are deliberately
  * separate objects rather than one shared one: rule A11 forbids either service seeing another's application
  * layer, and the thing that must not drift — the record, the outcome vocabulary and the port — is declared
  * once in `libs/security-core`, which is where all three read it from.
  *
  * ==Why a successful mutation invalidates the snapshot==
  *
  * The topic list is served from a per-cluster snapshot refreshed on a timer. Without this, an operator who
  * creates a topic is sent back to a list that will not contain it for up to a minute, and the obvious
  * conclusion — that the create silently failed — is wrong and unfalsifiable from the screen. So a successful
  * mutation asks for a re-scrape. It is a request, not a wait: the refresh runs on its own fiber and the
  * response does not depend on it, because a create that blocked on a scrape of a ten-thousand-topic cluster
  * would take the scrape's latency for a fact the operator already knows.
  */
trait MutationGuard[F[_]] {

  /** Wraps one mutation. The order is fixed and tested:
    *
    *   1. resolve the cluster's profile — a cluster this deployment does not have is `KUI-CLUSTER-NOT-FOUND`
    *      and is recorded, because an attempt to change a cluster that is not there is worth having noticed;
    *   1. if the profile says read-only, refuse and record the refusal, without running `op` at all;
    *   1. run `op`;
    *   1. record the outcome — always, including on failure and on cancellation;
    *   1. on success, ask for the cluster's topics to be re-scraped.
    *
    * @param principal
    *   who is doing this, as the gateway signed it and this service verified it. It is a parameter of the
    *   call rather than of [[MutationGuard.make]] because it is a property of the request in flight, and a
    *   guard built once at start-up cannot know it. Until authentication exists every request arrives as
    *   `Principal.Anonymous`, which is an honest record of a deployment with no login.
    * @param resource
    *   what was operated on, in the shape an operator recognises: `orders.v1`.
    * @param detail
    *   short strings worth recording — the partition count a topic was created with, the settings that were
    *   changed. **Never a credential**: an audit log is routinely more widely readable than the thing it
    *   describes.
    */
  def guard[A](
      principal: Principal,
      cluster: ClusterId,
      kind: TopicMutation,
      resource: String,
      detail: Map[String, String] = Map.empty
  )(op: F[Either[TopicError, A]]): F[Either[KuiError, A]]
}

object MutationGuard {

  /** The log context every line this guard writes carries, so an operator can grep one operation out of a
    * busy service.
    */
  val ServiceName: String = "kui-topic"

  /** The audit vocabulary for each of this service's mutations.
    *
    * A total function over the domain's enum rather than a map, so a fifth mutation added to
    * [[kui.topic.domain.TopicMutation]] fails to compile here instead of writing audit records under a
    * silently missing kind.
    */
  def kindOf(mutation: TopicMutation): MutationKind = mutation match {
    case TopicMutation.Create => MutationKind.CreateTopic
    case TopicMutation.AlterConfig => MutationKind.AlterTopicConfig
    case TopicMutation.IncreasePartitions => MutationKind.IncreasePartitions
    case TopicMutation.Delete => MutationKind.DeleteTopic
  }

  def make[F[_]: Temporal](
      profiles: ClusterProfiles[F],
      snapshots: TopicSnapshots[F],
      audit: AuditSink[F],
      logger: StructuredLogger[F],
      /** How a `TopicError` reaches the wire. Passed in rather than imported: `TopicErrors` lives in the api
        * module and rule A3 forbids this layer from seeing it.
        */
      toKui: TopicError => KuiError
  ): MutationGuard[F] =
    new MutationGuard[F] {

      def guard[A](
          principal: Principal,
          cluster: ClusterId,
          kind: TopicMutation,
          resource: String,
          detail: Map[String, String]
      )(op: F[Either[TopicError, A]]): F[Either[KuiError, A]] = {
        val context = Map(
          "service.name" -> ServiceName,
          "cluster.id" -> cluster.value,
          "operation" -> kind.operation,
          "resource" -> resource
        )

        def write(outcome: MutationOutcome, extra: Map[String, String]): F[Unit] =
          for {
            at <- Temporal[F].realTimeInstant
            _ <- audit
              .record(
                MutationRecord(
                  at = at,
                  principal = principal,
                  cluster = cluster,
                  kind = kindOf(kind),
                  resource = resource,
                  // These four operations have no scalar "before" and "after": what a create replaces is
                  // nothing, and what a configuration change replaced is a set of keys, which goes in
                  // `detail` where it can be read. Saying `None` is the honest answer and is different
                  // from saying the value was empty.
                  before = None,
                  after = None,
                  outcome = outcome,
                  detail = detail ++ extra
                )
              )
              // The sink never fails the operation it is recording. A sink that could would one day
              // refuse a topic delete because a log disk was full, and the operator's cluster matters
              // more than KUI's bookkeeping.
              .handleErrorWith(failure =>
                logger.error(context)(s"the audit record could not be written: ${failure.getMessage}")
              )
          } yield ()

        profiles.get(cluster).flatMap {
          case None =>
            val missing: KuiError =
              ApplicationError.NotFound("cluster", cluster.value, ErrorCode.ClusterNotFound)

            write(MutationOutcome.Failed, Map("reason" -> s"${missing.code.wire}: ${missing.message}"))
              .as(missing.asLeft[A])

          case Some(profile) if profile.readOnly =>
            val refusal: KuiError = ApplicationError.Refused(
              ErrorCode.ReadOnly,
              s"cluster ${profile.name} is configured read-only, so ${kind.operation} is not accepted"
            )

            logger.info(context)("refused: the cluster is read-only") >>
              write(MutationOutcome.Refused, Map("reason" -> refusal.message)).as(refusal.asLeft[A])

          case Some(_) =>
            op.guaranteeCase {
              // `guaranteeCase`, so a cancelled or errored mutation is recorded too. Kafka gives no
              // guarantee that a cancelled `deleteTopics` was *not* applied, and a record claiming
              // either way would be a lie; `Unknown` tells an operator to go and look, which is the
              // only honest thing this arm can offer. `Errored` is a different fact and stays
              // `Failed`: the admin client raised, so KUI knows the request did not complete.
              case Outcome.Succeeded(_) => Temporal[F].unit
              case Outcome.Errored(failure) =>
                write(MutationOutcome.Failed, Map("reason" -> Option(failure.getMessage).getOrElse("")))
              case Outcome.Canceled() =>
                write(
                  MutationOutcome.Unknown,
                  Map("reason" -> "the operation was cancelled after the request was sent")
                )
            }.flatMap {
              case Right(value) =>
                write(MutationOutcome.Succeeded, Map.empty) >>
                  snapshots.requestRefresh(cluster).as(value.asRight[KuiError])

              case Left(error) =>
                val wire = toKui(error)
                // A timeout is HTTP 408, so the status test alone would record it as a refusal. The
                // request reached the broker and may still have been applied; `Unknown` is that fact.
                val outcome =
                  if wire.code == ErrorCode.Timeout then MutationOutcome.Unknown
                  else if wire.code.httpStatus < 500 then MutationOutcome.Refused
                  else MutationOutcome.Failed

                write(outcome, Map("reason" -> s"${wire.code.wire}: ${wire.message}")).as(wire.asLeft[A])
            }
        }
      }
    }
}
