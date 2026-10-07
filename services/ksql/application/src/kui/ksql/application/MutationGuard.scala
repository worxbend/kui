package kui.ksql.application

import cats.effect.kernel.{Outcome, Temporal}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import org.typelevel.log4cats.StructuredLogger

import kui.kernel.ClusterId
import kui.kernel.error.{ApplicationError, ErrorCode, KuiError}
import kui.ksql.domain.KsqlStatement
import kui.security.Principal
import kui.security.audit.MutationOutcome

/** The only way a statement runs in this service (ADR-047).
  *
  * ==A cancelled statement is `Unknown`, and this is the fifth service to say so==
  *
  * Five services implement this classification and they used to disagree about one case. `AuditSink`'s own
  * words for why a cancellation is `MutationOutcome.Unknown` — "Kafka gives no guarantee that it was not
  * applied, so a record claiming either would be a lie" — were written about a Kafka write, and they survive
  * the move to a ksqlDB statement **more** strongly than they did to a Connect worker: a ksqlDB statement is
  * written to the command topic before the HTTP response is composed, so a cancellation that lands between
  * the request going out and the answer coming back has very often already created the stream. A record
  * saying `Failed` would tell an incident review that a topic was never dropped when it may well have been.
  * The topic and message services wrote `Failed` here and W6-A1 repaired both; this is not a third.
  *
  * ==The order, and it is asserted rather than described==
  *
  *   1. resolve the cluster's profile;
  *   1. if it is read-only, refuse with `KUI-READ-ONLY` **without calling the server**, and record the
  *      refusal — an attempt to change a read-only cluster is exactly what an audit trail exists to notice;
  *   1. run the operation, which is where the plan token is checked and where the server is called;
  *   1. record the outcome — always, including on failure, on refusal and on cancellation.
  *
  * There is no step 5. The consumer service's guard invalidates a snapshot at the end; this service holds no
  * snapshot at all, because every fact it reports is read from a server at the moment it is asked.
  *
  * ==On the read-only refusal being the second one==
  *
  * `RbacGuard.fromPolicy` applies the read-only gate at the route, before this runs, and applies it whether
  * or not RBAC is enabled — so in the wired product this branch is the second refusal and not the first. It
  * is here anyway and it is **reachable**, which is the difference between depth and decoration: a
  * composition root built with `RbacGuard.allowAll` — which `SecuredRoutes`' single-argument constructor
  * still offers — has no first refusal at all, and this guard is then the only thing between a read-only
  * cluster and a `DROP`. `MutationGuardSuite` drives it directly and `KsqlRoutesSuite` drives the wired pair,
  * so both statements are held by cases rather than by this paragraph.
  */
trait MutationGuard[F[_]] {

  def guard[A](
      principal: Principal,
      cluster: ClusterId,
      statement: KsqlStatement,
      operation: String,
      outcomeOf: A => MutationOutcome = (_: A) => MutationOutcome.Succeeded
  )(op: F[Either[KuiError, A]]): F[Either[KuiError, A]]
}

object MutationGuard {

  def make[F[_]: Temporal](
      profiles: ClusterKsqlSource[F],
      audit: KsqlStatementSink[F],
      logger: StructuredLogger[F]
  ): MutationGuard[F] =
    new MutationGuard[F] {

      def guard[A](
          principal: Principal,
          cluster: ClusterId,
          statement: KsqlStatement,
          operation: String,
          outcomeOf: A => MutationOutcome
      )(op: F[Either[KuiError, A]]): F[Either[KuiError, A]] = {
        val resource = KsqlStatementRecord.resourceOf(cluster)

        val context = Map(
          "service.name" -> KsqlService.Id.value,
          "cluster.id" -> cluster.value,
          "operation" -> operation,
          "resource" -> resource
        )

        def write(outcome: MutationOutcome, reason: Option[String]): F[Unit] =
          for {
            at <- Temporal[F].realTimeInstant
            _ <- audit
              .record(
                KsqlStatementRecord(
                  at = at,
                  principal = principal,
                  cluster = cluster,
                  statement = statement.canonical,
                  shape = statement.shape,
                  destructive = statement.destructive,
                  operation = operation,
                  outcome = outcome,
                  detail = reason.map("reason" -> _).toMap
                )
              )
              // The sink never fails the operation it is recording, for `LoggingAuditSink`'s reason.
              .handleErrorWith(failure =>
                logger.error(context)(s"the audit record could not be written: ${failure.getMessage}")
              )
          } yield ()

        profiles.profileOf(cluster).flatMap {
          case Left(error) =>
            write(MutationOutcome.Failed, Some(s"${error.code.wire}: ${error.message}")).as(error.asLeft[A])

          case Right(profile) if profile.readOnly =>
            val refusal = ApplicationError.Refused(
              ErrorCode.ReadOnly,
              s"cluster ${profile.displayName} is configured read-only, so no ksqlDB statement is accepted"
            )

            logger.info(context)("refused: the cluster is read-only") >>
              write(MutationOutcome.Refused, Some(s"${refusal.code.wire}: ${refusal.message}"))
                .as(refusal.asLeft[A])

          case Right(_) =>
            op.guaranteeCase {
              case Outcome.Succeeded(_) => Temporal[F].unit
              case Outcome.Errored(failure) =>
                write(
                  MutationOutcome.Failed,
                  Some(s"${ErrorCode.Internal.wire}: ${Option(failure.getMessage).getOrElse("")}")
                )
              case Outcome.Canceled() =>
                write(
                  MutationOutcome.Unknown,
                  Some("the statement was cancelled after the ksqlDB cluster was asked to run it")
                )
            }.flatMap {
              case Right(value) =>
                val outcome = outcomeOf(value)
                val reason = Option.when(outcome == MutationOutcome.Unknown)(
                  "the command is accepted but completion is still pending"
                )
                write(outcome, reason).as(value.asRight[KuiError])
              case Left(error) =>
                val outcome =
                  if error.code.httpStatus < 500 then MutationOutcome.Refused else MutationOutcome.Failed

                write(outcome, Some(s"${error.code.wire}: ${error.message}")).as(error.asLeft[A])
            }
        }
      }
    }
}
