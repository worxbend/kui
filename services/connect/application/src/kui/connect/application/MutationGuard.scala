package kui.connect.application

import cats.effect.kernel.{Outcome, Temporal}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import org.typelevel.log4cats.StructuredLogger

import kui.connect.domain.ConnectorOperation
import kui.kernel.error.{ApplicationError, ErrorCode, KuiError}
import kui.kernel.{ClusterId, ConnectName, ConnectorName}
import kui.security.Principal
import kui.security.audit.MutationOutcome

/** The only way a mutation happens in this service (ADR-047).
  *
  * ==It is the consumer service's guard, through the alerts service's copy of it==
  *
  * Four services implement this classification and they used to disagree about one case, which is the case
  * this service copies deliberately: a **cancelled** mutation is `MutationOutcome.Unknown`. `AuditSink`'s own
  * words for why — "Kafka gives no guarantee that it was not applied, so a record claiming either would be a
  * lie" — were written about a Kafka write, and they survive the move to an HTTP call to a Connect worker
  * unchanged, in fact more strongly: a cancellation lands between the request going out and the `202` coming
  * back, and the worker has very often already accepted it. A record saying `Failed` would tell an incident
  * review that a connector was never restarted when it may well have been. The topic and message services
  * wrote `Failed` here and W6-A1 repaired both; this is not a third.
  *
  * ==The order, and it is asserted rather than described==
  *
  *   1. resolve the cluster's profile;
  *   1. if it is read-only, refuse with `KUI-READ-ONLY` **without calling the worker**, and record the
  *      refusal — an attempt to change a read-only cluster is exactly what an audit trail exists to notice;
  *   1. run the operation;
  *   1. record the outcome — always, including on failure and on cancellation.
  *
  * There is no step 5. The consumer service's guard invalidates a snapshot at the end; this service holds no
  * snapshot at all, because every fact it reports is read from a worker at the moment it is asked.
  *
  * ==On refusing a pause on a read-only cluster==
  *
  * Pausing a connector writes nothing to Kafka, so this refusal is stricter than the operation strictly
  * needs. It is the answer `Action.ConnectOperate` already encodes — `isAlter = true` decides the audit
  * question and the read-only question with one field — and a read-only cluster whose sinks can still be
  * stopped by anyone with a browser is not read-only in any sense an operator means. ADR-054 §3 argues it;
  * `RbacLawsSuite` asserts the vocabulary half and this guard is the half that runs.
  */
trait MutationGuard[F[_]] {

  def guard[A](
      principal: Principal,
      cluster: ClusterId,
      connect: ConnectName,
      connector: ConnectorName,
      operation: ConnectorOperation
  )(op: F[Either[KuiError, A]]): F[Either[KuiError, A]]
}

object MutationGuard {

  def make[F[_]: Temporal](
      profiles: ClusterConnectSource[F],
      audit: ConnectorOperationSink[F],
      logger: StructuredLogger[F]
  ): MutationGuard[F] =
    new MutationGuard[F] {

      def guard[A](
          principal: Principal,
          cluster: ClusterId,
          connect: ConnectName,
          connector: ConnectorName,
          operation: ConnectorOperation
      )(op: F[Either[KuiError, A]]): F[Either[KuiError, A]] = {
        val resource = ConnectorOperationRecord.resourceOf(connect, connector)

        val context = Map(
          "service.name" -> ConnectService.Id.value,
          "cluster.id" -> cluster.value,
          "operation" -> operation.operation,
          "resource" -> resource
        )

        def write(outcome: MutationOutcome, reason: Option[String]): F[Unit] =
          for {
            at <- Temporal[F].realTimeInstant
            _ <- audit
              .record(
                ConnectorOperationRecord(
                  at = at,
                  principal = principal,
                  cluster = cluster,
                  connect = connect,
                  connector = connector,
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
              s"cluster ${profile.displayName} is configured read-only, so ${operation.operation} is " +
                "not accepted"
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
                  Some("the operation was cancelled after the Connect cluster was asked to apply it")
                )
            }.flatMap {
              case Right(value) => write(MutationOutcome.Succeeded, None).as(value.asRight[KuiError])
              case Left(error) =>
                write(MutationOutcome.ofError(error), Some(s"${error.code.wire}: ${error.message}"))
                  .as(error.asLeft[A])
            }
        }
      }
    }
}
