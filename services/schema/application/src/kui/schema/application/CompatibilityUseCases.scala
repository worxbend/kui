package kui.schema.application

import cats.effect.kernel.Temporal
import cats.effect.syntax.all.*
import cats.syntax.all.*
import org.typelevel.log4cats.StructuredLogger

import kui.kernel.error.{ApplicationError, ErrorCode, KuiError}
import kui.kernel.{ClusterId, Subject}
import kui.schema.domain.*
import kui.security.Principal
import kui.security.audit.{AuditSink, MutationKind, MutationOutcome, MutationRecord}

/** Would the registry accept this schema for this subject?
  *
  * A read that carries a body, and deliberately not a mutation: nothing is registered, nothing is stored, and
  * running it twice does the same thing as running it once. It refuses nothing on a read-only cluster either
  * — "may I look at whether this would work" is exactly the question a read-only KUI should still answer, and
  * refusing it would push an operator to try the change on the registry directly, which is the opposite of
  * what read-only mode is for.
  *
  * The check runs inside the registry (see [[SchemaRegistryPort.checkCompatibility]]). KUI does not implement
  * Avro, JSON-Schema or Protobuf compatibility rules and must not: the verdict that matters is the one the
  * registry will give when the schema is actually registered, and a second implementation would eventually
  * disagree with it in the direction that loses data.
  */
trait CompatibilityCheckUseCase[F[_]] {

  def check(
      cluster: ClusterId,
      subject: Subject,
      version: VersionSelector,
      proposed: ProposedSchema
  ): F[Either[KuiError, CompatibilityVerdict]]
}

object CompatibilityCheckUseCase {

  /** The largest schema document KUI will forward to a registry.
    *
    * One megabyte is far beyond any real schema — the largest in the reference corpora are tens of kilobytes
    * — and the bound exists because this endpoint takes text from a browser and hands it to an upstream. An
    * unbounded body is a way to make KUI's memory somebody else's decision.
    */
  val MaxDefinitionBytes: Int = 1024 * 1024

  def make[F[_]: Temporal](registries: ClusterRegistries[F]): CompatibilityCheckUseCase[F] =
    new CompatibilityCheckUseCase[F] {

      def check(
          cluster: ClusterId,
          subject: Subject,
          version: VersionSelector,
          proposed: ProposedSchema
      ): F[Either[KuiError, CompatibilityVerdict]] =
        if proposed.definition.trim.isEmpty then
          ApplicationError
            .Invalid("the proposed schema is empty; paste the schema text to check it", Nil)
            .asLeft[CompatibilityVerdict]
            .pure[F]
            .widen
        else if proposed.definition.length > MaxDefinitionBytes then
          ApplicationError
            .Invalid(
              s"the proposed schema is ${proposed.definition.length} characters, and the limit is " +
                s"$MaxDefinitionBytes",
              Nil
            )
            .asLeft[CompatibilityVerdict]
            .pure[F]
            .widen
        else
          RegistryQuery.on(registries, cluster)(
            _.checkCompatibility(subject, version, proposed)
              .map(_.flatMap(RegistryQuery.orNotFound("subject", subject.value)))
          )
    }
}

/** Setting a compatibility level, globally or on one subject.
  *
  * This is the service's only mutation, and it obeys the same three rules every other mutation in KUI does
  * (ADR-047): a read-only cluster is refused **before** the registry is contacted, every attempt is recorded
  * whether it succeeded, failed, was refused or was cancelled, and the record names the person the gateway
  * signed rather than a constant this file chose.
  *
  * The `before` value in the audit record is the level that was in force when the change was made, read for
  * that purpose. It costs one extra request and it is what turns the record from "somebody set BACKWARD" into
  * "somebody replaced FULL_TRANSITIVE with BACKWARD", which is the sentence an incident review needs. When it
  * cannot be read the record says so rather than omitting the field, because a blank `before` and an unknown
  * `before` mean different things.
  */
trait SetCompatibilityUseCase[F[_]] {

  def setGlobal(
      principal: Principal,
      cluster: ClusterId,
      level: CompatibilityLevel
  ): F[Either[KuiError, CompatibilityLevel]]

  def setForSubject(
      principal: Principal,
      cluster: ClusterId,
      subject: Subject,
      level: CompatibilityLevel
  ): F[Either[KuiError, SubjectCompatibility]]
}

object SetCompatibilityUseCase {

  /** What the audit record's `before` says when the previous level could not be read. */
  val UnknownBefore: String = "unknown (the registry did not answer)"

  def make[F[_]: Temporal](
      registries: ClusterRegistries[F],
      audit: AuditSink[F],
      logger: StructuredLogger[F]
  ): SetCompatibilityUseCase[F] =
    new SetCompatibilityUseCase[F] {

      def setGlobal(
          principal: Principal,
          cluster: ClusterId,
          level: CompatibilityLevel
      ): F[Either[KuiError, CompatibilityLevel]] =
        guarded(principal, cluster, MutationKind.SetGlobalCompatibility, "global", level) { port =>
          for {
            before <- port.globalCompatibility
            outcome <- port.setGlobalCompatibility(level)
          } yield (before.toOption.map(_.wire), outcome.map(_ => level))
        }

      def setForSubject(
          principal: Principal,
          cluster: ClusterId,
          subject: Subject,
          level: CompatibilityLevel
      ): F[Either[KuiError, SubjectCompatibility]] =
        guarded(principal, cluster, MutationKind.SetSubjectCompatibility, subject.value, level) { port =>
          for {
            before <- port.subjectCompatibility(subject)
            outcome <- port.setSubjectCompatibility(subject, level)
          } yield (
            // A subject that had no level of its own says so, rather than reporting the global level it
            // was following as though the operator had replaced a setting that was never there.
            before.toOption.map(_.fold("inherited from the global level")(_.wire)),
            // The answer is `own`, always: a subject that has just been given a level has one, whatever
            // it inherited a moment ago. That is the fact the screen must show, because the operator has
            // changed which setting governs this subject as well as what it says.
            outcome.map(_ => SubjectCompatibility.own(level))
          )
        }

      /** The read-only refusal, the audit record and the operation, in that order.
        *
        * The operation is passed as a function of the port so that nothing runs — not even the `before` read
        * — until the cluster has been resolved and the read-only check has passed. An implementation that
        * took an already-started effect would contact the registry of a read-only cluster before refusing,
        * which is a call an operator's proxy logs and then has to explain.
        */
      private def guarded[A](
          principal: Principal,
          cluster: ClusterId,
          kind: MutationKind,
          resource: String,
          level: CompatibilityLevel
      )(
          run: SchemaRegistryPort[F] => F[(Option[String], Either[KuiError, A])]
      ): F[Either[KuiError, A]] = {
        val context = Map(
          "service.name" -> SchemaService.Id.value,
          "cluster.id" -> cluster.value,
          "operation" -> kind.operation,
          "resource" -> resource
        )

        def write(
            outcome: MutationOutcome,
            before: Option[String],
            reason: Option[String]
        ): F[Unit] =
          for {
            at <- Temporal[F].realTimeInstant
            _ <- audit
              .record(
                MutationRecord(
                  at = at,
                  principal = principal,
                  cluster = cluster,
                  kind = kind,
                  resource = resource,
                  before = before,
                  after = Some(level.wire),
                  outcome = outcome,
                  detail = reason.map("reason" -> _).toMap
                )
              )
              // The sink never fails the operation it is recording, for the reason every other guard in
              // KUI gives: an operator's registry matters more than KUI's bookkeeping.
              .handleErrorWith(failure =>
                logger.error(context)(s"the audit record could not be written: ${failure.getMessage}")
              )
          } yield ()

        registries.profile(cluster).flatMap {
          case None =>
            val error = RegistryAccess.unknownCluster(cluster)
            write(MutationOutcome.Failed, None, Some(s"${error.code.wire}: ${error.message}"))
              .as(error.asLeft[A])

          case Some(profile) if profile.readOnly =>
            val refusal = ApplicationError.Refused(
              ErrorCode.ReadOnly,
              s"cluster ${profile.displayName} is configured read-only, so ${kind.operation} is not accepted"
            )
            logger.info(context)("refused: the cluster is read-only") >>
              write(MutationOutcome.Refused, None, Some(s"${refusal.code.wire}: ${refusal.message}"))
                .as(refusal.asLeft[A])

          case Some(_) =>
            registries.registry(cluster).flatMap {
              case None =>
                val error = RegistryAccess.notConfigured(cluster)
                write(MutationOutcome.Failed, None, Some(s"${error.code.wire}: ${error.message}"))
                  .as(error.asLeft[A])

              case Some(port) =>
                run(port)
                  .flatMap {
                    case (before, Right(value)) =>
                      write(MutationOutcome.Succeeded, before.orElse(Some(UnknownBefore)), None)
                        .as(value.asRight[KuiError])
                    case (before, Left(error)) =>
                      write(
                        MutationOutcome.ofError(error),
                        before.orElse(Some(UnknownBefore)),
                        Some(s"${error.code.wire}: ${error.message}")
                      ).as(error.asLeft[A])
                  }
                  .guaranteeCase {
                    // A cancelled or errored write is recorded too: an HTTP request that was cut off may
                    // still have been applied, and a record claiming either way would be a lie.
                    case cats.effect.kernel.Outcome.Succeeded(_) => Temporal[F].unit
                    case cats.effect.kernel.Outcome.Errored(failure) =>
                      write(
                        MutationOutcome.Failed,
                        None,
                        Some(s"${ErrorCode.Internal.wire}: ${Option(failure.getMessage).getOrElse("")}")
                      )
                    case cats.effect.kernel.Outcome.Canceled() =>
                      write(
                        MutationOutcome.Unknown,
                        None,
                        Some("the operation was cancelled after the request was sent")
                      )
                  }
            }
        }
      }
    }
}
