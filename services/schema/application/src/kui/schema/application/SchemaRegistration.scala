package kui.schema.application

import cats.effect.kernel.{Outcome, Temporal}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import org.typelevel.log4cats.StructuredLogger

import kui.kernel.error.{ApplicationError, ErrorCode, KuiError}
import kui.kernel.{ClusterId, Subject}
import kui.schema.domain.*
import kui.security.Principal
import kui.security.audit.{AuditSink, MutationKind, MutationOutcome, MutationRecord}

/** Registering a schema under a subject: the only thing KUI does that adds to a registry's contents.
  *
  * ==Why this is an endpoint of this service and not a fold in the gateway==
  *
  * Everything a registration needs is here. The registry client with its breaker, its bulkhead and its
  * credentials is this service's; the read-only flag is on this service's own cluster profile; the schema
  * text has to be validated against a bound before it is forwarded, and the bound is this service's. A
  * gateway fold would have to be given all four, and the gateway does not decode service request bodies at
  * all (`NameSource.RequestBody`) — it proxies them. `SchemaMutationEndpoints` is already in
  * `ServiceContracts.byService`, so publishing the write here gives the gateway its public route with no
  * gateway change.
  *
  * ==The four refusals, in the order they are decided==
  *
  *   1. an unknown cluster is a 404, because the caller followed a link to something that is not there;
  *   1. a **read-only** cluster is `KUI-READ-ONLY`, decided before the registry is contacted, so an
  *      operator's proxy never logs a write KUI was always going to refuse (ADR-047 §2);
  *   1. an empty or oversized document is `KUI-VALIDATION`, decided here rather than by the registry;
  *   1. a cluster with no registry is `KUI-UNSUPPORTED` naming the configuration key.
  *
  * The order is the list's and not the code's convenience, and it was the other way round until wave 5: the
  * document was validated first, so an oversized schema aimed at a cluster KUI has never heard of answered
  * `400` rather than `404`, and one aimed at a read-only cluster answered `400` rather than `405`. Neither
  * broke a rule — nothing leaves the process on either path, so ADR-047 §2 held — but "the schema you pasted
  * is too big" is the wrong first sentence for a link that points at nothing, and it is the sentence an
  * operator would have acted on. `SchemaRegistrationSuite` names both combinations.
  *
  * The permission half — `SCHEMA:CREATE` over the subject — is declared on the endpoint and enforced by
  * `SecuredRoutes`' guard and by the gateway, which is the same path both compatibility writes take. It is
  * deliberately not re-decided here: two spellings of one rule is how the two enforcement points come to
  * disagree.
  *
  * Every attempt crosses a structured audit finalizer, including cancellation and raised errors. Only schema
  * IDs and version numbers enter that record; the schema definition is never audit data.
  */
trait RegisterSchemaUseCase[F[_]] {

  def register(
      principal: Principal,
      cluster: ClusterId,
      subject: Subject,
      proposed: ProposedSchema
  ): F[Either[KuiError, RegisteredVersion]]
}

object RegisterSchemaUseCase {

  /** The largest schema document KUI will forward to a registry.
    *
    * The compatibility check's bound, deliberately the same value and read from the same place: a document
    * KUI will check and refuse to register, or the other way round, is a difference an operator would find by
    * hitting it. See [[CompatibilityCheckUseCase.MaxDefinitionBytes]] for why the bound exists at all.
    */
  val MaxDefinitionBytes: Int = CompatibilityCheckUseCase.MaxDefinitionBytes

  def make[F[_]: Temporal](
      registries: ClusterRegistries[F],
      audit: AuditSink[F],
      logger: StructuredLogger[F]
  ): RegisterSchemaUseCase[F] =
    new RegisterSchemaUseCase[F] {

      def register(
          principal: Principal,
          cluster: ClusterId,
          subject: Subject,
          proposed: ProposedSchema
      ): F[Either[KuiError, RegisteredVersion]] =
        run(principal, cluster, subject, proposed).guaranteeCase {
          case Outcome.Succeeded(result) =>
            result.flatMap {
              case Right(registered) =>
                val after =
                  s"schemaId=${registered.id.value};version=${registered.version.fold("unknown")(_.value.toString)}"
                record(principal, cluster, subject, MutationOutcome.Succeeded, Some(after), None)
              case Left(error) =>
                val outcome =
                  if error.code.httpStatus < 500 then MutationOutcome.Refused else MutationOutcome.Failed
                record(principal, cluster, subject, outcome, None, Some(error.code.wire))
            }
          case Outcome.Errored(_) =>
            record(principal, cluster, subject, MutationOutcome.Failed, None, Some(ErrorCode.Internal.wire))
          case Outcome.Canceled() =>
            record(
              principal,
              cluster,
              subject,
              MutationOutcome.Unknown,
              None,
              Some("the registration was cancelled; the registry may have applied it")
            )
        }

      private def record(
          principal: Principal,
          cluster: ClusterId,
          subject: Subject,
          outcome: MutationOutcome,
          after: Option[String],
          reason: Option[String]
      ): F[Unit] =
        Temporal[F].realTimeInstant
          .flatMap { at =>
            audit.record(
              MutationRecord(
                at,
                principal,
                cluster,
                MutationKind.RegisterSchema,
                subject.value,
                None,
                after,
                outcome,
                reason.map("reason" -> _).toMap
              )
            )
          }
          .handleErrorWith(_ =>
            logger
              .error(context(cluster, subject, principal))(
                "the registration audit record could not be written"
              )
              .handleError(_ => ())
          )

      private def run(
          principal: Principal,
          cluster: ClusterId,
          subject: Subject,
          proposed: ProposedSchema
      ): F[Either[KuiError, RegisteredVersion]] =
        registries.profile(cluster).flatMap {
          case None =>
            RegistryAccess.unknownCluster(cluster).asLeft[RegisteredVersion].pure[F]

          // Before the registry is contacted, and before the port is even resolved: a read-only
          // deployment that opened a connection to say no would still have said no in the registry's
          // access log, which is the thing an operator then has to explain.
          case Some(profile) if profile.readOnly =>
            val refusal = ApplicationError.Refused(
              ErrorCode.ReadOnly,
              s"cluster ${profile.displayName} is configured read-only, so " +
                s"$Operation is not accepted"
            )
            logger
              .info(context(cluster, subject, principal))("refused: the cluster is read-only")
              .as(refusal.asLeft[RegisteredVersion])

          // The document is checked once the cluster is known to be one this deployment writes to, so
          // that the answer to a bad link is "no such cluster" rather than a complaint about the body.
          case Some(_) =>
            validate(proposed) match {
              case Some(error) => error.asLeft[RegisteredVersion].pure[F]
              case None =>
                registries.registry(cluster).flatMap {
                  case None =>
                    RegistryAccess.notConfigured(cluster).asLeft[RegisteredVersion].pure[F]
                  case Some(port) => through(port, cluster, subject, principal, proposed)
                }
            }
        }

      /** The registry call and its operational log line, in addition to the structured audit finalizer.
        *
        * Both outcomes are logged, for the reason ADR-047 gives about records: what somebody *tried* to
        * register on a production cluster is often the more interesting half.
        */
      private def through(
          port: SchemaRegistryPort[F],
          cluster: ClusterId,
          subject: Subject,
          principal: Principal,
          proposed: ProposedSchema
      ): F[Either[KuiError, RegisteredVersion]] =
        port.register(subject, proposed).flatTap {
          case Right(registered) =>
            logger.info(context(cluster, subject, principal))(
              s"registered schema id ${registered.id.value} under ${subject.value}" +
                registered.version.fold(
                  ", and the registry did not say which version it became"
                )(version => s" as version ${version.value}")
            )
          case Left(error) =>
            logger.warn(context(cluster, subject, principal))(
              s"the registration was refused: ${error.code.wire}: ${error.message}"
            )
        }

      /** Two refusals KUI makes itself, before anything leaves the process.
        *
        * An empty document is refused rather than forwarded because the registry's own answer to it is a 500
        * on several implementations, and "the registry is broken" is the wrong sentence for "you did not
        * paste anything". The size bound is the one every endpoint that forwards operator text to an upstream
        * needs.
        */
      private def validate(proposed: ProposedSchema): Option[KuiError] =
        if proposed.definition.trim.isEmpty then
          Some(
            ApplicationError.Invalid(
              "the schema is empty; paste the schema text to register it",
              Nil
            )
          )
        else if proposed.definition.length > MaxDefinitionBytes then
          Some(
            ApplicationError.Invalid(
              s"the schema is ${proposed.definition.length} characters, and the limit is " +
                s"$MaxDefinitionBytes",
              Nil
            )
          )
        else None

      private def context(
          cluster: ClusterId,
          subject: Subject,
          principal: Principal
      ): Map[String, String] =
        Map(
          "service.name" -> SchemaService.Id.value,
          "cluster.id" -> cluster.value,
          "operation" -> Operation,
          "resource" -> subject.value,
          "principal" -> principal.name.value
        )
    }

  /** The operation name, in the one place the use case and its log lines read it from.
    *
    * It is the same string as `SchemaMutationEndpoints.RegisterVersionOperation`, and the api module's suite
    * — the only module that can see a contract and an application type at once — is what asserts that,
    * exactly as it does for the two compatibility writes.
    */
  val Operation: String = "schema.subject.version.register"
}
