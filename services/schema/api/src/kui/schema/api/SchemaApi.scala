package kui.schema.api

import java.time.Instant

import cats.Parallel
import cats.effect.kernel.{Async, Sync}
import cats.syntax.all.*
import org.typelevel.log4cats.StructuredLogger
import org.typelevel.otel4s.metrics.Counter
import sttp.apispec.openapi.OpenAPI
import sttp.tapir.AnyEndpoint
import sttp.tapir.docs.openapi.OpenAPIDocsInterpreter
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.interceptor.Interceptor

import kui.contracts.capability.ServiceCapabilities
import kui.http.ErrorInterceptor
import kui.http.health.{HealthEndpoints, ReadinessCheck}
import kui.http.principal.{PrincipalInterceptor, RbacGuard, SecuredRoutes}
import kui.kernel.ServiceId
import kui.observability.{KuiInterceptors, Telemetry}
import kui.schema.application.*
import kui.schema.contract.{SchemaEndpoints, SchemaMutationEndpoints}
import kui.security.PrincipalCodec

/** Everything `kui-schema-service` serves, and the one place a typed failure becomes an HTTP response.
  *
  * It is the consumer service's `api` module with a different set of routes, deliberately: the five things an
  * `api` module does are the same in every service, and a second shape for them would be a second place for
  * authentication, error mapping and the OpenAPI document to drift.
  *
  *   - it binds the endpoints `services/schema/contract` publishes to the use cases in
  *     `services/schema/application`, with no path written out here (ADR-003);
  *   - it verifies the gateway's signed principal before any use case runs (ADR-020);
  *   - it maps a `KuiError` to the one error envelope and the one status, through `ErrorEnvelope.statusOf`
  *     (ADR-034);
  *   - it maps application types to contract types ([[SchemaMapping]], ADR-033);
  *   - it starts nothing. `services/schema/app` is the only module in this service allowed to.
  */
object SchemaApi {

  /** The process's name, as it appears in `service.name` on every log line, span and metric. */
  val ServiceName: String = "kui-schema"

  /** This service's identity in a signed principal's `aud` claim and in a capability document.
    *
    * It has to equal the id the gateway is configured with — `kui.gateway.services.schema` — or every call
    * from the gateway is refused with a 401 that names no cause the caller can see.
    */
  val Id: ServiceId = ServiceId.unsafe("schema")

  // -----------------------------------------------------------------------------------------------
  // Routes
  // -----------------------------------------------------------------------------------------------

  /** The routes, in the order the router tries them.
    *
    * Health first, because nothing else can match its paths and a probe should travel the shortest route
    * through the router. Then the five reads in the contract's own order, then the four bodied routes.
    */
  def routes[F[_]: {Async, Parallel}](
      subjects: SubjectListUseCase[F],
      versions: SubjectVersionsUseCase[F],
      schema: SchemaVersionUseCase[F],
      compatibility: CompatibilityReadUseCase[F],
      set: SetCompatibilityUseCase[F],
      register: RegisterSchemaUseCase[F],
      check: CompatibilityCheckUseCase[F],
      readiness: List[ReadinessCheck[F]],
      capabilities: SchemaCapabilities[F],
      principals: PrincipalCodec[F],
      rejections: Counter[F, Long],
      logger: StructuredLogger[F],
      guard: RbacGuard[F]
  ): List[ServerEndpoint[Any, F]] = {
    val secured = Securing[F](principals, rejections, logger, guard)

    HealthEndpoints.make[F](readiness, capabilityDocument[F](capabilities, logger)) ++
      SchemaRoutes[F](subjects, versions, schema, compatibility, secured, guard) ++
      SchemaMutationRoutes[F](set, register, check, secured)
  }

  /** The cross-cutting chain, outermost first, in the order `libs/http`'s server wants it: principal, then
    * instrumentation, then error translation innermost.
    */
  def interceptors[F[_]: Async](
      telemetry: Telemetry[F],
      rejections: Counter[F, Long],
      logger: StructuredLogger[F]
  ): F[List[Interceptor[F]]] =
    KuiInterceptors
      .serverInterceptors[F](telemetry, ServiceName)
      .map(instrumentation =>
        PrincipalInterceptor.interceptor[F](logger, rejections) ::
          instrumentation ++ ErrorInterceptor.interceptors[F](logger)
      )

  /** Everything a route needs in order to be verified and to fail correctly, bundled once. The mechanism is
    * `kui.http.principal.SecuredRoutes`, shared by every service (ADR-020 Amendment 1).
    */
  type Securing[F[_]] = SecuredRoutes[F]

  def Securing[F[_]: Async](
      principals: PrincipalCodec[F],
      rejections: Counter[F, Long],
      logger: StructuredLogger[F]
  ): SecuredRoutes[F] = Securing[F](principals, rejections, logger, RbacGuard.allowAll[F])

  def Securing[F[_]: Async](
      principals: PrincipalCodec[F],
      rejections: Counter[F, Long],
      logger: StructuredLogger[F],
      guard: RbacGuard[F]
  ): SecuredRoutes[F] = new SecuredRoutes[F](principals, Id, rejections, logger, guard)

  // -----------------------------------------------------------------------------------------------
  // Capabilities
  // -----------------------------------------------------------------------------------------------

  /** What `GET /capabilities` answers, including when this service is having a bad day.
    *
    * A failure inside the report is logged and becomes a document reporting no clusters, rather than a 500:
    * the gateway's registry needs an answer most exactly when things are wrong.
    */
  def capabilityDocument[F[_]: Sync](
      capabilities: SchemaCapabilities[F],
      logger: StructuredLogger[F]
  ): F[ServiceCapabilities] =
    capabilities.report
      .map(clusters => ServiceCapabilities(Id, clusters))
      .handleErrorWith(error =>
        logger
          .error(error)("the capability report failed; answering that nothing is available")
          .as(ServiceCapabilities(Id, Map.empty))
      )

  // -----------------------------------------------------------------------------------------------
  // OpenAPI
  // -----------------------------------------------------------------------------------------------

  /** Every endpoint this service publishes, health included. */
  def documented[F[_]]: List[AnyEndpoint] =
    SchemaEndpoints.all ++ SchemaMutationEndpoints.all ++
      List(HealthEndpoints.live, HealthEndpoints.ready, HealthEndpoints.capabilities)

  def openApi[F[_]]: OpenAPI = OpenAPIDocsInterpreter().toOpenAPI(documented[F], Title, Version)

  val Title: String = "KUI Schema service"

  /** The document's version, which is the *contract's* version and not the build's. */
  val Version: String = "1.0.0"

  /** The instant a document is stamped with, when one has to be. Fixed, for the same reason as the version.
    */
  val GeneratedAt: Instant = Instant.EPOCH
}
