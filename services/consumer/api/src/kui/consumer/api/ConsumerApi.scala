package kui.consumer.api

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

import kui.consumer.application.*
import kui.consumer.contract.{ConsumerEndpoints, ConsumerMutationEndpoints}
import kui.contracts.capability.ServiceCapabilities
import kui.http.ErrorInterceptor
import kui.http.health.{HealthEndpoints, ReadinessCheck}
import kui.http.principal.{PrincipalInterceptor, RbacGuard, RequestContext, SecuredRoutes}
import kui.kernel.ServiceId
import kui.kernel.error.KuiError
import kui.observability.{KuiInterceptors, Telemetry}
import kui.security.PrincipalCodec

/** Everything `kui-consumer-service` serves, and the one place a typed failure becomes an HTTP response.
  *
  * It is the topic service's `api` module with a different set of routes, deliberately: the five things an
  * `api` module does are the same in every service, and a second shape for them would be a second place for
  * authentication, error mapping and the OpenAPI document to drift.
  *
  *   - it binds the endpoints `services/consumer/contract` publishes to the use cases in
  *     `services/consumer/application`, with no path written out here (ADR-003);
  *   - it verifies the gateway's signed principal before any use case runs (ADR-020);
  *   - it maps a `KuiError` to the one error envelope and the one status, through `ErrorEnvelope.statusOf`
  *     (ADR-034);
  *   - it maps application types to contract types ([[ConsumerMapping]], ADR-033);
  *   - it starts nothing. `services/consumer/app` is the only module in this service allowed to.
  */
object ConsumerApi {

  /** The process's name, as it appears in `service.name` on every log line, span and metric. */
  val ServiceName: String = "kui-consumer"

  /** This service's identity in a signed principal's `aud` claim and in a capability document.
    *
    * It has to equal the id the gateway is configured with — `kui.gateway.services.consumer` — or every call
    * from the gateway is refused with a 401 that names no cause the caller can see.
    */
  val Id: ServiceId = ServiceId.unsafe("consumer")

  // -----------------------------------------------------------------------------------------------
  // Routes
  // -----------------------------------------------------------------------------------------------

  /** The routes, in the order the router tries them.
    *
    * Health first, because nothing else can match its paths and a probe should travel the shortest route
    * through the router. Then the four reads, then the four mutation routes.
    */
  def routes[F[_]: {Async, Parallel}](
      list: GroupListUseCase[F],
      detail: GroupDetailUseCase[F],
      lag: LagPollUseCase[F],
      forTopic: GroupsForTopicUseCase[F],
      snapshots: GroupSnapshots[F],
      reset: OffsetResetUseCase[F],
      deleteGroup: DeleteGroupUseCase[F],
      deleteOffsets: DeleteOffsetsUseCase[F],
      readiness: List[ReadinessCheck[F]],
      capabilities: ConsumerCapabilities[F],
      principals: PrincipalCodec[F],
      rejections: Counter[F, Long],
      logger: StructuredLogger[F],
      guard: RbacGuard[F]
  ): List[ServerEndpoint[Any, F]] = {
    val secured = Securing[F](principals, rejections, logger, guard)

    HealthEndpoints.make[F](readiness, capabilityDocument[F](capabilities, logger)) ++
      ConsumerRoutes[F](list, detail, lag, forTopic, snapshots, secured, guard) ++
      ConsumerMutationRoutes[F](reset, deleteGroup, deleteOffsets, snapshots, secured)
  }

  /** The cross-cutting chain, outermost first, in the order `libs/http`'s server wants it and the topic
    * service already uses: principal, then instrumentation, then error translation innermost.
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

  // -----------------------------------------------------------------------------------------------
  // Binding an endpoint to a use case
  // -----------------------------------------------------------------------------------------------

  /** Everything a route needs in order to be verified and to fail correctly, bundled once.
    *
    * The mechanism is `kui.http.principal.SecuredRoutes`, shared by every service. It was written here first,
    * because this is the service that discovered the problem ADR-020 Amendment 1 settles: a bodied request
    * cannot be verified in Tapir's security stage, and the reset wizard was refused as `request_mismatch` the
    * first time it ran against a real cluster. A fix that stayed in this module would have been a fix the
    * message service had to find again, differently — so it moved to `libs/http` and both call it.
    *
    * The name stays so that this service's routes read as they always have.
    */
  type Securing[F[_]] = SecuredRoutes[F]

  def Securing[F[_]: Async](
      principals: PrincipalCodec[F],
      rejections: Counter[F, Long],
      logger: StructuredLogger[F],
      guard: RbacGuard[F]
  ): SecuredRoutes[F] = new SecuredRoutes[F](principals, Id, rejections, logger, guard)

  /** The two halves of an error response: the body the contract fixes and the status the code decides. */
  private[api] type Failure = SecuredRoutes.Failure

  private[api] def failure[F[_]: Sync](error: KuiError, ctx: RequestContext): F[Failure] =
    SecuredRoutes.failure[F](error, ctx)

  // -----------------------------------------------------------------------------------------------
  // Capabilities
  // -----------------------------------------------------------------------------------------------

  /** What `GET /capabilities` answers, including when this service is having a bad day.
    *
    * A failure inside the report is logged and becomes a document reporting no clusters, rather than a 500:
    * the gateway's registry needs an answer most exactly when things are wrong.
    */
  def capabilityDocument[F[_]: Sync](
      capabilities: ConsumerCapabilities[F],
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
    ConsumerEndpoints.all ++ ConsumerMutationEndpoints.all ++
      List(HealthEndpoints.live, HealthEndpoints.ready, HealthEndpoints.capabilities)

  def openApi[F[_]]: OpenAPI = OpenAPIDocsInterpreter().toOpenAPI(documented[F], Title, Version)

  val Title: String = "KUI Consumer service"

  /** The document's version, which is the *contract's* version and not the build's. */
  val Version: String = "1.0.0"

  /** The instant a document is stamped with, when one has to be. Fixed, for the same reason as the version.
    */
  val GeneratedAt: Instant = Instant.EPOCH
}
