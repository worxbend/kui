package kui.connect.app

import cats.Parallel
import cats.data.NonEmptyList
import cats.effect.kernel.{Async, Resource}
import cats.syntax.all.*
import org.typelevel.log4cats.StructuredLogger
import sttp.client4.Backend
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.interceptor.Interceptor

import kui.config.{
  ClusterConfig,
  ConnectClusterSettings,
  HttpTlsConfig,
  SafeUrl,
  UpstreamAuthConfig,
  UrlPolicy
}
import kui.connect.api.{ConnectApi, ConnectCapabilities}
import kui.connect.application.*
import kui.connect.domain.ConnectWorkerPort
import kui.connect.infrastructure.*
import kui.contracts.capability.ServiceCapabilities
import kui.http.health.ReadinessCheck
import kui.http.principal.{PrincipalVerification, RbacGuard}
import kui.http.upstream.{HttpTls, UpstreamClient, UpstreamConfig}
import kui.kernel.{ClusterId, ConnectName, PositiveInt}
import kui.observability.Telemetry
import kui.security.PrincipalCodec
import kui.security.rbac.{ClusterFlags, RbacPolicy}

/** Everything the connect service needs in order to be served, with no listener started.
  *
  * The same shape as every other service's (ADR-010): stopping one step short of a running server is what
  * lets the all-in-one deployment take these routes, add the rest, and start one listener over the lot.
  */
final case class ConnectServer[F[_]](
    routes: List[ServerEndpoint[Any, F]],
    interceptors: List[Interceptor[F]],
    readiness: List[ReadinessCheck[F]],
    capabilities: F[ServiceCapabilities]
)

/** The connect service's composition root.
  *
  * ==What it contacts, and when==
  *
  * Nothing. Building this opens no connection to any worker: an `UpstreamClient` is a circuit breaker, a
  * bulkhead and a failover list around a connection pool, and the pool dials on first use. A Connect cluster
  * that is down therefore delays no start-up and fails no start-up — the service starts, the capability
  * report says that cluster is degraded, and the screen says why.
  *
  * ==What it builds per Connect cluster, and what it does not==
  *
  * A cluster with no `connect` block gets **no client at all**: no HTTP pool, no circuit breaker, no upstream
  * metric series. `SchemaWiring`'s argument, which is not a micro-optimisation — an idle upstream publishes a
  * permanently zero series on every dashboard that charts it, and a metric nothing can ever make non-zero is
  * one an operator learns to ignore. The cluster still appears in the capability report, as `not_configured`,
  * which is the fact the browser needs in order to hide the row rather than break on it.
  *
  * Each configured Connect cluster gets its **own** upstream client, named after the cluster and the Connect
  * cluster. One shared breaker would mean one broken Connect cluster pausing KUI's calls to the healthy ones,
  * which is precisely the coupling the breaker exists to prevent — and this service is the first where two of
  * them routinely belong to *one* Kafka cluster, so the name carries both.
  */
object ConnectWiring {

  /** The instrumentation scope this service's tracer and meter are named after. */
  val Instrumentation: String = ConnectService.Instrumentation

  /** How many requests may be in flight to one Connect cluster at once.
    *
    * Eight, and smaller than the schema service's sixteen on purpose. A Connect worker serves its REST API
    * from the same JVM that runs the connector tasks, and its herder serialises anything that touches the
    * configuration; a burst of KUI requests is felt by the data plane, not just by an API. Eight is more than
    * a screen can generate — one list request per poll, plus whatever an operator clicks — and few enough
    * that KUI cannot be the reason a connector falls behind.
    */
  val MaxConcurrentPerWorker: PositiveInt = PositiveInt.unsafe(8)

  /** How many times a call is repeated when an address refuses a connection.
    *
    * Only the reads are ever repeated, and not because of this number: `RetryPolicy.IdempotentMethods` is
    * `GET`, `HEAD` and `OPTIONS`, so the `PUT` behind a pause and the `POST` behind a restart are never
    * retried whatever this says. That is the right split — a repeated restart is a second delivery gap — and
    * it is the reason a failover list is safe to give a service that has three mutations in it.
    */
  val MaxRetries: Int = 2

  /** Builds everything except the listener.
    *
    * @param clusters
    *   the configured clusters, from `kui.clusters[]`, read from the same file this process already loaded.
    *   Unlike the consumer and topic services this one needs no credentials from the cluster service: a
    *   Connect cluster's addresses and credentials are its own configuration block.
    * @param policy
    *   the address restriction applied to every worker URL. It is a parameter rather than `UrlPolicy.Strict`,
    *   because a Connect cluster is very often the one upstream that legitimately lives on a private network
    *   — `http://kafka-connect:8083` inside a Compose network is the ordinary arrangement — and the
    *   operator's `KUI_ALLOW_PRIVATE_UPSTREAMS` is what decides it.
    */
  def make[F[_]: {Async, Parallel}](
      clusters: List[ClusterConfig],
      policy: UrlPolicy,
      rbac: RbacPolicy,
      telemetry: Telemetry[F],
      principals: PrincipalCodec[F],
      logger: StructuredLogger[F]
  ): Resource[F, ConnectServer[F]] =
    for {
      meter <- Resource.eval(telemetry.meter(Instrumentation))
      rejections <- Resource.eval(PrincipalVerification.rejectionCounter[F](meter))
      interceptors <- Resource.eval(ConnectApi.interceptors[F](telemetry, rejections, logger))

      // One connection pool for the process, and none at all when no cluster configures Kafka Connect.
      backend <- httpBackend[F](clusters, policy)
      workers <- workersFor[F](clusters, backend, policy, telemetry, logger)
      _ <- Resource.eval(startupLog[F](clusters, logger))

      sources = new ConfiguredConnectSource[F](clusters, workers)

      audit = LoggingConnectorOperationSink.make[F](logger)
      // Who did it is not wired here. It is a parameter of every `guard` call, threaded from the principal
      // the gateway signed and the route verified (ADR-020), so an audit line names the person who made
      // the request rather than a constant this file chose.
      guard = MutationGuard.make[F](sources, audit, logger)
      useCases = ConnectUseCases.make[F](sources, guard)
      capabilities = ConnectCapabilities.make[F](sources, logger)

      // Readiness is deliberately empty, for the schema service's reason: "can this service answer" is true
      // as soon as it is wired. A check that waited for a worker would take the connect service out of
      // rotation whenever an *optional* dependency was slow — turning a component KUI treats as hostile
      // into a reason for KUI itself to be restarted.
      readiness = List.empty[ReadinessCheck[F]]

      // The permission check this service runs for itself, over the same declaration on the same endpoints
      // the gateway read (ADR-021). Read-only comes from this process's own `kui.clusters[]`, so a pause on
      // a read-only cluster is refused here whether or not the gateway was asked.
      permissions = RbacGuard.fromPolicy[F](
        rbac,
        cluster => ClusterFlags(clusters.find(_.id == cluster).exists(_.readOnly)),
        logger
      )
    } yield ConnectServer(
      routes = ConnectApi.routes[F](
        useCases,
        readiness,
        capabilities,
        principals,
        rejections,
        logger,
        permissions
      ),
      interceptors = interceptors,
      readiness = readiness,
      capabilities = ConnectApi.capabilityDocument[F](capabilities, logger)
    )

  /** The process's one HTTP connection pool, or none at all. */
  private def httpBackend[F[_]: Async](
      clusters: List[ClusterConfig],
      policy: UrlPolicy
  ): Resource[F, Option[Backend[F]]] =
    if clusters.exists(_.connect.nonEmpty) then
      HttpTls.resource[F](HttpTlsConfig.Default, policy).map(backend => Some(backend: Backend[F]))
    else Resource.pure[F, Option[Backend[F]]](None)

  /** One client per configured Connect cluster, each with its own breaker, bulkhead and failover list. */
  private def workersFor[F[_]: Async](
      clusters: List[ClusterConfig],
      backend: Option[Backend[F]],
      policy: UrlPolicy,
      telemetry: Telemetry[F],
      logger: StructuredLogger[F]
  ): Resource[F, Map[(ClusterId, ConnectName), ConnectWorkerPort[F]]] =
    backend match {
      case None => Resource.pure(Map.empty)
      case Some(transport) =>
        clusters
          .flatMap(cluster => cluster.connect.map(cluster.id -> _))
          .traverse((cluster, settings) =>
            workerFor[F](cluster, settings, transport, policy, telemetry, logger)
              .map((cluster, settings.name) -> _)
          )
          .map(_.toMap)
    }

  private def workerFor[F[_]: Async](
      cluster: ClusterId,
      settings: ConnectClusterSettings,
      transport: Backend[F],
      policy: UrlPolicy,
      telemetry: Telemetry[F],
      logger: StructuredLogger[F]
  ): Resource[F, ConnectWorkerPort[F]] =
    for {
      upstream <- UpstreamClient.resource[F](
        upstreamConfig(cluster, settings, policy),
        transport,
        telemetry,
        ConnectApi.Id,
        logger
      )
      tokenBackend <- tokenBackendFor[F](cluster, settings, transport, policy, telemetry, logger)
      credentials <- ConnectCredentials.fromConfig[F](settings.auth, tokenBackend, logger)
      // The first URL only decides how each path is joined; which worker a request actually goes to is the
      // resilient backend's decision, because failover may send it to the second address. Every worker in
      // a Connect cluster serves the same REST API and forwards what it cannot answer itself, so a second
      // address is genuine failover rather than a second system.
    } yield new ConnectHttp[F](upstream.backend, settings.urls.head, settings.name, credentials)

  /** `private[app]` so that `ConnectWiringSuite` can read the fields that decide where a request goes.
    *
    * `SchemaWiring`'s seam and its reason: the alternative is a suite that starts a server and watches which
    * port is dialled, which is what that rule had instead of a case — nothing, because building the whole
    * composition root needs a worker to dial. One keyword makes the addresses a value a case can assert.
    *
    * `retryableStatuses` is deliberately **empty**, and `RetryPolicy`'s own scaladoc names the status this is
    * about: Connect's `409` "rebalance in progress". Retrying it inside the call would hide, for as long as
    * the retries last, a state the screen is built to show — and a rebalance routinely outlives two backoffs,
    * so the retry would buy a slower request that reports the same thing. `ErrorCode.ConnectRebalancing` is
    * `retryable = true`, which tells the *caller* to ask again, and the screen polls. ADR-054 §4.
    */
  private[app] def upstreamConfig(
      cluster: ClusterId,
      settings: ConnectClusterSettings,
      policy: UrlPolicy
  ): UpstreamConfig =
    UpstreamConfig(
      // Named per cluster *and* per Connect cluster, because one Kafka cluster routinely has two and a
      // dashboard that said "kafka-connect is failing" would not say which.
      name = s"${ConnectHttp.upstreamName(settings.name)}.${cluster.value}",
      urls = settings.urls,
      callTimeout = settings.callTimeout,
      maxConcurrent = MaxConcurrentPerWorker,
      maxRetries = MaxRetries,
      urlPolicy = policy
    )

  /** The transport for an OAuth token endpoint, which is **not** the worker's resilient backend.
    *
    * That one fails over between the Connect cluster's own addresses; a token request routed to a worker
    * because the issuer was briefly slow would be a request carrying a client secret, sent to the wrong
    * system. It gets its own upstream client, with the same protections and its own name.
    *
    * `None` for the two mechanisms that need no issuer, so a deployment using basic or no authentication
    * opens nothing.
    */
  private def tokenBackendFor[F[_]: Async](
      cluster: ClusterId,
      settings: ConnectClusterSettings,
      transport: Backend[F],
      policy: UrlPolicy,
      telemetry: Telemetry[F],
      logger: StructuredLogger[F]
  ): Resource[F, Option[Backend[F]]] =
    settings.auth match {
      case UpstreamAuthConfig.OAuth(endpoint, _, _, _) =>
        UpstreamClient
          .resource[F](
            tokenUpstreamConfig(cluster, settings, endpoint, policy),
            transport,
            telemetry,
            ConnectApi.Id,
            logger
          )
          .map(client => Some(client.backend))
      case _ => Resource.pure(None)
    }

  /** The token endpoint's own upstream, as a value rather than as an expression inside a `Resource`.
    *
    * Lifted out for the reason [[upstreamConfig]] is `private[app]`: "the issuer's address and *only* the
    * issuer's address" is the whole rule, and while it lived inline the only way to check it was to watch a
    * socket. Aimed at `settings.urls` instead, every case in this service stays green and KUI posts a client
    * secret to a Kafka Connect worker.
    */
  private[app] def tokenUpstreamConfig(
      cluster: ClusterId,
      settings: ConnectClusterSettings,
      endpoint: SafeUrl,
      policy: UrlPolicy
  ): UpstreamConfig =
    UpstreamConfig(
      name = s"${ConnectCredentials.TokenUpstreamName}.${settings.name.value}.${cluster.value}",
      urls = NonEmptyList.one(endpoint),
      callTimeout = settings.callTimeout,
      maxConcurrent = MaxConcurrentPerWorker,
      // A token request is a POST, and a duplicate one costs an extra token rather than an extra side
      // effect: issuers treat client-credentials grants as repeatable.
      maxRetries = 1,
      urlPolicy = policy
    )

  /** One INFO line per Connect cluster an operator configured, and one that says when none is.
    *
    * "Which Connect cluster is this reading?" is the first question asked when the Connect screen shows
    * something unexpected, and after the fact it is unanswerable unless the process said so at startup. The
    * address is safe to log — it is the operator's own text — and the mechanism is named without its secret.
    *
    * The "no Connect cluster configured" line matters as much as the others: it is what tells an operator who
    * expected a Kafka Connect row that KUI is behaving as configured rather than failing.
    */
  private[app] def startupLog[F[_]: Async](
      clusters: List[ClusterConfig],
      logger: StructuredLogger[F]
  ): F[Unit] = {
    val configured = clusters.filter(_.connect.nonEmpty)

    if configured.isEmpty then
      logger.info(
        "no cluster configures kui.clusters.<n>.connect[].url, so every cluster reports the connect " +
          "feature as not configured and no Connect client is opened"
      )
    else
      configured.traverse_(cluster =>
        cluster.connect.traverse_(settings =>
          logger.info(
            Map(
              "cluster.id" -> cluster.id.value,
              "connect.name" -> settings.name.value,
              "connect.urls" -> settings.urls.toList.map(_.value).mkString(","),
              "connect.auth" -> settings.auth.describe
            )
          )(
            s"cluster ${cluster.id.value} reads the Kafka Connect cluster '${settings.name.value}' at " +
              settings.urls.head.value
          )
        )
      )
  }
}
