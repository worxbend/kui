package kui.gateway.app

import java.time.Instant

import cats.Parallel
import cats.data.NonEmptyList
import cats.effect.kernel.{Async, Resource}
import cats.syntax.all.*
import org.typelevel.log4cats.StructuredLogger
import sttp.capabilities.fs2.Fs2Streams
import sttp.client4.StreamBackend
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.interceptor.Interceptor

import kui.config.{HttpTlsConfig, PrincipalKeyConfig}
import kui.gateway.api.auth.SessionMiddleware
import kui.gateway.api.client.SttpServiceClient
import kui.gateway.api.openapi.DocsRoutes
import kui.gateway.api.routing.{ContractRouting, PolicyRbacPreCheck, RbacPreCheck, ServiceContracts}
import kui.gateway.api.search.{GroupSearchSource, SubjectSearchSource, TopicSearchSource}
import kui.gateway.api.{
  AlertsStreamRoutes,
  CapabilityRoutes,
  ClusterOverviewRoutes,
  EdgeHeaders,
  GatewayApi,
  InfoRoutes,
  KsqlStreamRoutes,
  MessageStreamRoutes,
  SearchRoutes,
  TopicOverviewRoutes
}
import kui.gateway.application.capability.{
  CapabilityRegistry,
  CapabilitySignals,
  CircuitFeed,
  ReadinessPoller,
  RegistryConfig
}
import kui.gateway.application.client.{ServiceClient, ServiceClients}
import kui.gateway.application.cluster.ClusterOverviewUseCase
import kui.gateway.application.search.{SearchSource, SearchUseCase}
import kui.gateway.application.session.{InMemorySessionStore, SessionConfig}
import kui.gateway.application.topic.{ConsumerGroupsSource, TopicOverviewUseCase}
import kui.http.health.ReadinessCheck
import kui.http.upstream.HttpTls
import kui.http.{BasePath, Cors, ErrorInterceptor}
import kui.kernel.ServiceId
import kui.observability.{KuiInterceptors, Telemetry}
import kui.security.rbac.ClusterFlags
import kui.security.{JwsPrincipalCodec, PrincipalCodec, SigningKey}

/** Everything a gateway needs in order to be served, with no listener started.
  *
  * Stopping one step short of a running server is what lets the all-in-one deployment (AIO-001) reuse this:
  * it takes these routes, adds the routes of every in-process service, and starts a single listener over the
  * lot. If `make` bound a port, the all-in-one would have to either start twelve servers or reimplement the
  * assembly.
  *
  * @param routes
  *   the endpoints, in match order, with `/api/v1` already applied but not the deployment's base path — that
  *   is `KuiServer`'s job, applied once over whatever list it is finally given
  * @param interceptors
  *   the cross-cutting chain, outermost first
  * @param readiness
  *   the checks behind `/api/v1/health/ready`, exposed so that a composition root that adds upstreams can add
  *   their checks too
  */
/** @param fillableTopicSections
  *   which sections of the topic-page aggregation this assembled gateway can actually fill, read from the
  *   sources that were wired rather than from a list somebody maintains. It is reported because the
  *   alternative is unobservable: a section whose source was never registered answers `not_configured`, which
  *   is indistinguishable from a section this build was never meant to serve, and that is exactly how the
  *   Consumers tab shipped empty against a healthy consumer service
  */
final case class GatewayServer[F[_]](
    routes: List[ServerEndpoint[Fs2Streams[F], F]],
    interceptors: List[Interceptor[F]],
    readiness: List[ReadinessCheck[F]],
    fillableTopicSections: Set[String]
)

/** The gateway's composition root (ADR-010).
  *
  * It is the only place in the gateway that constructs anything concrete. Every layer below it takes its
  * collaborators as constructor parameters, which is what makes them testable with hand-written fakes rather
  * than with a mocking framework, and what makes this file the one place to look to find out what is actually
  * wired to what.
  */
object GatewayWiring {

  /** Builds the gateway.
    *
    * ==It has no mandatory upstream, and that is a requirement rather than an accident==
    *
    * Nothing in here fails because a service is unreachable, and nothing in here waits for one. The gateway
    * is Core tier (PLAN §15): when it is down the browser has nowhere to go and shows the single full-screen
    * "cannot reach gateway" state (UI-011), so a gateway that refused to start until the schema service
    * answered would convert one optional service's outage into a total blackout — precisely when an operator
    * most needs a working UI to see what is wrong. Upstreams are polled after startup and their absence is
    * reported as capability state (GW-003), never as a failure to boot.
    *
    * ==Interceptor order==
    *
    * The chain is the edge policy, then instrumentation, then error handling, and each position is load
    * bearing:
    *
    *   1. CORS outermost when a deployment has enabled it, because a preflight `OPTIONS` has to be answered
    *      before anything tries to route it. It is off by default and the shipped deployment serves the shell
    *      from this same origin, so most installations have no CORS layer at all (ADR-019).
    *   1. `EdgeHeaders` next, because it decides what everything else is even allowed to see. If tracing ran
    *      ahead of it, a span would carry a correlation id a browser chose (ADR-040). `Cookie` is not an
    *      `X-Kui-*` header, so it survives this step untouched for the session middleware to read.
    *   1. `SessionMiddleware` next: attaches a session (creating one if the request had none) and, on a
    *      mutation, applies the CSRF verdict before any endpoint's own logic runs (ADR-019). This has to sit
    *      ahead of tracing and metrics for the same reason `EdgeHeaders` sits ahead of it — a request CSRF
    *      rejects should not be traced as if some endpoint's logic had run at all.
    *   1. `KuiInterceptors` next: correlation, tracing, metrics, in that order, so that a failing request is
    *      still inside a span and still records its duration.
    *   1. `ErrorInterceptor` last, so it is innermost and sees the failure closest to where it happened. It
    *      is appended here rather than inside `KuiInterceptors` because `libs/http` depends on
    *      `libs/observability` and so the dependency cannot point the other way.
    */
  def make[F[_]: {Async, Parallel}](
      config: GatewayServiceConfig,
      telemetry: Telemetry[F],
      logger: StructuredLogger[F]
  ): Resource[F, GatewayServer[F]] =
    over[F](config, telemetry, logger, httpUpstreams[F](config, telemetry, logger))

  /** The same gateway, over whichever set of service clients the composition root hands it.
    *
    * This is the seam ADR-005 needs and ADR-010 asks every service to expose. The distributed process builds
    * `httpUpstreams`; the all-in-one process (AIO-001) builds in-process clients over the very same services'
    * server logic. Everything after this parameter — the registry, the readiness poller, the circuit feed,
    * the contract-derived proxy routes, the merged documentation — is the same code in both shapes, because
    * there is only one copy of it and both callers reach it through here.
    *
    * @param clients
    *   one client per service this deployment can reach. A `Resource` rather than a value because the HTTP
    *   shape owns a connection pool and a circuit breaker that have to be released; the in-process shape
    *   hands over a `Resource.pure`.
    */
  def over[F[_]: {Async, Parallel}](
      config: GatewayServiceConfig,
      telemetry: Telemetry[F],
      logger: StructuredLogger[F],
      clients: Resource[F, ServiceClients[F]]
  ): Resource[F, GatewayServer[F]] = {
    val readiness = readinessChecks[F]

    for {
      _ <- Resource.eval(warnIfInsecureCookies[F](logger, config))
      sessions <- InMemorySessionStore.resource[F](SessionConfig.Default)
      registry <- CapabilityRegistry.resource[F](RegistryConfig.Default, telemetry, logger)
      clients <- clients
      // The service list comes from the clients rather than from `config.gateway.services`, because those
      // two are the same list only in the distributed shape. All-in-one configures no upstream URLs at all
      // and still reaches every service, so reading the configuration here would report a deployment that
      // works perfectly as one that knows about nothing.
      routed = clients.all.map(_.service)
      signals <- Resource.eval(
        CapabilitySignals.make[F](RegistryConfig.Default, registry, routed)
      )
      trigger <- ReadinessPoller.resource[F](
        clients,
        signals,
        config.gateway.readinessInterval,
        logger
      )
      _ <- Resource.eval(registry.attachProbe(trigger.probe))
      _ <- CircuitFeed.resource[F](clients, signals)
      docs <- Resource.eval(
        Async[F].fromEither(
          DocsRoutes
            .document[F](routed, List(publicBaseUrl(config)))
            .leftMap(BadContract.apply)
        )
      )
      // The edge's permission check, built from the deployment's roles and its per-cluster read-only
      // flags. Every proxied call goes through it before it leaves the gateway; a refusal costs the
      // service behind it nothing at all.
      rbac = new PolicyRbacPreCheck[F](
        config.rbac,
        cluster => Async[F].pure(config.clusterFlags.getOrElse(cluster, ClusterFlags.Writable)),
        logger
      )
      proxied <- Resource.eval(
        Async[F].fromEither(proxyRoutes[F](clients, signals, rbac).leftMap(BadContract.apply))
      )
      // The dashboard, which the gateway answers itself over the cluster service's client. A deployment
      // with no cluster service configured has no such client and therefore no route: the path 404s,
      // rather than answering an empty list that would read as "no clusters are configured".
      //
      // The topic and consumer clients are *optional* extras to it, and each one's absence is a section
      // the document reports as `NotConfigured` rather than a route that disappears. That asymmetry is
      // deliberate: without a cluster service there is no dashboard to draw, while without a consumer
      // service there is a dashboard with one panel that says this deployment has no such thing.
      topicClient = clients.all.find(_.service == TopicServiceId)
      consumerClient = clients.all.find(_.service == ConsumerServiceId)
      overview <- clients.all
        .find(_.service == ClusterServiceId)
        .traverse(
          ClusterOverviewUseCase
            .resource[F](_, registry, signals, logger, topics = topicClient, groups = consumerClient)
        )
      // The topic page's aggregation, on the same terms: a deployment with no topic service configured has
      // no client and therefore no route, so `/overview` 404s rather than answering a document whose topic
      // section is permanently unavailable for a service nobody deployed.
      // The Consumers tab is a map entry, which is what `TopicOverviewUseCase`'s extension point was built
      // for — and it was missing until now. A section with no source is reported `NotConfigured`, so a
      // deployment with a perfectly healthy consumer service was telling every operator that it did not
      // track consumer groups.
      overviewSources = ConsumerGroupsSource.sources[F](consumerClient)
      topicOverview <- topicClient.traverse(
        TopicOverviewUseCase.resource[F](_, signals, telemetry, overviewSources)
      )
      // Cross-entity search (ADR-049), on the same terms as the dashboard: it needs the cluster list to
      // know what to search, so a deployment with no cluster service has no route rather than an
      // endpoint that answers an empty document to every query. The three services it folds over are
      // each optional, and one that is missing is named in `partial` rather than silently dropped —
      // which is the whole reason the fold walks `SearchUseCase.Services` instead of whatever happens
      // to be configured.
      searchSources = SearchUseCase.Services.flatMap(clients.get).flatMap(searchSourceOf[F])
      search = clients.all.find(_.service == ClusterServiceId).map(SearchUseCase.of[F](_, searchSources))
      // The message browse stream, relayed rather than proxied. A deployment with no message service
      // configured has no client and therefore no route, so the address 404s instead of opening a
      // stream that could only ever end in an error.
      messages = clients.all.find(_.service == MessageServiceId)
      // The alerts change stream follows the same relay path. Its feed and acknowledgement are ordinary
      // derived proxy routes; only the event stream needs this client to be mounted separately.
      alerts = clients.all.find(_.service == AlertsServiceId)
      // The ksqlDB push query, relayed for the same reason and on the same terms: a deployment with no
      // ksql service configured has no client and therefore no stream route, so the address 404s rather
      // than opening a subscription that could only ever end in an error. Its object listing, plan and
      // apply are ordinary derived proxy routes and are unaffected by this line.
      ksql = clients.all.find(_.service == KsqlServiceId)
      // The identity service, which the sign-in routes call one hop inward. It is the one service whose
      // contract the gateway does *not* proxy: a login is the moment a browser is given a session, and
      // sessions live here, so `AuthRoutes` serves `/api/v1/auth/*` itself and calls this client.
      identity = clients.all.find(_.service == IdentityServiceId)
      instrumentation <- Resource.eval(
        KuiInterceptors.serverInterceptors[F](telemetry, GatewayApi.ServiceName)
      )
    } yield GatewayServer(
      routes = GatewayApi.routes[F](
        config.view,
        readiness,
        sessions,
        CapabilityRoutes[F](registry, trigger, telemetry, logger, rbac = config.rbac) ++
          overview.toList.flatMap(ClusterOverviewRoutes[F](_)) ++
          topicOverview.toList.flatMap(TopicOverviewRoutes[F](_)) ++
          search.toList.flatMap(SearchRoutes[F](_)) ++
          messages.toList.flatMap(MessageStreamRoutes[F](_)) ++
          alerts.toList.flatMap(AlertsStreamRoutes[F](_, rbac)) ++
          ksql.toList.flatMap(KsqlStreamRoutes[F](_, rbac)) ++
          proxied ++
          DocsRoutes[F](docs, BasePath.normalize(config.server.basePath)),
        identity
      ),
      interceptors = Cors.interceptor[F](config.gateway.cors).toList ++
        EdgeHeaders.interceptors[F] ++
        SessionMiddleware.interceptors[F](
          sessions,
          logger,
          BasePath.normalize(config.server.basePath),
          secureCookies = !config.gateway.devInsecureCookies,
          authenticationRequired = config.auth.authType != kui.config.AuthType.Disabled,
          trustedProxies = config.auth.trustedProxies
        ) ++
        instrumentation ++
        ErrorInterceptor.interceptors[F](logger),
      readiness = readiness,
      fillableTopicSections =
        TopicOverviewUseCase.fillable[F](overviewSources, hasTopicClient = topicClient.isDefined)
    )
  }

  /** The prominent warning ADR-019 requires whenever `server.devInsecureCookies` strips `Secure` off the
    * session cookie. It is a `WARN` and not a `DEBUG` on purpose: it is the one line in a deployment's log
    * that says its session cookie can be read by anyone on the same network, and it must be visible without
    * anyone having had to turn on verbose logging first to notice.
    */
  def warnIfInsecureCookies[F[_]: cats.Applicative](
      logger: StructuredLogger[F],
      config: GatewayServiceConfig
  ): F[Unit] =
    if config.gateway.devInsecureCookies then
      logger.warn(
        "server.devInsecureCookies=true: the session cookie is served without Secure. " +
          "This is meant for local development over plain HTTP only and must never be set in a " +
          "deployment reachable over the network."
      )
    else cats.Applicative[F].unit

  /** What the gateway checks before it says it is ready.
    *
    * Empty of upstream checks on purpose — see `make`'s note. The single check that the process is running is
    * not a tautology worth removing: `/health/ready` must answer with a report, and a report with no checks
    * in it reads like a bug, while one that names `process` says plainly that this service depends on nothing
    * to serve.
    */
  def readinessChecks[F[_]: cats.Applicative]: List[ReadinessCheck[F]] =
    List(ReadinessCheck.always[F]("process"))

  /** A contract that cannot be routed. Raised at startup, never at request time: a gateway that would serve a
    * broken route must refuse to start instead, where an operator sees it once rather than in every user's
    * browser.
    */
  /** A configuration the gateway will not start with. Refusing at startup is the point: a signing key too
    * short for HS256 weakens every principal the gateway mints, and a deployment must find that out from its
    * own logs rather than from an audit.
    */
  final case class Misconfigured(problem: String) extends Exception(problem)

  final case class BadContract(problem: String)
      extends Exception(s"the gateway cannot derive routes from a service contract: $problem")

  /** Every configured service, reached over HTTP, with the process-wide connection pool underneath.
    *
    * The pool is opened here and nowhere else. It is the distributed shape's one piece of real transport, so
    * this is also the whole of what the all-in-one shape replaces.
    */
  def httpUpstreams[F[_]: Async](
      config: GatewayServiceConfig,
      telemetry: Telemetry[F],
      logger: StructuredLogger[F]
  ): Resource[F, ServiceClients[F]] =
    HttpTls
      .resource[F](HttpTlsConfig.Default, config.urlPolicy)
      .flatMap(backend => upstreams[F](config, telemetry, logger, backend))

  /** One client per configured service, each with its own bulkhead and circuit breaker (PLAN §16.4).
    *
    * `config.urlPolicy` is passed through rather than left at `UpstreamConfig`'s strict default: the address
    * rule is re-applied to every request, so the client has to hold the same answer the configuration loader
    * held. See `SttpServiceClient.upstreamConfig` for what the mismatch cost.
    */
  def upstreams[F[_]: Async](
      config: GatewayServiceConfig,
      telemetry: Telemetry[F],
      logger: StructuredLogger[F],
      backend: StreamBackend[F, Fs2Streams[F]]
  ): Resource[F, ServiceClients[F]] =
    Resource
      .eval(Async[F].fromEither(principals[F](config).leftMap(Misconfigured.apply)))
      .flatMap(codec =>
        config.gateway.services.toList
          .sortBy(_._1.value)
          .traverse((service, upstream) =>
            SttpServiceClient
              .resource[F](service, upstream, codec, telemetry, logger, backend, config.urlPolicy)
          )
          .map(ServiceClients.of[F])
      )

  /** How the gateway signs the principal it hands to a service (ADR-020).
    *
    * With no keys configured it falls back to the in-process codec, which does not sign at all and says so
    * loudly on stderr. That is right for the all-in-one deployment, where the "network" between the gateway
    * and a service is a function call, and wrong for anything else -- which is why it announces itself rather
    * than being quietly convenient.
    */
  def principals[F[_]: Async](config: GatewayServiceConfig): Either[String, PrincipalCodec[F]] =
    NonEmptyList.fromList(config.gateway.principalKeys.map(signingKey)) match {
      case None => Right(PrincipalCodec.inProcess[F])
      case Some(keys) =>
        JwsPrincipalCodec
          .make[F](keys, GatewayApi.ServiceName)
          .leftMap(weak => s"kui.gateway.principalKeys: ${weak.message}")
    }

  private def signingKey(key: PrincipalKeyConfig): SigningKey =
    SigningKey(key.kid, key.key.map(_.getBytes(java.nio.charset.StandardCharsets.UTF_8)), key.notBefore)

  /** The `servers` entry of the served document.
    *
    * The base path is included because a deployment behind a reverse proxy at `/kui` must produce URLs that
    * work from the browser's point of view, not the server's. The host is deliberately left out: the gateway
    * does not reliably know the name it is reached by, and a wrong absolute URL in the docs is worse than a
    * relative one that always works.
    */
  def publicBaseUrl(config: GatewayServiceConfig): String = {
    val base = BasePath.normalize(config.server.basePath)
    if base.isEmpty then "/" else base
  }

  /** The service whose list endpoint the gateway aggregates rather than proxies. */
  val ClusterServiceId: ServiceId = ServiceId.unsafe("cluster")

  /** The service the topic-page aggregation and the dashboard's topic totals call. Its other endpoints are
    * proxied untouched.
    */
  val TopicServiceId: ServiceId = ServiceId.unsafe("topic")

  /** The service the dashboard's consumer-group totals call. Everything else it serves is proxied. */
  val ConsumerServiceId: ServiceId = ServiceId.unsafe("consumer")

  /** The service whose browse stream is relayed. It has no proxied endpoints: a stream cannot be called and
    * re-encoded without buffering it, so `MessageStreamRoutes` moves its bytes instead.
    */
  val MessageServiceId: ServiceId = ServiceId.unsafe("message")

  /** The service whose feed-change stream is relayed. Its JSON feed and acknowledgement remain ordinary
    * contract-derived proxy routes.
    */
  val AlertsServiceId: ServiceId = ServiceId.unsafe("alerts")

  /** The service whose push query is relayed. Its object listing, statement plan and statement apply remain
    * ordinary contract-derived proxy routes; only the unbounded query result needs this client mounted
    * separately, because a route that decoded and re-encoded it would never answer.
    */
  val KsqlServiceId: ServiceId = ServiceId.unsafe("ksql")

  /** Which search source a client answers for, or none when the search does not fold over that service.
    *
    * A `match` on the id rather than a map built at the call site, so that adding a fourth searchable service
    * is one case here and one entry in `SearchUseCase.Services` — and so that the compiler, rather than a
    * reader, is what notices when the two lists stop agreeing.
    */
  def searchSourceOf[F[_]: Async](client: ServiceClient[F]): Option[SearchSource[F]] =
    client.service match {
      case SearchUseCase.TopicService => Some(TopicSearchSource[F](client))
      case SearchUseCase.ConsumerService => Some(GroupSearchSource[F](client))
      case SearchUseCase.SchemaService => Some(SubjectSearchSource[F](client))
      case _ => None
    }

  /** The service that decides who somebody is. Its endpoints are never proxied: `ServiceContracts` says so at
    * the one place they would otherwise be registered, and `AuthRoutes` calls them itself so that a
    * successful sign-in leaves with a session cookie rather than with a JSON body.
    */
  val IdentityServiceId: ServiceId = ServiceId.unsafe("identity")

  /** The proxied routes: every configured service the gateway holds a contract for, minus the endpoints the
    * gateway answers itself.
    */
  def proxyRoutes[F[_]: Async](
      clients: ServiceClients[F],
      signals: CapabilitySignals[F],
      rbac: RbacPreCheck[F]
  ): Either[String, List[ServerEndpoint[Fs2Streams[F], F]]] =
    clients.all
      .traverse(client =>
        ContractRouting.derive[F](
          client.service,
          // `proxied`, not `of`: the cluster list is served by the aggregation above, and two routes
          // claiming one path is a collision nobody sees in a route list.
          ServiceContracts.proxied(client.service),
          client,
          signals,
          rbac
        )
      )
      .map(_.flatten)

  /** The one INFO line every KUI process writes as it starts.
    *
    * The build fields are the same values `GET /api/v1/info` reports, from the same object, so a log line and
    * the endpoint can be cross-checked rather than compared and hoped about. That matters in exactly the
    * situation the fields exist for: someone has two log files and an endpoint response and is trying to work
    * out which of three containers is the old one.
    */
  def startupLog[F[_]](
      logger: StructuredLogger[F],
      config: GatewayServiceConfig,
      at: Instant
  ): F[Unit] =
    logger.info(
      Map(
        "service" -> GatewayApi.ServiceName,
        "port" -> config.server.port.value.toString,
        "basePath" -> config.server.basePath,
        "services" -> config.gateway.services.keys.map(_.value).toList.sorted.mkString(","),
        "version" -> InfoRoutes.buildInfo.version,
        "gitCommit" -> InfoRoutes.buildInfo.gitCommitShort,
        "gitDirty" -> InfoRoutes.buildInfo.gitDirty.toString,
        "builtAt" -> InfoRoutes.buildInfo.builtAt.toString,
        "startedAt" -> at.toString
      )
    )(
      s"starting ${GatewayApi.ServiceName} ${InfoRoutes.buildInfo.version} (${InfoRoutes.buildInfo.gitCommitShort})"
    )
}
