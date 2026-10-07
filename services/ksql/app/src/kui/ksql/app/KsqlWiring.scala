package kui.ksql.app

import java.nio.charset.StandardCharsets
import java.security.SecureRandom

import cats.Parallel
import cats.data.NonEmptyList
import cats.effect.kernel.{Async, Resource}
import cats.syntax.all.*
import org.typelevel.log4cats.StructuredLogger
import sttp.capabilities.fs2.Fs2Streams
import sttp.client4.{Backend, StreamBackend}
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.interceptor.Interceptor

import kui.config.{ClusterConfig, HttpTlsConfig, KsqlSettings, SafeUrl, UpstreamAuthConfig, UrlPolicy}
import kui.contracts.capability.ServiceCapabilities
import kui.http.health.ReadinessCheck
import kui.http.principal.{PrincipalVerification, RbacGuard}
import kui.http.sse.SseConfig
import kui.http.upstream.{HttpTls, UpstreamClient, UpstreamConfig}
import kui.kernel.{ClusterId, PositiveInt, Secret}
import kui.ksql.api.{KsqlApi, KsqlCapabilities}
import kui.ksql.application.*
import kui.ksql.infrastructure.*
import kui.observability.Telemetry
import kui.security.PrincipalCodec
import kui.security.rbac.{ClusterFlags, RbacPolicy}

/** Everything the ksql service needs in order to be served, with no listener started.
  *
  * The same shape as every other service's (ADR-010): stopping one step short of a running server is what
  * lets the all-in-one deployment take these routes, add the rest, and start one listener over the lot.
  */
final case class KsqlServer[F[_]](
    routes: List[ServerEndpoint[Fs2Streams[F], F]],
    interceptors: List[Interceptor[F]],
    readiness: List[ReadinessCheck[F]],
    capabilities: F[ServiceCapabilities]
)

/** The ksql service's composition root.
  *
  * ==What it contacts, and when==
  *
  * Nothing. Building this opens no connection to any server: an `UpstreamClient` is a circuit breaker, a
  * bulkhead and a failover list around a connection pool, and the pool dials on first use. A ksqlDB that is
  * down therefore delays no start-up and fails no start-up — the service starts, the capability report says
  * that cluster is degraded, and the screen says why.
  *
  * ==Two backends per process, and it is not an oversight==
  *
  * The pooled transport is built once. Around it, each configured ksqlDB gets an `UpstreamClient` for the
  * calls that finish; the **push query uses the pooled transport directly**, because a `StreamRequest` needs
  * a `StreamBackend` — which the resilient wrapper is not — and because a bulkhead permit held for the length
  * of a push query is a permit held for minutes, which would let three open queries starve every other call
  * to the same server. `KsqlHttp`'s header says what that trades away and ADR-055 §5 records it.
  */
object KsqlWiring {

  /** The instrumentation scope this service's tracer and meter are named after. */
  val Instrumentation: String = KsqlService.Instrumentation

  /** How many requests that finish may be in flight to one ksqlDB at once.
    *
    * Eight, and for `ConnectWiring`'s reason rather than by copying its number: a ksqlDB server runs its REST
    * API in the same JVM that runs the persistent queries, and its command runner serialises anything that
    * touches the metastore. A burst of KUI requests is felt by the stream processing, not just by an API.
    * Eight is more than a screen can generate — one object listing per poll, plus whatever an operator runs —
    * and few enough that KUI cannot be the reason a query falls behind.
    *
    * Push queries are outside this bound entirely, which is the trade this object's header states.
    */
  val MaxConcurrentPerServer: PositiveInt = PositiveInt.unsafe(8)

  /** How many times a call is repeated when an address refuses a connection.
    *
    * Only the reads are ever repeated, and not because of this number: `RetryPolicy.IdempotentMethods` is
    * `GET`, `HEAD` and `OPTIONS`, and **every** ksqlDB request is a `POST`, so nothing in this service is
    * retried whatever this says. That is the right answer and it is stated rather than left to be inferred: a
    * repeated `CREATE STREAM` is a second statement on the command topic, and a repeated
    * `DROP … DELETE TOPIC` is a second deletion of a topic somebody may have recreated in between.
    */
  val MaxRetries: Int = 2

  /** Builds everything except the listener.
    *
    * @param policy
    *   the address restriction applied to every ksqlDB URL. It is a parameter rather than `UrlPolicy.Strict`,
    *   because a ksqlDB is very often on a private network — `http://ksqldb-server:8088` inside a Compose
    *   network is the ordinary arrangement — and the operator's `KUI_ALLOW_PRIVATE_UPSTREAMS` is what decides
    *   it.
    * @param cursorKey
    *   ADR-026's streaming cursor key, which is what plan tokens are signed with (ADR-045). One secret for a
    *   deployment to configure and one to rotate; this service's use is kept apart from the topic and
    *   consumer services' by the operation name inside the payload.
    */
  def make[F[_]: {Async, Parallel}](
      clusters: List[ClusterConfig],
      policy: UrlPolicy,
      rbac: RbacPolicy,
      cursorKey: Option[Secret[String]],
      telemetry: Telemetry[F],
      principals: PrincipalCodec[F],
      logger: StructuredLogger[F],
      sse: SseConfig = SseConfig.default
  ): Resource[F, KsqlServer[F]] =
    for {
      meter <- Resource.eval(telemetry.meter(Instrumentation))
      rejections <- Resource.eval(PrincipalVerification.rejectionCounter[F](meter))
      interceptors <- Resource.eval(KsqlApi.interceptors[F](telemetry, rejections, logger))

      // One connection pool for the process, and none at all when no cluster configures ksqlDB.
      transport <- httpBackend[F](clusters, policy)
      clients <- clientsFor[F](clusters, transport, policy, telemetry, logger)
      _ <- Resource.eval(startupLog[F](clusters, logger))

      sources = new ConfiguredKsqlSource[F](clusters, clients)

      key <- Resource.eval(signingKey[F](cursorKey, logger))
      tokens = KsqlPlanToken.make[F](key)
      audit = LoggingKsqlStatementSink.make[F](logger)
      // Who did it is not wired here. It is a parameter of every `guard` call, threaded from the principal
      // the gateway signed and the route verified (ADR-020), so an audit line names the person who made
      // the request rather than a constant this file chose.
      guard = MutationGuard.make[F](sources, audit, logger)
      useCases = KsqlUseCases.make[F](sources, tokens, guard)
      capabilities = KsqlCapabilities.make[F](sources, logger)

      // Readiness is deliberately empty, for the schema service's reason: "can this service answer" is true
      // as soon as it is wired. A check that waited for a ksqlDB would take this service out of rotation
      // whenever an *optional* dependency was slow — turning a component KUI treats as hostile into a
      // reason for KUI itself to be restarted.
      readiness = List.empty[ReadinessCheck[F]]

      // The permission check this service runs for itself, over the same declaration on the same endpoints
      // the gateway read (ADR-021). Read-only comes from this process's own `kui.clusters[]`, so a
      // statement on a read-only cluster is refused here whether or not the gateway was asked.
      permissions = RbacGuard.fromPolicy[F](
        rbac,
        cluster => ClusterFlags(clusters.find(_.id == cluster).exists(_.readOnly)),
        logger
      )
    } yield KsqlServer(
      routes = KsqlApi.routes[F](
        useCases,
        readiness,
        capabilities,
        principals,
        rejections,
        logger,
        permissions,
        telemetry,
        sse
      ),
      interceptors = interceptors,
      readiness = readiness,
      capabilities = KsqlApi.capabilityDocument[F](capabilities, logger)
    )

  /** The process's one HTTP connection pool, or none at all.
    *
    * Typed as a `StreamBackend` rather than widened to `Backend`, because the push query needs the stream
    * capability and the widening `ConnectWiring` does would throw it away.
    */
  private def httpBackend[F[_]: Async](
      clusters: List[ClusterConfig],
      policy: UrlPolicy
  ): Resource[F, Option[StreamBackend[F, Fs2Streams[F]]]] =
    if clusters.exists(_.ksql.isDefined) then
      HttpTls
        .resource[F](HttpTlsConfig.Default, policy)
        .map(backend => Some(backend: StreamBackend[F, Fs2Streams[F]]))
    else Resource.pure[F, Option[StreamBackend[F, Fs2Streams[F]]]](None)

  /** One client per configured ksqlDB, each with its own breaker, bulkhead and failover list. */
  private def clientsFor[F[_]: Async](
      clusters: List[ClusterConfig],
      transport: Option[StreamBackend[F, Fs2Streams[F]]],
      policy: UrlPolicy,
      telemetry: Telemetry[F],
      logger: StructuredLogger[F]
  ): Resource[F, Map[ClusterId, KsqlClient[F]]] =
    transport match {
      case None => Resource.pure(Map.empty)
      case Some(pooled) =>
        clusters
          .flatMap(cluster => cluster.ksql.map(cluster.id -> _))
          .traverse((cluster, settings) =>
            clientFor[F](cluster, settings, pooled, policy, telemetry, logger).map(cluster -> _)
          )
          .map(_.toMap)
    }

  private def clientFor[F[_]: Async](
      cluster: ClusterId,
      settings: KsqlSettings,
      pooled: StreamBackend[F, Fs2Streams[F]],
      policy: UrlPolicy,
      telemetry: Telemetry[F],
      logger: StructuredLogger[F]
  ): Resource[F, KsqlClient[F]] =
    for {
      upstream <- UpstreamClient.resource[F](
        upstreamConfig(cluster, settings, policy),
        pooled,
        telemetry,
        KsqlApi.Id,
        logger
      )
      tokenBackend <- tokenBackendFor[F](cluster, settings, pooled, policy, telemetry, logger)
      credentials <- KsqlCredentials.fromConfig[F](settings.auth, tokenBackend, logger)
      // The first URL only decides how each path is joined; which server a *call* actually goes to is the
      // resilient backend's decision, because failover may send it to the second address. A push query
      // goes to the first address and stays there, because it does not go through that backend at all —
      // and that is the honest behaviour for a query that is held open: failing it over mid-stream would
      // restart the query somewhere else and replay rows the client had already drawn.
    } yield new KsqlHttp[F](
      upstream.backend,
      pooled,
      settings.urls.head,
      settings.streamTimeout,
      credentials
    )

  /** `private[app]` so that `KsqlWiringSuite` can read the fields that decide where a request goes.
    *
    * `SchemaWiring`'s seam and its reason: the alternative is a suite that starts a server and watches which
    * port is dialled, which is what that rule had instead of a case — nothing, because building the whole
    * composition root needs a server to dial. One keyword makes the addresses a value a case can assert.
    *
    * `callTimeout` and **not** `streamTimeout`: this is the budget for the requests that finish, and a push
    * query is bounded by `KsqlHttp`'s own `interruptAfter` instead. Aiming this at the stream timeout would
    * give an object listing five minutes to answer, which is a screen that hangs rather than one that says
    * the server is slow.
    */
  private[app] def upstreamConfig(
      cluster: ClusterId,
      settings: KsqlSettings,
      policy: UrlPolicy
  ): UpstreamConfig =
    UpstreamConfig(
      name = s"${KsqlHttp.UpstreamName}.${cluster.value}",
      urls = settings.urls,
      callTimeout = settings.callTimeout,
      maxConcurrent = MaxConcurrentPerServer,
      maxRetries = MaxRetries,
      urlPolicy = policy
    )

  /** The transport for an OAuth token endpoint, which is **not** the server's resilient backend.
    *
    * That one fails over between the ksqlDB cluster's own addresses; a token request routed to a ksqlDB
    * because the issuer was briefly slow would be a request carrying a client secret, sent to the wrong
    * system. It gets its own upstream client, with the same protections and its own name.
    *
    * `None` for the two mechanisms that need no issuer, so a deployment using basic or no authentication
    * opens nothing.
    */
  private def tokenBackendFor[F[_]: Async](
      cluster: ClusterId,
      settings: KsqlSettings,
      transport: StreamBackend[F, Fs2Streams[F]],
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
            KsqlApi.Id,
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
    * secret to a ksqlDB server.
    */
  private[app] def tokenUpstreamConfig(
      cluster: ClusterId,
      settings: KsqlSettings,
      endpoint: SafeUrl,
      policy: UrlPolicy
  ): UpstreamConfig =
    UpstreamConfig(
      name = s"${KsqlCredentials.TokenUpstreamName}.${cluster.value}",
      urls = NonEmptyList.one(endpoint),
      callTimeout = settings.callTimeout,
      maxConcurrent = MaxConcurrentPerServer,
      // A token request is a POST, and a duplicate one costs an extra token rather than an extra side
      // effect: issuers treat client-credentials grants as repeatable.
      maxRetries = 1,
      urlPolicy = policy
    )

  /** The key plan tokens are signed with: the configured one, or a fresh one for this process.
    *
    * `TopicWiring.signingKey`'s decision, taken again rather than shared, because rule A11 forbids this
    * service from seeing that one's `app` module. The fallback is logged loudly rather than being silent,
    * because its consequence is invisible until a second replica exists: a plan minted by one process is
    * refused by the other, and the operator sees a confirmation that will not confirm.
    *
    * `private[app]` so that `KsqlWiringSuite` can assert both branches and the line each writes.
    */
  private[app] def signingKey[F[_]: Async](
      configured: Option[Secret[String]],
      logger: StructuredLogger[F]
  ): F[Secret[Array[Byte]]] =
    configured match {
      case Some(secret) =>
        Async[F]
          .pure(Secret(secret.value.getBytes(StandardCharsets.UTF_8)))
          .flatTap(_ =>
            logger.info("ksqlDB plan tokens are signed with the configured kui.streaming.cursorKey")
          )
      case None =>
        Async[F]
          .delay(Secret(randomKey()))
          .flatTap(_ =>
            logger.info(
              "no kui.streaming.cursorKey is configured; ksqlDB plan tokens are signed with a key " +
                "generated for this process. A restart invalidates an open confirmation, and a second " +
                "replica rejects this one's tokens. Configure the key before running more than one."
            )
          )
    }

  /** 256 bits from the platform's secure source. */
  private def randomKey(): Array[Byte] = {
    val bytes = new Array[Byte](32)
    SecureRandom.getInstanceStrong.nextBytes(bytes)
    bytes
  }

  /** One INFO line per configured ksqlDB, and one that says when none is.
    *
    * "Which ksqlDB is this reading?" is the first question asked when the ksqlDB screen shows something
    * unexpected, and after the fact it is unanswerable unless the process said so at startup. The address is
    * safe to log — it is the operator's own text — and the mechanism is named without its secret.
    *
    * The "no ksqlDB configured" line matters as much as the others: it is what tells an operator who expected
    * a ksqlDB row that KUI is behaving as configured rather than failing.
    */
  private[app] def startupLog[F[_]: Async](
      clusters: List[ClusterConfig],
      logger: StructuredLogger[F]
  ): F[Unit] = {
    val configured = clusters.filter(_.ksql.isDefined)

    if configured.isEmpty then
      logger.info(
        "no cluster configures kui.clusters.<n>.ksql.url, so every cluster reports the ksql feature as " +
          "not configured and no ksqlDB client is opened"
      )
    else
      configured.traverse_(cluster =>
        cluster.ksql.traverse_(settings =>
          logger.info(
            Map(
              "cluster.id" -> cluster.id.value,
              "ksql.urls" -> settings.urls.toList.map(_.value).mkString(","),
              "ksql.auth" -> settings.auth.describe,
              "ksql.streamTimeout" -> settings.streamTimeout.toString
            )
          )(s"cluster ${cluster.id.value} reads ksqlDB at ${settings.urls.head.value}")
        )
      )
  }
}
