package kui.topic.app

import java.nio.charset.StandardCharsets
import java.security.SecureRandom
import java.time.Instant

import scala.concurrent.duration.FiniteDuration

import cats.Parallel
import cats.effect.kernel.{Async, Clock, Resource}
import cats.syntax.all.*
import fs2.io.file.Files
import org.typelevel.log4cats.StructuredLogger
import sttp.model.Uri
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.interceptor.Interceptor

import kui.cache.CacheMetrics
import kui.cluster.client.{ClusterProfile, ClusterProfiles, ClusterProfilesConfig, HttpClusterProfiles}
import kui.config.{ClusterConfig, HttpTlsConfig, KuiConfig, SafeUrl, UrlPolicy}
import kui.contracts.capability.ServiceCapabilities
import kui.http.health.ReadinessCheck
import kui.http.principal.{PrincipalVerification, RbacGuard}
import kui.http.upstream.HttpTls
import kui.kafka.{AdminClientPool, AdminMetrics}
import kui.kernel.{Secret, ServiceId, UserName}
import kui.observability.Telemetry
import kui.observability.audit.LoggingAuditSink
import kui.security.PrincipalCodec
import kui.security.rbac.{ClusterFlags, RbacPolicy}
import kui.topic.api.{TopicApi, TopicErrors}
import kui.topic.application.{
  MutationGuard,
  TopicAdminUseCase,
  TopicCapabilityUseCase,
  TopicConfigUseCase,
  TopicDetailUseCase,
  TopicPlanToken
}
import kui.topic.domain.ClockPort
import kui.topic.infrastructure.{
  ConfiguredClusterProfiles,
  KafkaTopicAdmin,
  KafkaTopicWriter,
  LiveTopicSnapshots
}

/** Everything the topic service needs in order to be served, with no listener started.
  *
  * The same shape as `ClusterWiring` and for the same reason (ADR-010): stopping one step short of a running
  * server is what lets the all-in-one deployment take these routes, add every other service's, and start one
  * listener over the lot.
  */
final case class TopicServer[F[_]](
    routes: List[ServerEndpoint[Any, F]],
    interceptors: List[Interceptor[F]],
    readiness: List[ReadinessCheck[F]],
    capabilities: F[ServiceCapabilities]
)

/** The topic service's composition root.
  *
  * ==What it contacts, and when==
  *
  * It opens no Kafka connection while it is being built. `AdminClientPool` creates a client on first use, and
  * the first use is the first background scrape, which starts inside the returned `Resource` and runs on its
  * own fiber. So a broker that is down delays nothing and fails nothing here: the service starts, the topics
  * screen renders, and the cluster's snapshot reports itself `Unavailable` with the reason. That is the whole
  * design position of the product, and it would be lost if this method waited for a scrape.
  */
object TopicWiring {

  /** The instrumentation scope this service's tracer and meter are named after. */
  val Instrumentation: String = "kui.topic"

  /** The account the distributed topic process signs profile calls as. */
  val ProfileIdentity: UserName = UserName.unsafe("kui-topic")

  /** `kui.clusterProfiles.url`, or else the gateway's cluster upstream.
    *
    * `None` means the process has no profile client and [[standalone]] serves `kui.clusters[]`.
    */
  private[app] def profileTarget(config: KuiConfig): Option[SafeUrl] = {
    val configured = config.topics.clusterProfiles.map(_.url)
    val gateway = config.gateway.services.get(ServiceId.unsafe("cluster")).map(_.url)
    configured.orElse(gateway)
  }

  /** The distributed process: a profile client when a URL is configured, otherwise the static list.
    *
    * The client is held for the life of the server and its first fetch runs during acquisition, but this
    * wiring copies that snapshot once. A cluster created or edited after start is not applied here. Serde,
    * schema registry, Connect, ksql and masking are not on the profile and stay at [[ClusterConfig]]'s
    * defaults. Missing both URLs does not refuse startup: the static list is served and the gap is logged.
    */
  def standalone[F[_]: {Async, Parallel, Files}](
      config: KuiConfig,
      telemetry: Telemetry[F],
      principals: PrincipalCodec[F],
      logger: StructuredLogger[F],
      policy: UrlPolicy = UrlPolicy.fromEnv(sys.env)
  ): Resource[F, TopicServer[F]] =
    profileTarget(config) match {
      case None =>
        Resource.eval(
          logger.warn(
            "no kui.clusterProfiles.url (or kui.gateway.services.cluster.url) is configured; the topic " +
              "service is using the static kui.clusters list and will not see clusters created in the " +
              "cluster service"
          )
        ) >> make(
          config.clusters,
          config.rbac,
          config.topics.refreshInterval,
          config.topics.internalPrefix,
          config.streaming.cursorKey,
          telemetry,
          principals,
          logger
        )
      case Some(target) =>
        val configured = config.topics.clusterProfiles
        val settings = configured.fold(ClusterProfilesConfig.default)(value =>
          ClusterProfilesConfig(
            value.pollInterval,
            value.requestTimeout,
            value.reconnectBackoff,
            value.maxReconnectBackoff
          )
        )
        for {
          backend <- HttpTls.resource[F](HttpTlsConfig.Default, policy)
          profiles <- HttpClusterProfiles.resource[F](
            Uri.unsafeParse(target.value),
            backend,
            principals,
            ProfileIdentity,
            settings,
            telemetry,
            logger
          )
          server <- fromResolved(
            profiles,
            config.rbac,
            config.topics.refreshInterval,
            config.topics.internalPrefix,
            config.streaming.cursorKey,
            telemetry,
            principals,
            logger
          )
        } yield server
    }

  /** One startup snapshot of a profile client, then the same wiring [[make]] builds from a static list.
    *
    * Later profile changes are not applied. The fields the profile does not carry — serde, schema registry,
    * Connect, ksql, masking — stay at [[ClusterConfig]]'s defaults.
    */
  def fromResolved[F[_]: {Async, Parallel, Files}](
      profiles: ClusterProfiles[F],
      rbac: RbacPolicy,
      scrapeInterval: FiniteDuration,
      internalPrefix: String,
      cursorKey: Option[Secret[String]],
      telemetry: Telemetry[F],
      principals: PrincipalCodec[F],
      logger: StructuredLogger[F]
  ): Resource[F, TopicServer[F]] =
    Resource.eval(profiles.all).flatMap { resolved =>
      val clusters = resolved.values.toList.sortBy(_.id.value).map(configOf)
      make(
        clusters,
        rbac,
        scrapeInterval,
        internalPrefix,
        cursorKey,
        telemetry,
        principals,
        logger
      )
    }

  /** Copies the fields a profile actually carries. */
  private[app] def configOf(profile: ClusterProfile): ClusterConfig = {
    val connection = profile.connection
    ClusterConfig(
      profile.id,
      profile.name,
      connection.bootstrapServers,
      connection.security,
      connection.overrides,
      profile.readOnly,
      connection.admin
    )
  }

  /** Builds everything except the listener.
    *
    * @param clusters
    *   the clusters this process will scrape. Callers pass `kui.clusters[]`, or the list [[fromResolved]]
    *   copied from the cluster service. See [[ConfiguredClusterProfiles]] for the static adapter underneath.
    * @param rbac
    *   the deployment's roles. This service re-runs the same decision the gateway already took, from the
    *   principal the gateway signed, because the gateway is a door and not a wall: anything that can reach
    *   this port could otherwise call it with no permissions at all.
    * @param scrapeInterval
    *   how often each cluster's topic list is refreshed in the background. There is no TTL: a snapshot older
    *   than this is shown and marked stale, never withheld.
    * @param internalPrefix
    *   `kui.topics.internalPrefix`, passed to the adapter, which is the only place that also holds Kafka's
    *   own `isInternal` flag. A topic is internal when either says so.
    */
  def make[F[_]: {Async, Parallel, Files}](
      clusters: List[ClusterConfig],
      rbac: RbacPolicy,
      scrapeInterval: FiniteDuration,
      internalPrefix: String,
      cursorKey: Option[Secret[String]],
      telemetry: Telemetry[F],
      principals: PrincipalCodec[F],
      logger: StructuredLogger[F]
  ): Resource[F, TopicServer[F]] =
    for {
      meter <- Resource.eval(telemetry.meter(Instrumentation))
      rejections <- Resource.eval(PrincipalVerification.rejectionCounter[F](meter))
      interceptors <- Resource.eval(TopicApi.interceptors[F](telemetry, rejections, logger))
      profiles <- Resource.eval(ConfiguredClusterProfiles.of[F](clusters))
      adminMetrics <- Resource.eval(AdminMetrics.otel[F](telemetry))
      pool <- AdminClientPool.resource[F](adminMetrics, Some(logger))
      admin = new KafkaTopicAdmin[F](pool, profiles.connectionFor, internalPrefix, logger)
      writer = new KafkaTopicWriter[F](pool, profiles.connectionFor, logger)
      cacheMetrics <- Resource.eval(CacheMetrics.otel4s[F](meter))
      snapshots <- LiveTopicSnapshots.resource[F](profiles.ids, admin, scrapeInterval, cacheMetrics, logger)
      detail = TopicDetailUseCase.make[F](admin, snapshots)
      config = TopicConfigUseCase.make[F](admin)

      // The plan-signing key (ADR-045), which is ADR-026's streaming cursor key: one secret, one
      // rotation procedure, and this service's use kept apart from the consumer service's by the
      // operation name inside the payload.
      //
      // Configured, because a plan token that only the replica that minted it can verify is a
      // confirmation dialog that fails at the last step behind a load balancer, and a key regenerated
      // on restart is a plan an operator has to compose twice. It falls back to a fresh random rather
      // than refusing to start: one process with no replicas — the quickstart, a laptop — needs no
      // shared secret, and demanding one there would be a worse first five minutes for nothing gained.
      key <- Resource.eval(signingKey[F](cursorKey, logger))
      tokens = TopicPlanToken.make[F](key)
      audit = LoggingAuditSink.make[F](logger)
      // Who did it is not wired here. It is a parameter of every `guard` call, threaded from the
      // principal the gateway signed and the route verified (ADR-020), so an audit record names the
      // person who made the request rather than a constant this file chose.
      guard = MutationGuard.make[F](profiles, snapshots, audit, logger, TopicErrors.toKui)
      topicAdmin = TopicAdminUseCase
        .make[F](admin, writer, profiles, guard, tokens, logger, TopicErrors.toKui)
      capabilities = TopicCapabilityUseCase.make[F](profiles, snapshots)
      // Readiness is deliberately empty. "Can this service answer" is true as soon as it is wired: it
      // serves snapshots, and a snapshot that has not been taken yet is a state the screen renders
      // rather than an outage. A readiness check that waited for the first scrape would take the topic
      // service out of rotation whenever a broker was slow to answer, which is exactly the coupling the
      // snapshot exists to break.
      readiness = List.empty[ReadinessCheck[F]]

      // The permission check this service runs for itself, over the same declaration on the same
      // endpoints the gateway read. Read-only comes from the cluster list this wiring was given — the
      // static `kui.clusters[]` entries, or the resolved profile snapshot — so a read-only cluster is
      // refused here whether or not the request came through the gateway.
      permissions = RbacGuard.fromPolicy[F](
        rbac,
        cluster => ClusterFlags(clusters.find(_.id == cluster).exists(_.readOnly)),
        logger
      )
    } yield TopicServer(
      routes = TopicApi
        .routes[F](
          snapshots,
          detail,
          config,
          topicAdmin,
          capabilities,
          readiness,
          principals,
          rejections,
          logger,
          permissions
        ),
      interceptors = interceptors,
      readiness = readiness,
      capabilities = TopicApi.capabilityDocument[F](capabilities, logger)
    )

  /** The key plan tokens are signed with: the configured one, or a fresh one for this process.
    *
    * The fallback is logged loudly rather than being silent, because its consequence is invisible until a
    * second replica exists: a plan minted by one process is refused by the other, and the operator sees a
    * confirmation that will not confirm.
    *
    * `private[app]` so that `TopicWiringSuite` can assert both branches and the line each writes. Reaching
    * them through `make` would need a broker; this module had no test source at all until then.
    */
  private[app] def signingKey[F[_]: Async](
      configured: Option[Secret[String]],
      logger: StructuredLogger[F]
  ): F[Secret[Array[Byte]]] =
    configured match {
      case Some(secret) =>
        Async[F]
          .pure(Secret(secret.value.getBytes(StandardCharsets.UTF_8)))
          .flatTap(_ => logger.info("plan tokens are signed with the configured kui.streaming.cursorKey"))
      case None =>
        Async[F]
          .delay(Secret(randomKey()))
          .flatTap(_ =>
            logger.info(
              "no kui.streaming.cursorKey is configured; plan tokens are signed with a key generated for " +
                "this process. A restart invalidates an open confirmation, and a second replica rejects " +
                "this one's tokens. Configure the key before running more than one."
            )
          )
    }

  /** 256 bits from the platform's secure source. */
  private def randomKey(): Array[Byte] = {
    val bytes = new Array[Byte](32)
    SecureRandom.getInstanceStrong.nextBytes(bytes)
    bytes
  }

  /** The domain's clock port, over the effect's own clock. */
  def clock[F[_]: Clock]: ClockPort[F] = new ClockPort[F] {
    def now: F[Instant] = Clock[F].realTimeInstant
  }
}
