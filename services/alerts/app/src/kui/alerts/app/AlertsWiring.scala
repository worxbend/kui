package kui.alerts.app

import cats.Parallel
import cats.effect.kernel.{Async, Resource}
import cats.syntax.all.*
import fs2.Stream
import fs2.io.file.Files
import org.typelevel.log4cats.{LoggerFactory, StructuredLogger}
import sttp.capabilities.fs2.Fs2Streams
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.interceptor.Interceptor

import kui.alerts.api.{AlertsApi, AlertsCapabilities}
import kui.alerts.application.*
import kui.alerts.domain.{AlertLimits, ClusterFactsPort}
import kui.alerts.infrastructure.{
  ConfiguredProfileSource,
  DurableAlertStore,
  InMemoryAlertStore,
  KafkaClusterFacts,
  LoggingAcknowledgementSink
}
import kui.cache.CacheMetrics
import kui.cluster.client.ClusterProfiles
import kui.config.store.ConfigStoreResource
import kui.config.{AlertThresholds, AlertsConfig, ClusterConfig, KuiConfig, StoreConfig, UrlPolicy}
import kui.contracts.capability.ServiceCapabilities
import kui.http.ProcessLoggerFactory
import kui.http.health.ReadinessCheck
import kui.http.principal.{PrincipalVerification, RbacGuard}
import kui.kafka.admin.{KafkaClusterAdmin, KafkaGroupAdmin}
import kui.kafka.{AdminClientPool, AdminMetrics}
import kui.observability.Telemetry
import kui.security.PrincipalCodec
import kui.security.rbac.{ClusterFlags, RbacPolicy}

/** Everything the alerts service needs in order to be served, with no listener started.
  *
  * The same shape as every other service's (ADR-010): stopping one step short of a running server is what
  * lets the all-in-one deployment take these routes, add the rest, and start one listener over the lot.
  */
final case class AlertsServer[F[_]](
    routes: List[ServerEndpoint[Fs2Streams[F], F]],
    interceptors: List[Interceptor[F]],
    readiness: List[ReadinessCheck[F]],
    capabilities: F[ServiceCapabilities]
)

/** The alerts service's composition root.
  *
  * ==What it contacts, and when==
  *
  * It opens no Kafka connection while it is being built. `AdminClientPool` creates a client on first use, and
  * the first use is the first evaluation pass, which starts inside the returned `Resource` and runs on its
  * own fibre. A broker that is down therefore delays nothing and fails nothing here: the service starts, the
  * feed answers, and every rule's row says that its facts could not be read.
  *
  * ==One fibre per configured cluster, and none if there are none==
  *
  * A deployment with no `kui.clusters[]` builds no pool and no fibre. There is nothing to evaluate, and a
  * loop that woke every minute to evaluate nothing would be an entry in every profile nobody could explain.
  *
  * ==Where the thresholds are mapped==
  *
  * `kui.alerts.thresholds` is a `kui.config.AlertThresholds`, and the rules take a
  * `kui.alerts.domain.AlertLimits` — the domain's own copy, because rule A1 forbids it `libs/config`. The
  * mapping is [[limitsOf]] and it is the one place the two spellings can disagree, which is why
  * `AlertsWiringSuite` pins them together field by field rather than trusting five assignments.
  */
object AlertsWiring {

  /** Standalone production entry point: the remote profile resource outlives its collectors. */
  def standalone[F[_]: {Async, Parallel, Files}](
      config: KuiConfig,
      telemetry: Telemetry[F],
      principals: PrincipalCodec[F],
      logger: StructuredLogger[F],
      policy: UrlPolicy = UrlPolicy.fromEnv(sys.env)
  ): Resource[F, AlertsServer[F]] =
    ClusterProfileSource
      .remote(config, telemetry, principals, logger, policy)
      .flatMap(profiles => fromProfiles(profiles, config, telemetry, principals, logger))

  /** Both deployment shapes attach an authoritative, snapshot-on-subscription source here. */
  def fromProfiles[F[_]: {Async, Parallel, Files}](
      profiles: ClusterProfiles[F],
      config: KuiConfig,
      telemetry: Telemetry[F],
      principals: PrincipalCodec[F],
      logger: StructuredLogger[F]
  ): Resource[F, AlertsServer[F]] =
    Resource.eval(ClusterProfileSource.snapshot(profiles)).flatMap { current =>
      make(
        current,
        config.alerts,
        config.store,
        config.rbac,
        telemetry,
        principals,
        logger,
        clusterChanges = Some(ClusterProfileSource.changes(profiles))
      )
    }

  /** The instrumentation scope this service's tracer and meter are named after. */
  val Instrumentation: String = AlertsService.Instrumentation

  /** Store client identity visible in broker connection lists and quotas. */
  val StoreClientId: String = "kui-alerts-store"

  /** Builds everything except the listener.
    *
    * @param clusters
    *   the configured clusters, from `kui.clusters[]`, read from the same file this process already loaded.
    *   They are the list of rows the capability report has to have an answer for, and one evaluation fibre
    *   each.
    * @param alerts
    *   the `kui.alerts` section: `retention` bounds the store, `evaluationInterval` is the cadence, and
    *   `thresholds` are the five numbers the rules compare against.
    * @param clusterChanges
    *   the shared profile source's resolved snapshots, emitting its current snapshot on subscription. Each
    *   change replaces only affected collectors and updates authorization and capability lookups.
    */
  def make[F[_]: {Async, Parallel, Files}](
      clusters: List[ClusterConfig],
      alerts: AlertsConfig,
      storeConfig: StoreConfig,
      rbac: RbacPolicy,
      telemetry: Telemetry[F],
      principals: PrincipalCodec[F],
      logger: StructuredLogger[F],
      clusterChanges: Option[Stream[F, List[ClusterConfig]]] = None
  ): Resource[F, AlertsServer[F]] = {
    given LoggerFactory[F] = ProcessLoggerFactory.of(logger)

    for {
      meter <- Resource.eval(telemetry.meter(Instrumentation))
      rejections <- Resource.eval(PrincipalVerification.rejectionCounter[F](meter))
      interceptors <- Resource.eval(AlertsApi.interceptors[F](telemetry, rejections, logger))

      cacheMetrics <- Resource.eval(CacheMetrics.otel4s[F](meter))
      store <- alertStore[F](storeConfig, alerts, cacheMetrics, logger)

      collectors <- ClusterCollectors.resource[F, Unit](clusters)(cluster =>
        evaluators[F](List(cluster), alerts, store, telemetry, logger)
      )
      _ <- collectors.watch(clusterChanges.getOrElse(Stream.empty), logger)
      currentClusters = collectors.current.map(_.values.toList.map(_._1))
      profiles = ConfiguredProfileSource.live[F](currentClusters)

      audit = LoggingAcknowledgementSink.make[F](logger)
      // Who did it is not wired here. It is a parameter of every `guard` call, threaded from the
      // principal the gateway signed and the route verified (ADR-020), so an audit line names the person
      // who made the request rather than a constant this file chose.
      guard = MutationGuard.make[F](profiles, audit, logger)
      useCases = AlertUseCases.make[F](profiles, store, guard)
      capabilities = AlertsCapabilities.make[F](profiles)

      // Readiness is deliberately empty, for the reason the topic and metrics services give: "can this
      // service answer" is true as soon as it is wired. Every route answers a document whatever the state
      // of a broker, so there is nothing an upstream could make untrue. A check that waited for the first
      // pass would take the alerts service out of rotation whenever a broker was slow — which is exactly
      // when the feed is the screen somebody is looking at.
      readiness = List.empty[ReadinessCheck[F]]

      // The permission check this service runs for itself, over the same declaration on the same
      // endpoints the gateway read (ADR-021). Read-only comes from the current resolved profiles,
      // so an acknowledgement on a read-only cluster is refused here whether or not the gateway was asked.
      permissions = new RbacGuard[F] {
        def authorize(
            principal: kui.security.Principal,
            endpoint: sttp.tapir.AnyEndpoint,
            requestPath: String
        ): F[Either[kui.kernel.error.KuiError, Unit]] =
          currentClusters.flatMap(current =>
            RbacGuard
              .fromPolicy[F](
                rbac,
                cluster => ClusterFlags(current.find(_.id == cluster).forall(_.readOnly)),
                logger
              )
              .authorize(principal, endpoint, requestPath)
          )
      }
    } yield AlertsServer(
      routes = AlertsApi.routes[F](
        useCases,
        store,
        readiness,
        capabilities,
        principals,
        rejections,
        telemetry,
        logger,
        permissions
      ),
      interceptors = interceptors,
      readiness = readiness,
      capabilities = AlertsApi.capabilityDocument[F](capabilities, logger)
    )
  }

  /** Durable when a Kafka metadata store exists; explicit in-memory compatibility otherwise.
    *
    * A file store is read-only and therefore cannot honour read markers or acknowledgements. Keeping the
    * existing in-memory behavior for that deployment shape is more honest than constructing a durable adapter
    * whose first click always fails. Startup says which mode was selected.
    */
  private def alertStore[F[_]: {Async, Parallel, Files, LoggerFactory}](
      storeConfig: StoreConfig,
      alerts: AlertsConfig,
      cacheMetrics: CacheMetrics[F],
      logger: StructuredLogger[F]
  ): Resource[F, AlertStore[F]] =
    storeConfig.kafka match {
      case Some(_) =>
        ConfigStoreResource
          .resource[F](storeConfig, StoreClientId, logger)
          .evalTap(_ => logger.info("alerts: durable event history and read markers enabled"))
          .map(metadata => DurableAlertStore[F](metadata, alerts.retention, logger))
      case None =>
        Resource.eval(
          logger.warn(
            "alerts: no writable Kafka metadata store is configured; event history and read markers " +
              "will last only until this process restarts"
          )
        ) *> InMemoryAlertStore.resource[F](alerts.retention, cacheMetrics)
    }

  /** One evaluation fibre per configured cluster, over one shared admin pool.
    *
    * The pool, the two admin ports and every fibre live for the life of the returned `Resource`, which is
    * what stops a pass in flight at shutdown from outliving the process. A deployment with no clusters gets
    * no pool at all.
    */
  private def evaluators[F[_]: {Async, Files}](
      clusters: List[ClusterConfig],
      alerts: AlertsConfig,
      store: AlertStore[F],
      telemetry: Telemetry[F],
      logger: StructuredLogger[F]
  ): Resource[F, Unit] =
    if clusters.isEmpty then
      Resource.eval(
        logger.info(
          "no cluster is configured, so the alerts service evaluates nothing and every feed it serves " +
            "is empty; this is a deployment with no clusters rather than a cluster with no alerts"
        )
      )
    else
      for {
        adminMetrics <- Resource.eval(AdminMetrics.otel[F](telemetry))
        pool <- AdminClientPool.resource[F](adminMetrics, Some(logger))
        clusterAdmin = KafkaClusterAdmin[F](pool, Some(logger))
        groupAdmin = KafkaGroupAdmin[F](pool, Some(logger))
        limits = limitsOf(alerts.thresholds)
        _ <- clusters.traverse_ { cluster =>
          val facts: ClusterFactsPort[F] =
            new KafkaClusterFacts[F](cluster.id, cluster.connection, clusterAdmin, groupAdmin, pool, logger)

          AlertEvaluationLoop.resource[F](
            cluster.id,
            EvaluateAlerts.make[F](facts, store, limits),
            alerts.evaluationInterval,
            logger
          )
        }
      } yield ()

  /** The operator's thresholds, as the rules see them.
    *
    * Field for field and nothing else: no widening, no clamping and no defaulting. Every bound is already
    * applied by `KuiConfigSource`'s loader, which refuses a file rather than accepting one it has to correct,
    * and a second opinion here would make a configuration the loader refuses behave as though it had been
    * accepted.
    */
  def limitsOf(thresholds: AlertThresholds): AlertLimits =
    AlertLimits(
      offlinePartitions = thresholds.offlinePartitions,
      underReplicatedPartitions = thresholds.underReplicatedPartitions,
      rebalanceDuration = thresholds.rebalanceDuration,
      diskUsedWarningPercent = thresholds.diskUsedWarningPercent,
      diskUsedCriticalPercent = thresholds.diskUsedCriticalPercent
    )
}
