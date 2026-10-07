package kui.allinone

import java.time.Instant

import cats.Parallel
import cats.effect.kernel.{Async, Resource}
import cats.syntax.all.*
import fs2.io.file.Files
import org.typelevel.log4cats.StructuredLogger

import kui.alerts.api.AlertsApi
import kui.alerts.app.AlertsWiring
import kui.cluster.api.ClusterApi
import kui.cluster.app.{ClusterServiceConfig, ClusterWiring}
import kui.config.{
  AlertsConfig,
  AuthConfig,
  ClusterConfig,
  ConsumersConfig,
  KuiConfig,
  MetricsConfig,
  StoreConfig,
  StreamingConfig,
  TopicsConfig,
  UrlPolicy
}
import kui.connect.api.ConnectApi
import kui.connect.app.ConnectWiring
import kui.consumer.api.ConsumerApi
import kui.consumer.app.ConsumerWiring
import kui.gateway.api.InfoRoutes
import kui.gateway.app.{GatewayServer, GatewayWiring}
import kui.gateway.application.client.{ServiceClient, ServiceClients}
import kui.identity.api.IdentityApi
import kui.identity.app.IdentityWiring
import kui.kernel.ServiceId
import kui.ksql.api.KsqlApi
import kui.ksql.app.KsqlWiring
import kui.message.api.MessageApi
import kui.message.app.MessageWiring
import kui.metrics.api.MetricsApi
import kui.metrics.app.MetricsWiring
import kui.observability.Telemetry
import kui.schema.api.SchemaApi
import kui.schema.app.SchemaWiring
import kui.security.PrincipalCodec
import kui.security.rbac.RbacPolicy
import kui.topic.api.TopicApi
import kui.topic.app.TopicWiring

/** The all-in-one deployment's composition root (ADR-005, ADR-010).
  *
  * One process, one `IO` runtime, one otel4s provider, one Netty listener, and every KUI service inside it.
  * It is the shape a laptop and a small installation run, and it exists so that "what works locally works in
  * production" is a fact about the code rather than a hope: the gateway's own composition root is reused
  * whole, and the only thing that differs is where a service call lands.
  *
  * ==The wiring order, because it is easy to get wrong==
  *
  * {{{
  * telemetry ──▶ logger ──▶ config ──▶ PrincipalCodec.inProcess
  *                                          │
  *                                          ├──▶ ClusterWiring.make ──▶ InProcessServiceClient ──┐
  *                                          │                                                    │
  *                                          └────────────────────────────────────────────────────┤
  *                                                                                               ▼
  *                                                                                       ServiceClients
  *                                                                                               │
  *                                                                                               ▼
  *                                                             GatewayWiring.over (registry, poller,
  *                                                             circuit feed, proxy routes, documentation)
  *                                                                                               │
  *                                                                                               ▼
  *                                                                                     one Netty listener
  * }}}
  *
  * Two orderings in there are load bearing. The codec is built before any service, because both the caller
  * and the callee must hold the *same* one — a service handed a second instance would be verifying tokens
  * against claims it agreed with only by coincidence. And every service is wired before the gateway, because
  * the gateway's readiness poller starts polling as soon as it is constructed, and it can only poll something
  * that already exists.
  *
  * ==What this file does not do==
  *
  * It starts no listener, exactly like the two composition roots it composes. `AllInOne` binds the port. That
  * is what lets a suite start the whole product, ask it questions and stop it again in milliseconds, without
  * a socket and without a port to collide over — which is why `AllInOneWiringSuite` can afford to do it three
  * times in a row.
  */
object AllInOneWiring {

  /** The exact sentence ADR-005 requires when a configuration carries signing keys this shape will not use.
    *
    * It names the key so it can be searched for, and it says what to do about it, because the operator most
    * likely to see it is one who pointed the all-in-one image at the Compose configuration file — where those
    * keys are genuinely needed by the *other* deployment shape and genuinely useless here.
    */
  val IgnoredPrincipalKeys: String =
    "kui.gateway.principalKeys is ignored in all-in-one mode (in-process principal). Nothing is " +
      "signed because nothing leaves this process; the keys matter only when the gateway and the " +
      "services run as separate containers. Remove them, or run the distributed deployment."

  /** The same, for upstream addresses. A separate sentence rather than one combined warning, because the two
    * keys are set for different reasons and an operator may well have exactly one of them.
    */
  val IgnoredServiceUrls: String =
    "kui.gateway.services is ignored in all-in-one mode. Every service runs inside this process and is " +
      "called in memory, so no address is dialled and no container has to be reachable. Remove the " +
      "section, or run the distributed deployment."

  /** Builds the whole product, with no listener started.
    *
    * @param config
    *   the one loaded configuration, narrowed to what this shape reads
    * @param telemetry
    *   the single otel4s provider. One provider and not one per service, so a browser request produces one
    *   trace containing the gateway's span and the service's span in the same tree — the shape a developer
    *   will also see in production, learned here without Docker.
    * @param logger
    *   the process logger. `service.name` stays distinct per service on the log lines the services themselves
    *   write, so filtering by service works identically in both deployment shapes.
    */
  def resource[F[_]: {Async, Parallel, Files}](
      config: AllInOneConfig,
      telemetry: Telemetry[F],
      logger: StructuredLogger[F]
  ): Resource[F, GatewayServer[F]] =
    for {
      _ <- Resource.eval(warnAboutIgnoredKeys[F](logger, config))
      principals = PrincipalCodec.inProcess[F]
      // The cluster service's own configuration, taken from the same loaded file this process read.
      // Until this line existed the all-in-one handed the service `ClusterServiceConfig.Default`, so
      // `kui.clusters[]` and `kui.store.*` were silently dropped: the quickstart's configuration named a
      // broker, the startup log said "resolved 0 configured cluster(s)", and the dashboard was empty.
      clients <- services[F](
        config.clusterView,
        config.clusters,
        config.topics,
        config.consumers,
        config.streaming,
        config.auth,
        config.rbac,
        config.store,
        config.metrics,
        config.alerts,
        telemetry,
        principals,
        logger
      )
      gateway <- GatewayWiring.over[F](
        config.gatewayView,
        telemetry,
        logger,
        Resource.pure[F, ServiceClients[F]](clients)
      )
    } yield gateway

  /** The services this build contains, in the order `ServiceClients` keeps them — by id, alphabetically.
    *
    * Sorted rather than in the order the wiring adds them, because `ServiceClients.of` sorts, and
    * `AllInOneWiringSuite` asserts this list *equals* what the wiring produced. A list in a different order
    * would make that assertion fail for a reason that has nothing to do with a missing service, which is the
    * thing it exists to catch.
    *
    * It is written out rather than derived from the wiring because the startup log has to name them before
    * anything has been constructed, and because it is the list a reader checks against `ROADMAP.md` to see
    * which milestone's services are actually in this binary. [[services]] must agree with it, and
    * `AllInOneWiringSuite` asserts that it does rather than leaving the two to drift.
    */
  val Services: List[ServiceId] =
    List(
      AlertsApi.Id,
      ClusterApi.Id,
      ConnectApi.Id,
      ConsumerApi.Id,
      IdentityApi.Id,
      KsqlApi.Id,
      MessageApi.Id,
      MetricsApi.Id,
      SchemaApi.Id,
      TopicApi.Id
    )

  /** Every KUI service, wired in this process and reachable in memory.
    *
    * This list is the one place all-in-one grows as services arrive: M1 adds the topic service, M2 the schema
    * service, and each is three lines — call its `<Name>Wiring.make`, turn the result into a client, add it
    * here. Nothing else in this file or in the gateway changes, which is the property ADR-005 was written to
    * buy.
    */
  def services[F[_]: {Async, Parallel, Files}](
      cluster: ClusterServiceConfig,
      clusters: List[ClusterConfig],
      topics: TopicsConfig,
      consumers: ConsumersConfig,
      streaming: StreamingConfig,
      auth: AuthConfig,
      rbac: RbacPolicy,
      store: StoreConfig,
      // Passed rather than defaulted. It used to carry `MetricsConfig.Default` because `AllInOneConfig`
      // had no field to pass, which made the all-in-one deployment answer as though the operator had
      // configured nothing — the same status as a real source with no collector behind it, and a
      // different *reason*, which is the only thing separating "you configured nothing" from "we cannot
      // measure what you configured". Nothing observable differed while no collector existed; the reason
      // did, and a reason is what an operator reads.
      metrics: MetricsConfig,
      // Passed for the reason `metrics` above is passed, and the reason is worth repeating because the
      // defect it prevents is silent in a different way. A dropped `kui.metrics` makes a configured
      // deployment answer as though nothing were configured, which an operator eventually notices. A
      // dropped `kui.alerts` changes no status anywhere: the feed answers `ok`, the rules run, and the
      // thresholds are simply the defaults instead of the ones somebody wrote -- so a cluster tuned to
      // tolerate a migration starts opening events again and nothing says why.
      alerts: AlertsConfig,
      telemetry: Telemetry[F],
      principals: PrincipalCodec[F],
      logger: StructuredLogger[F]
  ): Resource[F, ServiceClients[F]] =
    for {
      clusterService <- ClusterWiring.make[F](cluster, telemetry, principals, logger)
      collectorProfiles <- InProcessClusterProfiles.resource(clusterService, telemetry, principals, logger)
      collectorConfig = KuiConfig.Default.copy(metrics = metrics, alerts = alerts, store = store, rbac = rbac)
      // The topic service reads the same `kui.clusters[]` this process already loaded rather than
      // asking the cluster service for it over HTTP (ADR-046's profile client). One process calling
      // itself over a socket to read a list it is holding in memory would add a listener, a timeout
      // and a failure mode to a lookup that cannot fail; see `ConfiguredClusterProfiles`.
      //
      // It takes `kui.streaming.cursorKey` for the same reason the consumer service does: M5's topic
      // deletion and partition increase are confirmed against a signed plan (ADR-045), and the key
      // that signs one is the key ADR-026 already made an operator configure. One secret, one
      // rotation procedure.
      topicService <- TopicWiring
        .make[F](
          clusters,
          rbac,
          topics.refreshInterval,
          topics.internalPrefix,
          streaming.cursorKey,
          telemetry,
          principals,
          logger
        )
      // The consumer service reads the same `kui.clusters[]`, for the same reason the topic service
      // does: this process is already holding the list, and calling itself over a socket to read it
      // would add a listener, a timeout and a failure mode to a lookup that cannot fail.
      //
      // Its refresh interval comes from `kui.consumers.refreshInterval` and not from
      // `kui.topics.refreshInterval`. Describing every consumer group on a cluster and describing its
      // topics are different costs against different broker paths, and one knob for both would mean
      // tuning the cheaper one by the expensive one.
      consumerService <- ConsumerWiring.make[F](
        clusters,
        rbac,
        consumers.refreshInterval,
        streaming.cursorKey,
        telemetry,
        principals,
        logger
      )
      // The message service reads the same `kui.clusters[]` again, and holds nothing else. Unlike the
      // topic and consumer services it keeps no snapshot and runs no background scrape: it opens a
      // Kafka consumer when somebody browses, streams what was asked for, and closes it again. So
      // there is no interval to configure here and nothing for a broker outage to make stale.
      messageService <- MessageWiring
        .make[F](clusters, rbac, streaming.cursorKey, telemetry, principals, logger)
      // The schema service, which is the first one that may have nothing to do. A deployment where no
      // cluster configures `schemaRegistry` still wires it, still serves its routes and still reports
      // every cluster as not_configured — because "this deployment has no registry" is an answer the
      // browser needs in order to hide the feature, and a service left out of the process altogether
      // would instead look like a service that is down.
      //
      // Its URL policy comes from the process environment, exactly as the configuration loader's does:
      // a registry at `http://schema-registry:8081` is the ordinary arrangement inside a Compose
      // network, and a stricter policy here than the one that accepted the address would mean a
      // registry KUI logged at startup and could never call.
      schemaEnvironment <- Resource.eval(Async[F].delay(sys.env))
      // The identity service, which in a deployment with `kui.auth.type: disabled` — the default, and the
      // demonstration environment — holds no accounts, opens no connection and answers `settings` with
      // `disabled`. It is wired in every shape rather than only in the ones that authenticate, so that
      // "this deployment has no login" is an answer the browser gets from a running service rather than a
      // route that is missing in half the builds.
      identityService <- IdentityWiring.make[F](auth, rbac, store, telemetry, principals, logger)
      schemaService <- SchemaWiring.make[F](
        clusters,
        UrlPolicy.fromEnv(schemaEnvironment),
        telemetry,
        principals,
        logger
      )
      // Both collectors consume the cluster service's resolved profiles (including stored overlays),
      // not the configuration bootstrap list. The shared client owns its change subscription and is
      // released after both services, so replacement/removal reaches every running collector.
      metricsService <- MetricsWiring
        .fromProfiles[F](collectorProfiles, collectorConfig, telemetry, principals, logger)
      //
      // It takes `rbac` because an acknowledgement is a mutation -- `AlertsAcknowledge` on
      // `Resource.Alerts` -- and is refused on a read-only cluster. It takes no cursor key: an
      // acknowledgement loses nothing, so it carries no ADR-045 plan token and there is nothing here to
      // sign.
      //
      // And the line before it, which is the only thing in this process that can tell a tuned deployment
      // from an untuned one. See `logAlertThresholds`.
      _ <- Resource.eval(logAlertThresholds[F](logger, alerts))
      alertsService <- AlertsWiring.fromProfiles[F](
        collectorProfiles,
        collectorConfig,
        telemetry,
        principals,
        logger
      )
      // The connect service, and the tenth. It reads the same `kui.clusters[]` as the four services
      // above -- the Connect workers' addresses are a per-cluster key, `kui.clusters.<n>.connect[]` --
      // and it holds no Kafka client at all: every fact it reports comes from a worker's REST API.
      //
      // It takes `rbac` because pause, resume and restart are mutations on `Resource.Connect` and are
      // refused on a read-only cluster. It takes no cursor key: none of the three loses anything the
      // opposite button cannot undo, so none carries an ADR-045 plan token and there is nothing to sign.
      //
      // Its URL policy comes from the process environment, exactly as the schema service's does and for
      // the same reason: a worker at `http://kafka-connect:8083` is the ordinary arrangement inside a
      // Compose network, and a stricter policy here than the one that accepted the address would mean a
      // worker KUI logged at startup and could never call.
      connectService <- ConnectWiring.make[F](
        clusters,
        UrlPolicy.fromEnv(schemaEnvironment),
        rbac,
        telemetry,
        principals,
        logger
      )
      // The ksql service, and the eleventh. It reads the same `kui.clusters[]` as the five services
      // above -- a ksqlDB address is the per-cluster key `kui.clusters.<n>.ksql` -- and, like the
      // connect service, it holds no Kafka client at all: every fact it reports comes from a ksqlDB
      // server's REST API.
      //
      // Its URL policy comes from the process environment, for the reason the schema and connect
      // services' does: a server at `http://ksqldb-server:8088` is the ordinary arrangement inside a
      // Compose network, and a stricter policy here than the one that accepted the address would mean
      // a server KUI logged at startup and could never call.
      //
      // IT IS THE ONLY OPTIONAL SERVICE IN THIS PROCESS THAT TAKES THE CURSOR KEY. A statement that
      // drops a stream, a table or a topic loses data that no opposite button restores, so it is an
      // ADR-045 plan->token->confirm mutation and `streaming.cursorKey` is what signs the token --
      // the same key ADR-026 already made an operator configure for the browse cursor, so a
      // deployment configures one secret rather than two. `KsqlWiring` decides what to do when the
      // key is absent and says so in its own start-up line; this file's job is only to hand it the
      // one the operator configured rather than a default of its own.
      ksqlService <- KsqlWiring.make[F](
        clusters,
        UrlPolicy.fromEnv(schemaEnvironment),
        rbac,
        streaming.cursorKey,
        telemetry,
        principals,
        logger
      )
    } yield ServiceClients.of[F](
      List[ServiceClient[F]](
        InProcessServiceClient.make[F](
          ClusterApi.Id,
          clusterService.routes,
          clusterService.interceptors,
          principals
        ),
        InProcessServiceClient.make[F](
          TopicApi.Id,
          topicService.routes,
          topicService.interceptors,
          principals
        ),
        InProcessServiceClient.make[F](
          ConsumerApi.Id,
          consumerService.routes,
          consumerService.interceptors,
          principals
        ),
        InProcessServiceClient.make[F](
          MessageApi.Id,
          messageService.routes,
          messageService.interceptors,
          principals
        ),
        InProcessServiceClient.make[F](
          SchemaApi.Id,
          schemaService.routes,
          schemaService.interceptors,
          principals
        ),
        InProcessServiceClient.make[F](
          IdentityApi.Id,
          identityService.routes,
          identityService.interceptors,
          principals
        ),
        InProcessServiceClient.make[F](
          MetricsApi.Id,
          metricsService.routes,
          metricsService.interceptors,
          principals
        ),
        InProcessServiceClient.make[F](
          AlertsApi.Id,
          alertsService.routes,
          alertsService.interceptors,
          principals
        ),
        InProcessServiceClient.make[F](
          ConnectApi.Id,
          connectService.routes,
          connectService.interceptors,
          principals
        ),
        InProcessServiceClient.make[F](
          KsqlApi.Id,
          ksqlService.routes,
          ksqlService.interceptors,
          principals
        )
      )
    )

  /** The numbers the alert rules in this process will actually compare against.
    *
    * IT EXISTS BECAUSE THE SEAM ABOVE IT COULD NOT FAIL. Until this line, replacing `config.alerts` with
    * `AlertsConfig.Default` in [[resource]]'s call to [[services]] left `./mill apps.allinone.test` entirely
    * green -- an operator's tuned thresholds silently swapped for the shipped ones, with nothing observable
    * anywhere. It is the same defect `kui.metrics` had and it fails more quietly: a dropped `kui.metrics`
    * makes a configured deployment answer `not_configured`, which somebody eventually argues with, while a
    * dropped `kui.alerts` changes no status at all. The feed answers `ok`, the rules run, and a cluster
    * deliberately tuned to tolerate a migration starts opening events again with nothing saying why.
    *
    * `MetricsWiring` writes the equivalent line inside the metrics service; this one is written here rather
    * than inside `AlertsWiring`, because the seam that needed gating is the argument [[resource]] passes to
    * [[services]] and this file is where that argument is chosen. It is produced from the `alerts` parameter
    * [[services]] actually received, so a caller that handed it the defaults cannot produce the tuned line —
    * which is exactly what makes the mutation visible.
    *
    * INFO and not WARN: nothing is wrong with either answer. `source` is what an operator reads -- it says
    * whether these five numbers came out of their YAML or out of the jar.
    */
  def logAlertThresholds[F[_]](
      logger: StructuredLogger[F],
      alerts: AlertsConfig
  ): F[Unit] = {
    val tuned = alerts != AlertsConfig.Default
    val thresholds = alerts.thresholds

    logger.info(
      Map(
        "alerts.source" -> (if tuned then "kui.alerts" else "shipped defaults"),
        "alerts.retention" -> alerts.retention.toString,
        "alerts.evaluationInterval" -> alerts.evaluationInterval.toString,
        "alerts.offlinePartitions" -> thresholds.offlinePartitions.toString,
        "alerts.underReplicatedPartitions" -> thresholds.underReplicatedPartitions.toString,
        "alerts.rebalanceDuration" -> thresholds.rebalanceDuration.toString,
        "alerts.diskUsedWarningPercent" -> thresholds.diskUsedWarningPercent.toString,
        "alerts.diskUsedCriticalPercent" -> thresholds.diskUsedCriticalPercent.toString
      )
    )(
      if tuned then "alert thresholds taken from kui.alerts"
      else "no kui.alerts section; the alert rules use the shipped default thresholds"
    )
  }

  /** Says out loud which configured keys this deployment shape is not going to act on.
    *
    * `WARN` and not `DEBUG`, for the same reason the gateway warns about insecure cookies: it is the line
    * that explains why a setting an operator deliberately wrote had no effect, and they must see it without
    * having had to turn on verbose logging first in order to suspect it.
    */
  def warnAboutIgnoredKeys[F[_]: cats.Applicative](
      logger: StructuredLogger[F],
      config: AllInOneConfig
  ): F[Unit] =
    logger.warn(IgnoredPrincipalKeys).whenA(config.hasIgnoredPrincipalKeys) *>
      logger.warn(IgnoredServiceUrls).whenA(config.hasIgnoredServiceUrls)

  /** The one INFO line this process writes as it starts.
    *
    * It reports the deployment shape as well as the build, because the first question anyone debugging a KUI
    * installation has to answer is which of the two shapes they are looking at, and the answer belongs on the
    * first line of the log rather than in whatever the reporter remembers about how they started it.
    */
  def startupLog[F[_]](
      logger: StructuredLogger[F],
      config: AllInOneConfig,
      at: Instant
  ): F[Unit] = {
    val services = Services.map(_.value)

    logger.info(
      Map(
        "service" -> AllInOne.ServiceName,
        "deployment" -> "all-in-one",
        "host" -> config.server.host.value,
        "port" -> config.server.port.value.toString,
        "basePath" -> config.server.basePath,
        "logFormat" -> config.telemetry.logFormat.wire,
        "services" -> services.sorted.mkString(","),
        "version" -> InfoRoutes.buildInfo.version,
        "gitCommit" -> InfoRoutes.buildInfo.gitCommitShort,
        "gitDirty" -> InfoRoutes.buildInfo.gitDirty.toString,
        "builtAt" -> InfoRoutes.buildInfo.builtAt.toString,
        "startedAt" -> at.toString
      )
    )(
      s"starting ${AllInOne.ServiceName} ${InfoRoutes.buildInfo.version} " +
        s"(${InfoRoutes.buildInfo.gitCommitShort}) with ${services.size} in-process service(s)"
    )
  }
}
