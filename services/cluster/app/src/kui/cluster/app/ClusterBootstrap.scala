package kui.cluster.app

import java.time.Instant

import scala.concurrent.duration.*

import cats.Parallel
import cats.data.NonEmptyList
import cats.effect.kernel.{Async, Clock, Resource}
import cats.syntax.all.*
import fs2.io.file.Files
import org.typelevel.log4cats.{LoggerFactory, StructuredLogger}

import kui.cache.CacheMetrics
import kui.cluster.application.*
import kui.cluster.domain.{
  ClockPort,
  ClusterAdmin as ClusterAdminPort,
  ClusterProfile,
  ProfileOrigin,
  ProfileVersion,
  StoreHealth
}
import kui.cluster.infrastructure.store.{ClusterConfigStoreAdapter, ProfileChangeListener}
import kui.cluster.infrastructure.{
  ClusterAdminAdapter,
  ClusterAdminClients,
  ConnectivityProbeAdapter,
  KafkaPartitionSweeper
}
import kui.config.store.*
import kui.config.{ClusterConfig, StoreConfig}
import kui.contracts.health.CheckResult
import kui.http.health.ReadinessCheck
import kui.kafka.admin.KafkaClusterAdmin
import kui.kafka.{AdminClientPool, AdminMetrics}
import kui.observability.Telemetry

/** The ordered startup of ADR-042, as one `Resource`.
  *
  * Each step is a separate resource and the ordering *is* the `for`-comprehension, which is the point of
  * writing it as one function rather than assembling it ad hoc: a reader must be able to check it against
  * ADR-042's bootstrap order line by line, and a reordering must appear as a diff in this file.
  *
  * {{{
  * static configuration
  *   -> the store's own Kafka clients
  *     -> topic bootstrap: create the topics or validate the ones that are there
  *       -> replay __kui_config to its end offset, bounded, with a named failure
  *         -> the registry: static configuration overlaid by the replayed records
  *           -> one admin client per configured cluster, created lazily by the pool
  *             -> the refresh loops, under a supervisor this resource owns
  *               -> readiness reports ready
  * }}}
  *
  * ==Why the order is one-directional==
  *
  * The service must not answer before it has read its own configuration. A cluster service that served an
  * empty list because its store replay had not finished looks exactly like a KUI nobody has configured a
  * cluster in, and an operator would spend the outage looking in the wrong place. Every step below therefore
  * completes before the next begins, and readiness is reported only at the end.
  *
  * ==Shutdown==
  *
  * Release is the exact reverse, and `Resource` gives that for free — which is the reason the ordering is
  * expressed as resource acquisition rather than as a sequence of effects. The refresh loops are cancelled
  * before the admin pool they call into is closed, and the pool before the store's consumer; a refresh loop
  * that outlived its pool would log an error on every shutdown and teach operators to ignore shutdown logs.
  */
object ClusterBootstrap {

  /** Everything the routes need, built and running. */
  final case class Bootstrapped[F[_]](
      store: ConfigStore[F],
      registry: ClusterRegistry[F],
      admin: ClusterAdminPort[F],
      topology: ClusterTopologyUseCase[F],
      brokers: BrokerDetailUseCase[F],
      write: ClusterWriteUseCase[F],
      probe: ClusterProbeUseCase[F],
      capabilities: CapabilityReportUseCase[F],
      health: F[StoreHealth],
      storeMode: String
  )

  /** The `client.id` prefix this process uses for its store clients, so a broker's connection list names it.
    */
  val StoreClientId: String = "kui-cluster-store"

  /** How often a configured cluster is scraped, and how often its feature probe is repeated
    * (`ARCHITECTURE.md` §9). Not per-cluster settings: `AdminTuning` carries per-cluster timeouts, and one
    * refresh cadence for the deployment is what makes "reads are at most 30 seconds old" a statement an
    * operator can rely on rather than a per-row property.
    */
  val RefreshInterval: FiniteDuration = 30.seconds

  val CapabilityProbeInterval: FiniteDuration = 1.hour

  /** How often the partition sweep runs.
    *
    * A minute rather than the topology's thirty seconds, and the number is the topic service's own
    * `kui.topics.refreshInterval` default for the same stated reason: a topic sweep is an order of magnitude
    * more expensive than a cluster one and its data changes an order of magnitude less often. It is a
    * constant here rather than a setting because `libs/config` has no `kui.clusters.*` key for it and adding
    * one is a configuration-compatibility change of its own.
    */
  val PartitionSweepInterval: FiniteDuration = 60.seconds

  /** The controller-uptime window, and the bucket it is measured in.
    *
    * Six hours at one minute: 360 buckets, which is the ring `SeriesWindow` retains. The step is twice the
    * refresh interval, so each bucket holds the newest of the two scrapes that land in it — a step shorter
    * than the refresh would leave every other bucket a gap and report a healthy cluster as half-observed.
    *
    * Six and not twenty-four because the window has to *fill* before it answers, and a KUI that had to run
    * for a day before its uptime card said anything would show nothing at all on the day anybody looked.
    */
  val ControllerUptimeWindow: FiniteDuration = 6.hours

  val ControllerUptimeStep: FiniteDuration = 1.minute

  /** The instrumentation scope the cache metrics are recorded under. */
  val CacheMeter: String = "kui.cache"

  def resource[F[_]: {Async, Parallel, Files, LoggerFactory}](
      clusters: List[ClusterConfig],
      store: StoreConfig,
      telemetry: Telemetry[F],
      logger: StructuredLogger[F]
  ): Resource[F, Bootstrapped[F]] =
    for {
      // Step 0. Static configuration becomes domain profiles, or the process does not start. This is the
      // one failure in the whole chain that is about what the operator *wrote* rather than about what it
      // points at, and ADR-013's rule applies: every violation at once, in one message.
      statics <- Resource.eval(profilesOf[F](clusters))
      // Steps 1 to 3. The store's clients, its topics, and the replay - all three inside `configStore`,
      // because they are one decision ("is there a Kafka store?") with one answer.
      configStore <- ConfigStoreResource.resource[F](store, StoreClientId, logger)
      clusterStore <- ClusterConfigStoreAdapter.resource[F](configStore, logger)
      // Step 4. The registry resolves once here, before anything downstream exists.
      registry <- ClusterRegistry.make[F](statics, clusterStore, clockOf[F], logger)
      _ <- Resource.eval(logResolved[F](registry, logger))
      // Step 5. The admin clients. The pool creates one per cluster on first use rather than eagerly: a
      // cluster whose address is wrong must not delay - or prevent - the start of the other nine.
      metrics <- Resource.eval(AdminMetrics.otel[F](telemetry))
      pool <- AdminClientPool.resource[F](metrics)
      clients <- ClusterAdminClients
        .resource[F](pool, logger, resolve = Some(id => registry.resolve(id).map(_.toOption)))
      // The sweeper takes the pool directly rather than going through `libs/kafka`'s `ClusterAdmin`:
      // `describeTopics` is a topic call and that port is the cluster context's, so the call shape is
      // local while the client, the timeouts and the metrics stay shared. See `KafkaPartitionSweeper`.
      admin <- Resource.eval(
        ClusterAdminAdapter.create[F](
          KafkaClusterAdmin[F](pool),
          new KafkaPartitionSweeper[F](pool, logger),
          clients,
          telemetry,
          logger
        )
      )
      // Step 6. The refresh loops, owned by this resource so that releasing it stops them.
      cacheMetrics <- Resource.eval(telemetry.meter(CacheMeter).flatMap(CacheMetrics.otel4s[F]))
      snapshots <- ClusterSnapshots.resource[F](
        registry,
        admin,
        cacheMetrics,
        ClusterSnapshots.Tuning(
          refreshInterval = RefreshInterval,
          capabilityInterval = CapabilityProbeInterval,
          sweepInterval = PartitionSweepInterval,
          uptimeWindow = ControllerUptimeWindow,
          uptimeStep = ControllerUptimeStep
        ),
        logger
      )
      // A profile edited in the store reaches this replica here: the listener reloads the registry, which
      // republishes, which is what makes an edit take effect without a restart.
      _ <- ProfileChangeListener.resource[F](clusterStore, _ => registry.reload.void, logger)
      topology = ClusterTopologyUseCase.make[F](registry, snapshots, logger)
      brokers = BrokerDetailUseCase.make[F](registry, snapshots, admin, logger)
      capabilities = CapabilityReportUseCase.make[F](registry, snapshots)
      write = ClusterWriteUseCase.make[F](registry, clusterStore, logger)
      // The connectivity probe. It has been a working, tested adapter with no constructor since M1 — the
      // "test connection" button it was written for did not exist, so nothing ever built one. It takes the
      // raw `KafkaClusterAdmin` rather than `admin` above, because it must not be traced and retried as a
      // read: it is a bounded yes/no with its own five-second timeout, and inheriting the read path's
      // minute would make the button useless on exactly the address it exists to diagnose.
      probe = ClusterProbeUseCase.make[F](
        new ConnectivityProbeAdapter[F](
          AdminClientPool.resource[F](metrics).map(KafkaClusterAdmin[F](_)),
          logger
        ),
        logger
      )
    } yield Bootstrapped(
      store = configStore,
      registry = registry,
      admin = admin,
      topology = topology,
      brokers = brokers,
      write = write,
      probe = probe,
      capabilities = capabilities,
      health = clusterStore.health,
      storeMode = if store.kafka.isDefined then "kafka" else if store.dir.isDefined then "file" else "none"
    )

  /** The checks behind `/health/ready`, in the order an operator wants to read them.
    *
    * **There is deliberately no per-cluster check.** Readiness is what the gateway polls, and an unready
    * cluster service dims the `cluster` capability for every cluster at once - which is exactly the failure
    * the milestone's decision D4 forbids, arriving by a different route. A cluster that cannot be reached is
    * reported per cluster, in `GET /capabilities` and in each row's section.
    *
    * `store` stays healthy while the store is *degraded*, for the same reason: once replay has completed, a
    * store outage leaves this service able to serve everything it already knows. Flipping readiness would
    * take it out of the gateway's rotation for a fault that costs it nothing.
    */
  def readiness[F[_]: Async](bootstrapped: Bootstrapped[F]): List[ReadinessCheck[F]] =
    List(
      ReadinessCheck.always[F]("process"),
      ReadinessCheck[F](
        "config",
        bootstrapped.registry.snapshot.map(snapshot =>
          CheckResult(
            "config",
            healthy = true,
            Some(s"${snapshot.size} cluster(s) at registry version ${snapshot.version.tag}")
          )
        )
      ),
      ReadinessCheck[F](
        "store",
        bootstrapped.health.map(health => CheckResult("store", healthy = true, Some(describe(health))))
      )
    )

  /** What the `store` check says. Degradation is reported in the detail, never as a failure. */
  private def describe(health: StoreHealth): String = health match {
    case StoreHealth.Online => "replayed and following the log"
    case StoreHealth.Degraded(reason, since) => s"degraded since $since: $reason"
    case StoreHealth.NotConfigured => "no metadata store is configured; clusters come from configuration"
  }

  // -----------------------------------------------------------------------------------------------

  /** Configuration to profiles, with every failure at once.
    *
    * The caller is a process starting up, so the requirement is ADR-013's: an operator who wrote two bad
    * clusters is told about both, in one message, rather than being made to fix them one restart at a time.
    */
  private[app] def profilesOf[F[_]: Async](clusters: List[ClusterConfig]): F[List[ClusterProfile]] = {
    val (failures, profiles) = clusters.map(profileOf).partitionMap(identity)

    NonEmptyList.fromList(failures) match {
      case None => Async[F].pure(profiles)
      case Some(problems) =>
        Async[F].raiseError(
          new IllegalArgumentException(
            s"the configured clusters are not valid:\n${problems.toList.mkString("\n")}"
          )
        )
    }
  }

  private def profileOf(cluster: ClusterConfig): Either[String, ClusterProfile] =
    ClusterProfile
      .from(
        id = cluster.id,
        displayName = cluster.name,
        bootstrap = cluster.bootstrapServers,
        security = cluster.security,
        properties = cluster.properties,
        admin = cluster.admin,
        readOnly = cluster.readOnly,
        colour = None,
        // Statically configured, so version zero: the store's records start at one and therefore always
        // win the overlay, which is the precedence rule stated as a number rather than as a branch.
        version = ProfileVersion.Static,
        origin = ProfileOrigin.Static
      )
      .leftMap(error => s"  - ${cluster.id.value}: ${error.message}")

  private def logResolved[F[_]: Async](
      registry: ClusterRegistry[F],
      logger: StructuredLogger[F]
  ): F[Unit] =
    registry.snapshot.flatMap { snapshot =>
      val stored = snapshot.profiles.values.count(_.version.value > 0L)

      logger.info(
        s"cluster registry: ${snapshot.size} cluster(s) " +
          s"(${snapshot.size - stored} from configuration, $stored from the store)"
      )
    }

  private def clockOf[F[_]: Clock]: ClockPort[F] = new ClockPort[F] {
    def now: F[Instant] = Clock[F].realTimeInstant
  }

}
