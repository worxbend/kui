package kui.cluster.application

import scala.concurrent.duration.FiniteDuration

import cats.effect.kernel.{Async, Ref, Resource, Temporal}
import cats.effect.std.{Semaphore, Supervisor}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import org.typelevel.log4cats.StructuredLogger

import kui.cache.{CacheMetrics, SeriesWindowCell, SnapshotCell, SnapshotLoadFailure}
import kui.cluster.domain.*
import kui.kernel.error.KuiError
import kui.kernel.{BrokerId, ClusterId}

/** The per-cluster snapshot cells, kept in step with the registry.
  *
  * Three cells and one window per cluster, and they are separate because their costs and their rates of
  * change have nothing in common — one cell refreshed at the fastest of the four cadences would either
  * re-probe six features every thirty seconds or leave the broker list a minute out of date:
  *
  *   - the **topology**, every thirty seconds. `describeCluster` plus three optional calls
  *   - the **capability probe**, hourly and on reconnect. Six calls, and the answer changes at upgrades
  *   - the **partition sweep**, every minute. One `describeTopics` over every topic in the cluster, which is
  *     an order of magnitude more expensive than the topology refresh and changes an order of magnitude less
  *     often — the same trade, and the same minute, `kui.topics.refreshInterval` documents for the topic
  *     service's own scrape
  *   - the **controller-uptime window**, which is not a cell at all: it is a `SeriesWindow` fed one boolean
  *     per successful topology refresh, because a percentage over six hours cannot come from a value that is
  *     overwritten every thirty seconds
  */
trait ClusterSnapshots[F[_]] {

  def topologyOf(id: ClusterId): F[Option[SnapshotCell[F, ClusterTopology]]]

  def capabilitiesOf(id: ClusterId): F[Option[SnapshotCell[F, ClusterFeatures]]]

  /** The last `describeTopics` sweep. Exposed for the same reason the other two are: a suite has to be able
    * to wait for it before asserting on figures derived from it.
    */
  def partitionsOf(id: ClusterId): F[Option[SnapshotCell[F, TopicSweep]]]

  /** Every cluster currently held, in registry order. */
  def all: F[List[(ClusterRef, SnapshotCell[F, ClusterTopology])]]

  /** Starts a refresh of one cluster's topology and returns immediately.
    *
    * It must not await completion: a forced refresh against a dead cluster would otherwise block for the full
    * admin timeout, and the button that triggered it would hang — which is the exact failure the whole
    * snapshot design exists to avoid. Deduplication is the cell's, so twenty presses are one admin call.
    *
    * The partition sweep is refreshed first and the topology after it, because the topology reads the sweep:
    * refreshing only the topology would answer a press of "refresh" with partition counts up to a minute old,
    * which is the figure the operator most likely pressed it for.
    */
  def requestRefresh(id: ClusterId): F[Boolean]
}

object ClusterSnapshots {

  /** Transfer a composed resource into the dynamic owner without a cancellation gap. */
  private[application] def handoff[F[_]: Async, A](resource: Resource[F, A])(
      register: (A, F[Unit]) => F[Unit]
  ): F[Unit] =
    Async[F].uncancelable { _ =>
      resource.allocated.flatMap { (value, release) =>
        register(value, release).onError(_ => release)
      }
    }

  val CacheName: String = "cluster.topology"

  val CapabilityCacheName: String = "cluster.capabilities"

  val PartitionCacheName: String = "cluster.partitions"

  /** The `cache` metric attribute the controller-uptime window is counted under. One string per *kind* of
    * series, never a per-cluster value: the cluster is its own attribute.
    */
  val UptimeSeriesName: String = "cluster.controller.uptime"

  /** The four cadences, in one value.
    *
    * A record rather than four positional durations, because four adjacent `FiniteDuration` parameters is a
    * signature two of whose arguments can be swapped without the compiler noticing — and the two that would
    * swap most plausibly are the hourly probe and the six-hour window.
    *
    * @param uptimeStep
    *   the bucket width of the controller-uptime window. A refresh lands in the bucket its instant falls in
    *   and the newest reading of a bucket wins, so a step at or above the refresh interval is what keeps the
    *   ring's size a function of the window rather than of how often KUI happens to scrape
    */
  final case class Tuning(
      refreshInterval: FiniteDuration,
      capabilityInterval: FiniteDuration,
      sweepInterval: FiniteDuration,
      uptimeWindow: FiniteDuration,
      uptimeStep: FiniteDuration
  ) {

    /** How many samples the uptime window retains: one per bucket of the window, and never fewer, because a
      * ring that dropped a bucket would answer a full window with a hole in it.
      */
    def uptimeSamples: Int =
      math.max(1, math.ceil(uptimeWindow.toMillis.toDouble / uptimeStep.toMillis.toDouble).toInt)
  }

  object Tuning {
    given CanEqual[Tuning, Tuning] = CanEqual.derived
  }

  /** Builds the cells and keeps them in step with the registry for as long as the resource is held.
    *
    * Releasing it cancels every refresh loop and every supervised forced refresh. That matters more than it
    * looks: a leaked refresh fiber is a fiber holding a Kafka admin client, authenticating every thirty
    * seconds to a cluster the operator has removed.
    */
  def resource[F[_]: Async](
      registry: ClusterRegistry[F],
      admin: ClusterAdmin[F],
      metrics: CacheMetrics[F],
      tuning: Tuning,
      logger: StructuredLogger[F]
  ): Resource[F, ClusterSnapshots[F]] =
    for {
      supervisor <- Supervisor[F]
      cells <- Resource.eval(Ref.of[F, Map[ClusterId, Entry[F]]](Map.empty))
      gate <- Resource.eval(Semaphore[F](1L))
      impl = new Impl[F](registry, admin, metrics, tuning, logger, supervisor, cells, gate)
      // Release every cell before the supervisor goes away, so that a cell's own finalizer runs
      // while its fiber can still be cancelled rather than after the supervisor has torn it down.
      _ <- Resource.onFinalize(impl.releaseAll)
      _ <- Resource.eval(registry.snapshot.flatMap(impl.sync))
      _ <- registry.changes.evalMap(impl.sync).compile.drain.background
    } yield impl

  /** The refresh: the ordered set of admin calls that produce one `ClusterTopology`.
    *
    * Only `describeCluster` is required. A cluster that answers it is reachable and its page must render: a
    * broker list with no disk figures is far more useful than an "unavailable" panel, and it is exactly what
    * a managed service or a cluster where KUI lacks `DESCRIBE_CONFIGS` looks like.
    *
    * The three optional calls run in parallel, and two of them are skipped entirely when the probed feature
    * set says the cluster cannot answer them — which is what probing buys: no `UnsupportedVersionException`
    * every thirty seconds against a ZooKeeper cluster.
    *
    * Optional failures are logged at DEBUG and not WARN. On a managed service they fire every thirty seconds
    * for ever, and a warning that always fires is noise that teaches an operator to filter the log.
    *
    * `sweep` is handed in rather than fetched here because it is refreshed on its own, slower cadence: this
    * function is called every thirty seconds and a `describeTopics` over four thousand topics is not. `None`
    * — no sweep has answered yet, or the last one failed — means every partition figure is absent, which is
    * the correct reading of "nobody has counted".
    */
  def refreshOne[F[_]: Temporal](
      admin: ClusterAdmin[F],
      profile: ClusterProfile,
      features: ClusterFeatures,
      sweep: Option[TopicSweep],
      logger: StructuredLogger[F]
  ): F[Either[KuiError, ClusterTopology]] = {
    val context = Map(
      "service.name" -> ClusterService.Id.value,
      "operation" -> ClusterTopologyUseCase.Operation,
      "cluster.id" -> profile.id.value
    )

    def optional[A](name: String, call: F[Either[KuiError, A]], fallback: A): F[A] =
      call.flatMap {
        case Right(value) => value.pure[F]
        case Left(error) =>
          logger
            .debug(context ++ Map("error.code" -> error.code.wire))(
              s"$name is not available on this cluster: ${error.message}"
            )
            .as(fallback)
      }

    admin.describeCluster(profile).flatMap {
      case Left(error) => error.asLeft[ClusterTopology].pure[F]
      case Right(description) =>
        val version = optional("version detection", admin.detectVersion(profile), None)

        val quorum =
          if features.has(ClusterFeature.KRaftQuorum) then
            optional("the metadata quorum", admin.describeQuorum(profile), None)
          else Option.empty[QuorumInfo].pure[F]

        val load =
          if features.has(ClusterFeature.LogDirs) then
            optional(
              "log directories",
              admin.describeLogDirs(profile, description.brokerIds).map(_.map(loadOf)),
              Map.empty[BrokerId, BrokerLoad]
            )
          else Map.empty[BrokerId, BrokerLoad].pure[F]

        // `both` rather than a `for`-comprehension: the three optional calls are independent, and
        // running them one after another would make a page wait for three round trips instead of
        // one. They are three calls to one already-bounded admin client, so nothing further limits
        // them.
        Temporal[F].both(version, Temporal[F].both(quorum, load)).map { (detected, rest) =>
          val (members, perBroker) = rest
          ClusterTopology(
            cluster = profile.ref,
            description = description,
            version = detected,
            quorum = members,
            features = features,
            load = BrokerLoad.withSkew(perBroker),
            // `complete` and not `census`: a sweep that could not describe one of its topics yields no
            // partition figures at all, here and per broker and cluster-wide together. The topic count
            // survives it, because that came from the listing rather than from the describes.
            census = sweep.flatMap(_.complete),
            topics = sweep.map(_.topics),
            // Filled by the loop that owns the window, which is the only thing that knows how long this
            // cluster has been observed. A refresh in isolation — which is what this function is — has no
            // window and says so.
            controllerUptime = None
          ).asRight[KuiError]
        }
    }
  }

  /** A skipped broker gets no `BrokerLoad` at all rather than an empty one: an empty load renders as a broker
    * with no disks, which is a different and wrong statement.
    */
  private def loadOf(
      dirs: PartialResult[BrokerId, List[LogDir]]
  ): Map[BrokerId, BrokerLoad] =
    dirs.values.map { (broker, logDirs) =>
      val replicas = logDirs.map(_.currentReplicas.size).sum
      broker -> BrokerLoad(replicas, skewPercent = None, logDirs = logDirs)
    }

  /** One cluster's cells and its uptime window, and the action that stops all of them.
    *
    * The window has nothing to release — no fiber, no native cache — but it is held here so that a profile
    * change drops it with the rest: its samples describe the cluster that *was* configured at that address,
    * and carrying them over would let a rotated profile answer "over the last 6h" from its first minute.
    */
  final private case class Entry[F[_]](
      profile: ClusterProfile,
      topology: SnapshotCell[F, ClusterTopology],
      capabilities: SnapshotCell[F, ClusterFeatures],
      partitions: SnapshotCell[F, TopicSweep],
      uptime: SeriesWindowCell[F, Boolean],
      release: F[Unit]
  )

  // `Async` rather than the `Temporal` the rest of this file makes do with, for one reason:
  // `SeriesWindowCell` is `Ref`-backed and asks for `Sync`, and asking for both separately makes every
  // `Monad[F]` in the class ambiguous. Nothing here suspends a side effect of its own.
  final private class Impl[F[_]: Async](
      registry: ClusterRegistry[F],
      admin: ClusterAdmin[F],
      metrics: CacheMetrics[F],
      tuning: Tuning,
      logger: StructuredLogger[F],
      supervisor: Supervisor[F],
      cells: Ref[F, Map[ClusterId, Entry[F]]],
      gate: Semaphore[F]
  ) extends ClusterSnapshots[F] {

    def topologyOf(id: ClusterId): F[Option[SnapshotCell[F, ClusterTopology]]] =
      cells.get.map(_.get(id).map(_.topology))

    def capabilitiesOf(id: ClusterId): F[Option[SnapshotCell[F, ClusterFeatures]]] =
      cells.get.map(_.get(id).map(_.capabilities))

    def partitionsOf(id: ClusterId): F[Option[SnapshotCell[F, TopicSweep]]] =
      cells.get.map(_.get(id).map(_.partitions))

    def all: F[List[(ClusterRef, SnapshotCell[F, ClusterTopology])]] =
      for {
        held <- cells.get
        order <- registry.refs
      } yield order.flatMap(ref => held.get(ref.id).map(entry => ref -> entry.topology))

    def requestRefresh(id: ClusterId): F[Boolean] =
      cells.get.map(_.get(id)).flatMap {
        case None => false.pure[F]
        // Sequenced, not parallel: the topology reads whatever the sweep cell holds when it runs, so a
        // sweep started beside it would land a moment too late to be in the topology it was asked for.
        case Some(entry) =>
          supervisor.supervise(entry.partitions.refresh >> entry.topology.refresh).as(true)
      }

    /** Starts cells for clusters that appeared, and cancels and drops cells for clusters that disappeared.
      *
      * A profile whose *contents* changed has its cells replaced rather than reused: a rotated password must
      * not keep being used by a loop that captured the old profile, and the previous topology describes a
      * cluster that may no longer be the one configured.
      */
    def sync(snapshot: RegistrySnapshot): F[Unit] =
      gate.permit.use { _ =>
        for {
          held <- cells.get
          wanted = snapshot.profiles
          obsolete = held.filter((id, entry) => !wanted.get(id).contains(entry.profile))
          fresh = wanted.filterNot((id, profile) => held.get(id).exists(_.profile == profile))
          _ <- Async[F].uncancelable(_ =>
            cells.update(_ -- obsolete.keySet) >> obsolete.values.toList.traverse_(_.release)
          )
          _ <- fresh.values.toList.traverse_(start)
        } yield ()
      }

    def releaseAll: F[Unit] =
      gate.permit.use(_ => cells.getAndSet(Map.empty).flatMap(_.values.toList.traverse_(_.release)))

    private def start(profile: ClusterProfile): F[Unit] = {
      val capabilities = SnapshotCell.resource[F, ClusterFeatures](
        CapabilityCacheName,
        profile.id,
        tuning.capabilityInterval,
        metrics
      )(admin.capabilities(profile))

      val partitions = SnapshotCell.resource[F, TopicSweep](
        PartitionCacheName,
        profile.id,
        tuning.sweepInterval,
        metrics
      )(sweep(profile))

      val resource = for {
        capsCell <- capabilities
        sweepCell <- partitions
        // The window starts collecting now, and not when the first sample lands: `startedAt` is what
        // makes the coverage refusal mean anything, and a window stamped with its first sample would
        // claim a full six hours the moment a cluster that had been down for six hours answered once.
        startedAt <- Resource.eval(Temporal[F].realTimeInstant)
        window <- Resource.eval(
          SeriesWindowCell.create[F, Boolean](
            UptimeSeriesName,
            profile.id,
            tuning.uptimeStep,
            tuning.uptimeWindow,
            tuning.uptimeSamples,
            startedAt,
            metrics
          )
        )
        // Was the last topology refresh a failure? It is the trigger for re-probing capabilities on
        // reconnect: the usual reason a cluster was offline is that it was being upgraded, and its
        // feature set is the thing most likely to have changed while it was away.
        wasOffline <- Resource.eval(Ref.of[F, Boolean](false))
        topology = SnapshotCell.resource[F, ClusterTopology](
          CacheName,
          profile.id,
          tuning.refreshInterval,
          metrics
        )(load(profile, capsCell, sweepCell, window, wasOffline))
        topologyCell <- topology
      } yield (topologyCell, capsCell, sweepCell, window)

      // Compose before allocating, then mask the handoff to the owning map.
      handoff(resource) { case ((topologyCell, capsCell, sweepCell, window), release) =>
        cells.update(
          _.updated(profile.id, Entry(profile, topologyCell, capsCell, sweepCell, window, release))
        )
      }
    }

    /** The sweep cell's `load`, raising for the same reason the topology's does. */
    private def sweep(profile: ClusterProfile): F[TopicSweep] =
      admin.sweepPartitions(profile).flatMap {
        case Left(error) => Temporal[F].raiseError[TopicSweep](SnapshotLoadFailure(error))
        case Right(swept) if swept.isComplete => swept.pure[F]
        case Right(swept) =>
          // One line per sweep, at DEBUG, naming a bounded sample of the topics rather than all of
          // them: on a cluster where KUI lacks DESCRIBE on a whole namespace this fires every minute
          // for ever, and four thousand names on one line is a log nobody reads twice.
          logger
            .debug(
              Map(
                "service.name" -> ClusterService.Id.value,
                "operation" -> ClusterTopologyUseCase.Operation,
                "cluster.id" -> profile.id.value
              )
            )(
              s"described ${swept.described} of ${swept.topics} topics; partition figures are withheld " +
                s"(for example ${swept.unreadable.take(3).map(_.value).toList.sorted.mkString(", ")})"
            )
            .as(swept)
      }

    /** The cell's `load`. It raises rather than returns the failure, because `SnapshotCell` catches a
      * `Throwable` and unwraps a `SnapshotLoadFailure` back into the `KuiError` the adapter already
      * classified — which is how a `KuiError` survives an effect that can only carry a `Throwable`.
      */
    private def load(
        profile: ClusterProfile,
        capabilities: SnapshotCell[F, ClusterFeatures],
        partitions: SnapshotCell[F, TopicSweep],
        uptime: SeriesWindowCell[F, Boolean],
        wasOffline: Ref[F, Boolean]
    ): F[ClusterTopology] =
      for {
        probed <- capabilities.get
        swept <- partitions.get
        now <- Temporal[F].realTimeInstant
        // A cluster whose capability cell has not answered yet gets `unprobed`, which skips the two
        // optional calls on this first pass and fills them in thirty seconds later. The alternative
        // is a first refresh that waits on a six-feature probe of a cluster that may be down.
        features = probed.value.getOrElse(ClusterFeatures.unprobed(now))
        // A sweep whose last attempt failed contributes nothing, even though its cell still holds the
        // one before it. The cell keeps that value so the *sweep* can be reported as stale; publishing
        // it inside a topology stamped with this instant would date a count from a cluster that has
        // since stopped answering as though it had just been taken.
        sweep = Option.unless(swept.status.isOffline)(swept.value).flatten
        result <- refreshOne(admin, profile, features, sweep, logger)
        topology <- result match {
          case Left(error) =>
            // Nothing is recorded in the uptime window. A scrape KUI could not make is not a cluster
            // without a controller, and a `false` here would report KUI's own outage as the cluster's.
            // The window's own contract covers it: an unsampled bucket is a gap, never a zero.
            wasOffline.set(true) >> Temporal[F].raiseError[ClusterTopology](SnapshotLoadFailure(error))
          case Right(value) =>
            for {
              _ <- uptime.record(now, value.description.controller.isDefined)
              measured <- uptimeOf(uptime, now)
              _ <- wasOffline.getAndSet(false).flatMap { recovered =>
                if recovered then supervisor.supervise(capabilities.refresh).void
                else Temporal[F].unit
              }
            } yield value.copy(controllerUptime = Some(measured))
        }
      } yield topology

    /** Reads the window once and does the arithmetic on what it hands back.
      *
      * Once, rather than asking it for its coverage and its buckets separately: every read of the cell is
      * counted through `CacheMetrics`, and two reads per refresh would double the hit count of a cache nobody
      * queried twice.
      */
    private def uptimeOf(cell: SeriesWindowCell[F, Boolean], now: java.time.Instant): F[ControllerUptime] =
      cell
        .read(now)
        .map(window =>
          ControllerUptime.of(
            tuning.uptimeWindow,
            window.coverage(now),
            window.valuesOver(tuning.uptimeWindow, now)
          )
        )
  }
}
