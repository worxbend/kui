package kui.cluster.application

import cats.data.NonEmptyList
import cats.effect.kernel.Temporal
import cats.syntax.all.*
import org.typelevel.log4cats.StructuredLogger

import kui.cluster.domain.*
import kui.kernel.error.{ApplicationError, FieldError, InfrastructureError, KuiError}
import kui.kernel.{BrokerId, ClusterId}

/** The broker detail reads.
  *
  * The split between what comes from the snapshot and what is read live is the decision this component exists
  * to make. The broker *list* is a list screen and comes from the snapshot: it must render for a dead
  * cluster, and a page listing thirty brokers must not make thirty admin calls. Log directories and
  * configuration are read *live*, per request, because a directory that went offline three seconds ago is the
  * reason the operator opened the page, and because scraping every broker's two hundred configuration entries
  * every thirty seconds would be pointless traffic.
  */
trait BrokerDetailUseCase[F[_]] {

  /** From the snapshot. Never calls a broker. `Left` only for an unknown *cluster*. */
  def brokers(cluster: ClusterId): F[Either[KuiError, BrokerList]]

  /** Live, with a snapshot fallback when the live call fails and the snapshot has directories. */
  def logDirs(cluster: ClusterId, broker: BrokerId): F[Either[KuiError, BrokerLogDirs]]

  /** Derived from the same live call as `logDirs`. Callers that need both should use `logDirsAndSizes` rather
    * than both of these, so that one page costs one admin call.
    */
  def partitionSizes(cluster: ClusterId, broker: BrokerId): F[Either[KuiError, PartitionSizes]]

  /** One call, both shapes. */
  def logDirsAndSizes(
      cluster: ClusterId,
      broker: BrokerId
  ): F[Either[KuiError, (BrokerLogDirs, PartitionSizes)]]

  /** Live, with no fallback.
    *
    * `Left(ApplicationError.Unsupported)` when the cluster refuses the call. It must **not** become
    * `Right(Nil)`: an empty configuration table and "this cluster does not expose broker configuration" look
    * identical to a user and mean opposite things.
    */
  def configs(
      cluster: ClusterId,
      broker: BrokerId,
      includeDocs: Boolean
  ): F[Either[KuiError, BrokerConfigView]]
}

object BrokerDetailUseCase {

  val Operation: String = "kui.cluster.broker"

  def make[F[_]: Temporal](
      registry: ClusterRegistry[F],
      snapshots: ClusterSnapshots[F],
      admin: ClusterAdmin[F],
      logger: StructuredLogger[F]
  ): BrokerDetailUseCase[F] =
    new BrokerDetailUseCase[F] {

      private val context: Map[String, String] =
        Map("service.name" -> ClusterService.Id.value, "operation" -> Operation)

      def brokers(cluster: ClusterId): F[Either[KuiError, BrokerList]] =
        resolved(cluster).flatMap {
          case Left(error) => error.asLeft[BrokerList].pure[F]
          case Right(profile) =>
            viewOf(profile).map { view =>
              BrokerList(
                profile.ref,
                rowsOf(view),
                view.freshness,
                // Straight off the same snapshot the rows came from, so the quorum's high watermark and
                // the brokers beside it are one moment rather than two.
                view.topology.flatMap(_.quorum)
              ).asRight[KuiError]
            }
        }

      def logDirs(cluster: ClusterId, broker: BrokerId): F[Either[KuiError, BrokerLogDirs]] =
        logDirsAndSizes(cluster, broker).map(_.map((dirs, _) => dirs))

      def partitionSizes(cluster: ClusterId, broker: BrokerId): F[Either[KuiError, PartitionSizes]] =
        logDirsAndSizes(cluster, broker).map(_.map((_, sizes) => sizes))

      def logDirsAndSizes(
          cluster: ClusterId,
          broker: BrokerId
      ): F[Either[KuiError, (BrokerLogDirs, PartitionSizes)]] =
        withBroker(cluster, broker) { (profile, view) =>
          admin
            .describeLogDirs(profile, NonEmptyList.one(broker))
            .map(_.flatMap { result =>
              result
                .get(broker)
                .toRight(result.skipped.get(broker) match {
                  case Some(SkipReason.Failed(error)) => error
                  case Some(SkipReason.Unauthorized) =>
                    ApplicationError.Forbidden("KUI is not authorized to read log directories")
                  case reason =>
                    InfrastructureError.Unreachable(
                      profile.label,
                      reason.fold("no log directories were reported for this broker")(_.describe)
                    )
                })
            })
            .flatMap {
              case Right(dirs) =>
                Temporal[F].realTimeInstant.map { now =>
                  shaped(profile.ref, broker, dirs, SnapshotFreshness.Fresh(now))
                }

              case Left(error) =>
                // The snapshot already holds this broker's directories from the last successful
                // refresh, so a live failure greys the panel rather than emptying the page. Only a
                // cluster that has never answered at all produces a `Left` here.
                fallbackDirs(view, broker, error) match {
                  case Some((dirs, freshness)) =>
                    logger
                      .warn(
                        context ++ Map(
                          "cluster.id" -> profile.id.value,
                          "broker.id" -> broker.value.toString,
                          "error.code" -> error.code.wire
                        )
                      )("the live log-directory read failed; serving the last snapshot")
                      .as(shaped(profile.ref, broker, dirs, freshness))

                  case None => error.asLeft[(BrokerLogDirs, PartitionSizes)].pure[F]
                }
            }
        }

      def configs(
          cluster: ClusterId,
          broker: BrokerId,
          includeDocs: Boolean
      ): F[Either[KuiError, BrokerConfigView]] =
        withBroker(cluster, broker) { (profile, view) =>
          // The capability decides, and the caller's flag can only narrow it: asking a 2.5 broker
          // for documentation raises an unsupported-version error and loses the whole call.
          val docs = includeDocs && view.topology.exists(_.has(ClusterFeature.ConfigDocumentation))

          admin.brokerConfigs(profile, broker, docs).map {
            case Left(error) => error.asLeft[BrokerConfigView]
            case Right(entries) =>
              BrokerConfigView(profile.ref, broker, entries.sortBy(_.name), docs).asRight[KuiError]
          }
        }

      /** Resolves the cluster, then checks the broker id against the snapshot before any call is made.
        *
        * Three consequences worth stating so nobody removes the check as redundant: a bad id costs no network
        * call; the 404 is an `ApplicationError` and so cannot dim a capability; and a cluster whose snapshot
        * has never been filled reports the *cluster's* failure rather than a misleading "broker not found",
        * because the correct answer to "does broker 3 exist" on an unreachable cluster is "I cannot tell
        * you".
        */
      private def withBroker[A](cluster: ClusterId, broker: BrokerId)(
          body: (ClusterProfile, TopologyView) => F[Either[KuiError, A]]
      ): F[Either[KuiError, A]] =
        resolved(cluster).flatMap {
          case Left(error) => error.asLeft[A].pure[F]
          case Right(profile) =>
            viewOf(profile).flatMap { view =>
              view.topology match {
                case None => unreachable(view).asLeft[A].pure[F]
                case Some(topology) if topology.description.broker(broker).isEmpty =>
                  unknownBroker(profile, broker).asLeft[A].pure[F]
                case Some(_) => body(profile, view)
              }
            }
        }

      private def resolved(cluster: ClusterId): F[Either[KuiError, ClusterProfile]] =
        registry.resolve(cluster)

      private def viewOf(profile: ClusterProfile): F[TopologyView] =
        snapshots.topologyOf(profile.id).flatMap {
          case None => TopologyView(profile.ref, None, SnapshotFreshness.Loading).pure[F]
          case Some(cell) =>
            cell.get.map { snapshot =>
              TopologyView(profile.ref, snapshot.value, ClusterTopologyUseCase.freshnessOf(snapshot))
            }
        }

      /** The broker list of a cluster that has never answered: an empty list *labelled* unavailable.
        *
        * `Left` would be wrong — the cluster is configured — and a bare empty list would be worse, because
        * "this cluster has no brokers" and "KUI has never reached this cluster" look identical on a screen.
        * The freshness field is what carries the difference, which is the whole argument for it being part of
        * every one of these types.
        */
      private def rowsOf(view: TopologyView): List[BrokerListRow] =
        view.topology.toList.flatMap { topology =>
          topology.description.brokers.toList.sorted.map { broker =>
            val load = topology.load.get(broker.id)

            BrokerListRow(
              broker = broker,
              isController = topology.description.controller.exists(_.id == broker.id),
              partitions = topology.partitionsOn(broker.id),
              leaders = topology.leadersOn(broker.id),
              replicas = load.map(_.replicas),
              skewPercent = load.flatMap(_.skewPercent),
              leaderSkewPercent = topology.leaderSkewOn(broker.id),
              totalBytes = load.flatMap(_.totalBytes),
              usableBytes = load.flatMap(_.usableBytes),
              usedByKafkaBytes = load.flatMap(_.usedByKafkaBytes),
              offlineDirCount = load.map(_.offlineDirs.size).getOrElse(0)
            )
          }
        }

      private def fallbackDirs(
          view: TopologyView,
          broker: BrokerId,
          liveFailure: KuiError
      ): Option[(List[LogDir], SnapshotFreshness)] =
        for {
          topology <- view.topology
          load <- topology.load.get(broker)
          if load.logDirs.nonEmpty
          at <- view.freshness.scrapedAtOption
        } yield (load.logDirs, staleOf(view.freshness, at, liveFailure))

      /** A fallback read is stale whatever the snapshot's own freshness says: the live call is the thing that
        * failed, and labelling the answer `Fresh` because the snapshot happens to be current would tell the
        * user the directories were read a moment ago when they were not.
        *
        * The error carried is the snapshot's when the snapshot was already failing — that outage is the older
        * and more informative of the two — and otherwise the live call's own, so that a page greyed out by a
        * timeout says `UPSTREAM_TIMEOUT` and one greyed out by a rejected credential says `UPSTREAM_AUTH`.
        */
      private def staleOf(
          freshness: SnapshotFreshness,
          at: java.time.Instant,
          liveFailure: KuiError
      ): SnapshotFreshness =
        freshness match {
          case SnapshotFreshness.Stale(_, error, since) => SnapshotFreshness.Stale(at, error, since)
          case _ => SnapshotFreshness.Stale(at, liveFailure, at)
        }

      /** Both shapes of one set of directories, carrying the same freshness.
        *
        * They are built together so that a caller can never end up with a fresh list of directories beside a
        * stale list of partition sizes derived from a different read.
        */
      private def shaped(
          cluster: ClusterRef,
          broker: BrokerId,
          dirs: List[LogDir],
          freshness: SnapshotFreshness
      ): Either[KuiError, (BrokerLogDirs, PartitionSizes)] =
        Right(
          (
            BrokerLogDirs(cluster, broker, dirs.sorted, freshness),
            PartitionSizes.of(cluster, broker, dirs, freshness)
          )
        )

      /** A broker id that is not in this cluster's current description.
        *
        * `ApplicationError` and never `InfrastructureError`, because only the second dims a capability and a
        * mistyped path segment must not switch a feature off for everybody else.
        *
        * There is no `KUI-BROKER-NOT-FOUND` code in `libs/kernel` and this area does not own that file, so
        * this is reported as a validation failure naming the field. The message is exact, the `details` entry
        * points at `brokerId`, and no existing code's documented meaning is contradicted — which is the same
        * trade the Kafka error mapper took for consumer groups.
        */
      private def unknownBroker(profile: ClusterProfile, broker: BrokerId): KuiError =
        ApplicationError.Invalid(
          s"broker '${broker.value}' does not exist on cluster '${profile.label}'",
          List(FieldError.of("brokerId", "must be the id of a broker in this cluster"))
        )

      private def unreachable(view: TopologyView): KuiError =
        view.freshness match {
          case SnapshotFreshness.Unavailable(error, _) =>
            InfrastructureError.Unreachable(view.cluster.displayName, error.message)
          case _ =>
            InfrastructureError.Unreachable(
              view.cluster.displayName,
              "no topology has been read from this cluster yet"
            )
        }
    }
}
