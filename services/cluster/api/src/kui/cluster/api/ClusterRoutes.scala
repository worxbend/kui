package kui.cluster.api

import java.time.Instant

import cats.effect.kernel.{Async, Clock}
import cats.syntax.all.*
import org.typelevel.log4cats.StructuredLogger
import org.typelevel.otel4s.metrics.Counter
import sttp.tapir.server.ServerEndpoint

import kui.cluster.application.*
import kui.cluster.contract.ClusterEndpoints
import kui.cluster.contract.dto.*
import kui.cluster.domain.{ClusterProfile, ClusterTopology}
import kui.contracts.Section
import kui.contracts.cluster.{ClusterRowDto, LogDirDto}
import kui.http.principal.RbacGuard
import kui.kernel.ClusterId
import kui.kernel.error.{ErrorCode, KuiError}
import kui.security.PrincipalCodec

/** The six read endpoints, bound to use cases.
  *
  * One rule shapes every route in this file, and it is the milestone's central promise written as code:
  *
  *   - a request that names something KUI has never heard of **fails**: an unknown cluster id is
  *     `404 KUI-CLUSTER-NOT-FOUND`;
  *   - a request that names something real which cannot be reached **succeeds**: 200, with the section that
  *     needed a broker marked `unavailable` and carrying the reason it failed.
  *
  * That is the line ADR-039 §6 draws for capabilities, applied to a response body. A cluster being down is a
  * fact about one section of one answer, never a failure of the answer — and a 500 here would take a
  * three-cluster dashboard down because one of the three is unreachable, which is the exact outcome this
  * milestone exists to prevent.
  *
  * Nothing in this file decides an HTTP status. `ErrorEnvelope.statusOf` is the only code-to-status table in
  * KUI, and `ClusterApi.Securing` is the only place it is consulted.
  */
object ClusterRoutes {

  def apply[F[_]: Async](
      registry: ClusterRegistry[F],
      topology: ClusterTopologyUseCase[F],
      brokers: BrokerDetailUseCase[F],
      principals: PrincipalCodec[F],
      rejections: Counter[F, Long],
      logger: StructuredLogger[F],
      guard: RbacGuard[F]
  ): List[ServerEndpoint[Any, F]] = {
    val secured = ClusterApi.Securing[F](principals, rejections, logger, guard)

    List(
      listClusters(registry, topology, secured, guard),
      getCluster(registry, topology, secured),
      listBrokers(brokers, secured),
      brokerConfigs(brokers, secured),
      logDirs(registry, topology, brokers, secured),
      refresh(topology, secured)
    )
  }

  /** Every configured cluster, each with its own freshness.
    *
    * It never fails. The list itself comes from the registry — local configuration overlaid by the replayed
    * store — and is readable whenever the process is; only each row's summary can be missing.
    */
  private def listClusters[F[_]: Async](
      registry: ClusterRegistry[F],
      topology: ClusterTopologyUseCase[F],
      secured: ClusterApi.Securing[F],
      guard: RbacGuard[F]
  ): ServerEndpoint[Any, F] =
    secured(ClusterEndpoints.listClusters) { ctx => _ =>
      for {
        configured <- registry.list
        profiles <- configured.filterA(profile =>
          guard
            .authorize(ctx, ClusterEndpoints.getCluster, s"/internal/v1/clusters/${profile.id.value}")
            .map(_.isRight)
        )
        views <- topology.viewAll
        now <- Clock[F].realTimeInstant
      } yield {
        val byId = views.map(view => view.cluster.id -> view).toMap
        val rows = profiles
          .sortBy(profile => (profile.label, profile.id.value))
          .map(profile => rowOf(profile, byId.get(profile.id), now))

        ClustersResponse(rows, now).asRight[KuiError]
      }
    }

  private def getCluster[F[_]: Async](
      registry: ClusterRegistry[F],
      topology: ClusterTopologyUseCase[F],
      secured: ClusterApi.Securing[F]
  ): ServerEndpoint[Any, F] =
    secured(ClusterEndpoints.getCluster) { _ => id =>
      resolved(registry, id) { profile =>
        for {
          view <- topology.view(id)
          now <- Clock[F].realTimeInstant
        } yield view.map(found => ClusterDetailResponse(rowOf(profile, Some(found), now)))
      }
    }

  private def rowOf(profile: ClusterProfile, view: Option[TopologyView], now: Instant): ClusterRowDto =
    ClusterMapping.row(
      profile,
      view.flatMap(_.topology),
      // A configured cluster whose snapshot cell does not exist yet is starting, not missing: the cells are
      // built from the registry a moment after it resolves, and a row that vanished for that moment would
      // flicker on every rollout.
      view.map(_.freshness).getOrElse(SnapshotFreshness.Loading),
      now
    )

  private def listBrokers[F[_]: Async](
      brokers: BrokerDetailUseCase[F],
      secured: ClusterApi.Securing[F]
  ): ServerEndpoint[Any, F] =
    secured(ClusterEndpoints.listBrokers) { _ => id =>
      for {
        result <- brokers.brokers(id)
        now <- Clock[F].realTimeInstant
      } yield result.map { list =>
        BrokersResponse(
          SectionMapping.of(Some(list.brokers), list.freshness, now)(_.map(ClusterMapping.broker)),
          // Beside the section rather than inside it: a cluster with no metadata quorum is not a cluster
          // whose broker list failed, and folding the two together would make a ZooKeeper cluster's
          // brokers table render as unavailable.
          list.quorum.map(ClusterMapping.quorum)
        )
      }
    }

  /** One broker's settings, read live.
    *
    * A cluster that authenticates but refuses `describeConfigs` — the permanent steady state on some managed
    * services — produces an `unavailable` section carrying the refusal, never an empty list. An empty
    * configuration table and "this cluster does not expose its configuration" look identical on a screen and
    * mean opposite things.
    */
  private def brokerConfigs[F[_]: Async](
      brokers: BrokerDetailUseCase[F],
      secured: ClusterApi.Securing[F]
  ): ServerEndpoint[Any, F] =
    secured(ClusterEndpoints.brokerConfigs) { _ =>
      { case (id, brokerId, docs) =>
        for {
          result <- brokers.configs(id, brokerId, docs)
          now <- Clock[F].realTimeInstant
        } yield sectionOf(result, now)(view => Section.Ok(view.entries.map(ClusterMapping.configEntry), now))
          .map(BrokerConfigsResponse(_))
      }
    }

  /** Log directories, for one broker or for the whole cluster.
    *
    * The two halves are answered from different places, and the split is the domain's: one broker's
    * directories are read *live*, because a disk that went offline three seconds ago is the reason the page
    * was opened, while every broker's come from the topology snapshot, because a list page must not make one
    * admin call per broker to fill a column.
    */
  private def logDirs[F[_]: Async](
      registry: ClusterRegistry[F],
      topology: ClusterTopologyUseCase[F],
      brokers: BrokerDetailUseCase[F],
      secured: ClusterApi.Securing[F]
  ): ServerEndpoint[Any, F] =
    secured(ClusterEndpoints.logDirs) { _ =>
      { case (id, brokerId) =>
        brokerId match {
          case Some(broker) =>
            for {
              result <- brokers.logDirs(id, broker)
              now <- Clock[F].realTimeInstant
            } yield sectionOf(result, now)(found =>
              SectionMapping.of(Some(found.dirs), found.freshness, now)(
                _.map(ClusterMapping.logDir(broker, _))
              )
            ).map(LogDirsResponse(_))

          case None =>
            resolved(registry, id) { _ =>
              for {
                view <- topology.view(id)
                now <- Clock[F].realTimeInstant
              } yield view.map(found =>
                LogDirsResponse(SectionMapping.of(found.topology, found.freshness, now)(everyLogDir))
              )
            }
        }
      }
    }

  /** Every broker's directories, from one snapshot read, in broker id order. */
  private def everyLogDir(topology: ClusterTopology): List[LogDirDto] =
    topology.load.toList
      .sortBy((brokerId, _) => brokerId.value)
      .flatMap((brokerId, load) => load.logDirs.map(ClusterMapping.logDir(brokerId, _)))

  /** Asks for a scrape and answers immediately.
    *
    * Waiting for the scrape would make the button's latency the cluster's latency, which is precisely what
    * the snapshot design took off the page. 202 says "taken"; the body says when.
    */
  private def refresh[F[_]: Async](
      topology: ClusterTopologyUseCase[F],
      secured: ClusterApi.Securing[F]
  ): ServerEndpoint[Any, F] =
    secured(ClusterEndpoints.refresh) { _ => id =>
      for {
        result <- topology.forceRefresh(id)
        now <- Clock[F].realTimeInstant
      } yield result.map(_ => RefreshAcceptedDto(id, now))
    }

  // -----------------------------------------------------------------------------------------------
  // The one distinction every route in this file makes
  // -----------------------------------------------------------------------------------------------

  /** Resolves the cluster first, so an unknown id is a 404 before anything tries to read it. */
  private def resolved[F[_]: Async, A](registry: ClusterRegistry[F], id: ClusterId)(
      andThen: ClusterProfile => F[Either[KuiError, A]]
  ): F[Either[KuiError, A]] =
    registry.resolve(id).flatMap {
      case Left(error) => error.asLeft[A].pure[F]
      case Right(profile) => andThen(profile)
    }

  /** A use case's failure becomes a *section* unless it says the cluster does not exist.
    *
    * The test is the error code rather than the branch of the error type, because "no such cluster" is the
    * one failure that is about the request instead of about the world. Everything else — unreachable,
    * refused, timed out, breaker open — is a fact about a cluster that does exist, and belongs inside a 200
    * where a screen can render it next to the cluster's name.
    */
  private def sectionOf[A, B](result: Either[KuiError, A], at: Instant)(
      render: A => Section[B]
  ): Either[KuiError, Section[B]] =
    result match {
      case Right(value) => render(value).asRight[KuiError]
      case Left(error) if error.code == ErrorCode.ClusterNotFound => error.asLeft[Section[B]]
      case Left(error) => Section.fromEither[B](error.asLeft[B], at).asRight[KuiError]
    }
}
