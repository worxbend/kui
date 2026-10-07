package kui.allinone

import java.lang.management.ManagementFactory
import java.time.Instant
import javax.management.ObjectName

import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.*

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import fs2.Stream
import fs2.concurrent.SignallingRef

import kui.alerts.app.AlertsWiring
import kui.cluster.api.ClusterApi
import kui.cluster.app.ClusterServer
import kui.cluster.application.{CapabilityReportUseCase, ClusterRegistry, RegistrySnapshot, RegistryVersion}
import kui.cluster.domain.{ClusterProfile, ClusterRef, ProfileOrigin, ProfileVersion, StoreHealth}
import kui.config.{KuiConfig, ProfileClientConfig, SafeUrl, ServerConfig, UrlPolicy}
import kui.contracts.capability.ServiceCapabilities
import kui.http.KuiServer
import kui.http.principal.{PrincipalVerification, RbacGuard}
import kui.kernel.cluster.{AdminTuning, BootstrapServers, ClientProperties, ClusterSecurity}
import kui.kernel.error.{ApplicationError, ErrorCode, KuiError}
import kui.kernel.{ClusterId, Host, Port}
import kui.metrics.app.MetricsWiring
import kui.observability.Telemetry
import kui.security.PrincipalCodec
import kui.testkit.KuiIOSuite
import kui.testkit.fakes.FakeStructuredLogger

/** Real profile routes, real SSE clients and both production collector roots. */
final class ProfileDeploymentSuite extends KuiIOSuite {
  private val id = ClusterId.unsafe("live-profile")
  private val telemetry = Telemetry.noop[IO]
  private val principals = PrincipalCodec.inProcess[IO]

  private def profile(name: String, version: Long): ClusterProfile =
    ClusterProfile
      .from(
        id,
        name,
        BootstrapServers.unsafe(s"127.0.0.1:$version"),
        ClusterSecurity.Plaintext,
        ClientProperties.empty,
        AdminTuning.default.copy(requestTimeout = 50.millis, apiTimeout = 100.millis),
        version > 1L,
        None,
        ProfileVersion.unsafe(version),
        ProfileOrigin.Stored
      )
      .toOption
      .get

  private def await[A](read: IO[A])(accept: A => Boolean): IO[A] = {
    def loop: IO[A] = read.flatMap(value => if accept(value) then IO.pure(value) else IO.cede *> loop)
    loop.timeout(10.seconds)
  }

  final private case class Fixture(
      server: ClusterServer[IO],
      publish: List[ClusterProfile] => IO[Unit],
      subscribed: IO[Unit],
      subscribers: Ref[IO, Int],
      releases: Ref[IO, Int]
  )

  private def fixture(logger: FakeStructuredLogger[IO]): IO[Fixture] =
    for {
      state <- SignallingRef[IO, RegistrySnapshot](
        RegistrySnapshot(Map.empty, RegistryVersion.Initial, StoreHealth.Online, Instant.EPOCH)
      )
      ready <- Ref.of[IO, Int](0)
      releases <- Ref.of[IO, Int](0)
      registry = new ClusterRegistry[IO] {
        def snapshot = state.get
        def list: IO[List[ClusterProfile]] = state.get.map(_.profiles.values.toList)
        def refs: IO[List[ClusterRef]] = state.get.map(_.refs)
        def registryVersion = state.get.map(_.version)
        def reload = state.get
        def resolve(cluster: ClusterId): IO[Either[KuiError, ClusterProfile]] =
          state.get.map(
            _.get(cluster).toRight(
              ApplicationError.NotFound("cluster", cluster.value, ErrorCode.ClusterNotFound)
            )
          )
        def changes: Stream[IO, RegistrySnapshot] = state.discrete.zipWithIndex
          .evalTap { case (_, index) => if index == 0L then ready.update(_ + 1) else IO.unit }
          .map(_._1)
          .onFinalize(releases.update(_ + 1))
      }
      meter <- telemetry.meter("profile-deployment-test")
      rejections <- PrincipalVerification.rejectionCounter[IO](meter)
      interceptors <- ClusterApi.interceptors[IO](telemetry, rejections, logger)
      routes = ClusterApi.routes[IO](
        registry,
        EmptyClusterUseCases.topology,
        EmptyClusterUseCases.brokers,
        EmptyClusterUseCases.writes,
        EmptyClusterUseCases.probe,
        EmptyClusterUseCases.uiSettings,
        CapabilityReportUseCase.constant[IO](Set.empty),
        Nil,
        principals,
        rejections,
        telemetry,
        logger,
        RbacGuard.allowAll[IO]
      )
      server = ClusterServer[IO](
        routes,
        interceptors,
        Nil,
        IO.raiseError(new IllegalStateException("capabilities unused by profile client"))
      )
    } yield Fixture(
      server,
      profiles =>
        state.update(previous =>
          previous.copy(profiles = profiles.map(p => p.id -> p).toMap, version = previous.version.next)
        ),
      await(ready.get)(_ >= 1).void,
      ready,
      releases
    )

  private def exercise(fixture: Fixture, reports: List[IO[ServiceCapabilities]]): IO[Unit] = {
    val clients = IO.blocking(
      ManagementFactory.getPlatformMBeanServer
        .queryNames(new ObjectName("kafka.admin.client:type=app-info,id=kui-admin-live-profile-*"), null)
        .asScala
        .toSet
    )
    def settled(name: Option[String]): IO[Unit] = reports.traverse_(report =>
      await(report)(document =>
        name.fold(document.clusters.isEmpty)(value =>
          document.clusters.get(id).exists(_.name.contains(value))
        )
      ).void
    )
    for {
      _ <- fixture.subscribed
      _ <- settled(None)
      _ <- fixture.publish(List(profile("Added", 1L)))
      _ <- settled(Some("Added"))
      before <- await(clients)(_.nonEmpty)
      _ <- fixture.publish(List(profile("Replaced", 2L)))
      _ <- settled(Some("Replaced"))
      _ <- await(clients)(names => names.nonEmpty && (names intersect before).isEmpty)
      alerts <- reports.head
      _ = assert(!alerts.clusters(id).features.contains("alerts.acknowledge"))
      _ <- fixture.publish(Nil)
      _ <- settled(None)
      _ <- await(clients)(_.isEmpty)
    } yield ()
  }

  test("all-in-one profile transport delivers add/change/remove to both roots and cancels its subscription") {
    for {
      logger <- FakeStructuredLogger[IO]
      source <- fixture(logger)
      _ <- (for {
        profiles <- InProcessClusterProfiles.resource(source.server, telemetry, principals, logger)
        alerts <- AlertsWiring.fromProfiles(profiles, KuiConfig.Default, telemetry, principals, logger)
        metrics <- MetricsWiring.fromProfiles(profiles, KuiConfig.Default, telemetry, principals, logger)
      } yield List(alerts.capabilities, metrics.capabilities)).use(exercise(source, _))
      released <- source.releases.get
      _ = assertEquals(released, 1)
    } yield ()
  }

  test("standalone roots attach HTTP profile subscriptions and replace live collectors without polling") {
    for {
      logger <- FakeStructuredLogger[IO]
      source <- fixture(logger)
      _ <- KuiServer
        .resource[IO](
          ServerConfig(Host.unsafe("127.0.0.1"), Port.unsafe(0), "/"),
          source.server.routes,
          source.server.interceptors,
          logger,
          10.millis
        )
        .use { binding =>
          val url = SafeUrl.unsafe(s"http://127.0.0.1:${binding.port}")
          val settings = ProfileClientConfig(url, 1.hour, 1.second, 100.millis, 1.second, 2.seconds)
          val config =
            KuiConfig.Default.copy(topics = KuiConfig.Default.topics.copy(clusterProfiles = Some(settings)))
          (for {
            alerts <- AlertsWiring.standalone(config, telemetry, principals, logger, UrlPolicy.Dev)
            metrics <- MetricsWiring.standalone(config, telemetry, principals, logger, UrlPolicy.Dev)
          } yield List(alerts.capabilities, metrics.capabilities)).use { reports =>
            // Both stream subscribers must have received their starting snapshot before the first edit.
            await(source.subscribers.get)(_ == 2) *> exercise(source, reports)
          }
        }
      released <- source.releases.get
      _ = assertEquals(released, 2)
    } yield ()
  }
}
