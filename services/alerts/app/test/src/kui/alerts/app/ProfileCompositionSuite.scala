package kui.alerts.app

import scala.concurrent.duration.DurationInt

import cats.effect.{IO, Ref}
import cats.syntax.all.*

import kui.cluster.client.{ClusterProfile, ClusterProfiles, ProfileChange, ProfileClientHealth}
import kui.config.KuiConfig
import kui.kernel.ClusterId
import kui.kernel.cluster.{
  AdminTuning,
  BootstrapServers,
  ClientProperties,
  ClusterConnection,
  ClusterSecurity
}
import kui.observability.Telemetry
import kui.security.PrincipalCodec
import kui.testkit.KuiIOSuite
import kui.testkit.fakes.FakeStructuredLogger

final class ProfileCompositionSuite extends KuiIOSuite {
  private val id = ClusterId.unsafe("dynamic")
  private val original = ClusterProfile(
    id,
    "Initial",
    false,
    ClusterConnection(
      id,
      BootstrapServers.unsafe("127.0.0.1:1"),
      ClusterSecurity.Plaintext,
      ClientProperties.empty,
      AdminTuning.default.copy(requestTimeout = 50.millis, apiTimeout = 100.millis)
    ),
    1L
  )

  private def await[A](read: IO[A])(accept: A => Boolean): IO[A] =
    read
      .flatMap(value => if accept(value) then IO.pure(value) else IO.cede *> await(read)(accept))
      .timeout(5.seconds)

  test("production profile attachment emits current, replaces, adds, removes and unsubscribes") {
    for {
      current <- Ref.of[IO, Map[ClusterId, ClusterProfile]](Map(id -> original))
      handler <- Ref.of[IO, Option[ProfileChange => IO[Unit]]](None)
      removed <- Ref.of[IO, Int](0)
      profiles = new ClusterProfiles[IO] {
        def all = current.get
        def get(cluster: ClusterId) = current.get.map(_.get(cluster))
        def health = IO.pure(ProfileClientHealth.initial)
        def onChange(callback: ProfileChange => IO[Unit]): IO[IO[Unit]] =
          handler.set(Some(callback)).as(handler.set(None) *> removed.update(_ + 1))
      }
      logger <- FakeStructuredLogger[IO]
      _ <- AlertsWiring
        .fromProfiles[IO](
          profiles,
          KuiConfig.Default,
          Telemetry.noop[IO],
          PrincipalCodec.inProcess[IO],
          logger
        )
        .use { service =>
          def change(next: Map[ClusterId, ClusterProfile], event: ProfileChange): IO[Unit] =
            current.set(next) *> handler.get.flatMap(_.traverse_(_(event)))
          for {
            _ <- await(handler.get)(_.nonEmpty)
            initial <- await(service.capabilities)(_.clusters.contains(id))
            _ = assertEquals(initial.clusters(id).name, Some("Initial"))
            updated = original.copy(
              name = "Changed",
              readOnly = true,
              connection =
                original.connection.copy(bootstrapServers = BootstrapServers.unsafe("127.0.0.1:2")),
              version = 2L
            )
            _ <- change(Map(id -> updated), ProfileChange.Updated(id, Some(1L), 2L))
            next <- await(service.capabilities)(_.clusters.get(id).exists(_.name.contains("Changed")))
            _ = assertEquals(next.clusters(id).name, Some("Changed"))
            added = original.copy(
              id = ClusterId.unsafe("added"),
              connection = original.connection.copy(id = ClusterId.unsafe("added"))
            )
            _ <- change(Map(id -> updated, added.id -> added), ProfileChange.Updated(added.id, None, 1L))
            _ <- await(service.capabilities)(_.clusters.keySet == Set(id, added.id))
            _ <- change(Map.empty, ProfileChange.Removed(id))
            _ <- await(service.capabilities)(_.clusters.isEmpty)
          } yield ()
        }
      subscribed <- handler.get
      count <- removed.get
      _ = assertEquals(subscribed.isEmpty, true)
      _ = assertEquals(count, 1)
    } yield ()
  }
}
