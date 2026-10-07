package kui.alerts.app

import java.time.Instant

import cats.data.NonEmptyList
import cats.effect.{Deferred, IO}
import fs2.Stream

import kui.config.{AlertsConfig, ClusterConfig, StoreConfig}
import kui.kernel.cluster.{AdminTuning, BootstrapServers, ClientProperties, ClusterSecurity}
import kui.kernel.{ClusterId, Secret}
import kui.observability.Telemetry
import kui.security.rbac.RbacPolicy
import kui.security.{JwsPrincipalCodec, SigningKey}
import kui.testkit.KuiIOSuite
import kui.testkit.fakes.FakeStructuredLogger

final class DynamicAlertsWiringSuite extends KuiIOSuite {
  test("the wired alerts profile and capabilities follow additions, read-only edits and removals") {
    val cluster = ClusterConfig(
      ClusterId.unsafe("dynamic"),
      "Dynamic",
      BootstrapServers.unsafe("127.0.0.1:1"),
      ClusterSecurity.Plaintext,
      ClientProperties.empty,
      false,
      AdminTuning.default
    )
    val key = SigningKey("test", Secret(Array.fill[Byte](32)(7)), Instant.parse("2020-01-01T00:00:00Z"))
    val codec = JwsPrincipalCodec.make[IO](NonEmptyList.one(key), "kui-gateway").toOption.get
    for {
      added <- Deferred[IO, Unit]
      change <- Deferred[IO, Unit]
      changed <- Deferred[IO, Unit]
      remove <- Deferred[IO, Unit]
      removed <- Deferred[IO, Unit]
      logger <- FakeStructuredLogger[IO]
      changes = Stream.emit(List(cluster)) ++ Stream.exec(added.complete(()).void *> change.get) ++
        Stream.emit(List(cluster.copy(name = "renamed", readOnly = true))) ++
        Stream.exec(changed.complete(()).void *> remove.get) ++ Stream.emit(Nil) ++
        Stream.exec(removed.complete(()).void) ++ Stream.never[IO]
      _ <- AlertsWiring
        .make[IO](
          Nil,
          AlertsConfig.Default,
          StoreConfig.Default,
          RbacPolicy.Disabled,
          Telemetry.noop[IO],
          codec,
          logger,
          Some(changes)
        )
        .use { service =>
          for {
            _ <- added.get
            first <- service.capabilities
            _ = assertEquals(first.clusters.keySet, Set(cluster.id))
            _ = assert(first.clusters(cluster.id).features.contains("alerts.acknowledge"))
            _ <- change.complete(())
            _ <- changed.get
            next <- service.capabilities
            _ = assertEquals(next.clusters(cluster.id).name, Some("renamed"))
            _ = assert(!next.clusters(cluster.id).features.contains("alerts.acknowledge"))
            _ <- remove.complete(())
            _ <- removed.get
            last <- service.capabilities
            _ = assertEquals(last.clusters.size, 0)
          } yield ()
        }
    } yield ()
  }
}
