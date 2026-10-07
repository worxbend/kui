package kui.allinone

import scala.concurrent.duration.DurationInt

import cats.effect.IO
import cats.syntax.all.*

import kui.cluster.app.ClusterServiceConfig
import kui.config.*
import kui.http.health.HealthEndpoints
import kui.kernel.cluster.{AdminTuning, BootstrapServers, ClientProperties, ClusterSecurity}
import kui.kernel.{ClusterId, ServiceId}
import kui.observability.Telemetry
import kui.security.PrincipalCodec
import kui.security.rbac.RbacPolicy
import kui.testkit.KuiIOSuite
import kui.testkit.fakes.FakeStructuredLogger

final class ProfileCompositionSuite extends KuiIOSuite {
  test("all-in-one collectors use the cluster service's resolved snapshot, not the bootstrap list") {
    val cluster = ClusterConfig(
      ClusterId.unsafe("authoritative"),
      "Authoritative",
      BootstrapServers.unsafe("127.0.0.1:1"),
      ClusterSecurity.Plaintext,
      ClientProperties.empty,
      false,
      AdminTuning.default.copy(requestTimeout = 50.millis, apiTimeout = 100.millis)
    )
    for {
      logger <- FakeStructuredLogger[IO]
      _ <- AllInOneWiring
        .services[IO](
          ClusterServiceConfig.Default.copy(clusters = List(cluster)),
          Nil,
          TopicsConfig.Default,
          ConsumersConfig.Default,
          StreamingConfig.Default,
          AuthConfig.Default,
          RbacPolicy.Disabled,
          StoreConfig.Default,
          MetricsConfig.Default,
          AlertsConfig.Default,
          Telemetry.noop[IO],
          PrincipalCodec.inProcess[IO],
          logger
        )
        .use { clients =>
          List("alerts", "metrics").traverse_ { name =>
            val client = clients.get(ServiceId.unsafe(name)).getOrElse(fail(s"missing $name"))
            client.callPublic(HealthEndpoints.capabilities, ())(AllInOneFixture.context).map { result =>
              val document = result.toOption.getOrElse(fail(s"$name capabilities failed"))
              assertEquals(document.clusters.keySet, Set(cluster.id))
            }
          }
        }
    } yield ()
  }
}
