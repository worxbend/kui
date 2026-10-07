package kui.cluster.infrastructure

import scala.concurrent.duration.*

import cats.effect.IO
import cats.effect.testkit.TestControl

import kui.cluster.domain.Connectivity
import kui.kernel.error.{ApplicationError, InfrastructureError}
import kui.testkit.KuiIOSuite
import kui.testkit.fakes.FakeStructuredLogger

/** The cheapest question KUI asks a cluster, and the three answers it is allowed to give.
  *
  * Two of these cases are about what the probe must *not* do: take longer than its own bound, and put a
  * broker's words on a screen.
  */
final class ConnectivityProbeAdapterSuite extends KuiIOSuite {

  private val profile = TestProfiles.profile()

  private def probeWith(stub: IO[StubKafkaClusterAdmin]): IO[Connectivity] =
    for {
      admin <- stub
      logger <- FakeStructuredLogger[IO]
      verdict <- new ConnectivityProbeAdapter[IO](cats.effect.Resource.pure(admin), logger).probe(profile)
    } yield verdict

  test("aHealthyClusterIsReachable") {
    probeWith(StubKafkaClusterAdmin()).assertEquals(Connectivity.Reachable)
  }

  test("rejectedCredentialsAreAuthenticationFailedAndNotUnreachable") {
    // The distinction is the whole value of the probe. "Unavailable" alone sends an operator to the logs;
    // "the cluster rejected KUI's credentials" sends them to the configuration.
    probeWith(StubKafkaClusterAdmin(describeCluster = Left(InfrastructureError.AuthFailed("kafka:local"))))
      .assertEquals(Connectivity.AuthenticationFailed(ConnectivityProbeAdapter.CredentialsRejected))
  }

  test("aRefusedRequestIsNotReportedAsAnUnreachableCluster") {
    // The cluster answered. Reporting it as unreachable would send an operator to the network when the
    // answer is an ACL, and would grey out a row that is in fact working.
    probeWith(StubKafkaClusterAdmin(describeCluster = Left(ApplicationError.Forbidden("no DESCRIBE"))))
      .assertEquals(Connectivity.AuthenticationFailed(ConnectivityProbeAdapter.NotAuthorized))
  }

  test("anUnreachableClusterIsUnreachable") {
    probeWith(
      StubKafkaClusterAdmin(describeCluster =
        Left(InfrastructureError.Unreachable("kafka:local", "TimeoutException"))
      )
    ).assertEquals(Connectivity.Unreachable(ConnectivityProbeAdapter.CouldNotConnect))
  }

  test("theProbeGivesUpOnItsOwnBoundAndNotTheAdminClients") {
    // The admin client's `default.api.timeout.ms` is a minute. A probe that inherited it would make the
    // dashboard exactly as slow as the dead cluster, which is the failure the milestone's exit criterion
    // forbids. Asserted with simulated time, so a regression that lets the bound grow fails the test rather
    // than merely slowing it.
    val bound = ConnectivityProbeAdapter.timeoutFor(profile)

    TestControl
      .executeEmbed(
        probeWith(StubKafkaClusterAdmin(describeClusterDelay = 10.minutes)).timed
      )
      .map { (elapsed, verdict) =>
        assertEquals(verdict, Connectivity.Unreachable(ConnectivityProbeAdapter.timedOutDetail(bound)))
        assertEquals(elapsed, bound)
      }
  }

  test("theBoundIsFiveSecondsOrTheClustersOwnRequestTimeoutWhicheverIsSmaller") {
    assertEquals(ConnectivityProbeAdapter.timeoutFor(profile), 5.seconds)
    assertEquals(ConnectivityProbeAdapter.DefaultProbeTimeout, 5.seconds)
  }

  test("noVerdictEverCarriesAHostAPasswordOrAnExceptionMessage") {
    // `detail` is display text drawn from a fixed set of sentences with at most one substitution — the
    // bound, which is KUI's own number. Nothing derived from a Kafka exception reaches it: a Kafka message
    // routinely carries the bootstrap string and, on some SASL paths, the principal.
    val leaky = Left(InfrastructureError.Unreachable("kafka:local", "broker-secret.internal:9092 refused"))

    probeWith(StubKafkaClusterAdmin(describeCluster = leaky)).map {
      case Connectivity.Reachable => fail("expected a failure verdict")
      case Connectivity.AuthenticationFailed(detail) => assertNoLeak(detail)
      case Connectivity.Unreachable(detail) => assertNoLeak(detail)
    }
  }

  private def assertNoLeak(detail: String): Unit = {
    assert(!detail.contains("broker-secret"), s"the verdict leaked a host: $detail")
    assert(!detail.contains("9092"), s"the verdict leaked a port: $detail")
    assert(!detail.contains("local"), s"the verdict leaked the cluster's connection details: $detail")
  }

  test("aRaisedExceptionIsAVerdictAndNotACrash") {
    // A dashboard row must never be able to take the page down. An exception here is a defect in
    // `libs/kafka`, and it still has to come back as an answer rather than as a failed effect.
    val exploding = new kui.kafka.admin.ClusterAdmin[IO] {
      def describeCluster(c: kui.kernel.cluster.ClusterConnection) =
        IO.raiseError(new IllegalStateException("bootstrap=broker-secret.internal:9092"))
      def version(c: kui.kernel.cluster.ClusterConnection) = IO.pure(Right(KafkaFixtures.version))
      def describeQuorum(c: kui.kernel.cluster.ClusterConnection) = IO.pure(Right(None))
      def brokerConfigs(c: kui.kernel.cluster.ClusterConnection, b: kui.kernel.BrokerId, d: Boolean) =
        IO.pure(Right(Nil))
      def describeLogDirs(c: kui.kernel.cluster.ClusterConnection, b: Set[kui.kernel.BrokerId]) =
        IO.pure(Right(kui.kafka.BatchResult(Map.empty, Map.empty)))
      def capabilities(c: kui.kernel.cluster.ClusterConnection) = IO.pure(KafkaFixtures.features)
    }

    for {
      logger <- FakeStructuredLogger[IO]
      verdict <- new ConnectivityProbeAdapter[IO](cats.effect.Resource.pure(exploding), logger).probe(profile)
    } yield {
      assertEquals(verdict, Connectivity.Unreachable(ConnectivityProbeAdapter.CouldNotConnect))
      assertNoLeak(ConnectivityProbeAdapter.CouldNotConnect)
    }
  }
}
