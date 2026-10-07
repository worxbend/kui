package kui.cluster.infrastructure

import scala.concurrent.duration.*

import cats.effect.testkit.TestControl
import cats.effect.{Deferred, IO, Ref, Resource}

import kui.cluster.domain.Connectivity
import kui.kafka.{admin as adm, AdminClientPool, AdminMetrics}
import kui.kernel.cluster.ClusterConnection
import kui.testkit.fakes.FakeStructuredLogger

final class IsolatedProbeSuite extends munit.CatsEffectSuite {
  private def probeAdmin(pool: AdminClientPool[IO], awaitResult: IO[Unit]): adm.ClusterAdmin[IO] =
    new adm.ClusterAdmin[IO] {
      def describeCluster(c: ClusterConnection) =
        pool.run(c, "describeCluster")(_ => awaitResult.as(Right(KafkaFixtures.description)))
      def version(c: ClusterConnection) = IO.pure(Right(KafkaFixtures.version))
      def describeQuorum(c: ClusterConnection) = IO.pure(Right(None))
      def brokerConfigs(c: ClusterConnection, b: kui.kernel.BrokerId, d: Boolean) = IO.pure(Right(Nil))
      def describeLogDirs(c: ClusterConnection, b: Set[kui.kernel.BrokerId]) =
        IO.pure(Right(kui.kafka.BatchResult(Map.empty, Map.empty)))
      def capabilities(c: ClusterConnection) = IO.pure(KafkaFixtures.features)
    }

  private def pool(opened: Ref[IO, List[String]], closed: Ref[IO, List[String]]) =
    AdminClientPool.resourceWith[IO](
      AdminMetrics.noop[IO],
      (connection, _, _) =>
        Resource.make(
          opened.update(_ :+ connection.bootstrapServers.value).as(StubAdmin(PartialFunction.empty))
        )(_ => closed.update(_ :+ connection.bootstrapServers.value))
    )

  test("replacement probe opens submitted settings without touching production and closes every probe") {
    for {
      opened <- Ref.of[IO, List[String]](Nil)
      closed <- Ref.of[IO, List[String]](Nil)
      logger <- FakeStructuredLogger[IO]
      _ <- pool(opened, closed).use { production =>
        for {
          _ <- production.run(TestProfiles.profile().connection, "describeCluster")(_ => IO.unit)
          probe = new ConnectivityProbeAdapter[IO](pool(opened, closed).map(probeAdmin(_, IO.unit)), logger)
          result <- probe.probe(TestProfiles.profile(bootstrap = "replacement:9092"))
          again <- probe.probe(TestProfiles.profile(bootstrap = "replacement:9092"))
          opens <- opened.get
          closes <- closed.get
        } yield {
          assertEquals(result, Connectivity.Reachable)
          assertEquals(again, Connectivity.Reachable)
          assertEquals(opens.takeRight(2), List("replacement:9092", "replacement:9092"))
          assertEquals(closes, List("replacement:9092", "replacement:9092"))
        }
      }
      opens <- opened.get
      closes <- closed.get
    } yield assertEquals(closes.sorted, opens.sorted)
  }

  test("cancelled and timed out probes release their isolated pool") {
    TestControl.executeEmbed(for {
      opened <- Ref.of[IO, List[String]](Nil)
      closed <- Ref.of[IO, List[String]](Nil)
      entered <- Deferred[IO, Unit]
      logger <- FakeStructuredLogger[IO]
      probe = new ConnectivityProbeAdapter[IO](
        pool(opened, closed).map(probeAdmin(_, entered.complete(()).void >> IO.never)),
        logger
      )
      fiber <- probe.probe(TestProfiles.profile()).start
      _ <- entered.get
      _ <- fiber.cancel
      afterCancel <- closed.get
      result <- probe.probe(TestProfiles.profile())
      opens <- opened.get
      closes <- closed.get
    } yield {
      assertEquals(afterCancel.size, 1)
      assertEquals(result, Connectivity.Unreachable(ConnectivityProbeAdapter.timedOutDetail(5.seconds)))
      assertEquals(closes, opens)
    })
  }
}
