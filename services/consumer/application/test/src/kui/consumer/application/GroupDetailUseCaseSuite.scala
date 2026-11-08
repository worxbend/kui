package kui.consumer.application

import java.time.Instant

import scala.concurrent.duration.*

import cats.effect.testkit.TestControl
import cats.effect.{Deferred, IO}
import fs2.Stream

import kui.cache.{Snapshot, SnapshotCell}
import kui.consumer.domain.*
import kui.consumer.domain.fixtures.GroupFixtures
import kui.kernel.error.KuiError
import kui.kernel.group.GroupState
import kui.kernel.{ClusterId, GroupId, Offset, TopicPartition}
import kui.testkit.KuiIOSuite
import kui.testkit.fakes.FakeStructuredLogger

/** Coalesced detail reads share one describe. Cancelling the fiber that started it must not invent an
  * upstream status for the fibers still waiting on that describe.
  */
final class GroupDetailUseCaseSuite extends KuiIOSuite {

  private val group: ConsumerGroup = GroupFixtures.stableGroup

  test("a joiner receives the describe after the starter is cancelled, not a fabricated 502") {
    val program = for {
      release <- Deferred[IO, Unit]
      started <- Deferred[IO, Unit]
      logger <- FakeStructuredLogger[IO]
      useCase = GroupDetailUseCase.make[IO](snapshots, _ => blockingPort(started, release), lastSeen, logger)
      owner <- useCase.detail(ConsumerRig.Cluster, group.groupId).start
      _ <- started.get
      joiner <- useCase.detail(ConsumerRig.Cluster, group.groupId).start
      // Park the joiner on the in-flight describe before cancelling the starter. Under TestControl
      // every runnable fiber runs before virtual time moves, so this sleep is the joiner's turn.
      _ <- IO.sleep(1.second)
      _ <- owner.cancel
      // If cancellation completed the shared gate with a 502, the joiner is already done. A joiner
      // that is still waiting can still receive the describe once the latch opens.
      early <- joiner.joinWithNever.map(Some(_)).timeoutTo(1.second, IO.pure(None))
      _ <- release.complete(())
      result <- joiner.joinWithNever.timeout(10.seconds)
    } yield (early, result)

    TestControl.executeEmbed(program).map { (early, result) =>
      assertEquals(early, None, "the joiner finished before the describe was released")
      result match {
        case Right(view) => assertEquals(view.group.groupId, group.groupId)
        case Left(error) =>
          fail(s"joiner was told ${error.code.wire}: ${error.message}")
      }
    }
  }

  test("a describe that raises is not reported as kafka having answered 502") {
    val port = new SilentPort {
      override def describe(ids: List[GroupId]): IO[Either[KuiError, Map[GroupId, ConsumerGroup]]] =
        IO.raiseError(new RuntimeException("broker down"))
    }

    val program = for {
      logger <- FakeStructuredLogger[IO]
      useCase = GroupDetailUseCase.make[IO](snapshots, _ => port, lastSeen, logger)
      result <- useCase.detail(ConsumerRig.Cluster, group.groupId)
    } yield result

    TestControl.executeEmbed(program).map {
      case Left(error) =>
        assert(
          !error.message.contains("502"),
          s"a raised describe was reported as an upstream status: ${error.message}"
        )
        assert(!error.message.contains("answered with status"), error.message)
      case Right(_) => fail("a describe that raised was reported as a group")
    }
  }

  private def snapshots: GroupSnapshots[IO] =
    new GroupSnapshots[IO] {
      private val cell =
        new SnapshotCell[IO, GroupSnapshot] {
          private val snapshot = Snapshot.online(GroupSnapshot.empty(ConsumerRig.At), ConsumerRig.At)
          def get = IO.pure(snapshot)
          def refresh = IO.pure(snapshot)
          def invalidate = IO.pure(snapshot)
          def updates = Stream.empty
        }

      def of(cluster: ClusterId) = IO.pure(Some(cell))
      def all = IO.pure(Nil)
      def previousOf(cluster: ClusterId) = IO.pure(None)
      def requestRefresh(cluster: ClusterId) = IO.pure(false)
      def invalidate(cluster: ClusterId, reason: String) = IO.unit
    }

  private val lastSeen: LastSeenAssignments[IO] =
    new LastSeenAssignments[IO] {
      def put(cluster: ClusterId, group: GroupId, members: List[GroupMember], at: Instant) = IO.unit
      def get(cluster: ClusterId, group: GroupId) = IO.pure(None)
      def invalidate(cluster: ClusterId, group: GroupId) = IO.unit
    }

  private def blockingPort(started: Deferred[IO, Unit], release: Deferred[IO, Unit]): GroupAdminPort[IO] =
    new SilentPort {
      override def describe(ids: List[GroupId]): IO[Either[KuiError, Map[GroupId, ConsumerGroup]]] =
        started.complete(()) >> release.get.as(Right(Map(group.groupId -> group)))
    }

  private class SilentPort extends GroupAdminPort[IO] {
    def list(states: Set[GroupState]) = IO.pure(Right(GroupListingPage.complete(Nil)))
    def describe(ids: List[GroupId]) = IO.pure(Right(Map.empty[GroupId, ConsumerGroup]))
    def exists(id: GroupId) = IO.pure(Right(true))
    def offsetWindow(group: GroupId, scope: ResetScope, at: Option[Instant]) =
      IO.pure(Right(OffsetWindow.Empty))
    def applyOffsets(group: GroupId, offsets: Map[TopicPartition, Offset]) = IO.pure(Right(()))
    def deleteOffsets(group: GroupId, partitions: Set[TopicPartition]) = IO.pure(Right(()))
    def deleteGroup(id: GroupId) = IO.pure(Right(()))
  }
}
