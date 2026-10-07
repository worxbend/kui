package kui.consumer.application

import java.time.Instant

import cats.effect.{Deferred, IO, Ref}
import fs2.Stream

import kui.consumer.domain.*
import kui.kernel.error.KuiError
import kui.kernel.group.GroupState
import kui.kernel.{ClusterId, GroupId, Offset, TopicPartition}
import kui.testkit.KuiIOSuite

final class GroupSnapshotLifecycleSuite extends KuiIOSuite {
  test("shutdown joins profile reconciliation before draining refresh resources") {
    for {
      events <- Ref.of[IO, List[String]](Nil)
      refreshing <- Deferred[IO, Unit]
      reconciling <- Deferred[IO, Unit]
      reads <- Ref.of[IO, Int](0)
      profiles = new ClusterProfileSource[IO] {
        def profileOf(cluster: ClusterId) = IO.pure(Right(ConsumerRig.profileView()))
        def all = reads.getAndUpdate(_ + 1).flatMap {
          case 0 => IO.pure(List(ConsumerRig.profileView()))
          case _ =>
            (reconciling.complete(()) >> IO.never[List[ClusterProfileView]])
              .onCancel(events.update(_ :+ "reconciliation"))
        }
        def changes = Stream.emit(ConsumerRig.Cluster)
      }
      port = blockedPort(refreshing.complete(()).void, events.update(_ :+ "refresh"))
      _ <- ConsumerRig.snapshots(port, profiles).use(_ => refreshing.get >> reconciling.get)
      released <- events.get
    } yield assertEquals(released, List("reconciliation", "refresh"))
  }

  test("cancelling the owner cancels an in-flight refresh and profile subscription") {
    for {
      events <- Ref.of[IO, List[String]](Nil)
      refreshing <- Deferred[IO, Unit]
      subscribed <- Deferred[IO, Unit]
      profiles = new ClusterProfileSource[IO] {
        def profileOf(cluster: ClusterId) = IO.pure(Right(ConsumerRig.profileView()))
        def all = IO.pure(List(ConsumerRig.profileView()))
        def changes = (Stream.eval(subscribed.complete(())).drain ++ Stream.never[IO])
          .onFinalize(events.update(_ :+ "subscription"))
      }
      port = blockedPort(refreshing.complete(()).void, events.update(_ :+ "refresh"))
      owner <- ConsumerRig.snapshots(port, profiles).use(_ => IO.never).start
      _ <- refreshing.get >> subscribed.get
      _ <- owner.cancel
      released <- events.get
    } yield assertEquals(released, List("subscription", "refresh"))
  }

  private def blockedPort(started: IO[Unit], cancelled: IO[Unit]): GroupAdminPort[IO] =
    new GroupAdminPort[IO] {
      def list(states: Set[GroupState]): IO[Either[KuiError, GroupListingPage]] =
        (started >> IO.never[Either[KuiError, GroupListingPage]]).onCancel(cancelled)
      def describe(ids: List[GroupId]) = IO.pure(Right(Map.empty[GroupId, ConsumerGroup]))
      def exists(id: GroupId) = IO.pure(Right(true))
      def offsetWindow(group: GroupId, scope: ResetScope, at: Option[Instant]) =
        IO.pure(Right(OffsetWindow.Empty))
      def applyOffsets(group: GroupId, offsets: Map[TopicPartition, Offset]) = IO.pure(Right(()))
      def deleteOffsets(group: GroupId, partitions: Set[TopicPartition]) = IO.pure(Right(()))
      def deleteGroup(id: GroupId) = IO.pure(Right(()))
    }
}
