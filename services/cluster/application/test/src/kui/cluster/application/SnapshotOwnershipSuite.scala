package kui.cluster.application

import scala.concurrent.duration.*

import cats.effect.testkit.TestControl
import cats.effect.{Deferred, IO, Ref, Resource}
import cats.syntax.all.*

import kui.cluster.domain.ClusterProfileFixtures

final class SnapshotOwnershipSuite extends munit.CatsEffectSuite {
  test("cancellation between allocation and registration cannot orphan finalizers") {
    for {
      acquired <- Deferred[IO, Unit]
      proceed <- Deferred[IO, Unit]
      released <- Ref.of[IO, Int](0)
      owner <- Ref.of[IO, IO[Unit]](IO.unit)
      resource = Resource.make(IO.unit)(_ => released.update(_ + 1))
      fiber <- ClusterSnapshots
        .handoff(resource) { (_, release) =>
          acquired.complete(()) >> proceed.get >> owner.set(release)
        }
        .start
      _ <- acquired.get
      cancelling <- fiber.cancel.start
      _ <- IO.cede
      _ <- proceed.complete(())
      _ <- cancelling.joinWithNever
      _ <- owner.get.flatten
      count <- released.get
    } yield assertEquals(count, 1)
  }

  test("partial acquisition and failed registration unwind every acquired cell") {
    for {
      released <- Ref.of[IO, Int](0)
      cell = Resource.make(IO.unit)(_ => released.update(_ + 1))
      partial = cell *> Resource.eval(IO.raiseError[Unit](new RuntimeException("acquire failed")))
      first <- ClusterSnapshots.handoff(partial)((_, _) => IO.unit).attempt
      second <- ClusterSnapshots
        .handoff(cell)((_, _) => IO.raiseError(new RuntimeException("register failed")))
        .attempt
      count <- released.get
    } yield {
      assert(first.isLeft)
      assert(second.isLeft)
      assertEquals(count, 2)
    }
  }

  test("closing during dynamic replacement cancels reconciliation before draining its cells") {
    TestControl.executeEmbed(for {
      allocated <- ClusterRig.resource(Nil, delay = 1.hour).allocated
      (rig, close) = allocated
      profile = ClusterProfileFixtures.plaintext("prod", "Production")
      _ <- rig.store.setProfiles(List(profile))
      _ <- rig.registry.reload
      _ <- ClusterRig.eventually(rig.admin.calls)(_.nonEmpty)
      _ <- rig.store.setProfiles(List(ClusterProfileFixtures.at(profile, "replacement:9092")))
      _ <- rig.registry.reload
      _ <- close
      before <- rig.admin.calls
      _ <- IO.sleep(3.hours)
      after <- rig.admin.calls
      cells <- rig.snapshots.topologyOf(profile.id)
    } yield {
      assertEquals(after, before)
      assertEquals(cells, None)
    })
  }
}
