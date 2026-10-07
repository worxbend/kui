package kui.cluster.infrastructure

import scala.concurrent.duration.*

import cats.effect.IO
import cats.effect.testkit.TestControl
import cats.syntax.all.*

import kui.testkit.KuiIOSuite
import kui.testkit.fakes.FakeStructuredLogger

/** The registry that keeps each cluster's admin client in step with the profile it was built from.
  *
  * The client lifecycle itself is `libs/kafka`'s `AdminClientPool` and is tested there. What is asserted here
  * is the one thing the pool cannot know: that a profile whose version moved is a different connection
  * wearing the same cluster id, and that its client is thrown away before anything talks through it again.
  */
final class ClusterAdminClientsSuite extends KuiIOSuite {

  private def registry[A](use: (ClusterAdminClients[IO], RecordingAdminPool) => IO[A]): IO[A] =
    for {
      pool <- RecordingAdminPool()
      logger <- FakeStructuredLogger[IO]
      result <- ClusterAdminClients.resource[IO](pool, logger).use(clients => use(clients, pool))
    } yield result

  test("generationSelectionAndUseAreAtomicAndOlderCallsCannotRestoreOldSettings") {
    TestControl.executeEmbed(registry { (clients, pool) =>
      for {
        entered <- cats.effect.Deferred[IO, Unit]
        resume <- cats.effect.Deferred[IO, Unit]
        old = TestProfiles.profile(version = 1L)
        latest = TestProfiles.profile(version = 2L, bootstrap = "new-broker:9092")
        first <- clients.withConnection(old)(_ => entered.complete(()) >> resume.get).start
        _ <- entered.get
        next <- clients.withConnection(latest)(c => IO.pure(c.bootstrapServers.value)).start
        _ <- IO.sleep(1.millisecond)
        before <- pool.events.get
        _ <- resume.complete(())
        _ <- first.joinWithNever
        selected <- next.joinWithNever
        delayed <- clients.withConnection(old)(c => IO.pure(c.bootstrapServers.value))
      } yield {
        assertEquals(before, Nil, "must not evict while an old generation is being acquired/used")
        assertEquals(selected, "new-broker:9092")
        assertEquals(delayed, "new-broker:9092")
      }
    })
  }

  test("closeWaitsForActiveGenerationAndCancelledWaiterCannotAcquire") {
    TestControl.executeEmbed(for {
      pool <- RecordingAdminPool()
      logger <- FakeStructuredLogger[IO]
      allocated <- ClusterAdminClients.resource[IO](pool, logger).allocated
      (clients, release) = allocated
      entered <- cats.effect.Deferred[IO, Unit]
      resume <- cats.effect.Deferred[IO, Unit]
      active <- clients.withConnection(TestProfiles.profile())(_ => entered.complete(()) >> resume.get).start
      _ <- entered.get
      waiter <- clients.withConnection(TestProfiles.profile(version = 2L))(_ => IO.unit).start
      _ <- IO.sleep(1.millisecond)
      _ <- waiter.cancel
      closing <- release.start
      _ <- IO.sleep(1.millisecond)
      before <- pool.events.get
      _ <- resume.complete(())
      _ <- active.joinWithNever
      _ <- closing.joinWithNever
      after <- pool.events.get
    } yield {
      assertEquals(before, Nil)
      assertEquals(after, List("evict:local"))
    })
  }

  test("sameVersionConnectionChangeStillRebuilds") {
    registry { (clients, pool) =>
      for {
        _ <- clients.withConnection(TestProfiles.profile())(_ => IO.unit)
        _ <- clients.withConnection(TestProfiles.profile(bootstrap = "replacement:9092"))(_ => IO.unit)
        events <- pool.events.get
      } yield assertEquals(events, List("evict:local"))
    }
  }

  test("authoritativeRegistryFencesRemovedAndRecreatedProfilesEvenWhenVersionRestarts") {
    val old = TestProfiles.profile(version = 8L)
    val replacement = TestProfiles.profile(version = 1L, bootstrap = "recreated:9092")
    for {
      current <- cats.effect.Ref.of[IO, Option[kui.cluster.domain.ClusterProfile]](Some(old))
      pool <- RecordingAdminPool()
      logger <- FakeStructuredLogger[IO]
      _ <- ClusterAdminClients.resource[IO](pool, logger, Some(_ => current.get)).use { clients =>
        for {
          _ <- clients.withConnection(old)(_ => IO.unit)
          _ <- current.set(None)
          removed <- clients.withConnection(old)(_ => IO.unit).attempt
          _ <- current.set(Some(replacement))
          selected <- clients.withConnection(old)(c => IO.pure(c.bootstrapServers.value))
        } yield {
          assert(removed.isLeft)
          assertEquals(selected, "recreated:9092")
        }
      }
    } yield ()
  }

  test("releasedRegistryCannotAcquireAgain") {
    for {
      pool <- RecordingAdminPool()
      logger <- FakeStructuredLogger[IO]
      pair <- ClusterAdminClients.resource[IO](pool, logger).allocated
      (clients, release) = pair
      _ <- clients.withConnection(TestProfiles.profile())(_ => IO.unit)
      _ <- release
      result <- clients.withConnection(TestProfiles.profile())(_ => IO.unit).attempt
    } yield assert(result.isLeft)
  }

  test("theFirstCallRegistersTheClusterAndEvictsNothing") {
    registry { (clients, pool) =>
      for {
        connection <- clients.connectionFor(TestProfiles.profile())
        open <- clients.openClients
        events <- pool.events.get
      } yield {
        assertEquals(connection.id.value, "local")
        assertEquals(open, 1)
        assertEquals(events, Nil)
      }
    }
  }

  test("tenConcurrentCallsForOneClusterEvictNothing") {
    // The pool's own per-cluster gate makes ten concurrent first calls create one client. What must not
    // happen here is ten *evictions*: an eviction storm on a cluster that is merely busy would rebuild the
    // client under every one of those calls.
    registry { (clients, pool) =>
      for {
        _ <- List.fill(10)(TestProfiles.profile()).parTraverse(clients.connectionFor)
        open <- clients.openClients
        events <- pool.events.get
      } yield {
        assertEquals(open, 1)
        assertEquals(events, Nil)
      }
    }
  }

  test("aNewerProfileVersionEvictsTheClient") {
    // The whole reason this component exists. Without it, a cluster whose bootstrap list or credentials were
    // edited in the metadata store would keep being served by the client built from the old ones.
    registry { (clients, pool) =>
      for {
        _ <- clients.connectionFor(TestProfiles.profile(version = 1L))
        _ <- clients.connectionFor(TestProfiles.profile(version = 2L, bootstrap = "broker-9:9092"))
        events <- pool.events.get
        open <- clients.openClients
      } yield {
        assertEquals(events, List("evict:local"))
        assertEquals(open, 1)
      }
    }
  }

  test("anOlderOrEqualProfileVersionDoesNotEvict") {
    // A replica that replays the log sees records it has already applied. Evicting on each of them would
    // reconnect every cluster on every store reconnect.
    registry { (clients, pool) =>
      for {
        _ <- clients.connectionFor(TestProfiles.profile(version = 5L))
        _ <- clients.connectionFor(TestProfiles.profile(version = 5L))
        _ <- clients.connectionFor(TestProfiles.profile(version = 3L))
        events <- pool.events.get
      } yield assertEquals(events, Nil)
    }
  }

  test("everyClusterIsTrackedSeparately") {
    registry { (clients, pool) =>
      for {
        _ <- clients.connectionFor(TestProfiles.profile(id = "prod"))
        _ <- clients.connectionFor(TestProfiles.profile(id = "staging"))
        _ <- clients.connectionFor(TestProfiles.profile(id = "prod", version = 2L))
        open <- clients.openClients
        events <- pool.events.get
      } yield {
        assertEquals(open, 2)
        // Only the cluster whose profile moved is rebuilt. A dead or edited cluster must never cost a
        // healthy one its connection.
        assertEquals(events, List("evict:prod"))
      }
    }
  }

  test("invalidateAsksThePoolToRebuildAndKeepsTheRegistration") {
    registry { (clients, pool) =>
      for {
        _ <- clients.connectionFor(TestProfiles.profile())
        _ <- clients.invalidate(kui.kernel.ClusterId.unsafe("local"))
        events <- pool.events.get
        open <- clients.openClients
      } yield {
        assertEquals(events, List("invalidate:local"))
        // The cluster is still configured; only its socket was thrown away. Forgetting the version here
        // would make the next call look like a profile change and evict a client that was just rebuilt.
        assertEquals(open, 1)
      }
    }
  }

  test("releasingTheResourceEvictsEveryRegisteredCluster") {
    for {
      pool <- RecordingAdminPool()
      logger <- FakeStructuredLogger[IO]
      _ <- ClusterAdminClients
        .resource[IO](pool, logger)
        .use { clients =>
          clients.connectionFor(TestProfiles.profile(id = "prod")) *>
            clients.connectionFor(TestProfiles.profile(id = "staging")).void
        }
      events <- pool.events.get
    } yield assertEquals(events.sorted, List("evict:prod", "evict:staging"))
  }

  test("aCancelledConnectionForLeavesTheRegistryAndThePoolInStep") {
    // The `Ref` update and the eviction are one uncancelable step. If a cancellation could land between
    // them, the registry would believe the client matches the profile while the pool still held the one
    // built from the old credentials — a stale connection nothing would ever evict again.
    registry { (clients, pool) =>
      for {
        _ <- clients.connectionFor(TestProfiles.profile(version = 1L))
        fiber <- clients.connectionFor(TestProfiles.profile(version = 2L)).start
        _ <- fiber.cancel
        events <- pool.events.get
        // Whatever the cancellation did, the two must agree: either nothing moved, or the version moved and
        // the old client is gone.
        open <- clients.openClients
        _ <- clients.connectionFor(TestProfiles.profile(version = 2L))
        after <- pool.events.get
      } yield {
        assertEquals(open, 1)
        assert(events.size <= 1, s"at most one eviction can have happened, got $events")
        assertEquals(
          after,
          List("evict:local"),
          s"exactly one eviction for the move to version 2, got $after"
        )
      }
    }
  }
}
