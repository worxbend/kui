package kui.cluster.infrastructure.store

import java.time.Instant

import scala.concurrent.duration.*

import cats.effect.{IO, Ref}

import kui.cluster.domain.{ClusterProfile, ProfileVersion, StoreHealth as DomainHealth}
import kui.cluster.infrastructure.TestProfiles
import kui.config.store.{StoreChange, StoreHealth as ConfigHealth, StoreRecord}
import kui.kernel.ClusterId
import kui.testkit.KuiIOSuite
import kui.testkit.fakes.FakeStructuredLogger

/** Store tail to registry reload, and the four ways it can be subtly wrong: reloading on its own writes,
  * walking a version backwards after a reconnect, losing an emission to one failure, and dying quietly.
  */
final class ProfileChangeListenerSuite extends KuiIOSuite {

  private val at = Instant.parse("2026-09-04T10:15:00Z")

  private def profile(id: String, version: Long): ClusterProfile =
    TestProfiles.profile(id = id, version = version)

  // ------------------------------------------------------------------ the pure part

  test("aClusterNobodyHasSeenIsAnAddition") {
    val (known, events) = ProfileChangeListener.diff(Map.empty, List(profile("prod", 3L)), at)

    assertEquals(events.map(e => (e.clusterId.value, e.kind)), List(("prod", ProfileChanged.Kind.Added)))
    assertEquals(known.get(ClusterId.unsafe("prod")).map(_.value), Some(3L))
  }

  test("aRecordAtAVersionAlreadyHeldIsANoOp") {
    // The store's feed carries this process's own writes back to it — that is how a write is confirmed at
    // all. Reacting to them would reload the registry once per write for no reason, and the first emission
    // after a subscribe, which repeats everything, would reload every cluster in the deployment.
    val known = Map(ClusterId.unsafe("prod") -> ProfileVersion.unsafe(3L))
    val (after, events) = ProfileChangeListener.diff(known, List(profile("prod", 3L)), at)

    assertEquals(events, Nil)
    assertEquals(after, known)
  }

  test("aReplayedOlderRecordNeverWalksTheVersionBackwards") {
    // A store reconnect replays records already applied. A version going backwards would make every
    // downstream service's comparison wrong in the silent direction: they would conclude nothing changed and
    // keep talking to the old brokers with the old credentials.
    val known = Map(ClusterId.unsafe("prod") -> ProfileVersion.unsafe(9L))
    val (after, events) = ProfileChangeListener.diff(known, List(profile("prod", 4L)), at)

    assertEquals(events, Nil)
    assertEquals(after.get(ClusterId.unsafe("prod")).map(_.value), Some(9L))
  }

  test("aClusterMissingFromACompleteListIsARemoval") {
    // Safe only because the store emits whole lists. With deltas, a removal missed during an outage would be
    // lost on that replica for ever, and it would keep serving a cluster that no longer exists.
    val known = Map(
      ClusterId.unsafe("prod") -> ProfileVersion.unsafe(1L),
      ClusterId.unsafe("staging") -> ProfileVersion.unsafe(1L)
    )
    val (after, events) = ProfileChangeListener.diff(known, List(profile("prod", 1L)), at)

    assertEquals(events.map(e => (e.clusterId.value, e.kind)), List(("staging", ProfileChanged.Kind.Removed)))
    assertEquals(after.keySet.map(_.value), Set("prod"))
  }

  test("anEmissionCanAddUpdateAndRemoveAtOnce") {
    val known = Map(
      ClusterId.unsafe("gone") -> ProfileVersion.unsafe(1L),
      ClusterId.unsafe("prod") -> ProfileVersion.unsafe(1L)
    )
    val (_, events) =
      ProfileChangeListener.diff(known, List(profile("prod", 2L), profile("new", 1L)), at)

    assertEquals(
      events.map(e => (e.clusterId.value, e.kind)).toSet,
      Set(
        ("prod", ProfileChanged.Kind.Updated),
        ("new", ProfileChanged.Kind.Added),
        ("gone", ProfileChanged.Kind.Removed)
      )
    )
  }

  // ------------------------------------------------------------------ the wired part

  private def listening[A](
      reconcile: List[ClusterProfile] => IO[Unit]
  )(use: (ProfileChangeListener[IO], StubConfigStore, ClusterConfigStoreAdapter.type) => IO[A]): IO[A] =
    for {
      store <- StubConfigStore()
      logger <- FakeStructuredLogger[IO]
      result <- ClusterConfigStoreAdapter
        .resource[IO](store, logger)
        .flatMap(adapter => ProfileChangeListener.resource[IO](adapter, reconcile, logger))
        .use(listener => use(listener, store, ClusterConfigStoreAdapter))
    } yield result

  private def recordFor(p: ClusterProfile, version: Long): StoreRecord =
    StoreRecord(
      1,
      ClusterConfigStoreAdapter.keyFor(p.id).toOption.get,
      version,
      StubConfigStore.At,
      "someone",
      deleted = false,
      ClusterRecordCodec.encode(p)
    )

  test("aRecordWrittenBeforeTheListenerSubscribesIsReconciled") {
    // The registry's first load and this listener's registration are not the same step. A cluster
    // write that lands in between is delivered to an empty handler map and is not replayed, so the
    // list is already different when the listener starts and no change event follows.
    for {
      store <- StubConfigStore()
      logger <- FakeStructuredLogger[IO]
      seen <- Ref.of[IO, List[List[String]]](Nil)
      known <- ClusterConfigStoreAdapter.resource[IO](store, logger).use { adapter =>
        for {
          _ <- store.hold(recordFor(profile("legacy", 4L), 4L))
          held <- ProfileChangeListener
            .resource[IO](
              adapter,
              profiles => seen.update(_ :+ profiles.map(_.id.value).sorted),
              logger
            )
            .use(_.known)
        } yield held
      }
      recorded <- seen.get
    } yield {
      assertEquals(recorded, List(List("legacy")))
      assertEquals(known.get(ClusterId.unsafe("legacy")).map(_.value), Some(4L))
    }
  }

  test("aStoreChangeReachesTheRegistryOnceAndIsPublished") {
    for {
      reconciles <- Ref.of[IO, Int](0)
      seen <- listening(_ => reconciles.update(_ + 1)) { (listener, store, adapter) =>
        for {
          _ <- store.hold(recordFor(profile("prod", 1L), 1L))
          _ <- store.push(StoreChange.Upserted(recordFor(profile("prod", 1L), 1L)))
          _ <- eventually(listener.known)(_.nonEmpty)
          // The same record again: the read-your-writes echo. It must change nothing.
          _ <- store.push(StoreChange.Upserted(recordFor(profile("prod", 1L), 1L)))
          _ <- IO.sleep(100.millis)
          known <- listener.known
          _ = assert(adapter.keyFor(ClusterId.unsafe("prod")).isRight)
        } yield known
      }
      count <- reconciles.get
    } yield {
      assertEquals(seen.get(ClusterId.unsafe("prod")).map(_.value), Some(1L))
      assertEquals(count, 1)
    }
  }

  test("aReconcileThatFailsIsRetriedAndTheEmissionIsNotLost") {
    // A store outage must degrade, not kill. A listener that gave up on the first failure would leave this
    // replica permanently behind, with no error anywhere after the first line.
    for {
      attempts <- Ref.of[IO, Int](0)
      reconcile = (_: List[ClusterProfile]) =>
        attempts.updateAndGet(_ + 1).flatMap { n =>
          if n < 2 then IO.raiseError(new RuntimeException("the registry was busy")) else IO.unit
        }
      known <- listening(reconcile) { (listener, store, _) =>
        for {
          _ <- store.hold(recordFor(profile("prod", 1L), 1L))
          _ <- store.push(StoreChange.Upserted(recordFor(profile("prod", 1L), 1L)))
          result <- eventually(listener.known)(_.nonEmpty)
          _ <- eventually(attempts.get)(_ >= 2)
        } yield result
      }
      tries <- attempts.get
    } yield {
      assertEquals(known.get(ClusterId.unsafe("prod")).map(_.value), Some(1L))
      assert(tries >= 2, s"the failed reconcile was retried, got $tries attempts")
    }
  }

  test("theListenerSurvivesAChangeItCannotDecode") {
    // The undecodable record is dropped by the store adapter, so the listener simply sees a list without it.
    // What must not happen is the feed stopping.
    for {
      reconciles <- Ref.of[IO, Int](0)
      known <- listening(_ => reconciles.update(_ + 1)) { (listener, store, _) =>
        val broken = recordFor(profile("broken", 1L), 1L).copy(payload = io.circe.Json.obj())

        for {
          _ <- store.hold(broken)
          _ <- store.push(StoreChange.Upserted(broken))
          _ <- IO.sleep(100.millis)
          _ <- store.hold(recordFor(profile("prod", 1L), 1L))
          _ <- store.push(StoreChange.Upserted(recordFor(profile("prod", 1L), 1L)))
          result <- eventually(listener.known)(_.nonEmpty)
        } yield result
      }
    } yield assertEquals(known.keySet.map(_.value), Set("prod"))
  }

  test("subscribersReceiveTheChangeAndNeverTheProfile") {
    // The event carries an id and a version and nothing else. A whole profile on this stream would carry
    // secrets to a subscriber entitled only to a redacted view.
    listening(_ => IO.unit) { (listener, store, _) =>
      for {
        seen <- Ref.of[IO, List[ProfileChanged]](Nil)
        watching <- listener.changes.evalMap(event => seen.update(event :: _)).compile.drain.start
        /*
         * Subscribing is not ordered with respect to pushing, and this test used to bridge the two with
         * `IO.sleep(50.millis)`. `changes` is `topic.subscribeUnbounded`, so the fibre above may still be
         * starting when the listener publishes, and a publication with no subscriber is dropped: the test
         * then waits out its five-second timeout on an event that was never delivered. That is the shape
         * that failed once under sixteen-way parallel load, and with the sleep at zero it fails three times
         * out of three on an idle machine. A budget is not an ordering. Pushing fresh clusters until one
         * arrives makes the subscription *observably* live before the change under test is pushed.
         */
        _ <- warmUntilSubscribed(store, seen)
        _ <- store.hold(recordFor(profile("prod", 1L), 1L))
        _ <- store.push(StoreChange.Upserted(recordFor(profile("prod", 1L), 1L)))
        _ <- eventually(seen.get)(_.exists(_.clusterId.value == "prod"))
        events <- seen.get.map(_.reverse.filter(_.clusterId.value == "prod"))
        _ <- watching.cancel
      } yield {
        assertEquals(events.map(_.clusterId.value), List("prod"))
        assertEquals(events.map(_.kind), List(ProfileChanged.Kind.Added))
      }
    }
  }

  test("theStoreGoingAwayAndComingBackIsLoggedOnceInEachDirection") {
    for {
      store <- StubConfigStore()
      logger <- FakeStructuredLogger[IO]
      _ <- ClusterConfigStoreAdapter
        .resource[IO](store, logger)
        .flatMap(adapter => ProfileChangeListener.resource[IO](adapter, _ => IO.unit, logger))
        .use { listener =>
          for {
            _ <- store.hold(recordFor(profile("prod", 1L), 1L))
            _ <- store.push(StoreChange.Upserted(recordFor(profile("prod", 1L), 1L)))
            _ <- eventually(listener.known)(_.nonEmpty)
            _ <- store.setHealth(ConfigHealth.Degraded("unreachable", StubConfigStore.At, 1L, Nil))
            _ <- store.hold(recordFor(profile("staging", 1L), 1L))
            _ <- store.push(StoreChange.Upserted(recordFor(profile("staging", 1L), 1L)))
            _ <- eventually(listener.known)(_.size == 2)
            _ <- store.setHealth(ConfigHealth.Healthy(2L, StubConfigStore.At, Nil))
            _ <- store.hold(recordFor(profile("third", 1L), 1L))
            _ <- store.push(StoreChange.Upserted(recordFor(profile("third", 1L), 1L)))
            _ <- eventually(listener.known)(_.size == 3)
          } yield ()
        }
      entries <- logger.entries
      messages = entries.map(_.message)
    } yield {
      // Once each, not once per emission: a line per change would bury the two transitions an operator is
      // actually looking for.
      assertEquals(messages.count(_.contains("has been degraded since")), 1, messages.toString)
      assertEquals(messages.count(_.contains("is available again")), 1, messages.toString)
    }
  }

  test("releasingTheResourceDeregistersTheHandler") {
    // A handler left registered on a store that outlives this resource would keep calling a reconcile whose
    // owner is gone — and, in a test, would keep a fiber alive after the suite finished.
    for {
      store <- StubConfigStore()
      logger <- FakeStructuredLogger[IO]
      reconciles <- Ref.of[IO, Int](0)
      adapterResource = ClusterConfigStoreAdapter.resource[IO](store, logger)
      _ <- adapterResource.use { adapter =>
        ProfileChangeListener
          .resource[IO](adapter, _ => reconciles.update(_ + 1), logger)
          .use { listener =>
            store.hold(recordFor(profile("prod", 1L), 1L)) *>
              store.push(StoreChange.Upserted(recordFor(profile("prod", 1L), 1L))) *>
              eventually(listener.known)(_.nonEmpty).void
          } *>
          // The listener is gone; the adapter is not. A further change must reach nobody.
          store.hold(recordFor(profile("staging", 1L), 1L)) *>
          store.push(StoreChange.Upserted(recordFor(profile("staging", 1L), 1L))) *>
          IO.sleep(150.millis)
      }
      count <- reconciles.get
    } yield assertEquals(count, 1)
  }

  test("theHealthProjectionTheListenerReadsIsTheDomainsOwn") {
    for {
      store <- StubConfigStore()
      logger <- FakeStructuredLogger[IO]
      health <- ClusterConfigStoreAdapter.resource[IO](store, logger).use { adapter =>
        store.setHealth(ConfigHealth.ReadOnly("running from files", Nil)) *> adapter.health
      }
    } yield assertEquals(health, DomainHealth.NotConfigured)
  }

  /** Pushes clusters nobody has seen until one comes back, which is the first moment the subscription in
    * `changes` is known to be registered. Each attempt uses a fresh id so that every push is a genuine
    * addition: re-pushing one the listener has already recorded publishes nothing at all, which is why
    * retrying the change under test would not have worked.
    */
  private def warmUntilSubscribed(store: StubConfigStore, seen: Ref[IO, List[ProfileChanged]]): IO[Unit] = {
    def attempt(n: Int): IO[Unit] = {
      val record = recordFor(profile(s"warm-$n", 1L), 1L)
      store.hold(record) *> store.push(StoreChange.Upserted(record)) *> IO.sleep(10.millis) *>
        seen.get.flatMap(observed => if observed.nonEmpty then IO.unit else attempt(n + 1))
    }

    attempt(1).timeout(5.seconds)
  }

  private def eventually[A](read: IO[A])(holds: A => Boolean): IO[A] =
    read
      .flatMap(value =>
        if holds(value) then IO.pure(value) else IO.sleep(10.millis) *> eventually(read)(holds)
      )
      .timeout(5.seconds)
}
