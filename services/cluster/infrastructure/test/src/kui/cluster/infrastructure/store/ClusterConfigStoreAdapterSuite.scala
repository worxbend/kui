package kui.cluster.infrastructure.store

import scala.concurrent.duration.*

import cats.effect.{IO, Ref}
import io.circe.Json

import kui.cluster.domain.{ClusterConfigStore, ClusterProfile, ProfileOrigin, ProfileVersion, StoreHealth}
import kui.cluster.infrastructure.TestProfiles
import kui.config.store.{StoreChange, StoreHealth as ConfigHealth, StoreKey, StoreRecord}
import kui.kernel.ClusterId
import kui.kernel.error.{ApplicationError, ErrorCode, InfrastructureError}
import kui.testkit.KuiIOSuite
import kui.testkit.fakes.FakeStructuredLogger

/** The cluster context's window onto the metadata store.
  *
  * Every case here is about a decision the adapter makes rather than about the store, which has its own suite
  * against a real broker: which key it writes, what a create looks like as opposed to an update, what happens
  * to one record it cannot read, and what the store's health means to a domain that must not see the store's
  * own types.
  */
final class ClusterConfigStoreAdapterSuite extends KuiIOSuite {

  private def adapter[A](use: (ClusterConfigStore[IO], StubConfigStore) => IO[A]): IO[A] =
    for {
      store <- StubConfigStore()
      logger <- FakeStructuredLogger[IO]
      result <- ClusterConfigStoreAdapter.resource[IO](store, logger).use(a => use(a, store))
    } yield result

  private def recordFor(profile: ClusterProfile, version: Long): StoreRecord =
    StoreRecord(
      envelopeVersion = 1,
      key = ClusterConfigStoreAdapter.keyFor(profile.id).toOption.get,
      version = version,
      updatedAt = StubConfigStore.At,
      updatedBy = "someone",
      deleted = false,
      payload = ClusterRecordCodec.encode(profile)
    )

  test("theKeyIsClusterSlashTheClusterId") {
    assertEquals(
      ClusterConfigStoreAdapter.keyFor(ClusterId.unsafe("prod-eu")).map(_.render),
      Right(StoreKey.cluster("prod-eu").toOption.get.render)
    )
    assertEquals(
      ClusterConfigStoreAdapter.clusterIdOf(StoreKey.cluster("prod-eu").toOption.get).map(_.value),
      Some("prod-eu")
    )
    // Everything outside this adapter's prefix is filtered out rather than misread as a cluster.
    assertEquals(ClusterConfigStoreAdapter.clusterIdOf(StoreKey.SettingsGlobal), None)
    assertEquals(ClusterConfigStoreAdapter.clusterIdOf(StoreKey.RbacRoles), None)
  }

  test("anEmptyStoreIsRightNilAndNotAFailure") {
    adapter((a, _) => a.list).assertEquals(Right(Nil))
  }

  test("listReturnsEveryStoredProfileInKeyOrder") {
    adapter { (a, store) =>
      for {
        _ <- store.hold(recordFor(TestProfiles.profile(id = "staging"), 3L))
        _ <- store.hold(recordFor(TestProfiles.profile(id = "prod"), 7L))
        listed <- a.list
      } yield listed match {
        case Left(error) => fail(s"list failed: ${error.code.wire}")
        case Right(profiles) =>
          assertEquals(profiles.map(_.id.value), List("prod", "staging"))
          // A stored record carries its own version and is marked as coming from the store; the registry
          // that overlays it on static configuration decides the final origin.
          assertEquals(profiles.map(_.version.value), List(7L, 3L))
          assertEquals(profiles.map(_.origin).distinct, List(ProfileOrigin.Stored))
      }
    }
  }

  test("oneMalformedRecordIsSkippedAndTheOthersStillLoad") {
    // A startup that dies on one bad row is an outage caused by a typo — and it locks the operator out of
    // the very screen that would let them fix it.
    adapter { (a, store) =>
      val broken = recordFor(TestProfiles.profile(id = "broken"), 1L)
        .copy(payload = Json.obj("displayName" -> Json.fromString("only half a record")))

      for {
        _ <- store.hold(recordFor(TestProfiles.profile(id = "prod"), 1L))
        _ <- store.hold(broken)
        listed <- a.list
      } yield assertEquals(listed.map(_.map(_.id.value)), Right(List("prod")))
    }
  }

  test("getDistinguishesAnAbsentKeyFromAnUnreadableOne") {
    adapter { (a, store) =>
      val broken = recordFor(TestProfiles.profile(id = "broken"), 1L).copy(payload = Json.obj())

      for {
        absent <- a.get(ClusterId.unsafe("nothing-here"))
        _ <- store.hold(broken)
        // Asked about *this* record by id, so a decode failure is a failure. Answering "there is no such
        // cluster" would send the caller off to create a duplicate.
        unreadable <- a.get(ClusterId.unsafe("broken"))
      } yield {
        assertEquals(absent, Right(None))
        assertEquals(unreadable.left.map(_.code), Left(ErrorCode.Validation))
      }
    }
  }

  test("aKeyTheStoreCouldNotDecryptIsNotAbsent") {
    // Decryption failure hides the record. Answering "no such cluster" sends the caller to create a
    // duplicate over a key the log still holds.
    adapter { (a, store) =>
      val key = ClusterConfigStoreAdapter.keyFor(ClusterId.unsafe("prod")).toOption.get
      for {
        _ <- store.setHealth(ConfigHealth.Healthy(4L, StubConfigStore.At, List(key)))
        result <- a.get(ClusterId.unsafe("prod"))
      } yield assertEquals(result.left.map(_.code), Left(ErrorCode.Validation))
    }
  }

  test("anUnreadableKeyIsLeftOutOfTheListAndTheOthersStillLoad") {
    adapter { (a, store) =>
      val prod = TestProfiles.profile(id = "prod")
      val key = ClusterConfigStoreAdapter.keyFor(prod.id).toOption.get
      for {
        _ <- store.hold(recordFor(prod, 3L))
        _ <- store.hold(recordFor(TestProfiles.profile(id = "staging"), 1L))
        _ <- store.setHealth(ConfigHealth.Healthy(4L, StubConfigStore.At, List(key)))
        listed <- a.list
      } yield assertEquals(listed.map(_.map(_.id.value)), Right(List("staging")))
    }
  }

  test("aCreateWritesWithNoBaseVersionAndAnUpdateWritesWithTheOneItRead") {
    // `ProfileVersion.Static` is zero and means "never stored". The store spells that as "this key must not
    // exist yet"; asking it to match version 0 instead would make every create look like a conflict.
    adapter { (a, store) =>
      for {
        _ <- a.put(TestProfiles.profile(id = "prod"), ProfileVersion.Static)
        _ <- a.put(TestProfiles.profile(id = "prod", version = 4L), ProfileVersion.unsafe(4L))
        writes <- store.writes.get
      } yield assertEquals(writes.map(_._2).reverse, List(None, Some(4L)))
    }
  }

  test("aSuccessfulWriteComesBackAtItsNewVersionAndAsStored") {
    adapter { (a, _) =>
      a.put(TestProfiles.profile(id = "prod"), ProfileVersion.Static).map {
        case Left(error) => fail(s"the write failed: ${error.code.wire}")
        case Right(profile) =>
          assertEquals(profile.version.value, 1L)
          assertEquals(profile.origin, ProfileOrigin.Stored)
      }
    }
  }

  test("aLostRaceComesBackAsTheVersionConflictTheApiAlreadyServes") {
    // The exit criterion names this code: the loser of a two-replica race sees exactly it. The store
    // classifies the failure; the adapter carries it through without reinterpreting it.
    adapter { (a, store) =>
      for {
        _ <- store.failWritesWith(
          ApplicationError.Remote(ErrorCode.ConfigVersionConflict, "stale version", Nil)
        )
        result <- a.put(TestProfiles.profile(), ProfileVersion.unsafe(2L))
      } yield assertEquals(result.left.map(_.code), Left(ErrorCode.ConfigVersionConflict))
    }
  }

  test("aWriteWithNoStoreIsRefusedAndNeverBuffered") {
    adapter { (a, store) =>
      for {
        _ <- store.failWritesWith(kui.config.store.ConfigStore.notConfigured)
        result <- a.put(TestProfiles.profile(), ProfileVersion.Static)
      } yield assertEquals(result.left.map(_.code), Left(ErrorCode.StoreNotConfigured))
    }
  }

  test("anUnreachableStoreRejectsAWriteRatherThanLosingIt") {
    // A queued configuration change that silently applies twenty minutes later is worse than a refusal.
    adapter { (a, store) =>
      for {
        _ <- store.failWritesWith(InfrastructureError.Unreachable("kui-store", "TimeoutException"))
        result <- a.put(TestProfiles.profile(), ProfileVersion.Static)
      } yield assertEquals(result.left.map(_.code), Left(ErrorCode.UpstreamUnavailable))
    }
  }

  test("healthIsProjectedOntoTheThreeCasesTheDomainReasonsAbout") {
    adapter { (a, store) =>
      for {
        online <- a.health
        _ <- store.setHealth(ConfigHealth.Degraded("unreachable", StubConfigStore.At, 1L, Nil))
        degraded <- a.health
        _ <- store.setHealth(ConfigHealth.ReadOnly("running from files", Nil))
        unconfigured <- a.health
      } yield {
        assertEquals(online, StoreHealth.Online)
        assertEquals(degraded, StoreHealth.Degraded("unreachable", StubConfigStore.At))
        assertEquals(unconfigured, StoreHealth.NotConfigured)
      }
    }
  }

  test("aHandlerSeesTheWholeListOnEveryClusterChange") {
    // Whole lists rather than deltas: a subscriber never has to reconcile against a separate `list` call,
    // and a change missed during an outage is picked up by the next emission rather than lost for ever.
    adapter { (a, store) =>
      for {
        seen <- Ref.of[IO, List[List[String]]](Nil)
        _ <- a.onChange(profiles => seen.update(profiles.map(_.id.value) :: _))
        _ <- store.hold(recordFor(TestProfiles.profile(id = "prod"), 1L))
        _ <- store.push(StoreChange.Upserted(recordFor(TestProfiles.profile(id = "prod"), 1L)))
        _ <- eventually(seen.get)(_.size == 1)
        _ <- store.hold(recordFor(TestProfiles.profile(id = "staging"), 1L))
        _ <- store.push(StoreChange.Upserted(recordFor(TestProfiles.profile(id = "staging"), 1L)))
        _ <- eventually(seen.get)(_.size == 2)
        emissions <- seen.get
      } yield assertEquals(emissions.reverse, List(List("prod"), List("prod", "staging")))
    }
  }

  test("aChangeToAnotherSectionWakesNobody") {
    // A settings or role write is not this adapter's business. Waking every cluster subscriber for one would
    // make an unrelated section's write rate the cluster registry's problem.
    adapter { (a, store) =>
      for {
        seen <- Ref.of[IO, Int](0)
        _ <- a.onChange(_ => seen.update(_ + 1))
        _ <- store.push(
          StoreChange.Upserted(
            StoreRecord(1, StoreKey.SettingsGlobal, 1L, StubConfigStore.At, "someone", false, Json.obj())
          )
        )
        _ <- store.push(StoreChange.Upserted(recordFor(TestProfiles.profile(id = "prod"), 1L)))
        _ <- eventually(seen.get)(_ == 1)
        count <- seen.get
      } yield assertEquals(count, 1)
    }
  }

  test("aDesynchronizedSubscriberAlwaysRereads") {
    // "You fell behind and lost changes" always concerns us: the view may be missing a removal, and only a
    // full re-read can tell. A subscriber that ignored it would show a cluster that is gone for the life of
    // the process.
    adapter { (a, store) =>
      for {
        seen <- Ref.of[IO, Int](0)
        _ <- a.onChange(_ => seen.update(_ + 1))
        _ <- store.push(StoreChange.Desynchronized(12L))
        _ <- eventually(seen.get)(_ == 1)
        count <- seen.get
      } yield assertEquals(count, 1)
    }
  }

  test("aDeregisteredHandlerStopsBeingCalledAndDeregisteringTwiceIsFine") {
    // Releasing a resource twice is ordinary; a deregistration that failed the second time would turn a
    // clean shutdown into a crash.
    adapter { (a, store) =>
      for {
        seen <- Ref.of[IO, Int](0)
        stop <- a.onChange(_ => seen.update(_ + 1))
        _ <- store.push(StoreChange.Upserted(recordFor(TestProfiles.profile(id = "prod"), 1L)))
        _ <- eventually(seen.get)(_ == 1)
        _ <- stop
        _ <- stop
        _ <- store.push(StoreChange.Upserted(recordFor(TestProfiles.profile(id = "staging"), 1L)))
        _ <- IO.sleep(150.millis)
        count <- seen.get
      } yield assertEquals(count, 1)
    }
  }

  test("aHandlerThatThrowsDoesNotTakeTheChangeFeedDown") {
    adapter { (a, store) =>
      for {
        seen <- Ref.of[IO, Int](0)
        _ <- a.onChange(_ => IO.raiseError(new RuntimeException("the handler exploded")))
        _ <- a.onChange(_ => seen.update(_ + 1))
        _ <- store.push(StoreChange.Upserted(recordFor(TestProfiles.profile(id = "prod"), 1L)))
        _ <- store.push(StoreChange.Upserted(recordFor(TestProfiles.profile(id = "staging"), 1L)))
        _ <- eventually(seen.get)(_ == 2)
        count <- seen.get
      } yield assertEquals(count, 2)
    }
  }

  /** Polls until the condition holds, so a test never depends on a fixed sleep being long enough. */
  private def eventually[A](read: IO[A])(holds: A => Boolean): IO[A] =
    read
      .flatMap(value =>
        if holds(value) then IO.pure(value) else IO.sleep(10.millis) *> eventually(read)(holds)
      )
      .timeout(5.seconds)
}
