package kui.cluster.client

import scala.concurrent.duration.*

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.testkit.TestControl
import cats.syntax.all.*
import fs2.Stream
import munit.{CatsEffectSuite, ScalaCheckSuite}
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

import kui.cluster.client.ClusterClientFixture.*
import kui.cluster.contract.dto.ClusterChangeDto

/** What this client does over time, on a virtual clock.
  *
  * Every behaviour ADR-046 asks of it is a statement about *what happens after a delay* — the fallback poll,
  * the reconnect backoff, the sticky failure instant, and above all what is still running after the resource
  * is released. `TestControl` runs those in microseconds and, more importantly, makes them deterministic: a
  * suite that slept through a sixty-second poll interval would be slow and flaky at the same time.
  */
final class HttpClusterProfilesSuite extends CatsEffectSuite {

  private val fastConfig = ClusterProfilesConfig(
    pollInterval = 60.seconds,
    requestTimeout = 5.seconds,
    reconnectBackoff = 1.second,
    maxReconnectBackoff = 30.seconds
  )

  /** A stream that stays open and silent: a healthy connection with nothing to say. */
  private val silent: Option[Stream[IO, Byte]] = Some(Stream.never[IO])

  // ---------------------------------------------------------------------------------------------
  // Start-up
  // ---------------------------------------------------------------------------------------------

  List(ClusterChangeDto.Updated, ClusterChangeDto.Removed).foreach { kind =>
    test(s"delayedPollCannotOverwriteSse-$kind") {
      TestControl.executeEmbed(for {
        entered <- cats.effect.Deferred[IO, Unit]
        release <- cats.effect.Deferred[IO, Unit]
        send <- cats.effect.Deferred[IO, Unit]
        stream = Stream.eval(send.get).drain ++ openStreamOf(changeBytes(Prod, 3L, kind))
        fake <- fake(Behaviour.of(profile(Prod, 1L)).copy(events = Some(stream)))
        _ <- client(fake, fastConfig).use { (profiles, _) =>
          for {
            _ <- fake.update(
              _.copy(
                profiles = Map(Prod -> profile(Prod, 2L)),
                beforeProfileAnswer = entered.complete(()).void >> release.get
              )
            )
            _ <- IO.sleep(60.seconds)
            _ <- entered.get
            _ <- fake.update(
              _.copy(
                profiles =
                  if kind == ClusterChangeDto.Removed then Map.empty else Map(Prod -> profile(Prod, 3L)),
                beforeProfileAnswer = IO.unit
              )
            )
            _ <- send.complete(())
            _ <- IO.sleep(1.second)
            _ <- release.complete(())
            _ <- IO.sleep(1.second)
            current <- profiles.get(Prod)
          } yield assertEquals(
            current.map(_.version),
            if kind == ClusterChangeDto.Removed then None else Some(3L)
          )
        }
      } yield ())
    }

  }

  test("startFetchesTheListAndEachProfile") {
    TestControl.executeEmbed(
      for {
        fake <- fake(Behaviour.of(profile(Prod, 1L), profile(Staging, 4L)))
        result <- client(fake, fastConfig).use { (profiles, _) =>
          for {
            all <- profiles.all
            calls <- fake.recorded
          } yield (all, calls)
        }
      } yield {
        val (all, calls) = result
        assertEquals(all.keySet.map(_.value), Set("prod-eu", "staging"))
        assertEquals(all(Prod).version, 1L)
        assertEquals(all(Staging).version, 4L)
        // The connection is rebuilt, credentials and all, which is the whole point of the fetch.
        assertEquals(all(Prod).connection.security.securityProtocol, "SASL_SSL")

        val paths = calls.map(_.path)
        assert(paths.contains("/internal/v1/clusters"), paths.toString)
        assert(paths.contains("/internal/v1/clusters/prod-eu/profile"), paths.toString)
        assert(paths.contains("/internal/v1/clusters/staging/profile"), paths.toString)
        // The first fetch of a profile this client has never seen carries no ETag: there is nothing to
        // be conditional about, and sending a stale one would answer 304 with a body it does not have.
        assertEquals(calls.filter(_.path.endsWith("/profile")).flatMap(_.ifNoneMatch), Nil)
      }
    )
  }

  test("aStartupFailureLeavesTheClientAvailableAndDegraded") {
    // Not a failed `Resource`. A service that refuses to start because the cluster service is briefly
    // down turns one outage into two, and makes container boot order a correctness requirement.
    TestControl.executeEmbed(
      for {
        fake <- fake(Behaviour(Map.empty, listFails = true, events = None))
        result <- client(fake, fastConfig).use { (profiles, _) =>
          (profiles.all, profiles.health).tupled
        }
      } yield {
        val (all, health) = result
        assertEquals(all, Map.empty[kui.kernel.ClusterId, ClusterProfile])
        assert(health.lastError.isDefined, health.toString)
        assert(health.failingSince.isDefined, health.toString)
        assertEquals(health.lastSuccessAt, None)
        assertEquals(health.subscribed, false)
      }
    )
  }

  test("theSignedTokenIsMintedPerRequest") {
    // ADR-020 binds a token to one method and one path, so a client holding a single pre-minted
    // principal would be authorised for its first fetch and refused for every fetch after it. Asserted
    // by observing that two different paths carried two different tokens.
    TestControl.executeEmbed(
      for {
        fake <- fake(Behaviour.of(profile(Prod, 1L)))
        _ <- client(fake, fastConfig).use((_, _) => IO.unit)
        calls <- fake.recorded
      } yield assert(calls.map(_.path).distinct.sizeIs >= 2, calls.toString)
    )
  }

  // ---------------------------------------------------------------------------------------------
  // Change notification
  // ---------------------------------------------------------------------------------------------

  test("anUnchangedProfileAnswers304AndFiresNoChange") {
    TestControl.executeEmbed(
      for {
        fake <- fake(
          Behaviour.of(profile(Prod, 1L)).copy(events = Some(openStreamOf(changeBytes(Prod, 1L))))
        )
        changes <- Ref.of[IO, List[ProfileChange]](Nil)
        health <- client(fake, fastConfig).use { (profiles, _) =>
          profiles.onChange(change => changes.update(_ :+ change)) *> IO.sleep(1.second) *> profiles.health
        }
        seen <- changes.get
        calls <- fake.recorded
      } yield {
        // The stream said "version 1", which is the version already held, so the refetch is conditional
        // and answers 304 — and a 304 is not a change.
        assertEquals(seen, Nil)
        val conditional = calls.filter(_.path.endsWith("/profile")).flatMap(_.ifNoneMatch)
        assertEquals(conditional, List("\"1\""))
        assertEquals(health.lastError, None, "a 304 is a successful answer, not a failure")
        assertEquals(health.subscribed, true)
      }
    )
  }

  test("aVersionBumpOnTheStreamRefetchesExactlyThatCluster") {
    TestControl.executeEmbed(
      for {
        fake <- fake(Behaviour.of(profile(Prod, 1L), profile(Staging, 4L)))
        changes <- Ref.of[IO, List[ProfileChange]](Nil)
        result <- client(fake, fastConfig).use { (profiles, _) =>
          for {
            _ <- profiles.onChange(change => changes.update(_ :+ change))
            _ <- fake.reset
            // The operator edits prod-eu. The service bumps its version and says so on the stream.
            _ <- fake.update(current =>
              current.copy(
                profiles = current.profiles.updated(Prod, profile(Prod, 2L)),
                events = Some(openStreamOf(changeBytes(Prod, 2L)))
              )
            )
            // The subscription reconnects on its backoff and then reads the frame.
            _ <- IO.sleep(5.seconds)
            all <- profiles.all
            calls <- fake.recorded
          } yield (all, calls)
        }
        seen <- changes.get
      } yield {
        val (all, calls) = result
        assertEquals(all(Prod).version, 2L)
        assertEquals(all(Staging).version, 4L)
        assertEquals(seen, List(ProfileChange.Updated(Prod, Some(1L), 2L)))
        // Exactly that cluster, and exactly once: the other cluster's profile was not refetched.
        assertEquals(calls.count(_.path == "/internal/v1/clusters/prod-eu/profile"), 1)
        assertEquals(calls.count(_.path == "/internal/v1/clusters/staging/profile"), 0)
      }
    )
  }

  test("handlersThatThrowDoNotKillTheSubscription") {
    // A consumer's bad callback degrades that consumer, not this client and not the other handlers.
    TestControl.executeEmbed(
      for {
        fake <- fake(Behaviour.of(profile(Prod, 1L)))
        good <- Ref.of[IO, List[ProfileChange]](Nil)
        result <- client(fake, fastConfig).use { (profiles, _) =>
          for {
            _ <- profiles.onChange(_ => IO.raiseError(new RuntimeException("a bad consumer")))
            _ <- profiles.onChange(change => good.update(_ :+ change))
            _ <- fake.update(current =>
              current.copy(
                profiles = current.profiles.updated(Prod, profile(Prod, 2L)),
                events = Some(openStreamOf(changeBytes(Prod, 2L)))
              )
            )
            _ <- IO.sleep(5.seconds)
            health <- profiles.health
          } yield health
        }
        seen <- good.get
      } yield {
        assertEquals(seen, List(ProfileChange.Updated(Prod, Some(1L), 2L)))
        assertEquals(result.subscribed, true)
      }
    )
  }

  test("deregisteringAHandlerStopsIt") {
    TestControl.executeEmbed(
      for {
        fake <- fake(Behaviour.of(profile(Prod, 1L)))
        seen <- Ref.of[IO, List[ProfileChange]](Nil)
        _ <- client(fake, fastConfig).use { (profiles, _) =>
          for {
            stop <- profiles.onChange(change => seen.update(_ :+ change))
            _ <- stop
            _ <- fake.update(current =>
              current.copy(
                profiles = current.profiles.updated(Prod, profile(Prod, 2L)),
                events = Some(openStreamOf(changeBytes(Prod, 2L)))
              )
            )
            _ <- IO.sleep(5.seconds)
          } yield ()
        }
        changes <- seen.get
      } yield assertEquals(changes, Nil)
    )
  }

  // ---------------------------------------------------------------------------------------------
  // The fallback poll
  // ---------------------------------------------------------------------------------------------

  test("theFallbackPollCatchesAChangeTheStreamMissed") {
    // The case that matters most in practice: a middlebox drops an idle socket without telling either
    // end, so the stream looks exactly like a quiet cluster. The poll is what bounds the damage.
    TestControl.executeEmbed(
      for {
        fake <- fake(Behaviour.of(profile(Prod, 1L)).copy(events = silent))
        changes <- Ref.of[IO, List[ProfileChange]](Nil)
        result <- client(fake, fastConfig).use { (profiles, _) =>
          for {
            _ <- profiles.onChange(change => changes.update(_ :+ change))
            _ <- fake.update(current =>
              current.copy(profiles = current.profiles.updated(Prod, profile(Prod, 9L)))
            )
            // Nothing has been said on the stream, so nothing has happened yet.
            _ <- IO.sleep(30.seconds)
            before <- profiles.all
            _ <- IO.sleep(31.seconds)
            after <- profiles.all
          } yield (before, after)
        }
        seen <- changes.get
      } yield {
        val (before, after) = result
        assertEquals(before(Prod).version, 1L, "nothing may change before the poll interval elapses")
        assertEquals(after(Prod).version, 9L)
        assertEquals(seen, List(ProfileChange.Updated(Prod, Some(1L), 9L)))
      }
    )
  }

  test("aQuietPollCostsOne304PerClusterAndNothingElse") {
    TestControl.executeEmbed(
      for {
        fake <- fake(Behaviour.of(profile(Prod, 1L)).copy(events = silent))
        calls <- client(fake, fastConfig).use { (_, _) =>
          fake.reset *> IO.sleep(61.seconds) *> fake.recorded
        }
      } yield {
        assertEquals(calls.count(_.path == "/internal/v1/clusters"), 1)
        assertEquals(calls.filter(_.path.endsWith("/profile")).flatMap(_.ifNoneMatch), List("\"1\""))
      }
    )
  }

  // ---------------------------------------------------------------------------------------------
  // Removal
  // ---------------------------------------------------------------------------------------------

  test("aDelayedRemovalCannotEraseAnAlreadyCurrentNewerProfile") {
    TestControl.executeEmbed(for {
      send <- cats.effect.Deferred[IO, Unit]
      processed <- cats.effect.Deferred[IO, Unit]
      bytes = changeBytes(Prod, 1L, ClusterChangeDto.Removed).getBytes("UTF-8").toList
      stream = Stream.eval(send.get).drain ++ Stream.emits(bytes).covary[IO] ++
        Stream.eval(processed.complete(())).drain ++ Stream.never[IO]
      fake <- fake(Behaviour.of(profile(Prod, 2L)).copy(events = Some(stream)))
      changes <- Ref.of[IO, List[ProfileChange]](Nil)
      _ <- client(fake, fastConfig).use { (profiles, _) =>
        for {
          _ <- profiles.onChange(change => changes.update(_ :+ change))
          _ <- fake.reset
          _ <- send.complete(())
          _ <- processed.get
          current <- profiles.get(Prod)
          seen <- changes.get
          calls <- fake.recorded
        } yield {
          assertEquals(current.map(_.version), Some(2L))
          assertEquals(seen, Nil)
          assertEquals(calls.count(_.path == "/internal/v1/clusters"), 1)
        }
      }
    } yield ())
  }

  List(1L, 2L).foreach { previousVersion =>
    test(s"aDelayedRemovalRefetchesARecreatedVersionOneFromVersion-$previousVersion") {
      TestControl.executeEmbed(for {
        send <- cats.effect.Deferred[IO, Unit]
        processed <- cats.effect.Deferred[IO, Unit]
        bytes = changeBytes(Prod, previousVersion, ClusterChangeDto.Removed).getBytes("UTF-8").toList
        stream = Stream.eval(send.get).drain ++ Stream.emits(bytes).covary[IO] ++
          Stream.eval(processed.complete(())).drain ++ Stream.never[IO]
        fake <- fake(Behaviour.of(profile(Prod, previousVersion)).copy(events = Some(stream)))
        changes <- Ref.of[IO, List[ProfileChange]](Nil)
        _ <- client(fake, fastConfig).use { (profiles, _) =>
          for {
            _ <- profiles.onChange(change => changes.update(_ :+ change))
            _ <- fake.update(_.copy(profiles = Map(Prod -> profile(Prod, 1L).copy(readOnly = true))))
            _ <- fake.reset
            _ <- send.complete(())
            _ <- processed.get
            current <- profiles.get(Prod)
            seen <- changes.get
            calls <- fake.recorded
          } yield {
            assertEquals(current.map(_.version), Some(1L))
            assertEquals(current.map(_.readOnly), Some(true))
            assertEquals(seen, List(ProfileChange.Updated(Prod, Some(previousVersion), 1L)))
            assertEquals(calls.count(_.path.endsWith("/profile")), 1)
            assertEquals(calls.filter(_.path.endsWith("/profile")).flatMap(_.ifNoneMatch), Nil)
          }
        }
      } yield ())
    }
  }

  List("listing", "profile").foreach { failingRequest =>
    test(s"aRemovalRevalidationFailurePreservesStateWithAReason-$failingRequest") {
      TestControl.executeEmbed(for {
        send <- cats.effect.Deferred[IO, Unit]
        processed <- cats.effect.Deferred[IO, Unit]
        bytes = changeBytes(Prod, 1L, ClusterChangeDto.Removed).getBytes("UTF-8").toList
        stream = Stream.eval(send.get).drain ++ Stream.emits(bytes).covary[IO] ++
          Stream.eval(processed.complete(())).drain ++ Stream.never[IO]
        fake <- fake(Behaviour.of(profile(Prod, 2L)).copy(events = Some(stream)))
        changes <- Ref.of[IO, List[ProfileChange]](Nil)
        _ <- client(fake, fastConfig).use { (profiles, _) =>
          for {
            _ <- profiles.onChange(change => changes.update(_ :+ change))
            _ <- fake.update(
              _.copy(listFails = failingRequest == "listing", profileFails = failingRequest == "profile")
            )
            _ <- send.complete(())
            _ <- processed.get
            current <- profiles.get(Prod)
            seen <- changes.get
            health <- profiles.health
          } yield {
            assertEquals(current.map(_.version), Some(2L))
            assertEquals(seen, Nil)
            assert(health.lastError.exists(_.message.nonEmpty), health.toString)
            assert(health.failingSince.isDefined, health.toString)
          }
        }
      } yield ())
    }
  }

  test("aFailedListFetchNeverFiresRemoved") {
    // The assertion that stops a blip from tearing down every Kafka client in the process. "I cannot
    // see the list" is not "the cluster was deleted", and only one of those is a reason to disconnect.
    TestControl.executeEmbed(
      for {
        fake <- fake(Behaviour.of(profile(Prod, 1L)).copy(events = silent))
        changes <- Ref.of[IO, List[ProfileChange]](Nil)
        held <- client(fake, fastConfig).use { (profiles, _) =>
          for {
            _ <- profiles.onChange(change => changes.update(_ :+ change))
            _ <- fake.update(_.copy(listFails = true))
            _ <- IO.sleep(5.minutes)
            all <- profiles.all
            health <- profiles.health
          } yield (all, health)
        }
        seen <- changes.get
      } yield {
        val (all, health) = held
        assertEquals(seen, Nil)
        assertEquals(all(Prod).version, 1L, "the last known profile is still served")
        assert(health.lastError.isDefined, health.toString)
      }
    )
  }

  test("aSuccessfulListWithoutAClusterFiresRemoved") {
    TestControl.executeEmbed(
      for {
        fake <- fake(Behaviour.of(profile(Prod, 1L), profile(Staging, 4L)).copy(events = silent))
        changes <- Ref.of[IO, List[ProfileChange]](Nil)
        all <- client(fake, fastConfig).use { (profiles, _) =>
          for {
            _ <- profiles.onChange(change => changes.update(_ :+ change))
            _ <- fake.update(current => current.copy(profiles = current.profiles.removed(Staging)))
            _ <- IO.sleep(61.seconds)
            all <- profiles.all
          } yield all
        }
        seen <- changes.get
      } yield {
        assertEquals(seen, List(ProfileChange.Removed(Staging)))
        assertEquals(all.keySet, Set(Prod))
      }
    )
  }

  test("aRemovedEventOnTheStreamDropsThatClusterImmediately") {
    TestControl.executeEmbed(
      for {
        send <- cats.effect.Deferred[IO, Unit]
        processed <- cats.effect.Deferred[IO, Unit]
        bytes = changeBytes(Staging, 4L, ClusterChangeDto.Removed).getBytes("UTF-8").toList
        stream = Stream.eval(send.get).drain ++ Stream.emits(bytes).covary[IO] ++
          Stream.eval(processed.complete(())).drain ++ Stream.never[IO]
        fake <- fake(
          Behaviour
            .of(profile(Prod, 1L), profile(Staging, 4L))
            .copy(events = Some(stream))
        )
        changes <- Ref.of[IO, List[ProfileChange]](Nil)
        all <- client(fake, fastConfig).use { (profiles, _) =>
          profiles.onChange(change => changes.update(_ :+ change)) *>
            fake.update(current => current.copy(profiles = current.profiles.removed(Staging))) *>
            send.complete(()) *> processed.get *> profiles.all
        }
        seen <- changes.get
      } yield {
        assertEquals(seen, List(ProfileChange.Removed(Staging)))
        assertEquals(all.keySet, Set(Prod))
      }
    )
  }

  // ---------------------------------------------------------------------------------------------
  // Reconnection and health
  // ---------------------------------------------------------------------------------------------

  test("oneWarningPerDisconnectNotOnePerAttempt") {
    // A log line per attempt turns a two-hour outage into thousands of identical lines that bury the
    // one that mattered.
    TestControl.executeEmbed(
      for {
        fake <- fake(Behaviour.of(profile(Prod, 1L)).copy(events = None))
        warnings <- client(fake, fastConfig).use { (_, logger) =>
          IO.sleep(10.minutes) *> logger.entries.map(_.filter(_.level == "warn"))
        }
      } yield {
        assertEquals(warnings.size, 1, warnings.map(_.message).mkString("\n"))
        assert(warnings.head.message.contains("cluster change stream"), warnings.head.message)
      }
    )
  }

  test("healthFailingSinceIsSticky") {
    // The question the field answers is "how long has this been broken". One that moved with every
    // retry would say "a second ago" during an outage that started at breakfast.
    TestControl.executeEmbed(
      for {
        fake <- fake(Behaviour.of(profile(Prod, 1L)).copy(events = silent))
        result <- client(fake, fastConfig).use { (profiles, _) =>
          for {
            _ <- fake.update(_.copy(listFails = true))
            _ <- IO.sleep(61.seconds)
            first <- profiles.health
            _ <- IO.sleep(5.minutes)
            later <- profiles.health
            _ <- fake.update(_.copy(listFails = false))
            // Long enough for the next poll after the recovery, not merely one interval: the poll runs
            // on its own schedule and the previous one had already fired.
            _ <- IO.sleep(2.minutes)
            recovered <- profiles.health
          } yield (first, later, recovered)
        }
      } yield {
        val (first, later, recovered) = result
        assertEquals(first.failingSince, later.failingSince, "failingSince must not move with each retry")
        assert(later.lastError.isDefined)
        // And it clears on recovery, so the next outage gets its own instant.
        assertEquals(recovered.failingSince, None)
        assertEquals(recovered.lastError, None)
        assert(recovered.lastSuccessAt.isDefined)
      }
    )
  }

  test("subscribedIsTrueOnlyWhileTheStreamIsActuallyOpen") {
    TestControl.executeEmbed(
      for {
        fake <- fake(Behaviour.of(profile(Prod, 1L)).copy(events = None))
        closed <- client(fake, fastConfig).use((profiles, _) => IO.sleep(5.seconds) *> profiles.health)
        healthy <- ClusterClientFixture.fake(Behaviour.of(profile(Prod, 1L)).copy(events = silent))
        open <- client(healthy, fastConfig).use((profiles, _) => IO.sleep(1.second) *> profiles.health)
      } yield {
        assertEquals(closed.subscribed, false)
        assertEquals(open.subscribed, true)
      }
    )
  }

  // ---------------------------------------------------------------------------------------------
  // Cancellation
  // ---------------------------------------------------------------------------------------------

  test("releasingTheClientLeavesNothingRunning") {
    // ADR-002: a resource's cancellation path is tested, not assumed. After release, an hour of virtual
    // time passes and the backend sees nothing — no poll, no reconnect, no request in flight.
    TestControl.executeEmbed(
      for {
        fake <- fake(Behaviour.of(profile(Prod, 1L)).copy(events = silent))
        changes <- Ref.of[IO, List[ProfileChange]](Nil)
        _ <- client(fake, fastConfig).use { (profiles, _) =>
          profiles.onChange(change => changes.update(_ :+ change)) *> IO.sleep(1.second)
        }
        _ <- fake.reset
        // Everything that could still be running would run in this hour: sixty poll intervals and, if
        // the subscription had survived, a hundred and twenty reconnects.
        _ <- IO.sleep(1.hour)
        after <- fake.recorded
        fired <- changes.get
      } yield {
        assertEquals(after, Nil, "a request after release means a fiber outlived the resource")
        assertEquals(fired, Nil)
      }
    )
  }

  test("releaseIsPromptEvenWhileTheStreamIsOpen") {
    // A release that waited for an SSE connection to end would wait for ever: a healthy stream does not
    // end, it goes quiet.
    TestControl.executeEmbed(
      for {
        fake <- fake(Behaviour.of(profile(Prod, 1L)).copy(events = silent))
        before <- IO.monotonic
        _ <- client(fake, fastConfig).use((_, _) => IO.sleep(10.seconds))
        after <- IO.monotonic
      } yield assertEquals(after - before, 10.seconds)
    )
  }
}

/** The backoff curve, on its own.
  *
  * It is a pure function so that it can be asserted as a property rather than observed through a client: the
  * thing that matters is the shape of the curve — it grows, it is capped, and it never reaches zero — and a
  * suite that inferred that from sleep times would be asserting the scheduler as well.
  */
final class ClusterProfilesConfigSuite extends ScalaCheckSuite {

  private val config = ClusterProfilesConfig(
    reconnectBackoff = 1.second,
    maxReconnectBackoff = 30.seconds
  )

  property("reconnectBackoffIsExponentialAndCapped") {
    forAll(Gen.chooseNum(1, 10000)) { (attempt: Int) =>
      val delay = config.backoffFor(attempt)
      assert(delay >= config.reconnectBackoff, s"attempt $attempt gave $delay")
      assert(delay <= config.maxReconnectBackoff, s"attempt $attempt gave $delay")
    }
  }

  property("theBackoffNeverDecreasesAsAttemptsMount") {
    forAll(Gen.chooseNum(1, 200)) { (attempt: Int) =>
      assert(config.backoffFor(attempt + 1) >= config.backoffFor(attempt), s"at attempt $attempt")
    }
  }

  test("theCurveIsTheOneAnOperatorWouldPredict") {
    assertEquals(
      (1 to 8).toList.map(config.backoffFor),
      List(1.second, 2.seconds, 4.seconds, 8.seconds, 16.seconds, 30.seconds, 30.seconds, 30.seconds)
    )
  }

  test("aVeryLateAttemptDoesNotOverflowIntoANegativeSleep") {
    // Doubling a `FiniteDuration` works in nanoseconds and overflows after about 63 steps. A negative
    // duration is not a long sleep, it is no sleep at all, so an overflow turns a backoff into a hot
    // loop against a service that is already struggling.
    assertEquals(config.backoffFor(Int.MaxValue), 30.seconds)
    assert(config.backoffFor(1000) > Duration.Zero)
  }
}
