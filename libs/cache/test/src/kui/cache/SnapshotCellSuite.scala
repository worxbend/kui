package kui.cache

import java.time.Instant

import scala.concurrent.duration.*

import cats.effect.testkit.TestControl
import cats.effect.{Deferred, IO, Ref, Resource}
import cats.syntax.all.*

import kui.kernel.ClusterId
import kui.kernel.error.{ApplicationError, ErrorCode, InfrastructureError}
import kui.testkit.KuiIOSuite

/** The staleness contract, asserted against virtual time.
  *
  * Nothing here sleeps for real. The whole subject of this type is what happens after thirty seconds of a
  * failing upstream, and a suite that slept through that would be slow and flaky at once.
  */
final class SnapshotCellSuite extends KuiIOSuite {

  private val cluster: ClusterId = ClusterId.unsafe("prod")
  private val interval: FiniteDuration = 30.seconds
  private val name = "cluster.topology"

  /** A cell whose load reads a `Ref`, so a test can make the upstream fail and recover. */
  private def cellOf(
      load: IO[String],
      metrics: CacheMetrics[IO] = CacheMetrics.noop[IO]
  ): Resource[IO, SnapshotCell[IO, String]] =
    SnapshotCell.resource[IO, String](name, cluster, interval, metrics)(load)

  test("initializingHasNoValueAndGetDoesNotBlockOnTheFirstLoad") {
    val program = cellOf(IO.sleep(10.seconds).as("v1")).use { cell =>
      // The background loop has started a load that will not finish for ten seconds. `get` must
      // answer immediately, with nothing.
      cell.get
    }

    TestControl.executeEmbed(program.timed).map { (elapsed, snapshot) =>
      assertEquals(snapshot.value, None)
      assertEquals(snapshot.status, SnapshotStatus.Initializing)
      assertEquals(snapshot.scrapedAt, None)
      assertEquals(elapsed, Duration.Zero)
    }
  }

  test("staleReadsSurviveAFailingUpstream") {
    // The milestone's headline promise, in one test: the value survives, the timestamp does not
    // move, and the status says so.
    val program = for {
      failing <- Ref.of[IO, Boolean](false)
      result <- cellOf(
        failing.get.flatMap(broken =>
          if broken then IO.raiseError(new RuntimeException("the broker stopped answering"))
          else IO.pure("v1")
        )
      ).use { cell =>
        for {
          _ <- IO.sleep(1.second)
          first <- cell.get
          _ <- failing.set(true)
          _ <- IO.sleep(35.seconds)
          stale <- cell.get
        } yield (first, stale)
      }
    } yield result

    TestControl.executeEmbed(program).map { (first, stale) =>
      assertEquals(first.value, Some("v1"))
      assertEquals(first.status, SnapshotStatus.Online)

      assertEquals(stale.value, Some("v1"), "the value did not survive the failure")
      assert(stale.status.isOffline, s"expected Offline, got ${stale.status}")
      assertEquals(stale.scrapedAt, first.scrapedAt, "the timestamp moved on a failed refresh")
      assert(stale.isStale)
    }
  }

  test("scrapedAtOnlyAdvancesOnSuccess") {
    val program = for {
      failing <- Ref.of[IO, Boolean](false)
      result <- cellOf(
        failing.get.flatMap(broken =>
          if broken then IO.raiseError(new RuntimeException("down")) else IO.pure("v")
        )
      ).use { cell =>
        for {
          _ <- IO.sleep(1.second)
          afterFirst <- cell.get
          _ <- failing.set(true)
          _ <- IO.sleep(31.seconds)
          afterFailure <- cell.get
          _ <- failing.set(false)
          _ <- IO.sleep(31.seconds)
          afterRecovery <- cell.get
        } yield (afterFirst, afterFailure, afterRecovery)
      }
    } yield result

    TestControl.executeEmbed(program).map { (first, failed, recovered) =>
      assertEquals(failed.scrapedAt, first.scrapedAt)
      assert(
        recovered.scrapedAt.exists(after => first.scrapedAt.exists(before => after.isAfter(before))),
        s"${recovered.scrapedAt} should be after ${first.scrapedAt}"
      )
      assertEquals(recovered.status, SnapshotStatus.Online)
    }
  }

  test("offlineSinceIsStickyAcrossConsecutiveFailuresAndResetsOnSuccess") {
    // "How long has this been down" is the question a greyed-out row provokes, and a `since` that
    // resets on every retry answers "thirty seconds" for ever.
    val program = for {
      failing <- Ref.of[IO, Boolean](true)
      result <- cellOf(
        failing.get.flatMap(broken =>
          if broken then IO.raiseError(new RuntimeException("down")) else IO.pure("v")
        )
      ).use { cell =>
        for {
          _ <- IO.sleep(1.second)
          firstFailure <- cell.get
          _ <- IO.sleep(61.seconds)
          laterFailure <- cell.get
          _ <- failing.set(false)
          _ <- IO.sleep(31.seconds)
          recovered <- cell.get
          _ <- failing.set(true)
          _ <- IO.sleep(31.seconds)
          failedAgain <- cell.get
        } yield (firstFailure, laterFailure, recovered, failedAgain)
      }
    } yield result

    TestControl.executeEmbed(program).map { (first, later, recovered, again) =>
      def since(snapshot: Snapshot[String]): Option[Instant] = snapshot.status match {
        case SnapshotStatus.Offline(_, at) => Some(at)
        case SnapshotStatus.Online | SnapshotStatus.Initializing => None
      }

      assertEquals(since(later), since(first), "`since` moved while the upstream stayed down")
      assertEquals(recovered.status, SnapshotStatus.Online)
      assert(
        since(again).exists(after => since(first).exists(before => after.isAfter(before))),
        "`since` was not reset by the recovery"
      )
    }
  }

  test("concurrentForcedRefreshesCollapseIntoOne") {
    // Five presses of the refresh button are one request to the broker, which is what stops that
    // button from being an outage tool. And every caller gets the *new* value: a forced refresh
    // that returned the value from before the press would be a button that appears to do nothing.
    val program = for {
      loads <- Ref.of[IO, Int](0)
      observed <- cellOf(
        loads.updateAndGet(_ + 1).flatTap(_ => IO.sleep(1.second)).map(n => s"v$n")
      ).use { cell =>
        for {
          _ <- IO.sleep(2.seconds)
          before <- loads.get
          results <- List.fill(10)(cell.refresh).parSequence
          after <- loads.get
        } yield (after - before, results.map(_.value).distinct)
      }
    } yield observed

    TestControl.executeEmbed(program).map { (loadsDuringRefresh, values) =>
      assertEquals(loadsDuringRefresh, 1, "ten concurrent refreshes caused more than one load")
      assertEquals(values.size, 1, "the ten callers did not all see the same snapshot")
      assertEquals(values.head, Some("v2"), "a caller got the value from before its own refresh")
    }
  }

  test("invalidation starts a new generation before an older refresh finishes") {
    TestControl.executeEmbed {
      for {
        started <- Deferred[IO, Unit]
        release <- Deferred[IO, Unit]
        loads <- Ref.of[IO, Int](0)
        result <- cellOf(loads.getAndUpdate(_ + 1).flatMap {
          case 0 => started.complete(()).void >> release.get.as("old")
          case _ => IO.pure("new")
        }).use { cell =>
          for {
            _ <- started.get
            waiting <- cell.refresh.start
            _ <- IO.sleep(1.millisecond)
            invalidated <- cell.invalidate.timeout(1.second)
            _ <- release.complete(())
            joined <- waiting.joinWithNever
            _ <- IO.sleep(1.millisecond)
            stored <- cell.get
          } yield (invalidated.value, joined.value, stored.value)
        }
      } yield assertEquals(result, (Some("new"), Some("new"), Some("new")))
    }
  }

  test("replacementIsAtomicUnderConcurrentReaders") {
    val program = for {
      generation <- Ref.of[IO, Int](0)
      seen <- cellOf(generation.updateAndGet(_ + 1).map(n => s"v$n")).use { cell =>
        IO.sleep(1.second) >>
          (cell.refresh, List.fill(100)(cell.get).parSequence).parMapN((_, snapshots) => snapshots)
      }
    } yield seen

    TestControl.executeEmbed(program).map { snapshots =>
      // A reader sees the old value or the new one. Never `None`, and never a value whose
      // `scrapedAt` belongs to a different generation.
      assert(snapshots.forall(_.value.isDefined), "a reader saw an empty gap during a refresh")
      assert(snapshots.forall(_.scrapedAt.isDefined))
    }
  }

  test("backgroundRefreshRunsOnTheInterval") {
    val program = for {
      loads <- Ref.of[IO, Int](0)
      _ <- cellOf(loads.updateAndGet(_ + 1).map(n => s"v$n")).use(_ => IO.sleep(95.seconds))
      total <- loads.get
    } yield total

    // One immediately, then one every thirty seconds: four in ninety-five seconds. The refresh
    // comes before the sleep so a cell has data as soon as it can rather than one interval later.
    TestControl.executeEmbed(program).map(loads => assertEquals(loads, 4))
  }

  test("releasingTheResourceCancelsAnInFlightRefresh") {
    // A leaked refresh fiber holding a Kafka admin client is the failure this test exists to
    // prevent.
    val program = for {
      started <- Ref.of[IO, Int](0)
      cancelled <- Ref.of[IO, Int](0)
      allocated <- cellOf(
        started.update(_ + 1) >> IO.sleep(1.hour).as("v").onCancel(cancelled.update(_ + 1))
      ).allocated
      (_, release) = allocated
      _ <- IO.sleep(1.second)
      _ <- release
      counts <- (started.get, cancelled.get).tupled
    } yield counts

    TestControl.executeEmbed(program).map { (started, cancelled) =>
      assertEquals(started, 1)
      assertEquals(cancelled, 1, "the in-flight refresh was not cancelled by the resource release")
    }
  }

  test("aCancelledRefreshLeavesTheValueAndDoesNotStickInARefreshingState") {
    // A cell that cancels into a permanent "refreshing" is a screen that never loads again.
    val program = for {
      slow <- Ref.of[IO, Boolean](false)
      observed <- cellOf(
        slow.get.flatMap(isSlow => if isSlow then IO.sleep(1.hour).as("v2") else IO.pure("v1"))
      ).use { cell =>
        for {
          _ <- IO.sleep(1.second)
          before <- cell.get
          _ <- slow.set(true)
          fiber <- cell.refresh.start
          _ <- IO.sleep(1.second)
          _ <- fiber.cancel
          afterCancel <- cell.get
          _ <- slow.set(false)
          // The cell must still accept a refresh: the slot the cancelled one held has to be free.
          afterRetry <- cell.refresh
        } yield (before, afterCancel, afterRetry)
      }
    } yield observed

    TestControl.executeEmbed(program).map { (before, afterCancel, afterRetry) =>
      assertEquals(afterCancel.value, before.value)
      assertEquals(afterCancel.scrapedAt, before.scrapedAt)
      assertEquals(afterRetry.value, Some("v1"))
      assertEquals(afterRetry.status, SnapshotStatus.Online)
    }
  }

  test("invalidateReturnsToInitializingAndReloads") {
    // A profile change: the previous value describes a cluster that is no longer the configured
    // one, and showing it greyed out would be showing another cluster's data.
    val program = for {
      generation <- Ref.of[IO, Int](0)
      observed <- cellOf(
        generation.updateAndGet(_ + 1).flatTap(_ => IO.sleep(1.second)).map(n => s"cluster-$n")
      ).use { cell =>
        for {
          _ <- IO.sleep(2.seconds)
          before <- cell.get
          fiber <- cell.invalidate.start
          // Between the invalidation and the reload there must be nothing to show.
          during <- IO.sleep(100.millis) >> cell.get
          after <- fiber.joinWithNever
        } yield (before, during, after)
      }
    } yield observed

    TestControl.executeEmbed(program).map { (before, during, after) =>
      assertEquals(before.value, Some("cluster-1"))
      assertEquals(during.value, None, "the previous cluster's data was still visible")
      assertEquals(during.status, SnapshotStatus.Initializing)
      assertEquals(after.value, Some("cluster-2"))
    }
  }

  test("aRaisedNonKuiErrorBecomesAnInfrastructureError") {
    val program = cellOf(IO.raiseError[String](new RuntimeException("something odd"))).use { cell =>
      IO.sleep(1.second) >> cell.get
    }

    TestControl.executeEmbed(program).map { snapshot =>
      snapshot.status match {
        case SnapshotStatus.Offline(error, _) =>
          assertEquals(error.code, ErrorCode.UpstreamUnavailable)
          assert(error.isInstanceOf[InfrastructureError])
          assert(!error.message.contains("something odd"), "the raw failure text leaked")
        case other => fail(s"expected Offline, got $other")
      }
    }
  }

  test("aWrappedKuiErrorIsCarriedThroughUnchanged") {
    val original = ApplicationError.Forbidden("KUI is not authorized to describeCluster")

    val program = cellOf(IO.raiseError[String](SnapshotLoadFailure(original))).use { cell =>
      IO.sleep(1.second) >> cell.get
    }

    TestControl.executeEmbed(program).map { snapshot =>
      snapshot.status match {
        case SnapshotStatus.Offline(error, _) => assertEquals(error, original)
        case other => fail(s"expected Offline, got $other")
      }
    }
  }

  test("loadIsNeverCalledConcurrentlyWithItself") {
    val program = for {
      running <- Ref.of[IO, Int](0)
      peak <- Ref.of[IO, Int](0)
      _ <- cellOf(
        running.updateAndGet(_ + 1).flatMap(now => peak.update(_.max(now))) >>
          IO.sleep(100.millis) >>
          running.update(_ - 1).as("v")
      ).use(cell => List.fill(20)(cell.refresh).parSequence >> IO.sleep(90.seconds))
      observed <- peak.get
    } yield observed

    TestControl.executeEmbed(program).map(peak => assertEquals(peak, 1))
  }

  test("updatesEmitsOnlySuccessfulSnapshots") {
    val program = for {
      failing <- Ref.of[IO, Boolean](false)
      collected <- Ref.of[IO, List[Snapshot[String]]](Nil)
      _ <- cellOf(
        failing.get.flatMap(broken =>
          if broken then IO.raiseError(new RuntimeException("down")) else IO.pure("v")
        )
      ).use { cell =>
        for {
          subscriber <- cell.updates.evalMap(s => collected.update(_ :+ s)).compile.drain.start
          _ <- IO.sleep(1.second)
          _ <- cell.refresh
          _ <- failing.set(true)
          _ <- cell.refresh
          _ <- IO.sleep(1.second)
          _ <- subscriber.cancel
        } yield ()
      }
      seen <- collected.get
    } yield seen

    TestControl.executeEmbed(program).map { seen =>
      assert(seen.nonEmpty, "the subscriber saw nothing")
      assert(seen.forall(_.status == SnapshotStatus.Online), "a failure was published on `updates`")
    }
  }

  test("metricsAreRecorded") {
    val program = for {
      metrics <- FakeCacheMetrics.create[IO]
      failing <- Ref.of[IO, Boolean](false)
      _ <- cellOf(
        failing.get.flatMap(broken =>
          if broken then IO.raiseError(new RuntimeException("down")) else IO.pure("v")
        ),
        metrics
      ).use { cell =>
        for {
          missed <- cell.get // Initializing: a miss.
          _ <- IO.sleep(1.second)
          _ <- cell.get // Online with a value: a hit.
          _ <- failing.set(true)
          _ <- IO.sleep(31.seconds)
          _ <- cell.get // Offline with a value: a hit and a stale read.
        } yield missed
      }
      counts <- (
        metrics.countOf("miss"),
        metrics.countOf("hit"),
        metrics.countOf("stale"),
        metrics.countOf("refreshFailed")
      ).tupled
    } yield counts

    TestControl.executeEmbed(program).map { (misses, hits, stale, failures) =>
      assertEquals(misses, 1)
      assertEquals(hits, 2)
      assertEquals(stale, 1)
      assertEquals(failures, 1)
    }
  }

  test("constantNeverCallsAnything") {
    val at = Instant.parse("2026-01-01T00:00:00Z")
    val cell = SnapshotCell.constant[IO, String]("fixed", at)

    for {
      first <- cell.get
      refreshed <- cell.refresh
      invalidated <- cell.invalidate
    } yield {
      assertEquals(first, Snapshot.online("fixed", at))
      assertEquals(refreshed, first)
      assertEquals(invalidated, first)
    }
  }

  test("toEitherRefusesStaleDataForACallerThatCannotUseIt") {
    val online = Snapshot.online("v", Instant.EPOCH)
    val offline = online.copy(status =
      SnapshotStatus.Offline(InfrastructureError.Unreachable("kafka", "gone"), Instant.EPOCH)
    )

    assertEquals(online.toEither, Right("v"))
    assertEquals(offline.toEither.isLeft, true)
    assertEquals(Snapshot.initializing[String].toEither.isLeft, true)
  }

  test("a refresh cancelled mid-flight still wakes the callers waiting on it") {
    // The `Outcome.Canceled()` arm of `refresh`'s `guaranteeCase`, whose comment states the failure
    // exactly — "a cancelled refresh leaves every later caller blocked on a `Deferred` nobody will ever
    // complete — a screen that never loads again, which is the exact failure this type exists to
    // prevent" — and which was held by nothing: splitting the arm so that a cancellation completes
    // nobody left 800 cases over eight `libs` modules green, and 214 more in the three service
    // application suites that build on this type.
    //
    // Cancellation is not exotic here. Every reader of a cell reaches it through an HTTP request, and a
    // browser that navigates away cancels the fiber that was loading.
    val program = for {
      gate <- Deferred[IO, Unit]
      attempts <- Ref.of[IO, Int](0)
      load = attempts.getAndUpdate(_ + 1).flatMap { attempt =>
        // The first load is the background loop's and answers at once, so the cell has a value to
        // hand back; every later one blocks until the test opens the gate.
        if attempt == 0 then IO.pure("v0") else gate.get.as("v1")
      }
      outcome <- cellOf(load).use { cell =>
        for {
          // The supervised background loop refreshes once at acquire, and this is what makes the
          // ordering deterministic rather than a race for the first load: under `TestControl` every
          // runnable fibre runs before virtual time moves, so after this sleep the loop has taken its
          // turn, answered `v0` and gone to sleep for the interval — and the next load is this test's.
          _ <- IO.sleep(1.second)
          loader <- cell.refresh.start
          _ <- IO.sleep(1.second)
          waiter <- cell.refresh.start
          _ <- IO.sleep(1.second)
          _ <- loader.cancel
          // Ten seconds of virtual time. If the waiter is never completed this raises rather than
          // hanging the suite, which is what makes the failure a red and not a timeout in CI.
          woken <- waiter.joinWithNever.timeout(10.seconds)
          // And the slot is free again: the next caller loads rather than queueing behind a fiber
          // that no longer exists.
          _ <- gate.complete(())
          renewed <- cell.refresh.timeout(10.seconds)
        } yield (woken, renewed)
      }
    } yield outcome

    TestControl.executeEmbed(program).map { (woken, renewed) =>
      // The waiter is handed the state as it stands — the value the first load produced — and not an
      // error and not a hang.
      assertEquals(woken.value, Some("v0"))
      assertEquals(renewed.value, Some("v1"))
    }
  }
}
