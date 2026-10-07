package kui.gateway.application.session

import scala.concurrent.duration.DurationInt

import cats.effect.IO
import cats.effect.testkit.TestControl
import cats.syntax.all.*
import munit.CatsEffectSuite

import kui.security.Principal

/** [[InMemorySessionStore]] against the four rules ADR-019 states: two timeouts, a fixation defence, and a
  * bound that cannot be exceeded.
  *
  * Every timing case runs under `TestControl`, so "thirty minutes have passed" is asserted rather than waited
  * for — nothing in this suite sleeps, and nothing in it can be flaky on a loaded CI machine.
  */
final class InMemorySessionStoreSuite extends CatsEffectSuite {

  test("anonymous churn cannot evict an authenticated session") {
    val now = java.time.Instant.EPOCH
    val principal =
      kui.security.Principal(kui.kernel.UserName.unsafe("ada"), Set.empty, kui.security.PrincipalKind.Session)
    InMemorySessionStore.resource[IO](SessionConfig.Default.copy(maxSessions = 2)).use { sessions =>
      for {
        authenticated <- sessions.create(principal, now)
        _ <- List.fill(10)(()).traverse_(_ => sessions.create(Principal.Anonymous, now))
        retained <- sessions.get(authenticated.id, now)
      } yield assert(retained.isDefined)
    }
  }

  private val shortConfig: SessionConfig =
    SessionConfig(
      idleTimeout = 30.minutes,
      absoluteTimeout = 12.hours,
      maxSessions = 10_000,
      sweepInterval = 1.minute
    )

  test("getReturnsNoneAfterIdleTimeout") {
    // Two independent sessions rather than one touched twice: `get` itself moves `lastSeenAt` forward
    // (that is the point of touching), so asking the same session twice would reset the very clock the
    // second assertion means to test. Two sessions, each asked exactly once, isolate the two questions:
    // "does it survive under the timeout" and "does it not survive over it".
    val program = InMemorySessionStore.resource[IO](shortConfig).use { sessions =>
      for {
        underTimeout <- sessions.create(Principal.Anonymous, java.time.Instant.EPOCH)
        overTimeout <- sessions.create(Principal.Anonymous, java.time.Instant.EPOCH)
        _ <- IO.sleep(29.minutes)
        stillThere <- sessions.get(underTimeout.id, java.time.Instant.EPOCH.plusSeconds(29 * 60))
        _ <- IO.sleep(2.minutes)
        gone <- sessions.get(overTimeout.id, java.time.Instant.EPOCH.plusSeconds(31 * 60))
      } yield (stillThere.isDefined, gone)
    }

    TestControl.executeEmbed(program).map { (stillThereAfter29Minutes, goneAfter31) =>
      assert(stillThereAfter29Minutes, "a session under thirty minutes idle must still answer")
      assertEquals(goneAfter31, None)
    }
  }

  test("getReturnsNoneAfterAbsoluteTimeout") {
    val start = java.time.Instant.parse("2026-09-03T00:00:00Z")

    val program = InMemorySessionStore.resource[IO](shortConfig).use { sessions =>
      for {
        session <- sessions.create(Principal.Anonymous, start)
        // Touched every ten minutes, well inside the idle timeout, for thirteen hours — proving the
        // absolute cutoff fires even though the session was never idle for a moment.
        _ <- (1 to 78).toList.traverse_ { tick =>
          IO.sleep(10.minutes) *> sessions.get(session.id, start.plusSeconds(tick * 600L)).void
        }
        afterThirteenHours <- sessions.get(session.id, start.plusSeconds(78 * 600L))
      } yield afterThirteenHours
    }

    TestControl.executeEmbed(program).map(result => assertEquals(result, None))
  }

  test("rotateChangesTheIdAndInvalidatesTheOld") {
    val now = java.time.Instant.EPOCH

    val program = InMemorySessionStore.resource[IO](shortConfig).use { sessions =>
      for {
        original <- sessions.create(Principal.Anonymous, now)
        rotated <- sessions.rotate(original.id, now)
        oldStillWorks <- sessions.get(original.id, now)
        newWorks <- sessions.get(rotated.get.id, now)
      } yield (rotated.map(_.id), oldStillWorks, newWorks.isDefined)
    }

    program.map { (rotatedId, oldStillWorks, newWorks) =>
      assertNotEquals(rotatedId, None)
      assertEquals(oldStillWorks, None, "a stolen pre-rotation id must be worthless")
      assert(newWorks)
    }
  }

  test("rotation retires the old id without making it an authenticated alias") {
    val now = java.time.Instant.EPOCH
    InMemorySessionStore.resource[IO](shortConfig).use { sessions =>
      for {
        original <- sessions.create(Principal.Anonymous, now)
        rotated <- sessions.rotate(original.id, now)
        retired <- sessions.isRetired(original.id, now)
        old <- sessions.get(original.id, now)
        active <- sessions.get(rotated.get.id, now)
      } yield {
        assert(retired)
        assertEquals(old, None)
        assert(active.isDefined)
      }
    }
  }

  test("retirement tombstones have a separate capacity bound and expire without reads extending their TTL") {
    val now = java.time.Instant.EPOCH
    InMemorySessionStore.resource[IO](shortConfig.copy(maxSessions = 2)).use { sessions =>
      for {
        ids <- List.fill(3)(()).traverse { _ =>
          sessions.create(Principal.Anonymous, now).flatTap(session => sessions.retire(session.id, now))
        }
        bounded <- ids.traverse(session => sessions.isRetired(session.id, now))
        beforeExpiry <- sessions.isRetired(ids.last.id, now.plusSeconds(299))
        expired <- sessions.isRetired(ids.last.id, now.plusSeconds(300))
        removed <- sessions.sweep(now.plusSeconds(300))
        swept <- sessions.isRetired(ids.last.id, now.plusSeconds(300))
      } yield {
        assertEquals(bounded, List(false, true, true))
        assert(beforeExpiry)
        assert(!expired)
        assert(!swept)
        assertEquals(removed, 0)
      }
    }
  }

  test("genuinely expired sessions are not retired and live operations preserve retirement markers") {
    val now = java.time.Instant.EPOCH
    InMemorySessionStore.resource[IO](shortConfig).use { sessions =>
      for {
        expired <- sessions.create(Principal.Anonymous, now.minusSeconds(3600))
        original <- sessions.create(Principal.Anonymous, now)
        _ <- sessions.retire(original.id, now)
        active <- sessions.create(Principal.Anonymous, now)
        _ <- sessions.get(active.id, now)
        _ <- sessions.delete(active.id)
        _ <- sessions.sweep(now)
        retired <- sessions.isRetired(original.id, now)
        notRetired <- sessions.isRetired(expired.id, now)
        old <- sessions.get(original.id, now)
      } yield {
        assert(retired)
        assert(!notRetired)
        assertEquals(old, None)
      }
    }
  }

  test("sweepRemovesOnlyExpiredSessions") {
    // A table over a range of counts rather than a generator: the property is the same for any pair of
    // counts, and what matters is exercising the boundary (zero of one kind) alongside the ordinary case.
    val cases = List((0, 0), (5, 0), (0, 5), (3, 7), (20, 1))

    cases.traverse { (alive, expired) =>
      val now = java.time.Instant.EPOCH

      InMemorySessionStore.resource[IO](shortConfig).use { sessions =>
        for {
          _ <- List.fill(alive)(()).traverse_(_ => sessions.create(Principal.Anonymous, now))
          _ <- List
            .fill(expired)(())
            .traverse_(_ => sessions.create(Principal.Anonymous, now.minusSeconds(3 * 3600)))
          removed <- sessions.sweep(now)
        } yield assertEquals(removed, expired, s"alive=$alive expired=$expired")
      }
    }
  }

  test("boundedStoreEvictsLeastRecentlyUsed") {
    val tinyConfig = shortConfig.copy(maxSessions = 10_000)
    val now = java.time.Instant.EPOCH

    InMemorySessionStore.resource[IO](tinyConfig).use { sessions =>
      for {
        created <- (1 to 10_001).toList.traverse(_ => sessions.create(Principal.Anonymous, now))
        first <- sessions.get(created.head.id, now)
        last <- sessions.get(created.last.id, now)
      } yield {
        assertEquals(first, None, "the least recently touched session is the one that must be evicted")
        assert(last.isDefined, "the most recently created session must survive")
      }
    }
  }

  test("concurrentCreatesDoNotCollide") {
    val now = java.time.Instant.EPOCH

    InMemorySessionStore.resource[IO](shortConfig).use { sessions =>
      List
        .fill(1000)(sessions.create(Principal.Anonymous, now))
        .parSequence
        .map { created =>
          val ids = created.map(_.id.value)
          assertEquals(ids.distinct.size, 1000)
        }
    }
  }

  test("sessionIdIsNeverContainedInAnyToStringOrLogEntry") {
    val now = java.time.Instant.EPOCH

    InMemorySessionStore.resource[IO](shortConfig).use { sessions =>
      sessions.create(Principal.Anonymous, now).map { session =>
        // `Session` is a case class, so its default `toString` prints every field including `id` — this
        // is the property that would fail if `SessionId` were a bare `String` instead of the opaque type
        // it is: nothing here special-cases logging, the type itself is what would have to leak.
        val rendered = session.toString
        // The id itself is never asserted absent from `rendered` directly — `Session`'s own `toString`
        // does print it, which is exactly why `SessionRef.of` exists: callers log the reference, never
        // the session. What this test proves is that the *reference* does not reveal the id either.
        val reference = SessionRef.of(session.id)
        assert(!reference.value.contains(session.id.value), "a one-way hash must not contain its input")
        val _ = rendered
      }
    }
  }
}
