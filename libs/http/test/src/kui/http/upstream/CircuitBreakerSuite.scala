package kui.http.upstream

import scala.concurrent.duration.DurationInt

import cats.effect.IO
import cats.effect.kernel.{Deferred, Ref, Resource}
import cats.effect.testkit.TestControl
import cats.syntax.all.*
import fs2.{Pipe, Stream}
import fs2.concurrent.Topic
import munit.CatsEffectSuite
import sttp.capabilities.StreamMaxLengthExceededException

import kui.kernel.PositiveInt

/** That KUI stops calling something that is plainly down, and finds out when it is back.
  *
  * Every case runs under `TestControl`, so the reset timer is asserted rather than waited for: there is no
  * `sleep` in this suite and nothing in it can be flaky on a loaded CI machine.
  */
final class CircuitBreakerSuite extends CatsEffectSuite {

  private val upstream = "schema-registry"
  private val threshold = PositiveInt.unsafe(3)
  private val reset = 30.seconds

  private def breaker: IO[CircuitBreaker[IO]] =
    CircuitBreaker.make[IO](upstream, threshold, reset)

  private val boom = new RuntimeException(
    "https://admin:secret@example.invalid/api/v1/query?query=sensitive_promql"
  )

  private def fail(b: CircuitBreaker[IO]): IO[Unit] =
    b.protect(IO.raiseError[Unit](boom)).attempt.void

  private def succeed(b: CircuitBreaker[IO]): IO[Unit] =
    b.protect(IO.unit).attempt.void

  private def ignore(b: CircuitBreaker[IO]): IO[Unit] =
    b
      .protectClassified(IO.raiseError[Unit](new StreamMaxLengthExceededException(64L)))(_ => true)(_ =>
        false
      )
      .attempt
      .void

  test("opensAfterConsecutiveFailuresAndNotAfterInterleavedSuccesses") {
    val consecutive = for {
      b <- breaker
      _ <- fail(b).replicateA_(3)
      state <- b.state
    } yield state

    val interleaved = for {
      b <- breaker
      // Two failures, a success, two more failures. An upstream answering one call in three is
      // degraded, not down: cutting it off entirely would lose the calls that were working.
      _ <- fail(b) *> fail(b) *> succeed(b) *> fail(b) *> fail(b)
      state <- b.state
    } yield state

    TestControl.executeEmbed((consecutive, interleaved).tupled).map { (opened, stillClosed) =>
      assertEquals(opened, CircuitState.Open)
      assertEquals(stillClosed, CircuitState.Closed)
    }
  }

  test("a terminal non-counting failure preserves a real failure streak but never opens a circuit alone") {
    val preserveStreak = for {
      b <- CircuitBreaker.make[IO](upstream, PositiveInt.unsafe(2), reset)
      _ <- fail(b) *> ignore(b) *> fail(b)
      state <- b.state
    } yield state

    val ignoredOnly = for {
      b <- CircuitBreaker.make[IO](upstream, PositiveInt.unsafe(2), reset)
      _ <- ignore(b).replicateA_(10)
      state <- b.state
    } yield state

    TestControl.executeEmbed((preserveStreak, ignoredOnly).tupled).map { (streakState, ignoredState) =>
      assertEquals(streakState, CircuitState.Open)
      assertEquals(ignoredState, CircuitState.Closed)
    }
  }

  test("openRejectsImmediatelyWithoutCallingTheBackend") {
    val program = for {
      b <- breaker
      calls <- Ref.of[IO, Int](0)
      _ <- fail(b).replicateA_(3)
      // Twenty more attempts while open. Not one of them may reach the backend: that is the whole
      // point of the breaker, and counting is the only way to prove it.
      outcomes <- b.protect(calls.update(_ + 1)).attempt.replicateA(20)
      reached <- calls.get
    } yield (reached, outcomes)

    TestControl.executeEmbed(program).map { (reached, outcomes) =>
      assertEquals(reached, 0, "a call reached the backend while the circuit was open")
      assert(outcomes.forall(_.isLeft))
      assert(
        outcomes.forall {
          case Left(CircuitOpenException(name, _)) => name == upstream
          case _ => false
        },
        outcomes.toString
      )
    }
  }

  test("halfOpenAdmitsExactlyOneProbe") {
    val program = for {
      b <- breaker
      admitted <- Ref.of[IO, Int](0)
      _ <- fail(b).replicateA_(3)
      _ <- IO.sleep(reset + 1.second)
      // Ten callers arrive at once the moment the circuit becomes eligible. Sending ten probes at
      // a recovering upstream is how it goes down again, so exactly one may pass.
      _ <- List
        .fill(10)(b.protect(admitted.update(_ + 1) *> IO.sleep(1.second)).attempt)
        .parSequence
      probes <- admitted.get
    } yield probes

    TestControl.executeEmbed(program).map(probes => assertEquals(probes, 1))
  }

  test("successInHalfOpenCloses") {
    val program = for {
      b <- breaker
      _ <- fail(b).replicateA_(3)
      _ <- IO.sleep(reset + 1.second)
      _ <- succeed(b)
      state <- b.state
      // And the circuit really is usable again, not merely labelled closed.
      allowed <- b.protect(IO.pure(42)).attempt
    } yield (state, allowed)

    TestControl.executeEmbed(program).map { (state, allowed) =>
      assertEquals(state, CircuitState.Closed)
      assertEquals(allowed, Right(42))
    }
  }

  test("failureInHalfOpenReopensAndResetsTheTimer") {
    val program = for {
      b <- breaker
      _ <- fail(b).replicateA_(3)
      _ <- IO.sleep(reset + 1.second)
      _ <- fail(b)
      reopened <- b.state
      // The failed probe restarts the full timer. Half a reset later the circuit is still shut:
      // the upstream had its chance and is still unwell, so the next probe is a whole reset away.
      _ <- IO.sleep(reset / 2)
      midway <- b.protect(IO.unit).attempt
      _ <- IO.sleep(reset)
      afterwards <- b.protect(IO.unit).attempt
    } yield (reopened, midway, afterwards)

    TestControl.executeEmbed(program).map { (reopened, midway, afterwards) =>
      assertEquals(reopened, CircuitState.Open)
      assert(midway.isLeft, "a probe was admitted before the reset timer had run again")
      assertEquals(afterwards, Right(()))
    }
  }

  test("a terminal non-counting half-open probe reopens without wedging or immediately probing again") {
    val program = for {
      b <- breaker
      _ <- fail(b).replicateA_(3)
      _ <- IO.sleep(reset + 1.second)
      _ <- ignore(b)
      reopened <- b.state
      immediate <- b.protect(IO.unit).attempt
      _ <- IO.sleep(reset + 1.second)
      _ <- succeed(b)
      recovered <- b.state
    } yield (reopened, immediate, recovered)

    TestControl.executeEmbed(program).map { (reopened, immediate, recovered) =>
      assertEquals(reopened, CircuitState.Open)
      assert(immediate.left.toOption.exists(_.isInstanceOf[CircuitOpenException]))
      assertEquals(recovered, CircuitState.Closed)
    }
  }

  test("a response that arrived but was not a success still counts as a failure") {
    // An upstream answering 503 to everything is as down as one refusing connections. A breaker
    // that only noticed exceptions would never open for it.
    val program = for {
      b <- breaker
      _ <- b.protect(IO.pure(503))(status => status < 500).replicateA_(3)
      state <- b.state
    } yield state

    TestControl.executeEmbed(program).map(state => assertEquals(state, CircuitState.Open))
  }

  test("emitsOneCircuitEventPerTransitionAndNoneForRepeats") {
    val program = for {
      b <- breaker
      collected <- b.events.take(3).compile.toList.start
      _ <- IO.sleep(1.second)
      _ <- fail(b).replicateA_(3) // -> Open, one event
      _ <- fail(b).replicateA_(5) // still open, refused, no events
      _ <- IO.sleep(reset + 1.second)
      _ <- succeed(b) // -> HalfOpen then Closed, two events
      events <- collected.joinWithNever
    } yield events

    TestControl.executeEmbed(program).map { events =>
      assertEquals(
        events.map(_.state),
        List(CircuitState.Open, CircuitState.HalfOpen, CircuitState.Closed)
      )
      assert(events.forall(_.upstream == upstream), events.toString)
      assertEquals(events.head.lastError, Some("transport"))
      assert(!events.head.toString.contains("secret"), events.head.toString)
      assert(!events.head.toString.contains("sensitive_promql"), events.head.toString)
      // The transition that closed it carries no error, so a reader can tell recovery from failure
      // without parsing the state name.
      assertEquals(events.last.lastError, None)
    }
  }

  test("a cancelled probe releases the claim rather than wedging the circuit half-open") {
    // The likeliest fate of a probe sent at a hung upstream: the caller's own call timeout
    // cancels it. Nothing observes a cancellation except a cancellation-aware finaliser, so
    // without one the probe claim is never released and every later call — including the
    // readiness poll — is refused forever.
    val program = for {
      b <- breaker
      _ <- fail(b).replicateA_(3) // -> Open
      _ <- IO.sleep(reset + 1.second)
      // The probe: admitted, never answers, cancelled from outside.
      _ <- b.protect(IO.never[Unit]).timeoutTo(5.seconds, IO.unit)
      afterCancellation <- b.state
      // A fresh timer, so the upstream is left alone for another full reset...
      refused <- b.protect(IO.unit).attempt
      _ <- IO.sleep(reset + 1.second)
      // ...and then the circuit can be probed again and recover, which is the whole point.
      _ <- succeed(b)
      recovered <- b.state
    } yield (afterCancellation, refused, recovered)

    TestControl.executeEmbed(program).map { (afterCancellation, refused, recovered) =>
      assertEquals(afterCancellation, CircuitState.Open, "a cancelled probe left the circuit half-open")
      assert(
        refused.left.toOption.exists(_.isInstanceOf[CircuitOpenException]),
        s"the reopened circuit admitted a second probe immediately: $refused"
      )
      assertEquals(recovered, CircuitState.Closed, "the circuit could never recover after a cancelled probe")
    }
  }

  test("a probe cancelled while half-open is being announced releases the claim") {
    // The claim is taken before this publish, and half-open with a claim refuses every later caller
    // with no timer. Suspending the announcement is the gap a disconnect can land in: the call's
    // own finaliser does not exist yet, so nothing else can put the claim back.
    val program = for {
      announced <- Deferred[IO, Unit]
      release <- Deferred[IO, Unit]
      b <- CircuitBreaker.make[IO](upstream, threshold, reset, new HoldingTopic(announced, release))
      _ <- fail(b).replicateA_(3)
      _ <- IO.sleep(reset + 1.second)
      probe <- b.protect(IO.unit).start
      _ <- announced.get
      _ <- probe.cancel
      afterCancel <- b.state
      refused <- b.protect(IO.unit).attempt
      _ <- release.complete(())
      _ <- IO.sleep(reset + 1.second)
      recovered <- b.protect(IO.pure(1)).attempt
    } yield (afterCancel, refused, recovered)

    TestControl.executeEmbed(program).map { (afterCancel, refused, recovered) =>
      assertEquals(afterCancel, CircuitState.Open, "a cancelled announcement left the probe claim held")
      assert(
        refused.left.toOption.exists(_.isInstanceOf[CircuitOpenException]),
        s"the reopened circuit admitted a second probe immediately: $refused"
      )
      assertEquals(recovered, Right(1), "the circuit never admitted a probe again after the leaked claim")
    }
  }

  test("publishing a transition never waits for a subscriber that stopped reading") {
    // `subscribe(n)` would give each subscriber a bounded, backpressuring queue: once full, the
    // publisher blocks. The publisher here is the request fiber that was calling the upstream,
    // holding a bulkhead permit, so a stalled subscriber would not miss transitions — it would
    // stop every in-flight call to that upstream.
    val fastToOpen = CircuitBreaker.make[IO](upstream, PositiveInt.unsafe(1), reset)

    val program = for {
      b <- fastToOpen
      // A subscriber that subscribes and then never reads another element.
      stalled <- b.events.evalMap(_ => IO.never[Unit]).compile.drain.start
      _ <- IO.sleep(1.second)
      // Three transitions per cycle (Open, HalfOpen, Closed), well past any plausible bound.
      _ <- (fail(b) *> IO.sleep(reset + 1.second) *> succeed(b)).replicateA_(20)
      _ <- stalled.cancel
      state <- b.state
    } yield state

    TestControl.executeEmbed(program).map(state => assertEquals(state, CircuitState.Closed))
  }

  test("the state has a numeric form, so the gauge can be graphed") {
    assertEquals(CircuitState.Closed.code, 0L)
    assertEquals(CircuitState.Open.code, 1L)
    assertEquals(CircuitState.HalfOpen.code, 2L)
  }
}

/** Publishes every transition immediately, except the half-open announcement, which waits.
  *
  * That announcement is the step `admit` runs after it has already taken the probe claim and before the
  * call's finaliser exists.
  */
final private class HoldingTopic(announced: Deferred[IO, Unit], release: Deferred[IO, Unit])
    extends Topic[IO, CircuitEvent] {

  def publish: Pipe[IO, CircuitEvent, Nothing] = _ => Stream.empty

  def publish1(event: CircuitEvent): IO[Either[Topic.Closed, Unit]] =
    if event.state == CircuitState.HalfOpen then announced.complete(()).void *> release.get.as(Right(()))
    else IO.pure(Right(()))

  def subscribe(maxQueued: Int): Stream[IO, CircuitEvent] = Stream.empty

  def subscribeAwait(maxQueued: Int): Resource[IO, Stream[IO, CircuitEvent]] =
    Resource.pure(Stream.empty)

  def subscribers: Stream[IO, Int] = Stream.empty

  def close: IO[Either[Topic.Closed, Unit]] = IO.pure(Right(()))

  def isClosed: IO[Boolean] = IO.pure(false)

  def closed: IO[Unit] = IO.never
}
