package kui.http.upstream

import scala.concurrent.duration.DurationInt

import cats.data.NonEmptyList
import cats.effect.testkit.TestControl
import cats.effect.{IO, Ref}
import cats.syntax.all.*
import munit.CatsEffectSuite
import sttp.capabilities.StreamMaxLengthExceededException
import sttp.client4.*
import sttp.client4.impl.cats.implicits.*
import sttp.client4.testing.{BackendStub, StubBody}
import sttp.model.{Method, StatusCode}

import kui.kernel.PositiveInt
import kui.kernel.error.{InfrastructureError, KuiError}

/** That one slow or dead upstream cannot take a KUI process down, starve another upstream, or hide the fact
  * that it is failing.
  *
  * This is the mechanical half of the product's central promise (PLAN §2.1), so the assertions are mostly
  * about what did *not* happen: calls that never reached the network, concurrency that never exceeded its
  * cap, latency that did not move. Every case uses `TestControl`, so the delays the assertions depend on are
  * simulated: no case waits a real second for a real timeout.
  *
  * ==The flaky case, and what was actually wrong with it==
  *
  * "one log line per circuit transition" failed roughly once in five whole-repository runs and never on its
  * own, and its timeout was raised to three minutes as a mitigation. That was never a diagnosis, and the
  * diagnosis turned out not to be slowness at all: the case took eighteen milliseconds of simulated time, so
  * no amount of machine load could push it past a thirty-second wall clock.
  *
  * What it was racing was a subscription. `UpstreamClient` used to write its circuit-transition log lines
  * from a background fiber subscribed to an `fs2.Topic`, started with `.background` — which returns once the
  * fiber has been *started*, not once the stream has subscribed. `Topic` delivers only to subscribers that
  * already exist, so whether the line was written depended on whether that fiber got a turn before the third
  * failing call tripped the breaker. The case papered over it with a one-second sleep before reading the log,
  * which is ordering by hope; a scheduler that ran the fibers in a different order lost the event, and the
  * case then waited for a line that was never going to arrive.
  *
  * The fix is in the product, not in the test: `CircuitBreaker.subscribed` registers the subscription during
  * resource acquisition, so the client cannot be handed out before something is listening. That closes a real
  * defect as well as the flake — an upstream that is already down when KUI starts trips its circuit during
  * start-up, and that transition used to be logged to nobody.
  *
  * That closed the lost-event half of the problem and the raised timeout went with it. It did **not** close
  * all of it, and a whole-repository run on 2026-09-04 failed the second of the two cases again:
  * `expected 1, obtained 0`. The residual race is a different one. Nothing is lost any more, but the log line
  * is *written* by the background fiber that drains the subscription, and a test that calls `logger.entries`
  * in the same breath as the last failing request is asking what the log said before the writer has had a
  * turn. Whether it had one is up to the scheduler.
  *
  * So both cases now put an `IO.sleep(1.second)` between the requests and the read. That is not the old
  * "order by hope" sleep: every case here runs under `TestControl`, where time is simulated, so the second
  * costs no wall clock, and `TestControl` only advances the clock once every runnable fiber has gone idle —
  * which means the drain fiber is *guaranteed* to have run before the read, rather than likely to have.
  */
final class UpstreamClientSuite extends CatsEffectSuite {

  private val target = uri"http://ignored/subjects"

  private def request(method: Method) = basicRequest.method(method, target)

  // ---------------------------------------------------------------------------------------------
  // Retry
  // ---------------------------------------------------------------------------------------------

  test("retriesGetButNotPost") {
    // A GET that failed may or may not have reached the upstream, and repeating it changes nothing
    // either way. A POST may have created something, and repeating it may create a second one.
    val table = List(
      Method.GET -> 3,
      Method.HEAD -> 3,
      Method.OPTIONS -> 3,
      Method.POST -> 1,
      Method.PUT -> 1,
      Method.PATCH -> 1,
      Method.DELETE -> 1
    )

    table.traverse { (method, expectedAttempts) =>
      val program = UpstreamFixture.recording(ResponseKind.Refused).flatMap { stub =>
        UpstreamFixture.client(UpstreamFixture.single(), stub.backend).use { client =>
          request(method).send(client.backend).attempt *> stub.calls.map(_ -> method)
        }
      }

      TestControl.executeEmbed(program).map { (attempts, m) =>
        assertEquals(attempts, expectedAttempts, clue = s"$m made $attempts attempts")
      }
    }.void
  }

  test("doesNotRetryAFourHundredResponse") {
    // The upstream answered. Asking again gets the same answer and wastes the caller's budget.
    val program = UpstreamFixture.recording(ResponseKind.Status(400)).flatMap { stub =>
      UpstreamFixture.client(UpstreamFixture.single(), stub.backend).use { client =>
        request(Method.GET).send(client.backend) *> stub.calls
      }
    }

    TestControl.executeEmbed(program).map(attempts => assertEquals(attempts, 1))
  }

  test("retriesAConfiguredRetryableStatus") {
    // Kafka Connect answers 409 while it is rebalancing, and it means "ask me again shortly". Only
    // a status the caller named is treated that way; nothing is guessed.
    val config = UpstreamFixture.single().copy(retryableStatuses = Set(409))

    val program = UpstreamFixture.recording(ResponseKind.Status(409)).flatMap { stub =>
      UpstreamFixture.client(config, stub.backend).use { client =>
        request(Method.GET).send(client.backend) *> stub.calls
      }
    }

    TestControl.executeEmbed(program).map(attempts => assertEquals(attempts, 3))
  }

  test("a retryable status on a non-idempotent method is still not retried") {
    val config = UpstreamFixture.single().copy(retryableStatuses = Set(409))

    val program = UpstreamFixture.recording(ResponseKind.Status(409)).flatMap { stub =>
      UpstreamFixture.client(config, stub.backend).use { client =>
        request(Method.POST).send(client.backend) *> stub.calls
      }
    }

    TestControl.executeEmbed(program).map(attempts => assertEquals(attempts, 1))
  }

  test("backoff is full jitter, so a recovering upstream is not hit by every client at once") {
    val base = 100.milliseconds

    // The wait is drawn from the whole interval rather than being a fixed backoff with a wobble.
    assertEquals(RetryPolicy.backoff(0, base, random = 1.0), base)
    assertEquals(RetryPolicy.backoff(0, base, random = 0.0), 0.milliseconds)
    assertEquals(RetryPolicy.backoff(1, base, random = 1.0), 200.milliseconds)
    assertEquals(RetryPolicy.backoff(3, base, random = 1.0), 800.milliseconds)
    // And it stops growing, so a long-lived client cannot end up waiting for minutes.
    assert(RetryPolicy.backoff(50, base, random = 1.0) <= 30.seconds)
  }

  // ---------------------------------------------------------------------------------------------
  // Bulkhead
  // ---------------------------------------------------------------------------------------------

  test("bulkheadCapsConcurrency") {
    val config = UpstreamFixture.single().copy(maxConcurrent = PositiveInt.unsafe(4))

    val program = UpstreamFixture.recording(ResponseKind.Slow(1.second, ResponseKind.Ok)).flatMap { stub =>
      UpstreamFixture.client(config, stub.backend).use { client =>
        for {
          _ <- List.fill(100)(request(Method.GET).send(client.backend).attempt).parSequence
          peak <- stub.peakInFlight
        } yield peak
      }
    }

    TestControl.executeEmbed(program).map { peak =>
      assert(peak <= 4, s"$peak calls were in flight at once, the cap is 4")
      assert(peak > 0, "no call reached the backend at all")
    }
  }

  test("a call turned away by a full bulkhead fails fast rather than queueing") {
    // Queueing in front of a slow upstream does not reduce the load, it hides it, and the requests
    // that eventually get through are answering questions the user stopped caring about.
    val config = UpstreamFixture.single().copy(maxConcurrent = PositiveInt.unsafe(1))

    val program = UpstreamFixture.recording(ResponseKind.Slow(5.seconds, ResponseKind.Ok)).flatMap { stub =>
      UpstreamFixture.client(config, stub.backend).use { client =>
        for {
          slow <- request(Method.POST).send(client.backend).attempt.start
          _ <- IO.sleep(100.milliseconds)
          turnedAway <- request(Method.POST).send(client.backend).attempt.timed
          _ <- slow.joinWithNever
        } yield turnedAway
      }
    }

    TestControl.executeEmbed(program).map { (elapsed, outcome) =>
      assertEquals(elapsed, 0.milliseconds, "the rejected call waited instead of failing fast")
      assertEquals(
        outcome.left.toOption.collect { case UpstreamFailure(e: InfrastructureError.Timeout) => e.afterMs },
        Some(0L)
      )
    }
  }

  test("slowUpstreamDoesNotStarveAnother") {
    // The property PLAN §16.4 asks for, and the reason bulkheads exist at all: two upstreams, one
    // hung, and the healthy one's latency must be untouched.
    val program = for {
      hung <- UpstreamFixture.recording(ResponseKind.Never)
      healthy <- UpstreamFixture.recording(ResponseKind.Ok)
      result <- UpstreamFixture.client(UpstreamFixture.single("slow"), hung.backend).use { slow =>
        UpstreamFixture.client(UpstreamFixture.single("fast"), healthy.backend).use { fast =>
          for {
            saturate <- List.fill(50)(request(Method.GET).send(slow.backend).attempt).parSequence.start
            _ <- IO.sleep(1.second)
            timing <- request(Method.GET).send(fast.backend).timed
            _ <- saturate.cancel
          } yield timing
        }
      }
    } yield result

    TestControl.executeEmbed(program).map { (elapsed, response) =>
      assertEquals(response.code, StatusCode.Ok)
      assertEquals(elapsed, 0.milliseconds, "the healthy upstream was slowed by the hung one")
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Timeout
  // ---------------------------------------------------------------------------------------------

  test("timeoutCancelsTheUnderlyingCall") {
    val config = UpstreamFixture.single().copy(callTimeout = 2.seconds)

    val program = UpstreamFixture.recording(ResponseKind.Never).flatMap { stub =>
      UpstreamFixture.client(config, stub.backend).use { client =>
        for {
          outcome <- request(Method.POST).send(client.backend).attempt.timed
          started <- stub.peakInFlight
          // The stub decrements its *live* in-flight count in a `bracket` finaliser, so this
          // reaching zero is the stub observing the cancellation rather than the call merely being
          // abandoned by the caller with the effect still running. The high-water mark cannot say
          // that: it only ever goes up.
          stillRunning <- stub.inFlight
          calls <- stub.calls
        } yield (outcome, started, stillRunning, calls)
      }
    }

    TestControl.executeEmbed(program).map { case ((elapsed, outcome), peak, stillRunning, calls) =>
      assertEquals(elapsed, 2.seconds)
      assertEquals(calls, 1)
      assertEquals(peak, 1)
      assertEquals(stillRunning, 0, "the timeout returned but left the upstream call running")
      assertEquals(
        outcome.left.toOption.collect { case UpstreamFailure(e: InfrastructureError.Timeout) => e.afterMs },
        Some(2000L)
      )
    }
  }

  test("whole-call deadlines open the circuit while caller cancellation does not") {
    val config =
      UpstreamFixture.single().copy(callTimeout = 1.second, failureThreshold = PositiveInt.unsafe(2))
    TestControl.executeEmbed {
      UpstreamFixture.recording(ResponseKind.Never).flatMap { stub =>
        UpstreamFixture.client(config, stub.backend).use { client =>
          for {
            canceled <- request(Method.GET).send(client.backend).start
            _ <- IO.sleep(1.millisecond) >> canceled.cancel
            afterCancel <- client.currentState
            _ <- request(Method.GET).send(client.backend).attempt.replicateA_(2)
            afterTimeouts <- client.currentState
          } yield {
            assertEquals(afterCancel, CircuitState.Closed)
            assertEquals(afterTimeouts, CircuitState.Open)
          }
        }
      }
    }
  }

  test("the timeout bounds the whole call, not each attempt") {
    // A caller told "at most ten seconds" gets ten seconds, not ten seconds times the retry count.
    val config = UpstreamFixture.single().copy(callTimeout = 3.seconds, maxRetries = 5)

    val program =
      UpstreamFixture.recording(ResponseKind.Slow(2.seconds, ResponseKind.Refused)).flatMap { stub =>
        UpstreamFixture.client(config, stub.backend).use { client =>
          request(Method.GET).send(client.backend).attempt.timed
        }
      }

    TestControl.executeEmbed(program).map((elapsed, _) => assertEquals(elapsed, 3.seconds))
  }

  // ---------------------------------------------------------------------------------------------
  // Failover
  // ---------------------------------------------------------------------------------------------

  test("a refused address is stepped over and the next one answers") {
    val config = UpstreamFixture.config(
      "registry",
      NonEmptyList.of(
        UpstreamFixture.url("http://registry-a:8081"),
        UpstreamFixture.url("http://registry-b:8081")
      )
    )

    val program = UpstreamFixture.recording(ResponseKind.Ok).flatMap { stub =>
      stub.setFor("registry-a", ResponseKind.Refused) *>
        UpstreamFixture.client(config, stub.backend).use { client =>
          for {
            response <- request(Method.POST).send(client.backend)
            hosts <- stub.hosts
          } yield (response.code, hosts)
        }
    }

    TestControl.executeEmbed(program).map { (status, hosts) =>
      assertEquals(status, StatusCode.Ok)
      assertEquals(hosts, List("registry-a", "registry-b"))
    }
  }

  test("a connection reset cannot replay a mutation on the next address") {
    val config = UpstreamFixture
      .config(
        "registry",
        NonEmptyList.of(
          UpstreamFixture.url("http://registry-a:8081"),
          UpstreamFixture.url("http://registry-b:8081")
        )
      )
      .copy(maxRetries = 0)
    List(Method.POST -> 1, Method.PUT -> 1, Method.DELETE -> 1, Method.GET -> 2).traverse_ {
      (method, expected) =>
        for {
          calls <- Ref.of[IO, Int](0)
          stub = BackendStub[IO](summon[sttp.monad.MonadError[IO]]).whenAnyRequest.thenRespondF { _ =>
            calls.update(_ + 1) >> IO.raiseError[Response[StubBody]](
              new java.net.SocketException("reset after commit")
            )
          }
          _ <- UpstreamFixture
            .client(config, stub)
            .use(client => request(method).send(client.backend).attempt)
          count <- calls.get
        } yield assertEquals(count, expected, method.method)
    }
  }

  test("when every address refuses, the error is Unreachable") {
    val config = UpstreamFixture.config(
      "registry",
      NonEmptyList.of(
        UpstreamFixture.url("http://registry-a:8081"),
        UpstreamFixture.url("http://registry-b:8081")
      )
    )

    val program = UpstreamFixture.recording(ResponseKind.Refused).flatMap { stub =>
      UpstreamFixture.client(config, stub.backend).use { client =>
        request(Method.POST).send(client.backend).attempt
      }
    }

    TestControl.executeEmbed(program).map { outcome =>
      assertEquals(
        outcome.left.toOption.collect { case UpstreamFailure(e: InfrastructureError.Unreachable) =>
          e.upstream
        },
        Some("registry")
      )
    }
  }

  test("an address that answered 500 is not failed over to the next one") {
    // It is reachable and it is answering. Asking the next machine the same question gives the
    // same answer and hides from the operator that the cluster is unwell rather than unreachable.
    val config = UpstreamFixture.config(
      "registry",
      NonEmptyList.of(
        UpstreamFixture.url("http://registry-a:8081"),
        UpstreamFixture.url("http://registry-b:8081")
      )
    )

    val program = UpstreamFixture.recording(ResponseKind.Status(500)).flatMap { stub =>
      UpstreamFixture.client(config, stub.backend).use { client =>
        request(Method.POST).send(client.backend) *> stub.hosts
      }
    }

    TestControl.executeEmbed(program).map(hosts => assertEquals(hosts, List("registry-a")))
  }

  // ---------------------------------------------------------------------------------------------
  // The error table, and what must never be in it
  // ---------------------------------------------------------------------------------------------

  private val config = UpstreamFixture.single("schema-registry")

  private def errorFor(outcome: Either[Throwable, Int]): Option[KuiError] =
    UpstreamClient.errorFor(config, outcome)

  test("the error table of HTTP-003, case by case") {
    assertEquals(errorFor(Right(200)), None)
    assertEquals(errorFor(Right(204)), None)

    assertEquals(errorFor(Right(401)), Some(InfrastructureError.AuthFailed("schema-registry")))
    assertEquals(errorFor(Right(403)), Some(InfrastructureError.AuthFailed("schema-registry")))
    assertEquals(errorFor(Right(500)), Some(InfrastructureError.Upstream("schema-registry", 500)))
    assertEquals(errorFor(Right(418)), Some(InfrastructureError.Upstream("schema-registry", 418)))

    assertEquals(
      errorFor(Left(new java.util.concurrent.TimeoutException("slow"))),
      Some(InfrastructureError.Timeout("schema-registry", config.callTimeout.toMillis))
    )

    assertEquals(
      errorFor(Left(BulkheadFullException("schema-registry"))),
      Some(InfrastructureError.Timeout("schema-registry (bulkhead full)", 0))
    )

    val since = java.time.Instant.parse("2026-09-03T10:11:12Z")
    assertEquals(
      errorFor(Left(CircuitOpenException("schema-registry", since))),
      Some(InfrastructureError.CircuitOpen("schema-registry", since))
    )

    val transportCanary = "https://user:transport-secret@registry.internal"
    val transport = errorFor(Left(new java.net.ConnectException(transportCanary)))
    assertEquals(
      transport,
      Some(InfrastructureError.Unreachable("schema-registry", "ConnectException"))
    )
    assert(!transport.toString.contains("transport-secret"), transport.toString)
  }

  test("a response body limit remains a non-transport contract failure through wrapper exceptions") {
    val limitFailure = new StreamMaxLengthExceededException(64L)
    val wrapped = new RuntimeException("response body canary", limitFailure)

    val error = errorFor(Left(wrapped)).getOrElse(fail("expected a classified response limit failure"))

    assert(error.isInstanceOf[InfrastructureError.Remote], error.toString)
    assertEquals(error.code, kui.kernel.error.ErrorCode.UpstreamUnavailable)
    assert(!error.message.contains("64"), error.message)
    assert(!error.message.contains("canary"), error.message)
  }

  test("a response body limit is terminal, circuit-neutral and identified without display-message matching") {
    val configured = UpstreamFixture
      .single("schema-registry")
      .copy(
        failureThreshold = PositiveInt.unsafe(2),
        maxRetries = 2
      )
    val forged = InfrastructureError.Remote(
      kui.kernel.error.ErrorCode.UpstreamUnavailable,
      "unrelated response exceeded its configured size limit",
      Nil
    )

    val program = for {
      calls <- Ref.of[IO, Int](0)
      backend = BackendStub[IO](summon[sttp.monad.MonadError[IO]]).whenAnyRequest.thenRespondF { _ =>
        calls.update(_ + 1) *>
          IO.raiseError[Response[StubBody]](new StreamMaxLengthExceededException(64L))
      }
      observed <- UpstreamFixture.client(configured, backend).use { client =>
        for {
          outcomes <- request(Method.GET).send(client.backend).attempt.replicateA(3)
          attempts <- calls.get
          state <- client.currentState
        } yield (outcomes, attempts, state)
      }
    } yield observed

    TestControl.executeEmbed(program).map { case (outcomes, attempts, state) =>
      assertEquals(attempts, 3, "a terminal body-limit failure was retried")
      assertEquals(state, CircuitState.Closed)
      assert(outcomes.forall(_.left.exists(UpstreamClient.isResponseLimitFailure)), outcomes.toString)
      assert(!UpstreamClient.isResponseLimitFailure(UpstreamFailure(forged)))
    }
  }

  test("upstreamBodyIsNotIncludedInTheError") {
    // ADR-034: an upstream's body can carry its own internal detail, or its credentials. The type
    // carries a status and a name and has nowhere to put a body, which is what makes the rule
    // impossible to break rather than merely discouraged.
    val program = UpstreamFixture.recording(ResponseKind.Status(500)).flatMap { stub =>
      UpstreamFixture.client(config, stub.backend).use { client =>
        request(Method.POST).send(client.backend).map(response => response.code.code)
      }
    }

    TestControl.executeEmbed(program).map { status =>
      val error = errorFor(Right(status))
      assertEquals(error.map(_.message), Some("schema-registry answered with status 500"))
      assertEquals(error.map(_.details), Some(Nil))
      assert(!error.map(_.message).exists(_.contains("detail")), error.toString)
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Logging and the circuit feed
  // ---------------------------------------------------------------------------------------------

  test("one log line per circuit transition, and none per failed call") {
    // A dead upstream that logs on every attempt floods the log at exactly the moment an operator
    // needs to read it. This is the flooding footgun the reference implementation has.
    val config = UpstreamFixture.single().copy(failureThreshold = PositiveInt.unsafe(3), maxRetries = 0)

    val program = UpstreamFixture.recording(ResponseKind.Refused).flatMap { stub =>
      UpstreamFixture.clientAndLog(config, stub.backend).use { (client, logger) =>
        // The subscription is in place before `use` runs, so every transition published here has
        // somewhere to go — nothing can be lost. The line is still *written* by a background fiber,
        // so the read has to let that fiber run: `IO.sleep` under `TestControl` is simulated time,
        // which costs no wall clock and, unlike a bare read, cannot come out differently depending
        // on which fiber the scheduler happened to pick first.
        request(Method.POST).send(client.backend).attempt.replicateA_(20) *>
          IO.sleep(1.second) *> logger.entries
      }
    }

    TestControl.executeEmbed(program).map { entries =>
      val transitions = entries.filter(_.context.contains("upstream"))
      assertEquals(transitions.size, 1, s"expected one transition line, got ${transitions.map(_.message)}")
      assertEquals(transitions.head.level, "info")
      assertEquals(transitions.head.context.get("state"), Some("open"))
      assertEquals(transitions.head.context.get("error.last"), Some("connection"))
    }
  }

  test("aCircuitThatOpensImmediatelyAfterStartUpIsStillLogged") {
    // The same fact as the test above with the waiting taken out. `UpstreamClient.resource` starts the
    // log-writing subscription in the background, and `Topic` delivers only to subscribers that are
    // already there — so an upstream that is dead the moment KUI starts can trip its breaker before the
    // subscription exists, and the one line an operator needs is never written.
    val config = UpstreamFixture.single().copy(failureThreshold = PositiveInt.unsafe(3), maxRetries = 0)

    val program = UpstreamFixture.recording(ResponseKind.Refused).flatMap { stub =>
      UpstreamFixture.clientAndLog(config, stub.backend).use { (client, logger) =>
        request(Method.POST).send(client.backend).attempt.replicateA_(3) *>
          IO.sleep(1.second) *> logger.entries
      }
    }

    TestControl.executeEmbed(program).map { entries =>
      assertEquals(
        entries.filter(_.context.contains("upstream")).size,
        1,
        "the circuit opened and nothing was logged; the subscription was not in place yet"
      )
    }
  }

  test("circuitStates feeds the transitions the capability registry subscribes to") {
    val config = UpstreamFixture.single().copy(failureThreshold = PositiveInt.unsafe(2), maxRetries = 0)

    val program = UpstreamFixture.recording(ResponseKind.Refused).flatMap { stub =>
      UpstreamFixture.client(config, stub.backend).use { client =>
        for {
          collected <- client.circuitStates.take(1).compile.toList.start
          _ <- IO.sleep(1.second)
          _ <- request(Method.POST).send(client.backend).attempt.replicateA_(2)
          events <- collected.joinWithNever
          state <- client.currentState
        } yield (events, state)
      }
    }

    TestControl.executeEmbed(program).map { (events, state) =>
      assertEquals(events.map(_.state), List(CircuitState.Open))
      assertEquals(events.map(_.upstream), List("registry"))
      assertEquals(state, CircuitState.Open)
    }
  }

  test("an open circuit refuses without touching the network") {
    val config = UpstreamFixture.single().copy(failureThreshold = PositiveInt.unsafe(2), maxRetries = 0)

    val program = UpstreamFixture.recording(ResponseKind.Refused).flatMap { stub =>
      UpstreamFixture.client(config, stub.backend).use { client =>
        for {
          _ <- request(Method.POST).send(client.backend).attempt.replicateA_(2)
          before <- stub.calls
          outcome <- request(Method.POST).send(client.backend).attempt
          after <- stub.calls
        } yield (before, after, outcome)
      }
    }

    TestControl.executeEmbed(program).map { (before, after, outcome) =>
      assertEquals(after, before, "a call reached the network while the circuit was open")
      assertEquals(
        outcome.left.toOption.collect { case UpstreamFailure(e: InfrastructureError.CircuitOpen) =>
          e.upstream
        },
        Some("registry")
      )
    }
  }
}
