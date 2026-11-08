package kui.http.sse

import scala.concurrent.duration.DurationInt

import cats.effect.IO
import cats.effect.kernel.{Deferred, Ref}
import fs2.Stream
import io.circe.Json
import munit.CatsEffectSuite
import sttp.capabilities.fs2.Fs2Streams
import sttp.tapir.*
import sttp.tapir.json.circe.jsonBody
import sttp.tapir.server.ServerEndpoint

import kui.contracts.ErrorEnvelope
import kui.contracts.ErrorEnvelope.given
import kui.contracts.sse.DoneReason
import kui.http.TestServer
import kui.observability.Telemetry
import kui.testkit.fakes.FakeStructuredLogger

/** That a stream stops costing anything the moment the client goes away.
  *
  * This is the one property that cannot be checked without a real socket, and it is the one that matters most
  * operationally. If a closed browser tab left its producer running, every tab anyone ever opened would leak
  * a Kafka consumer, and the service would die of exhaustion hours later with nothing in the log to explain
  * it.
  *
  * Cancellation itself was measured at 8 ms on this server before Netty was adopted, which is why there is no
  * idle-timeout guard anywhere in `libs/http`. What this suite adds is that KUI's own wrapping — the buffer,
  * the heartbeats, the metrics — does not break that chain.
  */
final class SseCancellationSuite extends CatsEffectSuite {

  /** A stream that never ends on its own, and records when it was torn down. */
  private def endless(finalised: Deferred[IO, Unit], emitted: Ref[IO, Int]): Stream[IO, SseEvent] =
    Stream
      .awakeEvery[IO](50.milliseconds)
      .evalMap(_ => emitted.update(_ + 1).as(SseEvent.data("row", Json.obj())))
      .onFinalize(finalised.complete(()).void)

  private def endpointFor(source: Stream[IO, SseEvent]): IO[ServerEndpoint[Fs2Streams[IO], IO]] =
    FakeStructuredLogger[IO].map { logger =>
      endpoint.get
        .in("stream")
        .out(Sse.body[IO])
        .errorOut(jsonBody[ErrorEnvelope])
        .name("stream")
        .serverLogicSuccess[IO] { _ =>
          IO.pure(
            Sse.encode(
              Sse.stream[IO](
                source,
                SseConfig.default.copy(heartbeatInterval = 200.milliseconds),
                "capabilities",
                Telemetry.noop[IO],
                logger
              )
            )
          )
        }
    }

  test("clientDisconnectCancelsTheSourceWithinOneElement") {
    val program = for {
      finalised <- Deferred[IO, Unit]
      emitted <- Ref.of[IO, Int](0)
      endpoint <- endpointFor(endless(finalised, emitted))
      torn <- TestServer.resource(List(endpoint)).use { server =>
        for {
          // Read for a moment, then abandon the request. Cancelling the effect is what a browser
          // closing a tab does to the connection underneath.
          reading <- server.get("/stream").start
          // An emission is the proof the request started streaming. A fixed sleep can expire
          // before that, and the counts below then stay zero without a disconnect ever happening.
          _ <- emitted.get.iterateUntil(_ > 0).timeout(5.seconds)
          _ <- reading.cancel
          // The source registers a finaliser; the flag flipping is the source itself being torn
          // down, not merely the response being abandoned. `timeout` fails the wait, whereas a
          // successful fallback would make the attempt succeed either way.
          cancelled <- finalised.get.timeout(2.seconds).attempt
          _ <- IO.sleep(100.milliseconds)
          before <- emitted.get
          _ <- IO.sleep(300.milliseconds)
          after <- emitted.get
        } yield (cancelled, before, after)
      }
    } yield torn

    program.map { (cancelled, before, after) =>
      assert(
        cancelled.isRight,
        s"the source was never finalised after the client went away: $cancelled"
      )
      assertEquals(after, before, "the source kept producing after the client had disconnected")
    }
  }

  test("streamActiveGaugeReturnsToZeroAfterDisconnect") {
    // The leak detector. The gauge is incremented on subscribe and decremented in a resource
    // finaliser, so an open-streams count that never comes back down is the shape a leak has.
    val program = for {
      opened <- Ref.of[IO, Int](0)
      finalised <- Deferred[IO, Unit]
      emitted <- Ref.of[IO, Int](0)
      logger <- FakeStructuredLogger[IO]
      counted = Stream
        .resource(
          cats.effect.kernel.Resource.make(opened.update(_ + 1))(_ => opened.update(_ - 1))
        )
        .flatMap(_ => endless(finalised, emitted))
      _ <- Sse
        .stream[IO](counted, SseConfig.default, "capabilities", Telemetry.noop[IO], logger)
        .take(3)
        .compile
        .drain
      after <- opened.get
    } yield after

    program.map(after => assertEquals(after, 0, "an open stream was left behind"))
  }

  test("serverShutdownEndsOpenStreamsCleanly") {
    val program = for {
      finalised <- Deferred[IO, Unit]
      emitted <- Ref.of[IO, Int](0)
      endpoint <- endpointFor(endless(finalised, emitted))
      _ <- TestServer.resource(List(endpoint)).use { server =>
        server.get("/stream").start *> IO.sleep(300.milliseconds)
      }
      // The resource has closed, which stops the server. Any stream still open has to end with it
      // rather than hold the process open.
      cancelled <- finalised.get.timeout(2.seconds).attempt
    } yield cancelled

    program.map { cancelled =>
      assert(cancelled.isRight, s"a stream outlived the server that served it: $cancelled")
    }
  }

  test("a terminating stream ends the response rather than hanging on a heartbeat") {
    val finite = Stream.emits(
      List(SseEvent.data("row", Json.obj()), SseEvent.done(DoneReason.Exhausted, Some("cursor-1")))
    )

    endpointFor(finite).flatMap { endpoint =>
      TestServer.resource(List(endpoint)).use { server =>
        server.get("/stream").map { response =>
          assertEquals(response.code.code, 200)
          assertEquals(
            response.header("Content-Type").map(_.takeWhile(_ != ';')),
            Some("text/event-stream")
          )
          // The bytes on the wire are the golden format, produced by a real server this time.
          assertEquals(
            response.body,
            "event: row\ndata: {}\n\n" +
              "event: done\nid: cursor-1\ndata: " +
              """{"reason":"exhausted","cursor":"cursor-1"}""" + "\n\n"
          )
        }
      }
    }
  }
}
