package kui.http.upstream

import scala.concurrent.duration.DurationInt

import cats.effect.std.Queue
import cats.effect.{Deferred, IO, Ref, Resource}
import cats.syntax.all.*
import com.comcast.ip4s.{Ipv4Address, Port, SocketAddress}
import fs2.Chunk
import fs2.io.net.Network
import sttp.capabilities.fs2.Fs2Streams
import sttp.client4.{asStreamAlways, asStreamAlwaysUnsafe, asStreamUnsafe, asStringAlways, basicRequest}
import sttp.model.Uri

import kui.config.{HttpTlsConfig, UrlPolicy}
import kui.testkit.KuiIOSuite

final class TransportCapacitySuite extends KuiIOSuite {
  test("ten live streamed responses do not starve a finite request on the same authority and path") {
    fixture.use { server =>
      HttpTls.resource[IO](HttpTlsConfig.Default, UrlPolicy.Dev).use { backend =>
        for {
          bodies <- List.fill(10)(()).traverse { _ =>
            basicRequest
              .get(server.uri)
              .response(asStreamAlwaysUnsafe(Fs2Streams[IO]).map(identity))
              .send(backend)
              .map(_.body)
          }
          peers <- List.fill(10)(()).traverse(_ => server.peers.take)
          consumers <- bodies.traverse { body =>
            for {
              seen <- Deferred[IO, Unit]
              consuming <- body.evalTap(_ => seen.complete(()).void).compile.drain.start
              _ <- seen.get.timeout(2.seconds)
            } yield consuming
          }
          result <- basicRequest
            .get(server.uri)
            .header("X-Finite", "true")
            .readTimeout(1.second)
            .send(backend)
            .guarantee(peers.traverse_(_.finish.complete(())) *> consumers.parTraverse_(_.joinWithNever))
          _ <- peers.traverse_(_.disconnected.get.timeout(3.seconds))
        } yield assertEquals(result.body, Right("ok"))
      }
    }
  }

  test("shutdown closes ten active sockets after an eleventh acquisition times out") {
    fixture.use { server =>
      for {
        allocated <- HttpTls.resource[IO](HttpTlsConfig.Default, UrlPolicy.Dev).allocated
        (backend, release) = allocated
        close <- release.memoize
        active <- List.fill(10)(()).traverse(_ => basicRequest.get(server.uri).send(backend).attempt.start)
        peers <- List.fill(10)(()).traverse(_ => server.peers.take)
        _ <- (for {
          queued <- basicRequest
            .get(server.uri)
            .readTimeout(100.millis)
            .send(backend)
            .attempt
            .timeout(2.seconds)
          _ <- IO(assert(queued.isLeft))
          _ <- close.timeout(3.seconds)
          _ <- peers.parTraverse_(_.disconnected.get.timeout(3.seconds))
          _ <- active.parTraverse_(_.join.timeout(3.seconds))
        } yield ())
          .guarantee(peers.traverse_(_.finish.complete(())) *> active.parTraverse_(_.cancel) *> close)
      } yield ()
    }
  }

  test("queued stream cancellation removes owners and permits without later network dispatch") {
    fixture.use { server =>
      managed(HttpTls.Capacity(finite = 1, streaming = 1, queuedPerPool = 2)).use { (backend, diagnostics) =>
        val request = basicRequest.get(server.uri).response(asStreamAlwaysUnsafe(Fs2Streams[IO]))
        for {
          first <- request.send(backend)
          peer <- server.peers.take
          queued <- request.send(backend).start
          _ <- awaitValue(diagnostics.streamingSlots, -1L)
          other <- request.send(backend).start
          _ <- awaitValue(diagnostics.streamingSlots, -2L)
          refused <- request.send(backend).attempt.timeout(2.seconds)
          _ <- IO(assert(refused.isLeft, "bounded admission must reject excess waiting calls"))
          _ <- queued.cancel.timeout(2.seconds)
          _ <- queued.join.map(result => assert(result.isCanceled))
          _ <- awaitValue(diagnostics.streamingSlots, -1L)
          _ <- other.cancel.timeout(2.seconds)
          _ <- awaitValue(diagnostics.streamingSlots, 0L)
          _ <- awaitValue(diagnostics.ownedRequests, 1)
          timed <- request.readTimeout(100.millis).send(backend).attempt.timeout(2.seconds)
          _ <- IO(assert(timed.isLeft))
          _ <- awaitValue(diagnostics.streamingSlots, 0L)
          _ <- awaitValue(diagnostics.ownedRequests, 1)
          _ <- peer.finish.complete(())
          _ <- first.body.compile.drain.timeout(2.seconds)
          _ <- awaitValue(diagnostics.streamingSlots, 1L)
          _ <- awaitValue(diagnostics.ownedRequests, 0)
          barrier <- request.header("X-Finite", "true").send(backend)
          _ <- barrier.body.compile.drain.timeout(2.seconds)
          _ <- awaitValue(diagnostics.ownedRequests, 0)
          requests <- server.requests.get
        } yield assertEquals(requests.size, 2, "canceled and timed-out waiters must never reach the socket")
      }
    }
  }

  test("canceling a consumed conditional stream releases the only streaming permit") {
    fixture.use { server =>
      managed(HttpTls.Capacity(finite = 1, streaming = 1, queuedPerPool = 1)).use { (backend, diagnostics) =>
        for {
          first <- basicRequest
            .get(server.uri)
            .response(asStreamUnsafe(Fs2Streams[IO]).map(identity))
            .send(backend)
          peer <- server.peers.take
          body <- IO.fromEither(first.body.left.map(new RuntimeException(_)))
          seen <- Deferred[IO, Unit]
          consuming <- body.evalTap(_ => seen.complete(()).void).compile.drain.start
          _ <- seen.get.timeout(2.seconds)
          _ <- consuming.cancel.timeout(2.seconds)
          _ <- peer.disconnected.get.timeout(2.seconds)
          _ <- awaitValue(diagnostics.streamingSlots, 1L)
          _ <- awaitValue(diagnostics.ownedRequests, 0)
          next <- basicRequest
            .get(server.uri)
            .header("X-Finite", "true")
            .response(asStreamAlwaysUnsafe(Fs2Streams[IO]))
            .send(backend)
          _ <- next.body.compile.drain.timeout(2.seconds)
          _ <- awaitValue(diagnostics.streamingSlots, 1L)
        } yield ()
      }
    }
  }

  test("safe streamed response handling uses the streaming budget even when its result is finite") {
    fixture.use { server =>
      managed(HttpTls.Capacity(finite = 1, streaming = 1, queuedPerPool = 1)).use { (backend, diagnostics) =>
        for {
          safe <- basicRequest
            .get(server.uri)
            .response(asStreamAlways(Fs2Streams[IO])(_.compile.drain))
            .send(backend)
            .attempt
            .start
          peer <- server.peers.take
          _ <- awaitValue(diagnostics.streamingSlots, 0L)
          finite <- basicRequest
            .get(server.uri)
            .header("X-Finite", "true")
            .response(asStringAlways)
            .send(backend)
            .timeout(2.seconds)
          _ <- peer.finish.complete(())
          result <- safe.joinWithNever.timeout(2.seconds)
          _ <- IO(assert(result.isRight))
          _ <- awaitValue(diagnostics.streamingSlots, 1L)
        } yield assertEquals(finite.body, "ok")
      }
    }
  }

  test("shutdown owns unconsumed unsafe bodies and pending acquisitions; all callback fibers terminate") {
    fixture.use { server =>
      for {
        allocated <- managed(HttpTls.Capacity(finite = 1, streaming = 1, queuedPerPool = 2)).allocated
        ((backend, diagnostics), release) = allocated
        close <- release.memoize
        _ <- (for {
          _ <- basicRequest.get(server.uri).response(asStreamAlwaysUnsafe(Fs2Streams[IO])).send(backend)
          streamPeer <- server.peers.take
          active <- basicRequest.get(server.uri).send(backend).attempt.start
          finitePeer <- server.peers.take
          queued <- basicRequest.get(server.uri).send(backend).attempt.start
          _ <- awaitValue(diagnostics.finiteSlots, -1L)
          queuedStream <- basicRequest
            .get(server.uri)
            .response(asStreamAlwaysUnsafe(Fs2Streams[IO]))
            .send(backend)
            .attempt
            .start
          _ <- awaitValue(diagnostics.streamingSlots, -1L)
          _ <- close.timeout(3.seconds)
          _ <- List(streamPeer, finitePeer).parTraverse_(_.disconnected.get.timeout(2.seconds))
          results <- List(active.joinWithNever, queued.joinWithNever).sequence.timeout(2.seconds)
          streamResult <- queuedStream.joinWithNever.timeout(2.seconds)
          _ <- IO(assert(results.forall(_.isLeft) && streamResult.isLeft))
          _ <- diagnostics.ownedRequests.map(count => assertEquals(count, 0))
          _ <- diagnostics.finiteSlots.map(count => assertEquals(count, 1L))
          _ <- diagnostics.streamingSlots.map(count => assertEquals(count, 1L))
          refused <- basicRequest.get(server.uri).send(backend).attempt.timeout(2.seconds)
          requests <- server.requests.get
        } yield {
          assert(refused.isLeft)
          assertEquals(requests.size, 2)
        }).guarantee(close)
      } yield ()
    }
  }

  test("a conditional finite error response returns its reserved streaming capacity") {
    fixture.use { server =>
      managed(HttpTls.Capacity(finite = 1, streaming = 1, queuedPerPool = 0)).use { (backend, diagnostics) =>
        for {
          response <- basicRequest
            .get(server.uri)
            .header("X-Finite", "true")
            .header("X-Status", "500")
            .response(asStreamUnsafe(Fs2Streams[IO]))
            .send(backend)
          _ <- IO(assertEquals(response.body, Left("ok")))
          _ <- awaitValue(diagnostics.streamingSlots, 1L)
          _ <- awaitValue(diagnostics.ownedRequests, 0)
          active <- basicRequest.get(server.uri).response(asStreamAlwaysUnsafe(Fs2Streams[IO])).send(backend)
          peer <- server.peers.take
          overflow <- basicRequest
            .get(server.uri)
            .response(asStreamAlwaysUnsafe(Fs2Streams[IO]))
            .send(backend)
            .attempt
            .timeout(2.seconds)
          _ <- IO(assert(overflow.isLeft))
          _ <- peer.finish.complete(())
          _ <- active.body.compile.drain
          _ <- awaitValue(diagnostics.streamingSlots, 1L)
        } yield ()
      }
    }
  }

  test("response classification follows both branches and ignores request body capabilities") {
    val stream = asStreamAlwaysUnsafe(Fs2Streams[IO]).delegate
    assert(HttpTls.streamsResponse(sttp.client4.ResponseAsBoth(stream, asStringAlways.delegate)))
    assert(!HttpTls.streamsResponse(asStringAlways.delegate))
    val _ = intercept[IllegalArgumentException](HttpTls.Capacity(finite = 0))
    val _ = intercept[IllegalArgumentException](HttpTls.Capacity(streaming = 0))
    val _ = intercept[IllegalArgumentException](HttpTls.Capacity(queuedPerPool = -1))
  }

  private def managed(capacity: HttpTls.Capacity) =
    HttpTls.resourceWithDiagnostics[IO](
      HttpTlsConfig.Default,
      UrlPolicy.Dev,
      java.net.InetAddress.getAllByName,
      capacity
    )

  // An observed semaphore-state barrier, not a timing delay: no release until the request really queued.
  private def awaitValue[A](read: IO[A], expected: A): IO[Unit] = {
    def loop: IO[Unit] = read.flatMap(value => if value == expected then IO.unit else IO.cede *> loop)
    loop.timeout(3.seconds)
  }

  final private case class Peer(finish: Deferred[IO, Unit], disconnected: Deferred[IO, Unit])
  final private case class Server(uri: Uri, peers: Queue[IO, Peer], requests: Ref[IO, List[String]])

  private def fixture: Resource[IO, Server] =
    for {
      peers <- Resource.eval(Queue.unbounded[IO, Peer])
      requests <- Resource.eval(Ref.of[IO, List[String]](Nil))
      listener <- Network[IO].bind(SocketAddress(Ipv4Address.fromString("127.0.0.1").get, Port.Wildcard))
      _ <- listener.accept
        .map { socket =>
          fs2.Stream.eval {
            for {
              headers <- readHeaders(socket, "").map(_.split("\r\n").toList)
              _ <- requests.update(headers.headOption.getOrElse("") :: _)
              finite = headers.exists(_.equalsIgnoreCase("X-Finite: true"))
              status =
                if headers.exists(_.equalsIgnoreCase("X-Status: 500")) then "500 Internal Server Error"
                else "200 OK"
              _ <-
                if finite then
                  socket.write(bytes(s"HTTP/1.1 $status\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok"))
                else
                  for {
                    finish <- Deferred[IO, Unit]
                    disconnected <- Deferred[IO, Unit]
                    event = "data: x\n\n"
                    _ <- socket.write(
                      bytes(
                        s"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n${event.length.toHexString}\r\n$event\r\n"
                      )
                    )
                    _ <- peers.offer(Peer(finish, disconnected))
                    _ <- (finish.get *> socket.write(bytes("0\r\n\r\n"))).race(socket.read(1)).void
                    _ <- disconnected.complete(())
                  } yield ()
            } yield ()
          }
        }
        .parJoin(64)
        .compile
        .drain
        .background
    } yield Server(
      Uri.unsafeParse(s"http://127.0.0.1:${listener.address.asIpUnsafe.port.value}/same"),
      peers,
      requests
    )

  private def readHeaders(socket: fs2.io.net.Socket[IO], acc: String): IO[String] =
    if acc.endsWith("\r\n\r\n") then IO.pure(acc)
    else
      socket.read(1).flatMap {
        case Some(chunk) => readHeaders(socket, acc + chunk(0).toChar)
        case None => IO.raiseError(new IllegalStateException("peer closed before headers"))
      }

  private def bytes(value: String): Chunk[Byte] =
    Chunk.array(value.getBytes(java.nio.charset.StandardCharsets.US_ASCII))
}
