package kui.http.upstream

import java.net.{InetAddress, ServerSocket, Socket}
import java.nio.charset.StandardCharsets

import scala.concurrent.duration.DurationInt

import cats.effect.{Deferred, IO, Resource}
import sttp.capabilities.fs2.Fs2Streams
import sttp.client4.{asStreamAlwaysUnsafe, basicRequest}
import sttp.model.Uri

import kui.config.{HttpTlsConfig, UrlPolicy}
import kui.testkit.KuiIOSuite

final class TransportCancellationSuite extends KuiIOSuite {
  test("canceling before response headers closes the active socket, not just the waiting fiber") {
    listener.use { server =>
      for {
        received <- Deferred[IO, Unit]
        disconnected <- Deferred[IO, Int]
        _ <- peer(server, received, disconnected).background.use { _ =>
          HttpTls.resource[IO](HttpTlsConfig.Default, UrlPolicy.Dev).use { backend =>
            val uri = Uri.unsafeParse(s"http://127.0.0.1:${server.getLocalPort}/hang")
            for {
              fiber <- basicRequest.get(uri).send(backend).start
              _ <- received.get.timeout(5.seconds)
              _ <- fiber.cancel.timeout(5.seconds)
              eof <- disconnected.get.timeout(5.seconds)
            } yield assertEquals(eof, -1)
          }
        }
      } yield ()
    }
  }

  test("request readTimeout closes a socket stalled before headers") {
    listener.use { server =>
      for {
        received <- Deferred[IO, Unit]
        disconnected <- Deferred[IO, Int]
        _ <- peer(server, received, disconnected).background.use { _ =>
          HttpTls.resource[IO](HttpTlsConfig.Default, UrlPolicy.Dev).use { backend =>
            val uri = Uri.unsafeParse(s"http://127.0.0.1:${server.getLocalPort}/hang")
            for {
              result <- basicRequest.get(uri).readTimeout(200.millis).send(backend).attempt.timeout(2.seconds)
              eof <- disconnected.get.timeout(5.seconds)
            } yield {
              assert(result.isLeft)
              assertEquals(eof, -1)
            }
          }
        }
      } yield ()
    }
  }

  test("an unsafe FS2 body streams before EOF and canceling its consumer closes the socket") {
    listener.use { server =>
      for {
        received <- Deferred[IO, Unit]
        disconnected <- Deferred[IO, Int]
        firstByte <- Deferred[IO, Byte]
        _ <- peer(server, received, disconnected, streaming = true).background.use { _ =>
          HttpTls.resource[IO](HttpTlsConfig.Default, UrlPolicy.Dev).use { backend =>
            val uri = Uri.unsafeParse(s"http://127.0.0.1:${server.getLocalPort}/stream")
            for {
              response <- basicRequest.get(uri).response(asStreamAlwaysUnsafe(Fs2Streams[IO])).send(backend)
              fiber <- response.body.evalTap(byte => firstByte.complete(byte).void).compile.drain.start
              byte <- firstByte.get.timeout(5.seconds)
              _ <- IO(assertEquals(byte, 'x'.toByte))
              _ <- fiber.cancel.timeout(5.seconds)
              eof <- disconnected.get.timeout(5.seconds)
            } yield assertEquals(eof, -1)
          }
        }
      } yield ()
    }
  }

  private def peer(
      server: ServerSocket,
      received: Deferred[IO, Unit],
      disconnected: Deferred[IO, Int],
      streaming: Boolean = false
  ): IO[Unit] =
    Resource.make(IO.blocking(server.accept()))(socket => IO.blocking(socket.close())).use { socket =>
      IO.blocking {
        socket.setSoTimeout(3000)
        readHeaders(socket)
        if streaming then {
          socket.getOutputStream.write(
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nx\r\n"
              .getBytes(StandardCharsets.US_ASCII)
          )
          socket.getOutputStream.flush()
        }
      } *> received
        .complete(())
        .void *> IO.blocking(socket.getInputStream.read()).flatMap(disconnected.complete).void
    }

  private def readHeaders(socket: Socket): Unit = {
    val input = socket.getInputStream
    val bytes = new java.io.ByteArrayOutputStream()
    Iterator
      .continually(input.read())
      .takeWhile(_ >= 0)
      .takeWhile { byte =>
        bytes.write(byte)
        !bytes.toString(StandardCharsets.US_ASCII).endsWith("\r\n\r\n")
      }
      .foreach(_ => ())
  }

  private def listener: Resource[IO, ServerSocket] =
    Resource.make(IO.blocking(new ServerSocket(0, 10, InetAddress.getByName("127.0.0.1"))))(server =>
      IO.blocking(server.close())
    )
}
