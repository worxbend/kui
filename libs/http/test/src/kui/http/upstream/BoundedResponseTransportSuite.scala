package kui.http.upstream

import java.io.IOException
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets

import scala.concurrent.duration.DurationInt

import cats.data.NonEmptyList
import cats.effect.{IO, Resource}
import com.sun.net.httpserver.HttpServer
import sttp.client4.*
import sttp.model.Uri

import kui.config.{HttpTlsConfig, UrlPolicy}
import kui.kernel.PositiveInt
import kui.testkit.KuiIOSuite

/** That a byte-bounded response can be read over the transport production actually uses.
  *
  * Every production upstream call goes through [[HttpTls.resource]], which is sttp's `Http4sBackend`. That
  * backend cannot hand a body over as a `java.io.InputStream`, so a bounded reader built on one failed every
  * call it was used for — every ksqlDB pull query — with `UnsupportedOperationException`, and each failure
  * counted towards opening the ksqlDB circuit. `BoundedResponseSuite` drives the reader through
  * `SyncBackendStub`, which supports input streams, and could not see it.
  */
final class BoundedResponseTransportSuite extends KuiIOSuite {

  test("a bounded body is read over the production transport") {
    served(Body.Fixed("x" * 100)).use { uri =>
      HttpTls.resource[IO](HttpTlsConfig.Default, UrlPolicy.Dev).use { backend =>
        BoundedResponse
          .asString(basicRequest.get(uri), 100L)
          .send(backend)
          .map(response => assertEquals(response.body, "x" * 100))
      }
    }
  }

  test("the production transport stops reading an endless body at the ceiling") {
    // Nothing is buffered past the ceiling: an answer that never ends still fails, promptly, as a limit.
    served(Body.Endless).use { uri =>
      HttpTls.resource[IO](HttpTlsConfig.Default, UrlPolicy.Dev).use { backend =>
        BoundedResponse
          .asString(basicRequest.get(uri), 64L * 1024)
          .send(backend)
          .attempt
          .timeout(20.seconds)
          .map { result =>
            assert(result.left.exists(UpstreamClient.isResponseLimitCause), result.toString)
          }
      }
    }
  }

  test("an oversized answer through the resilient client is a limit failure that never opens the circuit") {
    val threshold = 3
    served(Body.Fixed("x" * 200)).use { uri =>
      HttpTls.resource[IO](HttpTlsConfig.Default, UrlPolicy.Dev).use { transport =>
        val config = UpstreamFixture
          .config("ksql.local", NonEmptyList.one(UpstreamFixture.url(uri.toString)))
          .copy(failureThreshold = PositiveInt.unsafe(threshold))
        UpstreamFixture.client(config, transport).use { client =>
          // POST, as a pull query is: not retried, so every call is one breaker outcome.
          val call = BoundedResponse
            .asString(basicRequest.post(uri).body("{}"), 100L)
            .send(client.backend)
          for {
            outcomes <- call.attempt.replicateA(threshold + 1)
            state <- client.currentState
          } yield {
            assert(outcomes.forall(_.left.exists(UpstreamClient.isResponseLimitFailure)), outcomes.toString)
            assertEquals(state, CircuitState.Closed)
          }
        }
      }
    }
  }

  private enum Body {
    case Fixed(text: String)
    case Endless
  }

  private def served(body: Body): Resource[IO, Uri] =
    Resource
      .make(IO.blocking {
        val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
        val _ = server.createContext(
          "/",
          exchange => {
            val _ = exchange.getRequestBody.readAllBytes()
            body match {
              case Body.Fixed(text) =>
                val bytes = text.getBytes(StandardCharsets.UTF_8)
                exchange.sendResponseHeaders(200, bytes.length.toLong)
                exchange.getResponseBody.write(bytes)
              case Body.Endless =>
                exchange.sendResponseHeaders(200, 0L)
                val chunk = Array.fill[Byte](8192)('x'.toByte)
                try while true do exchange.getResponseBody.write(chunk)
                catch { case _: IOException => () }
            }
            exchange.close()
          }
        )
        server.start()
        server
      })(server => IO.blocking(server.stop(0)))
      .map(server => Uri.unsafeParse(s"http://127.0.0.1:${server.getAddress.getPort}/query"))
}
