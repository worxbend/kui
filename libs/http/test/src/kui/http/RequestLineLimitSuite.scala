package kui.http

import cats.effect.IO
import munit.CatsEffectSuite
import sttp.capabilities.fs2.Fs2Streams
import sttp.tapir.*
import sttp.tapir.server.ServerEndpoint

final class RequestLineLimitSuite extends CatsEffectSuite {
  private val browse: ServerEndpoint[Fs2Streams[IO], IO] =
    endpoint.get
      .in("api" / "v1" / "clusters" / "demo" / "topics" / "orders" / "messages" / "stream")
      .in(query[String]("cursor"))
      .out(stringBody)
      .serverLogicSuccess[IO](cursor => IO.pure(cursor.length.toString))

  test("an 8192-byte cursor plus path and framing reaches the endpoint") {
    TestServer.resource(List(browse), basePath = "/kui").use { server =>
      val path = "/kui/api/v1/clusters/demo/topics/orders/messages/stream?cursor="
      for {
        short <- server.get(path + "a")
        response <- server.get(path + "a" * 8192)
      } yield {
        assertEquals(short.code.code, 200)
        assertEquals(short.body, "1")
        assertEquals(response.code.code, 200)
        assertEquals(response.body, "8192")
      }
    }
  }

  test("oversized request lines remain bounded") {
    TestServer.resource(List(browse)).use { server =>
      val path = "/api/v1/clusters/demo/topics/orders/messages/stream?cursor=" + "a" * (32 * 1024)
      server.get(path).map(response => assert(response.code.isClientError))
    }
  }
}
