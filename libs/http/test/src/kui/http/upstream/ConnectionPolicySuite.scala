package kui.http.upstream

import java.net.{InetAddress, InetSocketAddress, ServerSocket, SocketTimeoutException}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import cats.effect.{IO, Resource}
import com.sun.net.httpserver.{HttpExchange, HttpServer}
import org.http4s.client.RequestKey
import sttp.client4.basicRequest
import sttp.model.Uri

import kui.config.{HttpTlsConfig, UrlPolicy}
import kui.testkit.KuiIOSuite

final class ConnectionPolicySuite extends KuiIOSuite {
  test("strict transport refuses a loopback connection before sending HTTP") {
    val requests = new AtomicInteger(0)
    server(requests).use { port =>
      HttpTls.resource[IO](HttpTlsConfig.Default).use { backend =>
        basicRequest.get(Uri.unsafeParse(s"http://127.0.0.1:$port/probe")).send(backend).attempt.map {
          result =>
            assert(result.isLeft, "strict transport connected to loopback")
            assertEquals(requests.get(), 0)
        }
      }
    }
  }

  test("strict transport refuses DNS names resolving to loopback before opening a socket") {
    val requests = new AtomicInteger(0)
    val lookups = new AtomicInteger(0)
    server(requests).use { port =>
      HttpTls
        .resourceWithResolver[IO](
          HttpTlsConfig.Default,
          UrlPolicy.Strict,
          _ => {
            val _ = lookups.incrementAndGet()
            Array(InetAddress.getByName("127.0.0.1"))
          }
        )
        .use { backend =>
          basicRequest
            .get(Uri.unsafeParse(s"http://registry.invalid:$port/probe"))
            .send(backend)
            .attempt
            .map { result =>
              assert(result.isLeft)
              assertEquals(requests.get(), 0)
              assertEquals(lookups.get(), 1)
            }
        }
    }
  }

  test("permitted internal name connects to the resolver's exact address without another lookup") {
    val requests = new AtomicInteger(0)
    val lookups = new AtomicInteger(0)
    val host = new AtomicReference[String]("")
    val auth = new AtomicReference[String]("")
    server(
      requests,
      exchange => {
        host.set(exchange.getRequestHeaders.getFirst("Host"))
        auth.set(exchange.getRequestHeaders.getFirst("Authorization"))
      }
    ).use { port =>
      HttpTls
        .resourceWithResolver[IO](
          HttpTlsConfig.Default,
          UrlPolicy.Dev,
          _ => {
            val count = lookups.incrementAndGet()
            // The authority cannot resolve in system DNS. A second resolver call would select a dead peer.
            Array(InetAddress.getByName(if count == 1 then "127.0.0.1" else "127.0.0.2"))
          }
        )
        .use { backend =>
          basicRequest
            .get(Uri.unsafeParse(s"http://registry.invalid:$port/probe"))
            .header("Authorization", "Bearer fixture-token")
            .send(backend)
            .map { result =>
              assertEquals(result.code.code, 200)
              assertEquals(requests.get(), 1)
              assertEquals(lookups.get(), 1)
              assertEquals(host.get(), s"registry.invalid:$port")
              assertEquals(auth.get(), "Bearer fixture-token")
            }
        }
    }
  }

  test("a fresh connection rechecks a rebinding answer rather than trusting a previous resolution") {
    val requests = new AtomicInteger(0)
    val lookups = new AtomicInteger(0)
    val policy = UrlPolicy.Strict.copy(allowLoopback = true)
    server(requests).use { port =>
      HttpTls
        .resourceWithResolver[IO](
          HttpTlsConfig.Default,
          policy,
          _ => {
            val count = lookups.incrementAndGet()
            Array(InetAddress.getByName(if count == 1 then "127.0.0.1" else "169.254.169.254"))
          }
        )
        .use { backend =>
          val request = basicRequest.get(Uri.unsafeParse(s"http://registry.invalid:$port/probe"))
          for {
            first <- request.send(backend)
            second <- request.send(backend).attempt
          } yield {
            assertEquals(first.code.code, 200)
            assert(second.isLeft)
            assertEquals(requests.get(), 1)
            assertEquals(lookups.get(), 2)
          }
        }
    }
  }

  test("resolved policy rejects every private family and mixed public/private DNS answers") {
    val key = RequestKey.fromRequest(
      org.http4s.Request[IO](uri = org.http4s.Uri.unsafeFromString("https://registry.invalid/probe"))
    )
    val rejected = List(
      "127.0.0.1",
      "0.0.0.0",
      "10.0.0.1",
      "172.16.0.1",
      "192.168.1.1",
      "169.254.169.254",
      "100.64.0.1",
      "::1",
      "::",
      "fc00::1",
      "fe80::1",
      "::ffff:127.0.0.1",
      "224.0.0.1"
    )
    rejected.foreach { value =>
      val address = InetAddress.getByName(value)
      assert(HttpTls.destination(key, UrlPolicy.Strict, _ => Array(address)).isLeft, value)
      assert(
        HttpTls
          .destination(key, UrlPolicy.Strict, _ => Array(InetAddress.getByName("8.8.8.8"), address))
          .isLeft,
        value
      )
    }
    assert(HttpTls.destination(key, UrlPolicy.Strict, _ => Array.empty[InetAddress]).isLeft)
    val lookups = new AtomicInteger(0)
    val resolve: String => Array[InetAddress] = _ => {
      val count = lookups.incrementAndGet()
      Array(InetAddress.getByName(if count == 1 then "8.8.8.8" else "127.0.0.1"))
    }
    val first =
      HttpTls.destination(key, UrlPolicy.Strict, resolve).toOption.getOrElse(fail("public address rejected"))
    assert(!first.isUnresolved)
    assertEquals(first.getAddress.getHostAddress, "8.8.8.8")
    assertEquals(first.getPort, 443)
    assert(HttpTls.destination(key, UrlPolicy.Strict, resolve).isLeft)
    assertEquals(first.getAddress.getHostAddress, "8.8.8.8")
    assertEquals(lookups.get(), 2)
  }

  test("a mixed DNS answer never opens the forbidden listening socket") {
    Resource
      .make(IO.blocking {
        val socket = new ServerSocket(0, 10, InetAddress.getByName("127.0.0.1"))
        socket.setSoTimeout(200)
        socket
      })(socket => IO.blocking(socket.close()))
      .use { listener =>
        HttpTls
          .resourceWithResolver[IO](
            HttpTlsConfig.Default,
            UrlPolicy.Strict,
            _ => Array(InetAddress.getByName("127.0.0.1"), InetAddress.getByName("8.8.8.8"))
          )
          .use { backend =>
            for {
              result <- basicRequest
                .get(Uri.unsafeParse(s"http://registry.invalid:${listener.getLocalPort}/probe"))
                .send(backend)
                .attempt
              connected <- IO.blocking {
                try {
                  val socket = listener.accept()
                  socket.close()
                  true
                } catch {
                  case _: SocketTimeoutException => false
                }
              }
            } yield {
              assert(result.isLeft)
              assert(!connected, "a policy rejection occurred only after TCP connected")
            }
          }
      }
  }

  test("loopback and private permissions remain independent") {
    val loopback = InetAddress.getByName("127.0.0.1")
    val privateIp = InetAddress.getByName("10.0.0.1")
    assert(UrlPolicy.allowsAddress(loopback, UrlPolicy.Strict.copy(allowLoopback = true)))
    assert(!UrlPolicy.allowsAddress(privateIp, UrlPolicy.Strict.copy(allowLoopback = true)))
    assert(!UrlPolicy.allowsAddress(loopback, UrlPolicy.Strict.copy(allowPrivate = true)))
    assert(UrlPolicy.allowsAddress(privateIp, UrlPolicy.Strict.copy(allowPrivate = true)))
  }

  private def server(requests: AtomicInteger, observe: HttpExchange => Unit = _ => ()): Resource[IO, Int] =
    Resource
      .make(IO.blocking {
        val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
        val _ = server.createContext(
          "/probe",
          exchange => {
            observe(exchange)
            val _ = requests.incrementAndGet()
            exchange.getResponseHeaders.set("Connection", "close")
            exchange.sendResponseHeaders(200, -1)
            exchange.close()
          }
        )
        server.start()
        server
      })(server => IO.blocking(server.stop(0)))
      .map(_.getAddress.getPort)
}
