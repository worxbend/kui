package kui.gateway.api.auth

import cats.syntax.all.*
import io.circe.parser.decode

import kui.config.AuthConfig
import kui.gateway.api.GatewayTestServer
import kui.gateway.contract.dto.AuthMeResponse
import kui.testkit.KuiIOSuite

final class TrustedProxySuite extends KuiIOSuite {
  test("forwarded chain stops at the first untrusted hop and ignores spoofed prefixes") {
    assertEquals(
      ClientAddress
        .resolve("127.0.0.1", List("203.0.113.9, 198.51.100.2, 10.0.0.1"), Set("127.0.0.1", "10.0.0.1")),
      "198.51.100.2"
    )
    assertEquals(ClientAddress.resolve("198.51.100.2", List("203.0.113.9"), Set("127.0.0.1")), "198.51.100.2")
  }

  test("malformed, duplicate and hostname forwarding headers fall back to the socket") {
    List(
      List("example.com"),
      List("198.51.100.2,"),
      List("1.2.3.4", "5.6.7.8"),
      List("999.1.1.1"),
      List("[::1]:8080")
    ).foreach { headers =>
      assertEquals(ClientAddress.resolve("127.0.0.1", headers, Set("127.0.0.1")), "127.0.0.1")
    }
    assertEquals(
      ClientAddress.resolve("::1", List("2001:db8::1"), Set("0:0:0:0:0:0:0:1")),
      "2001:db8:0:0:0:0:0:1"
    )
  }

  List(false, true).foreach { trusted =>
    test(
      s"forwarded clients have separate password budgets only through an explicit trusted proxy: $trusted"
    ) {
      val auth = AuthConfig.Default.copy(trustedProxies =
        if trusted then Set("127.0.0.1", "::1") else Set.empty
      )
      GatewayTestServer.resource(auth = auth).use { server =>
        for {
          me <- server.get("/api/v1/auth/me")
          headers = Map(
            "Cookie" -> me.header("Set-Cookie").get.takeWhile(_ != ';'),
            SessionMiddleware.CsrfHeaderName -> decode[AuthMeResponse](me.body).toOption.get.csrfToken
          )
          responses <- (1 to 11).toList.traverse { index =>
            val address = if index <= 10 then "198.51.100.1" else "198.51.100.2"
            server.postJson(
              "/api/v1/auth/password",
              """{"challenge":"x","newPassword":"long-password"}""",
              headers + ("X-Forwarded-For" -> address)
            )
          }
        } yield {
          assert(responses.take(10).forall(_.code.code == 501))
          assertEquals(responses.last.code.code, if trusted then 501 else 429)
        }
      }
    }
  }
}
