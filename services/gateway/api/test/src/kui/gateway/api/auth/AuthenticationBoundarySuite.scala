package kui.gateway.api.auth

import cats.effect.IO
import cats.syntax.all.*
import io.circe.parser.decode
import sttp.tapir.*

import kui.config.{AuthConfig, AuthType}
import kui.gateway.api.GatewayTestServer
import kui.gateway.contract.dto.AuthMeResponse
import kui.kernel.UserName
import kui.security.{Principal, PrincipalKind}
import kui.testkit.KuiIOSuite

final class AuthenticationBoundarySuite extends KuiIOSuite {
  private val protectedRead = endpoint.get
    .in("api" / "v1" / "protected")
    .out(stringBody)
    .serverLogicSuccess[IO](_ => IO.pure("private"))
  private val protectedWrite = endpoint.post
    .in("api" / "v1" / "protected")
    .out(stringBody)
    .serverLogicSuccess[IO](_ => IO.pure("changed"))

  test("anonymous session creation is throttled without blocking existing sessions") {
    GatewayTestServer.resource().use { server =>
      for {
        me <- server.get("/api/v1/auth/me")
        cookie = me.header("Set-Cookie").get.takeWhile(_ != ';')
        flood <- (1 to 30).toList.traverse(_ => server.get("/api/v1/auth/me"))
        retained <- server.get("/api/v1/auth/me", Map("Cookie" -> cookie))
      } yield {
        assertEquals(flood.last.code.code, 429)
        assertEquals(flood.last.header("Set-Cookie"), None)
        assertEquals(retained.code.code, 200)
      }
    }
  }

  test("a throttled sender cannot spend the global session budget for other clients") {
    val auth = AuthConfig.Default.copy(trustedProxies = Set("127.0.0.1", "::1"))
    GatewayTestServer.resource(auth = auth).use { server =>
      for {
        flood <- (1 to 600).toList
          .traverse(_ => server.get("/api/v1/auth/me", Map("X-Forwarded-For" -> "198.51.100.1")))
        other <- server.get("/api/v1/auth/me", Map("X-Forwarded-For" -> "198.51.100.2"))
      } yield {
        assertEquals(flood.count(_.code.code == 200), 30)
        assert(flood.drop(30).forall(_.code.code == 429))
        assert(flood.drop(30).forall(_.header("Set-Cookie").isEmpty))
        assertEquals(other.code.code, 200, other.body)
        assert(other.header("Set-Cookie").isDefined)
      }
    }
  }

  List(AuthType.Form, AuthType.Oidc).foreach { mode =>
    test(s"$mode without RBAC still requires authentication for reads and writes") {
      GatewayTestServer
        .resource(auth = AuthConfig(mode, Nil, None), extraRoutes = List(protectedRead, protectedWrite))
        .use { server =>
          for {
            me <- server.get("/api/v1/auth/me")
            csrf = decode[AuthMeResponse](me.body).toOption.get.csrfToken
            cookie = me.header("Set-Cookie").get.takeWhile(_ != ';')
            read <- server.get("/api/v1/protected", Map("Cookie" -> cookie))
            write <- server.post(
              "/api/v1/protected",
              Map("Cookie" -> cookie, SessionMiddleware.CsrfHeaderName -> csrf)
            )
            info <- server.get("/api/v1/info")
            settings <- server.get("/api/v1/auth/settings")
          } yield {
            assertEquals(read.code.code, 401)
            assertEquals(write.code.code, 401)
            assertEquals(info.code.code, 200)
            assertEquals(settings.code.code, 200)
          }
        }
    }
  }

  test("authentication-disabled deployments retain anonymous access") {
    GatewayTestServer.resource(extraRoutes = List(protectedRead)).use { server =>
      server.get("/api/v1/protected").map(response => assertEquals(response.code.code, 200))
    }
  }

  test("a signed-in session accesses mounted APIs but still needs CSRF for writes") {
    GatewayTestServer
      .resource(
        basePath = "/console",
        auth = AuthConfig(AuthType.Form, Nil, None),
        extraRoutes = List(protectedRead, protectedWrite)
      )
      .use { server =>
        for {
          now <- IO.realTimeInstant
          session <- server.sessions.create(
            Principal(UserName.unsafe("ada"), Set.empty, PrincipalKind.Session),
            now
          )
          headers = Map("Cookie" -> s"${SessionMiddleware.CookieName}=${session.id.value}")
          read <- server.get("/console/api/v1/protected", headers)
          forged <- server.post("/console/api/v1/protected", headers)
          write <- server.post(
            "/console/api/v1/protected",
            headers.updated(SessionMiddleware.CsrfHeaderName, session.csrfSecret.value)
          )
          anonymous <- server.get("/console/api/v1/protected")
        } yield {
          assertEquals(read.code.code, 200)
          assertEquals(forged.code.code, 403)
          assertEquals(write.code.code, 200)
          assertEquals(anonymous.code.code, 401)
        }
      }
  }

  test("build info and auth settings agree in a secured deployment") {
    GatewayTestServer.resource(auth = AuthConfig(AuthType.Form, Nil, None)).use { server =>
      server.get("/api/v1/info").map { response =>
        assertEquals(
          io.circe.parser.parse(response.body).toOption.get.hcursor.get[String]("authType"),
          Right("form")
        )
      }
    }
  }
}
