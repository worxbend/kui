package kui.gateway.api.auth

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.syntax.all.*
import io.circe.parser.decode
import io.circe.syntax.*
import sttp.tapir.*

import kui.config.{AuthConfig, AuthType, OidcConfig}
import kui.contracts.ErrorEnvelope
import kui.gateway.api.GatewayTestServer
import kui.gateway.api.client.{ServiceBehaviour, ServiceClientFixture}
import kui.gateway.application.client.ServiceClient
import kui.gateway.contract.GatewayEndpoints
import kui.gateway.contract.dto.AuthMeResponse
import kui.identity.contract.dto.{AuthSettingsDto, IdentityPrincipalDto, LoginResponse}
import kui.kernel.error.ErrorCode
import kui.kernel.{Secret, ServiceId}
import kui.testkit.KuiIOSuite

/** `/auth/login`, `/auth/password` and the OpenID Connect pair, driven through a real gateway.
  *
  * ==Why this file did not exist until wave 11==
  *
  * Every suite in this tree built a gateway with no identity service, so all four routes answered
  * `KUI-UNSUPPORTED` before any of their own logic ran. W10-A1 recorded the consequence: `AuthRoutes` was the
  * one production file in this repository with a whole public surface and no behavioural case, and it closed
  * the single rule it could reach only by lifting `replaceSession` out as a `private[auth]` function. It
  * filed the seam — an identity `ServiceClient` on `GatewayTestServer.resource` — as needing an owner. W11-A1
  * owns `GatewayTestServer.scala`, so the seam is open and these are the rules behind it.
  *
  * The stub is a real `SttpServiceClient` over a stub backend rather than a hand-written `ServiceClient`: the
  * identity service's own contract values decide the path, the method and the codecs, so a case here cannot
  * pass against a shape the service does not actually serve.
  */
final class SignInRoutesSuite extends KuiIOSuite {

  private def withOidcFlow[A](answer: ServiceBehaviour, basePath: String = "")(
      run: (GatewayTestServer.Running, Map[String, String]) => IO[A]
  ): IO[A] = {
    val started = kui.identity.contract.dto.OidcStartResponse("https://issuer.example/authorize", "the-state")
    ServiceClientFixture.stub(ServiceBehaviour.Ok(started.asJson)).flatMap { stub =>
      ServiceClientFixture.client(ServiceId.unsafe("identity"), stub).use { identity =>
        GatewayTestServer.resource(identity = Some(identity), basePath = basePath).use { server =>
          for {
            me <- server.get(s"$basePath/api/v1/auth/me")
            headers = Map(
              "Cookie" -> me.header("Set-Cookie").get.takeWhile(_ != ';'),
              SessionMiddleware.CsrfHeaderName -> decode[AuthMeResponse](me.body).toOption.get.csrfToken
            )
            start <- server.post(s"$basePath/api/v1/auth/oidc/start", headers)
            _ = assertEquals(start.code.code, 200, start.body)
            _ <- stub.set(answer)
            result <- run(server, headers)
          } yield result
        }
      }
    }
  }

  test("OIDC state belongs to its initiating browser and is single use") {
    withOidcFlow(signedIn) { (server, owner) =>
      for {
        victim <- server.getWithoutFollowing(s"$api/auth/oidc/callback?code=c&state=the-state")
        legitimate <- server.getWithoutFollowing(s"$api/auth/oidc/callback?code=c&state=the-state", owner)
        replay <- server.getWithoutFollowing(s"$api/auth/oidc/callback?code=c&state=the-state", owner)
      } yield {
        assertEquals(victim.code.code, 401, victim.body)
        assertEquals(legitimate.code.code, 302, legitimate.body)
        assertEquals(replay.code.code, 401, replay.body)
      }
    }
  }

  private val api = GatewayEndpoints.ApiPrefix

  test("login budgets canonicalize padded and mixed-case account names") {
    identityAnswering(changeRequired).use { identity =>
      GatewayTestServer.resource(identity = Some(identity)).use { server =>
        for {
          me <- server.get(s"$api/auth/me")
          headers = Map(
            "Cookie" -> me.header("Set-Cookie").get.takeWhile(_ != ';'),
            SessionMiddleware.CsrfHeaderName -> decode[AuthMeResponse](me.body).toOption.get.csrfToken
          )
          responses <- List("ada", "ADA", " ada", "ada ", " Ada ", "  ada  ").traverse { name =>
            server.postJson(
              s"$api/auth/login",
              io.circe.Json.obj("username" -> name.asJson, "password" -> "wrong".asJson).noSpaces,
              headers
            )
          }
        } yield assertEquals(responses.map(_.code.code), List(200, 200, 200, 200, 200, 429))
      }
    }
  }

  private val ada: IdentityPrincipalDto =
    IdentityPrincipalDto("ada", List("operators"), "session")

  private def identityAnswering(behaviour: ServiceBehaviour): Resource[IO, ServiceClient[IO]] =
    Resource
      .eval(ServiceClientFixture.stub(behaviour))
      .flatMap(stub => ServiceClientFixture.client(ServiceId.unsafe("identity"), stub))

  private def signedIn: ServiceBehaviour =
    ServiceBehaviour.Ok((LoginResponse.SignedIn(ada): LoginResponse).asJson)

  private def changeRequired: ServiceBehaviour =
    ServiceBehaviour.Ok((LoginResponse.PasswordChangeRequired("the-challenge"): LoginResponse).asJson)

  test("a slow pre-login response cannot overwrite the rotated session cookie") {
    for {
      entered <- cats.effect.Deferred[IO, Unit]
      release <- cats.effect.Deferred[IO, Unit]
      slow = endpoint.get
        .in("api" / "v1" / "slow")
        .out(stringBody)
        .serverLogicSuccess[IO](_ => entered.complete(()).void *> release.get.as("done"))
      _ <- identityAnswering(signedIn).use { identity =>
        GatewayTestServer.resource(identity = Some(identity), extraRoutes = List(slow)).use { server =>
          for {
            me <- server.get(s"$api/auth/me")
            headers = Map(
              "Cookie" -> me.header("Set-Cookie").get.takeWhile(_ != ';'),
              SessionMiddleware.CsrfHeaderName -> decode[AuthMeResponse](me.body).toOption.get.csrfToken
            )
            pending <- server.get(s"$api/slow", headers).start
            _ <- entered.get
            login <- server.postJson(s"$api/auth/login", """{"username":"ada","password":"x"}""", headers)
            _ <- release.complete(())
            response <- pending.joinWithNever
          } yield {
            assertEquals(login.code.code, 200)
            assert(login.header("Set-Cookie").isDefined)
            assertEquals(response.header("Set-Cookie"), None)
          }
        }
      }
    } yield ()
  }

  test("a pre-login cookie arriving after sign-in cannot replace the authenticated browser cookie") {
    identityAnswering(signedIn).use { identity =>
      GatewayTestServer.resource(identity = Some(identity), auth = AuthConfig(AuthType.Form, Nil, None)).use {
        server =>
          for {
            me <- server.get(s"$api/auth/me")
            oldCookie = me.header("Set-Cookie").get.takeWhile(_ != ';')
            headers = Map(
              "Cookie" -> oldCookie,
              SessionMiddleware.CsrfHeaderName -> decode[AuthMeResponse](me.body).toOption.get.csrfToken
            )
            login <- server.postJson(s"$api/auth/login", """{"username":"ada","password":"x"}""", headers)
            authenticatedCookie = login.header("Set-Cookie").get.takeWhile(_ != ';')
            late <- server.get(s"$api/auth/me", headers)
            // Apply responses in browser arrival order: a late Set-Cookie would overwrite the login.
            browserCookie = late.header("Set-Cookie").fold(authenticatedCookie)(_.takeWhile(_ != ';'))
            current <- server.get(s"$api/auth/me", Map("Cookie" -> browserCookie))
          } yield {
            assertEquals(login.code.code, 200, login.body)
            assertEquals(late.header("Set-Cookie"), None)
            assertEquals(late.code.code, 401, late.body)
            assertEquals(decode[AuthMeResponse](current.body).toOption.get.principal.name, "ada")
          }
      }
    }
  }

  /** The cookie value a `Set-Cookie` header carries, whichever attributes follow it. */
  private def sessionIdIn(header: String): String =
    header.stripPrefix(s"${SessionMiddleware.CookieName}=").takeWhile(_ != ';')

  private def envelope(body: String): ErrorEnvelope =
    decode[ErrorEnvelope](body).fold(error => fail(s"$body ($error)"), identity)

  // -----------------------------------------------------------------------------------------------
  // The sentence a deployment with no identity service gets
  // -----------------------------------------------------------------------------------------------

  test("every sign-in route names the missing upstream rather than answering 404") {
    // `AuthRoutes.withIdentity` argues for this at length — a `404` on `/auth/login` in a deployment whose
    // configuration says `type: form` sends an operator into the browser's network tab — and nothing
    // asserted it. Deleting the `NotConfigured` branch is not even a refusal that a suite would miss: it is
    // the difference between a sentence and a route that is not there.
    GatewayTestServer.resource().use { server =>
      for {
        me <- server.get(s"$api/auth/me")
        csrf = decode[AuthMeResponse](me.body).fold(e => fail(s"${me.body} ($e)"), _.csrfToken)
        cookie = me.header("Set-Cookie").getOrElse(fail("no session cookie"))
        headers = Map(
          SessionMiddleware.CsrfHeaderName -> csrf,
          "Cookie" -> s"${SessionMiddleware.CookieName}=${sessionIdIn(cookie)}"
        )
        login <- server.postJson(s"$api/auth/login", """{"username":"ada","password":"x"}""", headers)
        password <- server.postJson(
          s"$api/auth/password",
          """{"challenge":"c","newPassword":"a-long-enough-password"}""",
          headers
        )
        oidcStart <- server.post(s"$api/auth/oidc/start", headers)
        oidcBack <- server.getWithoutFollowing(s"$api/auth/oidc/callback?code=c&state=s")
      } yield List(login, password, oidcStart, oidcBack).foreach { response =>
        val problem = envelope(response.body)
        assertEquals(problem.code, ErrorCode.Unsupported.wire, response.body)
        assert(
          problem.message.contains("kui.gateway.services.identity"),
          s"the refusal does not name the key to set: ${problem.message}"
        )
      }
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Signing in
  // -----------------------------------------------------------------------------------------------

  test("a successful sign-in replaces the session, and the response carries exactly one session cookie") {
    identityAnswering(signedIn).use { identity =>
      GatewayTestServer.resource(identity = Some(identity)).use { server =>
        for {
          me <- server.get(s"$api/auth/me")
          before = sessionIdIn(me.header("Set-Cookie").getOrElse(fail("no session cookie")))
          csrf = decode[AuthMeResponse](me.body).fold(e => fail(s"${me.body} ($e)"), _.csrfToken)
          login <- server.postJson(
            s"$api/auth/login",
            """{"username":"ada","password":"correct horse battery staple"}""",
            Map(
              SessionMiddleware.CsrfHeaderName -> csrf,
              "Cookie" -> s"${SessionMiddleware.CookieName}=$before"
            )
          )
          stale <- server.sessions.get(
            kui.gateway.application.session.SessionId.unsafe(before),
            java.time.Instant.now()
          )
        } yield {
          assertEquals(login.code.code, 200, login.body)
          assertEquals(
            decode[LoginResponse](login.body).toOption,
            Some(LoginResponse.SignedIn(ada)),
            login.body
          )

          // One `Set-Cookie`, not two. `SessionMiddleware` stands aside because the route set its own, and
          // two headers for one name is a browser-dependent coin toss over which session the operator ends
          // up in — with the losing side being the one that was just thrown away.
          val cookies = login.headers.filter(_.is("Set-Cookie"))
          assertEquals(cookies.size, 1, cookies.toString)

          // ADR-019's session-fixation defence, end to end for the first time: the id changed, and the id
          // the request arrived on no longer resolves to anything at all.
          val after = sessionIdIn(cookies.head.value)
          assertNotEquals(after, before)
          assertEquals(stale, None, "the session the sign-in arrived on survived it")
        }
      }
    }
  }

  test("a signed-in principal is a Session principal, so it is still held to the CSRF check") {
    // W11-A1: `principalOf` sets `PrincipalKind.Session` here rather than reading the kind off the wire,
    // and the scaladoc says why — "how KUI came to believe an identity is this process's conclusion, not a
    // caller's assertion". Changing that one word to `PrincipalKind.Bearer` left the whole gateway module
    // green, and it is not a labelling mistake: `CsrfCheck.verdict` exempts `Bearer` outright, because a
    // script that sends a token chose to send it. A cookie-authenticated operator marked `Bearer` is a
    // browser session with CSRF protection switched off — every mutation in the product forgeable from any
    // page the operator visits.
    identityAnswering(signedIn).use { identity =>
      GatewayTestServer.resource(identity = Some(identity)).use { server =>
        for {
          me <- server.get(s"$api/auth/me")
          before = sessionIdIn(me.header("Set-Cookie").getOrElse(fail("no session cookie")))
          csrf = decode[AuthMeResponse](me.body).fold(e => fail(s"${me.body} ($e)"), _.csrfToken)
          login <- server.postJson(
            s"$api/auth/login",
            """{"username":"ada","password":"x"}""",
            Map(
              SessionMiddleware.CsrfHeaderName -> csrf,
              "Cookie" -> s"${SessionMiddleware.CookieName}=$before"
            )
          )
          after = sessionIdIn(login.headers.filter(_.is("Set-Cookie")).head.value)
          cookie = Map("Cookie" -> s"${SessionMiddleware.CookieName}=$after")
          who <- server.get(s"$api/auth/me", cookie)
          forged <- server.post(s"$api/auth/logout", cookie)
        } yield {
          val principal =
            decode[AuthMeResponse](who.body).fold(e => fail(s"${who.body} ($e)"), _.principal)
          assertEquals(principal.name, "ada", who.body)
          assertEquals(principal.kind, "session", who.body)

          // The consequence, asserted rather than inferred: a mutation from this signed-in session with no
          // CSRF header is still refused.
          assertEquals(forged.code.code, 403, forged.body)
          assert(forged.body.contains("KUI-FORBIDDEN"), forged.body)
        }
      }
    }
  }

  test("a required password change grants no session and must not restamp a stale cookie") {
    // `AuthRoutes` states it in the branch itself: "handing out a cookie here would be handing out a
    // session to somebody the server has just decided may not have one". Replacing `currentCookie` with
    // `signIn` in that branch is a one-word edit that no case saw.
    identityAnswering(changeRequired).use { identity =>
      GatewayTestServer.resource(identity = Some(identity)).use { server =>
        for {
          me <- server.get(s"$api/auth/me")
          before = sessionIdIn(me.header("Set-Cookie").getOrElse(fail("no session cookie")))
          csrf = decode[AuthMeResponse](me.body).fold(e => fail(s"${me.body} ($e)"), _.csrfToken)
          headers = Map(
            SessionMiddleware.CsrfHeaderName -> csrf,
            "Cookie" -> s"${SessionMiddleware.CookieName}=$before"
          )
          login <- server.postJson(s"$api/auth/login", """{"username":"ada","password":"x"}""", headers)
          who <- server.get(s"$api/auth/me", Map("Cookie" -> s"${SessionMiddleware.CookieName}=$before"))
        } yield {
          assertEquals(
            decode[LoginResponse](login.body).toOption,
            Some(LoginResponse.PasswordChangeRequired("the-challenge")),
            login.body
          )

          val cookies = login.headers.filter(_.is("Set-Cookie"))
          assertEquals(cookies.size, 0, cookies.toString)

          // And the session it left behind is still anonymous: nobody was signed in.
          val principal =
            decode[AuthMeResponse](who.body).fold(e => fail(s"${who.body} ($e)"), _.principal)
          assertEquals(principal.kind, "anonymous", who.body)
        }
      }
    }
  }

  test("completing a password change issues no session of its own") {
    // The change endpoint deliberately grants nothing: the caller signs in again with the new password,
    // which is one way to obtain a session rather than two. What is asserted here is the *absence* — the
    // only cookie on the response is the stamp for the session the request already had.
    identityAnswering(ServiceBehaviour.Ok(io.circe.Json.Null)).use { identity =>
      GatewayTestServer.resource(identity = Some(identity)).use { server =>
        for {
          me <- server.get(s"$api/auth/me")
          before = sessionIdIn(me.header("Set-Cookie").getOrElse(fail("no session cookie")))
          csrf = decode[AuthMeResponse](me.body).fold(e => fail(s"${me.body} ($e)"), _.csrfToken)
          changed <- server.postJson(
            s"$api/auth/password",
            """{"challenge":"the-challenge","newPassword":"a-long-enough-password"}""",
            Map(
              SessionMiddleware.CsrfHeaderName -> csrf,
              "Cookie" -> s"${SessionMiddleware.CookieName}=$before"
            )
          )
        } yield {
          assertEquals(changed.code.code, 200, changed.body)
          val cookies = changed.headers.filter(_.is("Set-Cookie"))
          assertEquals(cookies.size, 0, cookies.toString)
        }
      }
    }
  }

  // -----------------------------------------------------------------------------------------------
  // The provider pair
  // -----------------------------------------------------------------------------------------------

  test("a provider callback that comes back signed in is redirected to the interface with a new session") {
    withOidcFlow(signedIn) { (server, headers) =>
      server.getWithoutFollowing(s"$api/auth/oidc/callback?code=the-code&state=the-state", headers).map {
        response =>
          assertEquals(response.code.code, 302, response.body)
          assertEquals(response.header("Location"), Some("/ui/"))
          val cookies = response.headers.filter(_.is("Set-Cookie"))
          assertEquals(cookies.size, 1, cookies.toString)
          assert(cookies.head.value.startsWith(s"${SessionMiddleware.CookieName}="), cookies.toString)
      }
    }
  }

  test("a provider callback that asks for a password change is refused, and nobody is signed in") {
    // `AuthRoutes` argues this one out: a provider sign-in cannot produce a KUI password change, because
    // there is no KUI password behind it to change, and redirecting a browser into a flow with no next step
    // is worse than a refusal. Turning that branch into the `SignedIn` one is a two-line edit that left the
    // gateway module green.
    withOidcFlow(changeRequired) { (server, headers) =>
      server.getWithoutFollowing(s"$api/auth/oidc/callback?code=the-code&state=the-state", headers).map {
        response =>
          assertEquals(envelope(response.body).code, ErrorCode.InvalidState.wire, response.body)
          assertEquals(response.header("Location"), None, "a refused callback still redirected the browser")
          assertEquals(
            response.headers
              .filter(_.is("Set-Cookie"))
              .map(header => sessionIdIn(header.value))
              .distinct
              .size,
            0,
            "more than one session cookie on a refused callback"
          )
      }
    }
  }

  test("a deployment under a base path sends a completed provider sign-in to its own interface") {
    // `landingPage` is two branches and a string. Collapsing it to "/ui/" strands every reverse-proxied
    // deployment at a path its gateway does not serve, and nothing saw it.
    withOidcFlow(signedIn, "/kui") { (server, headers) =>
      server.getWithoutFollowing(s"/kui$api/auth/oidc/callback?code=c&state=the-state", headers).map {
        response =>
          assertEquals(response.header("Location"), Some("/kui/ui/"), response.headers.toString)
      }
    }
  }

  // -----------------------------------------------------------------------------------------------
  // What the login screen is told
  // -----------------------------------------------------------------------------------------------

  test("settings reports this deployment's own mode and provider label, and never a credential") {
    val oidc = OidcConfig(
      issuer = "https://accounts.example.com",
      clientId = "kui",
      clientSecret = Secret("s3cret"),
      redirectUri = "http://localhost:8080/api/v1/auth/oidc/callback",
      scopes = List("openid"),
      usernameClaim = "email",
      groupsClaim = None,
      label = "Example"
    )

    GatewayTestServer.resource(auth = AuthConfig(AuthType.Oidc, Nil, Some(oidc))).use { server =>
      server.get(s"$api/auth/settings").map { response =>
        val settings =
          decode[AuthSettingsDto](response.body).fold(e => fail(s"${response.body} ($e)"), identity)
        assertEquals(settings.authType, "oidc")
        assertEquals(settings.providerLabel, Some("Example"))
        assertEquals(settings.rbacEnabled, false)
        // The type cannot carry one, and the body proves the type is the one being served.
        assert(!response.body.contains("s3cret"), response.body)
        assert(!response.body.contains("accounts.example.com"), response.body)
      }
    }
  }
}
