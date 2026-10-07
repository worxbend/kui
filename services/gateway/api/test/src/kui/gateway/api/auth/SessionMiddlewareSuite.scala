package kui.gateway.api.auth

import java.time.Instant

import scala.concurrent.duration.DurationInt

import cats.effect.IO
import io.circe.parser.decode

import kui.gateway.api.{GatewayApi, GatewayTestServer}
import kui.gateway.application.session.{InMemorySessionStore, SessionConfig, SessionId, SessionRef}
import kui.gateway.contract.GatewayEndpoints
import kui.gateway.contract.dto.AuthMeResponse
import kui.security.rbac.{ClusterScope, Resource}
import kui.testkit.KuiIOSuite

/** The session and CSRF boundary, exercised through a real server (ADR-019, GW-009).
  *
  * `CsrfCheckSuite` proves the decision function; this proves it is actually wired to every request — the
  * cookie really gets set with the exact attributes ADR-019 specifies, `/auth/me`'s token really is the one
  * `/auth/logout` requires, and a stale token really is refused.
  */
final class SessionMiddlewareSuite extends KuiIOSuite {

  private val meUri = s"${GatewayEndpoints.ApiPrefix}/auth/me"
  private val logoutUri = s"${GatewayEndpoints.ApiPrefix}/auth/logout"

  test("cookieAttributesAreExactlyAsSpecified") {
    GatewayTestServer.resource(devInsecureCookies = false).use { server =>
      server.get(meUri).map { response =>
        val cookie = response.header("Set-Cookie").getOrElse(fail("no Set-Cookie on /auth/me"))
        assert(cookie.startsWith(s"${SessionMiddleware.CookieName}="), cookie)
        assert(cookie.contains("HttpOnly"), cookie)
        assert(cookie.contains("Secure"), cookie)
        assert(cookie.contains("SameSite=Lax"), cookie)
        assert(cookie.contains("Path=/"), cookie)
      }
    }
  }

  test("secureIsOmittedOnlyWhenDevInsecureCookiesIsSet") {
    GatewayTestServer.resource(devInsecureCookies = true).use { server =>
      server.get(meUri).map { response =>
        val cookie = response.header("Set-Cookie").getOrElse(fail("no Set-Cookie on /auth/me"))
        assert(!cookie.contains("Secure"), cookie)
      }
    }
  }

  test("authMeAnswersAnonymousInM0") {
    GatewayTestServer.resource().use { server =>
      server.get(meUri).map { response =>
        val body =
          decode[AuthMeResponse](response.body).fold(error => fail(s"${response.body} ($error)"), identity)
        assertEquals(body.authType, "disabled")
        assertEquals(body.principal.kind, "anonymous")
        assertEquals(body.principal.name, "anonymous")
        assert(body.csrfToken.nonEmpty)
      }
    }
  }

  test("authMeCarriesTheExpandedPermissionSetTheBrowserGatesOn") {
    // E4. With no roles configured, the answer is a wildcard grant per resource over every cluster, not an
    // empty list: an empty list would read in the browser as "you may do nothing", and the interface would
    // hide every write control in the deployment that has asked for no authorization at all.
    GatewayTestServer.resource().use { server =>
      server.get(meUri).map { response =>
        val body =
          decode[AuthMeResponse](response.body).fold(error => fail(s"${response.body} ($error)"), identity)

        assertEquals(body.permissions.map(_.resource).toSet, Resource.values.map(_.wire).toSet)
        assert(body.permissions.forall(_.clusters == List(ClusterScope.EveryWire)))
        assert(body.permissions.forall(_.value.contains(".*")))

        val topic = body.permissions.find(_.resource == Resource.Topic.wire).getOrElse(fail("no TOPIC grant"))
        assertEquals(topic.actions.toSet, Resource.Topic.allActions.map(_.wire))
        // Sorted, so two responses describing the same permissions are byte-identical.
        assertEquals(topic.actions, topic.actions.sorted)
      }
    }
  }

  test("theCsrfTokenFromAuthMeWorksAndAStaleOneDoesNot") {
    GatewayTestServer.resource().use { server =>
      for {
        first <- server.get(meUri)
        cookie = cookieOf(first)
        token = tokenOf(first)
        allowed <- server.post(logoutUri, Map("Cookie" -> cookie, SessionMiddleware.CsrfHeaderName -> token))
        second <- server.get(meUri)
        secondCookie = cookieOf(second)
        // Logging out cleared the session; the new cookie belongs to a fresh session with a different
        // token, so replaying the first token against it must fail.
        stale <- server.post(
          logoutUri,
          Map("Cookie" -> secondCookie, SessionMiddleware.CsrfHeaderName -> token)
        )
      } yield {
        assertEquals(allowed.code.code, 200, allowed.body)
        assertEquals(stale.code.code, 403, stale.body)
      }
    }
  }

  test("postWithNoCsrfHeaderIsForbidden") {
    GatewayTestServer.resource().use { server =>
      for {
        first <- server.get(meUri)
        cookie = cookieOf(first)
        response <- server.post(logoutUri, Map("Cookie" -> cookie))
      } yield {
        assertEquals(response.code.code, 403, response.body)
        assert(response.body.contains("KUI-FORBIDDEN"), response.body)
      }
    }
  }

  test("aCsrfRejectionIsLoggedAtWarnAndNamesTheSessionByItsRefRatherThanItsId") {
    // Two rules on one log line, and W12-03 measured both ungated over the *whole* gateway module on
    // 2026-09-12 — `./mill --no-daemon services.gateway.api.test` printed `1270/1270, SUCCESS` under each:
    //
    //   `SessionRef.of(active.id).value` -> `active.id.value`    — the rejection line carries the cookie
    //   `logger.warn(...)`               -> `logger.debug(...)`  — the rejection stops being a signal
    //
    // The first is the serious one, and this is the only place in the gateway that logs anything about a
    // session at all. `Session.scala` states the rule the `SessionRef` type exists for — a rejected request
    // is logged with `session.ref`, never the id, "so that a log file is not itself a way to hijack the
    // session it describes" — and a CSRF rejection is precisely what an attack produces, so the attacker's
    // own noise is what fills the file with live session ids. Anyone who can read a log could then replay
    // them: the id is the whole credential, and `Secret` protects the CSRF token but the cookie value is a
    // plain `String`.
    //
    // The second is what `SessionMiddleware`'s own scaladoc argues for in its own words — a CSRF rejection
    // is "a signal worth alerting on, not a client error to note and move past". At `debug` it sits below
    // every deployment's default level, and the alert nobody is watching is the alert that does not exist.
    GatewayTestServer.resource().use { server =>
      for {
        first <- server.get(meUri)
        cookie = cookieOf(first)
        id = cookie.split('=').last
        refused <- server.post(logoutUri, Map("Cookie" -> cookie))
        logged <- server.logger.entriesWith("session.ref")
      } yield {
        assertEquals(refused.code.code, 403, refused.body)
        // The vacuity guard: with nothing logged at all, every assertion below holds over an empty list.
        val entry = logged.headOption.getOrElse(fail(s"the CSRF rejection logged no session at all: $logged"))

        assertEquals(entry.level, "warn", s"a CSRF rejection was logged at ${entry.level}: $entry")
        assertEquals(entry.context.get("path"), Some(logoutUri), entry.context.toString)
        assertEquals(
          entry.context.get("session.ref"),
          Some(SessionRef.of(SessionId.unsafe(id)).value),
          "the rejection did not name the session by its one-way ref"
        )
        // The direction that matters, asserted over the whole line rather than over the one field, so that
        // a key added later cannot reintroduce the leak past this case.
        assert(!entry.message.contains(id), s"the session id reached the log message: ${entry.message}")
        assert(
          entry.context.values.forall(!_.contains(id)),
          s"the session id reached the log context: ${entry.context}"
        )
      }
    }
  }

  test("logoutRequiresPostAndClearsTheCookie") {
    GatewayTestServer.resource().use { server =>
      for {
        first <- server.get(meUri)
        cookie = cookieOf(first)
        token = tokenOf(first)
        loggedOut <- server.post(
          logoutUri,
          Map("Cookie" -> cookie, SessionMiddleware.CsrfHeaderName -> token)
        )
        afterLogout <- server.get(meUri, Map("Cookie" -> cookie))
        afterBody = decode[AuthMeResponse](afterLogout.body).toOption.get
        // W11-A1: nothing here asked the *store*. Replacing `store.delete(session.id)` with `unit` — a
        // logout that logs nobody out — left the whole gateway module green, because every assertion
        // below is satisfied by a session that is still very much alive: the reply is still `200`, and
        // `/auth/me` still says `disabled` whether the old session survived or a new one replaced it.
        // A revoked session that is not revoked is the failure an operator reaches for logout to prevent.
        revoked <- server.sessions.get(SessionId.unsafe(cookie.split('=').last), Instant.now())
      } yield {
        assertEquals(loggedOut.code.code, 200, loggedOut.body)
        assertEquals(revoked, None, "the session survived the logout that was supposed to delete it")
        // The old session is gone; a request that presents its cookie again gets a *new* anonymous
        // session rather than an error — anonymous mode has nothing to fail on, and a fresh session is
        // exactly what ADR-019 says happens for a cookie the store no longer recognises.
        assertEquals(afterBody.authType, "disabled")
      }
    }
  }

  test("getLogoutIsRouteNotFoundAndStillTheKuiEnvelope") {
    // `/auth/logout` is declared `.post` only, so a `GET` against it is a decode failure on the endpoint's
    // *method*. `libs/http`'s `ErrorInterceptor.shouldRespond` treats that as a routing question and lets
    // the router try the next endpoint, exactly as it does for a path that does not match; when no
    // endpoint claims the method either, the request reaches the reject handler and comes back as
    // `KUI-ROUTE-NOT-FOUND`, naming the method and the path.
    //
    // It used to answer `400 KUI-VALIDATION` from the first endpoint that declared this path, which meant
    // no *later* endpoint was ever tried — including one declared for a different method on the same path.
    // That is what made `HEAD /ui/main.js` answer `400` while `GET /ui/main.js` answered `200`. The error
    // shape a caller sees is still KUI's own envelope, which was the point of answering here at all; only
    // the code and the status changed, and 404 is the honest one for "nothing serves this".
    GatewayTestServer.resource().use { server =>
      server.get(logoutUri).map { response =>
        assertEquals(response.code.code, 404, response.body)
        assert(response.body.contains("KUI-ROUTE-NOT-FOUND"), response.body)
        assert(response.body.contains("GET"), response.body)
      }
    }
  }

  test("noSessionCookieIsStampedOntoAStaticAssetOrAHealthProbe") {
    // Two problems, one cause. A hashed asset is served `Cache-Control: public, max-age=31536000,
    // immutable`; a response that is simultaneously cacheable by a shared proxy for a year and carrying a
    // per-user session credential hands every later visitor the first visitor's session. And because a
    // session was minted for every cookie-less request, an unauthenticated client could evict every real
    // session out of the bounded store just by fetching `/ui/` in a loop.
    GatewayTestServer.resource().use { server =>
      for {
        asset <- server.get("/ui/main-a1b2c3d4.js")
        index <- server.get("/ui/")
        health <- server.get(s"${GatewayEndpoints.ApiPrefix}/health/live")
        unmatched <- server.get("/nothing-here")
        api <- server.get(meUri)
      } yield {
        // `/ui/main-<hash>.js` is the shape a linker-hashed asset has, and `StaticRoutesSuite` proves
        // that shape is served `public, max-age=31536000, immutable`. (This fixture has no linked
        // frontend, so the path falls through to the SPA shell; the subject here is the cookie.)
        assertEquals(asset.header("Set-Cookie"), None, "an immutably cacheable asset carries a session")
        assertEquals(index.header("Set-Cookie"), None, "the SPA shell carries a session")
        assertEquals(health.header("Set-Cookie"), None, "a health probe mints a session")
        assertEquals(unmatched.header("Set-Cookie"), None, "an unmatched path mints a session")
        // The gateway's own API still issues one, which is what the CSRF machinery runs on.
        assert(api.header("Set-Cookie").isDefined, "the API stopped issuing sessions")
      }
    }
  }

  test("everyInboundXKuiHeaderIsStrippedBeforeTheHandler") {
    // The two boundaries compose: a forged principal header does not survive to influence which session
    // this request gets, because `EdgeHeaders` runs first in the chain `GatewayWiring` builds.
    GatewayTestServer.resource().use { server =>
      server.get(meUri, Map("X-Kui-Principal" -> "forged-admin")).map { response =>
        val body = decode[AuthMeResponse](response.body).toOption.get
        assertEquals(body.principal.name, "anonymous")
        assertEquals(body.principal.kind, "anonymous")
      }
    }
  }

  test("theSessionCookieIsFoundByItsExactNameAndNotBySubstring") {
    // W10-A1: `_.find(_.name == CookieName)` had no case, and loosening it to `_.name.contains(...)` left
    // the whole gateway module green. A `Cookie` header carries every cookie for the domain, so a reverse
    // proxy or another application on the same host setting `kui_session_backup` — or an attacker setting
    // one from a subdomain — would be picked up first and the operator's real session would be dropped on
    // the floor, silently, on every request.
    GatewayTestServer.resource().use { server =>
      for {
        first <- server.get(meUri)
        session = cookieOf(first)
        token = tokenOf(first)
        again <- server.get(meUri, Map("Cookie" -> s"kui_session_backup=planted; $session"))
        body = decode[AuthMeResponse](again.body).fold(error => fail(s"${again.body} ($error)"), identity)
      } yield
        // Same session, so the CSRF secret the browser already holds still works. Under a substring match
        // the decoy is found first, `planted` resolves to nothing, and a brand new session is minted.
        assertEquals(body.csrfToken, token, "a decoy cookie displaced the real session")
    }
  }

  test("aDeploymentUnderABasePathStillGetsASessionAndScopesItsCookieToThatPath") {
    // W10-A1: two rules, both ungated, both only visible when `server.basePath` is set — which no case in
    // this tree did while asserting a cookie. `needsSession` drops the base path's segments before it looks
    // for the API prefix, and `setCookie` scopes `Path` to the base path. Dropping either left the whole
    // gateway module green: without the first, a deployment behind a reverse proxy issues no session at
    // all and every mutation is refused for want of a CSRF token; without the second, the cookie is
    // written at `/` and two KUI deployments on one host overwrite each other's sessions.
    GatewayTestServer.resource(basePath = "/kui", devInsecureCookies = false).use { server =>
      for {
        mounted <- server.get(s"/kui$meUri")
        health <- server.get(s"/kui${GatewayEndpoints.ApiPrefix}/health/live")
      } yield {
        val cookie = mounted.header("Set-Cookie").getOrElse(fail(s"no session under /kui: ${mounted.body}"))
        assert(cookie.startsWith(s"${SessionMiddleware.CookieName}="), cookie)
        assert(cookie.contains("Path=/kui/"), cookie)
        // The health exclusion moves with the base path too, so an orchestrator's timer still mints
        // nothing and cannot evict the bounded store one probe at a time.
        assertEquals(health.header("Set-Cookie"), None, "a health probe under a base path mints a session")
      }
    }
  }

  test("everyMutatingRouteTheGatewayServesSitsInsideTheSessionMintingSet") {
    // W11-A1, and it is an *invariant* rather than a behaviour: `SessionMiddleware`'s CSRF step reads
    // `session.fold(PrincipalKind.Anonymous)(_.principal.kind)`, and replacing that default with
    // `PrincipalKind.Bearer` is a fail-open — a mutation with no session at all would be exempted from the
    // CSRF check outright. W10-A1 argued that mutant down as equivalent, correctly, and said why it is only
    // equivalent *by accident of the route table*: nothing outside `/api/v1` declares a non-safe method, so
    // no request that reaches the check can be missing a session.
    //
    // "By accident of the route table" is not a property anything was checking. The first `POST` added
    // under `/ui/`, or under a new prefix, turns an equivalent mutant into a live CSRF hole with no test
    // going red anywhere. This case is the guard on that accident: every endpoint this gateway serves whose
    // method is not safe must sit on a path `needsSession` says gets one.
    val safeMethods = Set("GET", "HEAD", "OPTIONS")

    InMemorySessionStore.resource[IO](SessionConfig.Default).use { sessions =>
      IO {
        val routes = GatewayApi.routes[IO](GatewayTestServer.configView("/"), Nil, sessions)

        val mutating = routes.flatMap { route =>
          val method = route.endpoint.method.map(_.method).getOrElse("GET")
          Option.when(!safeMethods.contains(method))(
            method -> route.endpoint.showPathTemplate().split('/').toList.filter(_.nonEmpty)
          )
        }

        // If this is ever empty the case has stopped measuring anything, which is the failure mode a
        // green gate hides best.
        assert(mutating.nonEmpty, "no non-safe-method endpoint was found, so this case proves nothing")

        mutating.foreach { (method, path) =>
          assert(
            SessionMiddleware.needsSession(path, ""),
            s"$method /${path.mkString("/")} is served outside the session-minting set, so a request to " +
              "it can reach the CSRF check with no session — and the Anonymous default is what refuses it"
          )
        }
      }
    }
  }

  test("aSessionPastItsIdleTimeoutIsRefusedOnTheRequestPathAndReplaced") {
    // FILED AS V2 BY THIS WAVE'S VERIFICATION PASS OVER W12-03, and it is the serious one.
    //
    // `store.get(id, now)` inside `SessionMiddleware.ensureSession` is the ONLY expiry check a browser
    // request ever meets, and `attachInterceptor` is its only caller for one: it both refuses a session
    // past `SessionConfig`'s idleTimeout and absoluteTimeout, and slides `lastSeenAt` for one that is
    // still alive. Measured on 2026-09-12 with `now <- Clock[F].realTimeInstant` in `attachInterceptor`
    // replaced by `now <- Sync[F].pure(Instant.EPOCH)` — one line —
    // `./mill --no-daemon services.gateway.api.test` printed `1270/1270, SUCCESS` in 108s over all 303
    // cases. With the clock frozen, `Session.isExpired(now, idleTimeout)` is
    // `now.isAfter(absoluteExpiry) || now.isAfter(lastSeenAt.plus(idleTimeout))`, which is false for ever:
    // no session in the deployment expires, and a captured cookie is valid for the life of the process.
    //
    // `InMemorySessionStoreSuite` asserts the STORE's arithmetic against an `Instant` it chooses itself,
    // which is a different statement: it says the rule is computable, not that anything applies it to a
    // real request with a real clock. Nothing could say the second, because every gateway this tree builds
    // ran on `SessionConfig.Default` — thirty minutes idle, twelve hours absolute. So the seam is a
    // `sessionConfig` parameter on `GatewayTestServer.resource` and the timeout is shorter than the test.
    //
    // The assertion is on the ID, not on a status code: `/auth/me` answers 200 either way, because a
    // request whose session has expired is given a NEW one rather than refused. A different id in the
    // second response's `Set-Cookie` is exactly what "the cookie the browser presented no longer resolves"
    // looks like from outside, and it is the only externally visible difference there is.
    val brief = SessionConfig.Default.copy(idleTimeout = 1.second, absoluteTimeout = 1.hour)
    GatewayTestServer.resource(sessionConfig = brief).use { server =>
      for {
        first <- server.get(meUri)
        cookie = cookieOf(first)
        // Well inside the idle timeout: the same cookie must come back resolving to the same session, or
        // the case below would pass on a gateway that simply minted a new session on every request.
        stillAlive <- server.get(meUri, Map("Cookie" -> cookie))
        _ <- IO.sleep(1600.millis)
        afterIdle <- server.get(meUri, Map("Cookie" -> cookie))
      } yield {
        val presented = cookie.stripPrefix(s"${SessionMiddleware.CookieName}=")
        assert(presented.nonEmpty, cookie)

        // Existing sessions retain their CSRF token and never restamp a potentially stale cookie.
        assertEquals(stillAlive.header("Set-Cookie"), None)
        assertEquals(tokenOf(stillAlive), tokenOf(first))

        val replaced = cookieOf(afterIdle).stripPrefix(s"${SessionMiddleware.CookieName}=")
        assertNotEquals(
          replaced,
          presented,
          "a session presented after its idle timeout had passed was accepted and stamped back " +
            "unchanged, so nothing on the request path applies SessionConfig.idleTimeout. ADR-019's " +
            "session lifetime is then configuration that no request ever reads, and a captured cookie " +
            "stays valid for the life of the process."
        )
      }
    }
  }

  private def cookieOf(response: sttp.client4.Response[String]): String =
    response
      .header("Set-Cookie")
      .flatMap(_.split(";").headOption)
      .getOrElse(fail("no Set-Cookie header"))

  private def tokenOf(response: sttp.client4.Response[String]): String =
    decode[AuthMeResponse](response.body).fold(error => fail(s"${response.body} ($error)"), _.csrfToken)
}
