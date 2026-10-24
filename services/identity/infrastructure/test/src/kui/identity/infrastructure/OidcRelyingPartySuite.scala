package kui.identity.infrastructure

import java.time.Instant
import java.util.Date

import cats.effect.IO
import cats.syntax.all.*
import com.nimbusds.jose.crypto.RSASSASigner
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator
import com.nimbusds.jose.jwk.{JWKSet, RSAKey}
import com.nimbusds.jose.{JWSAlgorithm, JWSHeader}
import com.nimbusds.jwt.{JWTClaimsSet, SignedJWT}
import sttp.client4.Backend
import sttp.client4.impl.cats.implicits.*
import sttp.client4.testing.{BackendStub, ResponseStub}
import sttp.model.StatusCode

import kui.config.OidcConfig
import kui.identity.application.PendingLogin
import kui.kernel.Secret
import kui.kernel.error.ErrorCode
import kui.testkit.KuiIOSuite
import kui.testkit.fakes.FakeStructuredLogger

/** The OpenID Connect relying party, against a provider this suite plays the part of.
  *
  * ==What is being pinned==
  *
  * Every one of the five checks on an ID token, individually. A token that is correctly signed by the right
  * provider but issued for somebody else's client is not a login here; nor is one that is expired, one whose
  * nonce belongs to another sign-in, or one signed by a key the provider does not publish. Each of those is a
  * real, documented way that relying parties have been broken, and each of them is one line to get wrong.
  *
  * The signing key is generated per run and the provider is a stub backend, so nothing here reaches the
  * network and nothing depends on a secret checked into the repository.
  */
final class OidcRelyingPartySuite extends KuiIOSuite {

  private val issuer: String = "https://accounts.example.com"

  private val config: OidcConfig =
    OidcConfig(
      issuer = issuer,
      clientId = "kui",
      clientSecret = Secret("s3cret"),
      redirectUri = "http://localhost:8080/api/v1/auth/oidc/callback",
      scopes = List("openid", "profile", "email"),
      usernameClaim = "email",
      groupsClaim = Some("groups"),
      label = "Example"
    )

  private val key: RSAKey =
    new RSAKeyGenerator(2048).keyID("test-key").generate()

  /** Another provider's key, for the case where the signature is valid but not by anybody we trust. */
  private val otherKey: RSAKey = new RSAKeyGenerator(2048).keyID("test-key").generate()

  private val discovery: String = discoveryNaming(issuer)

  /** A discovery document whose `issuer` is `named`, with endpoints on the configured provider's hosts. */
  private def discoveryNaming(named: String): String =
    s"""{
       |  "issuer": "$named",
       |  "authorization_endpoint": "$issuer/authorize",
       |  "token_endpoint": "$issuer/token",
       |  "jwks_uri": "https://keys.example.com/jwks"
       |}""".stripMargin

  private val jwks: String = new JWKSet(key.toPublicJWK).toString

  private def idToken(
      signingKey: RSAKey = key,
      audience: String = "kui",
      tokenIssuer: String = issuer,
      nonce: String = "the-nonce",
      expiresAt: Instant = Instant.now().plusSeconds(300),
      subject: String = "ada@example.com",
      groups: Option[AnyRef] = Some(java.util.List.of("platform", "oncall"))
  ): String = {
    val builder = new JWTClaimsSet.Builder()
      .issuer(tokenIssuer)
      .audience(audience)
      .subject(subject)
      .claim("email", subject)
      .claim("nonce", nonce)
      .expirationTime(Date.from(expiresAt))
    val claims = groups.fold(builder)(builder.claim("groups", _)).build()

    val jwt =
      new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(signingKey.getKeyID).build(), claims)
    jwt.sign(new RSASSASigner(signingKey))
    jwt.serialize
  }

  /** A provider that answers discovery, the token exchange and the key set. */
  private def provider(token: String, document: String = discovery): Backend[IO] =
    BackendStub[IO](summon)
      .whenRequestMatches(_.uri.toString.contains("openid-configuration"))
      .thenRespondAdjust(document)
      .whenRequestMatches(_.uri.toString.endsWith("/token"))
      .thenRespondAdjust(s"""{"access_token":"opaque","id_token":"$token","token_type":"Bearer"}""")
      .whenRequestMatches(_.uri.toString.contains("jwks"))
      .thenRespondAdjust(jwks)

  private def party(backend: Backend[IO], configured: OidcConfig = config) =
    FakeStructuredLogger[IO].flatMap(logger =>
      OidcRelyingParty.resource[IO](configured, backend, logger).allocated.map(_._1)
    )

  private val pending: PendingLogin = PendingLogin("the-nonce", Secret("the-verifier"))

  // -----------------------------------------------------------------------------------------------

  test("the authorization URL carries everything the provider needs, and the PKCE challenge is a hash") {
    party(provider(idToken())).flatMap(_.start("the-state")).map {
      case Left(error) => fail(error.message)
      case Right((url, waiting)) =>
        assert(url.startsWith(s"$issuer/authorize?"), url)
        assert(url.contains("response_type=code"), url)
        assert(url.contains("client_id=kui"), url)
        assert(url.contains("state=the-state"), url)
        assert(url.contains("code_challenge_method=S256"), url)
        assert(url.contains("scope=openid+profile+email"), url)

        // The verifier stays here; only its hash was sent. That is the whole of PKCE.
        assert(!url.contains(waiting.codeVerifier.value), "the PKCE verifier was sent to the provider")
        assert(
          url.contains(s"code_challenge=${OidcRelyingParty.codeChallenge(waiting.codeVerifier.value)}"),
          url
        )
        // And the nonce that has to come back inside the token.
        assert(url.contains(s"nonce=${waiting.nonce}"), url)
    }
  }

  test("a good token becomes an identity, with the username claim and the groups claim the operator named") {
    party(provider(idToken())).flatMap(_.complete("the-code", pending)).map {
      case Left(error) => fail(error.message)
      case Right(identity) =>
        assertEquals(identity.name.value, "ada@example.com")
        assertEquals(identity.groups, Set("platform", "oncall"))
    }
  }

  test("a token signed by a key the provider does not publish is refused") {
    party(provider(idToken(signingKey = otherKey))).flatMap(_.complete("the-code", pending)).map { result =>
      assertEquals(result.left.toOption.map(_.code), Some(ErrorCode.Unauthenticated))
    }
  }

  test("a token for another client is refused, however correctly it is signed") {
    party(provider(idToken(audience = "somebody-else"))).flatMap(_.complete("the-code", pending)).map {
      result => assertEquals(result.left.toOption.map(_.code), Some(ErrorCode.Unauthenticated))
    }
  }

  test("a token from another issuer is refused") {
    party(provider(idToken(tokenIssuer = "https://evil.example.com")))
      .flatMap(_.complete("the-code", pending))
      .map(result => assertEquals(result.left.toOption.map(_.code), Some(ErrorCode.Unauthenticated)))
  }

  test("an expired token is refused") {
    party(provider(idToken(expiresAt = Instant.now().minusSeconds(60))))
      .flatMap(_.complete("the-code", pending))
      .map(result => assertEquals(result.left.toOption.map(_.code), Some(ErrorCode.Unauthenticated)))
  }

  test("a token carrying somebody else's nonce is refused, which is what stops a replay") {
    party(provider(idToken(nonce = "another-sign-in")))
      .flatMap(_.complete("the-code", pending))
      .map(result => assertEquals(result.left.toOption.map(_.code), Some(ErrorCode.Unauthenticated)))
  }

  test("every refusal says the same thing, so nothing tells an attacker which check failed") {
    for {
      badKey <- party(provider(idToken(signingKey = otherKey))).flatMap(_.complete("c", pending))
      badAudience <- party(provider(idToken(audience = "somebody-else"))).flatMap(_.complete("c", pending))
      badNonce <- party(provider(idToken(nonce = "another"))).flatMap(_.complete("c", pending))
    } yield {
      val messages = List(badKey, badAudience, badNonce).map(_.left.toOption.map(_.message))
      assertEquals(messages.distinct.size, 1, messages.toString)
    }
  }

  test("a token with no username claim is refused with a message naming the claim to configure") {
    val nameless = party(provider(idToken(subject = "")))

    nameless.flatMap(_.complete("the-code", pending)).map { result =>
      assert(result.left.toOption.exists(_.message.contains("email")), result.toString)
    }
  }

  test("a provider that is down is an upstream failure, not a refused login") {
    val down = BackendStub[IO](summon).whenAnyRequest.thenRespondServerError()

    party(down).flatMap(_.complete("the-code", pending)).map { result =>
      // `KUI-UPSTREAM-*` rather than `KUI-UNAUTHENTICATED`: the person's credentials were never the
      // problem, and telling them to try their password again would be wrong advice.
      assert(
        result.left.toOption.exists(_.code != ErrorCode.Unauthenticated),
        result.toString
      )
    }
  }

  test("the provider's error body is never echoed, because it routinely repeats the request") {
    val refusing = BackendStub[IO](summon)
      .whenRequestMatches(_.uri.toString.contains("openid-configuration"))
      .thenRespondAdjust(discovery)
      .whenRequestMatches(_.uri.toString.endsWith("/token"))
      .thenRespondAdjust(
        """{"error":"invalid_client","error_description":"client kui secret s3cret is wrong"}""",
        StatusCode.BadRequest
      )

    party(refusing).flatMap(_.complete("the-code", pending)).map { result =>
      val message = result.left.toOption.map(_.message).getOrElse("")
      assert(!message.contains("s3cret"), message)
      assert(!message.contains("invalid_client"), message)
    }
  }

  test("a deployment with no provider configured says so rather than failing obscurely") {
    UnconfiguredOidcProvider[IO].start("state").map { result =>
      assertEquals(result.left.toOption.map(_.code), Some(ErrorCode.Unsupported))
      assert(result.left.toOption.exists(_.message.contains("kui.auth.oidc")), result.toString)
    }
  }

  // -----------------------------------------------------------------------------------------------
  // W11-A1's four
  // -----------------------------------------------------------------------------------------------

  test("the callback half refuses too, so an unconfigured deployment cannot be signed in to") {
    // W11-A1: only `start` was asserted. Replacing `UnconfiguredOidcProvider.complete`'s refusal with a
    // `Right(Identity(...))` left `services.identity.__.test` green — and `complete` is the half that
    // hands somebody a principal. The object exists so that the OIDC use case is built in *every*
    // deployment rather than in some of them, which means its fail-open half is reachable in all of them.
    UnconfiguredOidcProvider[IO].complete("a-code", pending).map { result =>
      assertEquals(result.left.toOption.map(_.code), Some(ErrorCode.Unsupported), result.toString)
      assert(result.left.toOption.exists(_.message.contains("kui.auth.oidc")), result.toString)
    }
  }

  test("a token with no expiry at all is refused, which nimbus does not do for us") {
    // W11-A1: "an expired token is refused" above stays green when `checkClaims`'s expiry branch is
    // disabled, because the JWT processor rejects a *past* `exp` itself. What only this file's own check
    // refuses is a token that carries **no** `exp` — a token that never stops being a login. Measured:
    // `expiry.forall(_.isBefore(now)) && false` left all twelve cases here green.
    party(provider(tokenWithoutExpiry)).flatMap(_.complete("the-code", pending)).map { result =>
      assertEquals(result.left.toOption.map(_.code), Some(ErrorCode.Unauthenticated), result.toString)
    }
  }

  test("the PKCE challenge is SHA-256, pinned to RFC 7636's own vector rather than to our own function") {
    // W11-A1: the authorization-URL case above asserts
    // `url.contains(s"code_challenge=${OidcRelyingParty.codeChallenge(verifier)}")` — the product compared
    // against itself. Replacing the digest with the verifier's plain bytes kept it green, which is PKCE
    // downgraded to `plain` while the request still says `S256`: an intercepted authorization code becomes
    // exchangeable again. The vector is RFC 7636 Appendix B, so nothing here can drift with the code.
    assertEquals(
      OidcRelyingParty.codeChallenge("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"),
      "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
    )
  }

  test("a failed discovery is not remembered, so the next sign-in tries the provider again") {
    // W11-A1: caching a placeholder on the failure path left every case here green. The scaladoc states
    // the rule — "a provider that was unreachable during the first sign-in of the morning must not be
    // unreachable for the rest of the day because this process remembered a `None`" — and a poisoned cache
    // is indistinguishable, from the outside, from a provider that is still down.
    for {
      failFirst <- IO.ref(true)
      backend = BackendStub[IO](summon)
        .whenRequestMatches(_.uri.toString.contains("openid-configuration"))
        .thenRespondF(
          failFirst.getAndSet(false).map {
            case true => ResponseStub.adjust("gateway timeout", StatusCode.GatewayTimeout)
            case false => ResponseStub.adjust(discovery)
          }
        )
        .whenRequestMatches(_.uri.toString.endsWith("/token"))
        .thenRespondAdjust(s"""{"access_token":"opaque","id_token":"${idToken()}"}""")
        .whenRequestMatches(_.uri.toString.contains("jwks"))
        .thenRespondAdjust(jwks)
      relyingParty <- party(backend)
      first <- relyingParty.complete("the-code", pending)
      second <- relyingParty.complete("the-code", pending)
    } yield {
      assert(first.isLeft, "the first attempt should have failed at discovery")
      assertEquals(
        second.toOption.map(_.name.value),
        Some("ada@example.com"),
        s"discovery was still broken on the second attempt: $second"
      )
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Q02: the discovery document is bound to the configured issuer
  // -----------------------------------------------------------------------------------------------

  /** Signs in against a provider whose discovery document names `discovered` and whose token says `iss`. */
  private def signInWhereDiscoveryNames(discovered: String, iss: String, configured: OidcConfig = config) =
    party(provider(idToken(tokenIssuer = iss), discoveryNaming(discovered)), configured)
      .flatMap(_.complete("the-code", pending))

  test("a discovery document naming another issuer is refused, even with a token that issuer signed") {
    // Q02: the token's `iss` used to be compared with whatever discovery returned, so a substituted
    // document for `evil` plus a token from `evil` signed by the keys that document published was a login.
    signInWhereDiscoveryNames("https://evil.example.com", "https://evil.example.com").map { result =>
      assert(result.isLeft, s"a foreign issuer's discovery document was trusted: $result")
    }
  }

  test("a discovery document naming another issuer refuses the authorization request too") {
    party(provider(idToken(), discoveryNaming("https://evil.example.com"))).flatMap(_.start("s")).map {
      result => assert(result.isLeft, s"an authorization URL was built from a foreign document: $result")
    }
  }

  test("a trailing-slash difference between discovery and configuration is a mismatch, not an alias") {
    // OpenID Connect Discovery 1.0 §4.3 compares the two as strings; both directions are refused.
    for {
      slashDiscovered <- signInWhereDiscoveryNames(s"$issuer/", s"$issuer/")
      slashConfigured <- signInWhereDiscoveryNames(issuer, issuer, config.copy(issuer = s"$issuer/"))
    } yield {
      assert(slashDiscovered.isLeft, s"'$issuer/' was accepted for configured '$issuer': $slashDiscovered")
      assert(slashConfigured.isLeft, s"'$issuer' was accepted for configured '$issuer/': $slashConfigured")
    }
  }

  test("an issuer that matches exactly still signs in, trailing slash and all") {
    signInWhereDiscoveryNames(s"$issuer/", s"$issuer/", config.copy(issuer = s"$issuer/")).map { result =>
      assertEquals(result.toOption.map(_.name.value), Some("ada@example.com"), result.toString)
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Q03: every documented shape of the groups claim, and a typed refusal for the rest
  // -----------------------------------------------------------------------------------------------

  private def groupsFrom(groups: Option[AnyRef]) =
    party(provider(idToken(groups = groups))).flatMap(_.complete("the-code", pending))

  test("a groups claim that is a single string is one group") {
    groupsFrom(Some("platform")).map(result =>
      assertEquals(result.map(_.groups), Right(Set("platform")), result.toString)
    )
  }

  test("a groups claim that is a comma-separated string is split and trimmed") {
    groupsFrom(Some("platform, oncall ,")).map(result =>
      assertEquals(result.map(_.groups), Right(Set("platform", "oncall")), result.toString)
    )
  }

  test("an empty groups array, and an absent groups claim, are no groups rather than an error") {
    for {
      empty <- groupsFrom(Some(java.util.List.of()))
      absent <- groupsFrom(None)
    } yield {
      assertEquals(empty.map(_.groups), Right(Set.empty[String]), empty.toString)
      assertEquals(absent.map(_.groups), Right(Set.empty[String]), absent.toString)
    }
  }

  test("a groups claim of any other shape is a typed refusal naming the claim, never an exception") {
    val malformed: List[AnyRef] = List(
      Integer.valueOf(42),
      java.lang.Boolean.TRUE,
      java.util.Map.of("platform", "oncall"),
      java.util.List.of("platform", Integer.valueOf(7))
    )

    malformed
      .traverse(shape => groupsFrom(Some(shape)).attempt.map(shape -> _))
      .map(_.foreach { case (shape, outcome) =>
        outcome match {
          case Left(thrown) => fail(s"groups $shape escaped as an exception: $thrown")
          case Right(result) =>
            assertEquals(
              result.left.toOption.map(_.code),
              Some(ErrorCode.Unauthenticated),
              s"$shape: $result"
            )
            assert(result.left.toOption.exists(_.message.contains("'groups'")), s"$shape: $result")
        }
      })
  }

  /** An ID token that is correct in every way except that it never expires. */
  private def tokenWithoutExpiry: String = {
    val claims = new JWTClaimsSet.Builder()
      .issuer(issuer)
      .audience("kui")
      .subject("ada@example.com")
      .claim("email", "ada@example.com")
      .claim("nonce", "the-nonce")
      .build()

    val jwt = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(key.getKeyID).build(), claims)
    jwt.sign(new RSASSASigner(key))
    jwt.serialize
  }
}
