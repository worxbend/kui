package kui.identity.infrastructure

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.security.{MessageDigest, SecureRandom}
import java.util.Base64

import scala.jdk.CollectionConverters.*

import cats.effect.kernel.{Async, Resource, Sync}
import cats.syntax.all.*
import com.nimbusds.jose.JWSAlgorithm
import com.nimbusds.jose.jwk.JWKSet
import com.nimbusds.jose.jwk.source.ImmutableJWKSet
import com.nimbusds.jose.proc.{JWSVerificationKeySelector, SecurityContext}
import com.nimbusds.jwt.JWTClaimsSet
import com.nimbusds.jwt.proc.DefaultJWTProcessor
import io.circe.parser
import org.typelevel.log4cats.StructuredLogger
import sttp.client4.{basicRequest, Backend}
import sttp.model.{MediaType, Uri}

import kui.config.OidcConfig
import kui.identity.application.{OidcProviderPort, PendingLogin}
import kui.identity.domain.Identity
import kui.kernel.error.{ApplicationError, InfrastructureError, KuiError}
import kui.kernel.{Secret, UserName}

/** An OpenID Connect relying party, hand-rolled over sttp and nimbus-jose-jwt (ADR-015).
  *
  * ==What it implements, and what each piece is for==
  *
  *   - **discovery** — `<issuer>/.well-known/openid-configuration`, read once per process and kept, so an
  *     operator configures one URL rather than four. It is fetched lazily on the first sign-in, not at
  *     start-up: a provider that is down must not stop KUI from starting, because the operator most likely to
  *     need KUI at that moment is the one investigating why the provider is down;
  *   - **PKCE** (RFC 7636) — a random verifier is kept here and only its SHA-256 goes to the provider, so an
  *     authorization code intercepted on its way back cannot be exchanged by whoever intercepted it;
  *   - **nonce** — a random value in the request that has to reappear inside the ID token, which is what
  *     stops a valid token from another session being replayed into this one;
  *   - **ID token validation** — signature against the provider's published keys, issuer, audience, expiry,
  *     and the nonce. All five, because any four of them is a hole: a token that is correctly signed by the
  *     right provider for somebody else's client is not a login here.
  *
  * ==What never leaves this file==
  *
  * The client secret, the access token and the ID token. The service's answer is a name and a set of groups.
  * A product that hands its browser the provider's tokens has turned every cross-site scripting bug into an
  * account takeover at the identity provider rather than merely at KUI.
  *
  * ==What is deliberately not implemented==
  *
  * Per-vendor group extraction — GitHub organisations and teams, Google's `hd`, Cognito's `cognito:groups` —
  * is RB-002 and needs one adapter per provider, each with its own extra HTTP call. What is here is the
  * generic case every compliant provider can be configured for: a claim that holds the name and a claim that
  * holds the groups, both named by the operator. `userinfo` is not called either: everything KUI needs is in
  * the ID token when the provider is configured to put it there.
  */
object OidcRelyingParty {

  /** How long any single call to the provider may take.
    *
    * The provider is reached with a plain sttp backend rather than through `libs/http`'s `UpstreamClient`,
    * and that is a deliberate exception with one reason: `UpstreamClient` rebases every request onto the
    * configured address, which is exactly right for a KUI service behind a known URL and exactly wrong here.
    * A provider's discovery document names its own token endpoint and its own key set, and those are
    * routinely on different hosts — Google publishes keys on `www.googleapis.com` — so rebasing them onto the
    * issuer would send the token exchange to an address that does not serve it. What is kept is the part that
    * matters for a call a person is waiting on: a bound on how long it may take.
    */
  val CallTimeout: scala.concurrent.duration.FiniteDuration =
    scala.concurrent.duration.FiniteDuration(10, java.util.concurrent.TimeUnit.SECONDS)

  /** The well-known path RFC 8414 fixes. */
  val DiscoveryPath: String = "/.well-known/openid-configuration"

  /** What discovery tells us. Four fields; the rest of the document is ignored. */
  final case class Discovered(
      issuer: String,
      authorizationEndpoint: String,
      tokenEndpoint: String,
      jwksUri: String
  )

  def resource[F[_]: Async](
      config: OidcConfig,
      backend: Backend[F],
      logger: StructuredLogger[F]
  ): Resource[F, OidcProviderPort[F]] =
    Resource.eval(
      for {
        random <- Sync[F].delay(new SecureRandom())
        cache <- cats.effect.std.AtomicCell[F].of(none[Discovered])
      } yield new Impl[F](config, backend, random, cache, logger)
    )

  final private class Impl[F[_]: Async](
      config: OidcConfig,
      backend: Backend[F],
      random: SecureRandom,
      discovery: cats.effect.std.AtomicCell[F, Option[Discovered]],
      logger: StructuredLogger[F]
  ) extends OidcProviderPort[F] {

    def start(state: String): F[Either[KuiError, (String, PendingLogin)]] =
      discovered.flatMap {
        case Left(error) => error.asLeft[(String, PendingLogin)].pure[F]
        case Right(provider) =>
          for {
            nonce <- randomValue
            verifier <- randomValue
          } yield {
            val challenge = codeChallenge(verifier)
            val url = authorizationUrl(provider, state, nonce, challenge)
            (url, PendingLogin(nonce, Secret(verifier))).asRight[KuiError]
          }
      }

    def complete(code: String, pending: PendingLogin): F[Either[KuiError, Identity]] =
      discovered.flatMap {
        case Left(error) => error.asLeft[Identity].pure[F]
        case Right(provider) =>
          exchange(provider, code, pending).flatMap {
            case Left(error) => error.asLeft[Identity].pure[F]
            case Right(idToken) =>
              keys(provider).map(_.flatMap(verify(_, idToken, pending.nonce)))
          }
      }

    // ---------------------------------------------------------------------------------------------
    // Discovery
    // ---------------------------------------------------------------------------------------------

    /** The discovery document, fetched at most once per process while it keeps succeeding.
      *
      * A failure is *not* cached: a provider that was unreachable during the first sign-in of the morning
      * must not be unreachable for the rest of the day because this process remembered a `None`.
      */
    private def discovered: F[Either[KuiError, Discovered]] =
      discovery.evalModify {
        case Some(cached) => (cached.some, cached.asRight[KuiError]).pure[F]
        case None =>
          fetchDiscovery.map {
            case Right(value) => (value.some, value.asRight[KuiError])
            case Left(error) => (none[Discovered], error.asLeft[Discovered])
          }
      }

    /** Reads the discovery document and refuses one that names a different issuer than the one configured.
      *
      * OpenID Connect Discovery 1.0 §4.3: the `issuer` in the document must be *identical* to the issuer it
      * was fetched for, compared as strings — so a trailing slash on one side and not the other is a
      * mismatch, not an alias. Without this, a misdirected or substituted document would replace the
      * configured trust identity, and every token its keys sign would then pass the issuer check. A mismatch
      * is a failure, so it is not cached either.
      */
    private def fetchDiscovery: F[Either[KuiError, Discovered]] =
      parseDiscovery.flatMap {
        case Right(document) if document.issuer != config.issuer =>
          logger
            .warn(Map("configured" -> config.issuer, "discovered" -> document.issuer))(
              "identity: the OIDC discovery document names a different issuer than the one configured"
            )
            .as(malformed("the discovery document names a different issuer than the one configured").asLeft)
        case other => other.pure[F]
      }

    private def parseDiscovery: F[Either[KuiError, Discovered]] =
      get(s"${config.issuer.stripSuffix("/")}$DiscoveryPath").map(_.flatMap { body =>
        parser
          .parse(body)
          .leftMap(_ => malformed("the discovery document is not JSON"))
          .flatMap { json =>
            val cursor = json.hcursor
            (
              cursor.get[String]("issuer").toOption,
              cursor.get[String]("authorization_endpoint").toOption,
              cursor.get[String]("token_endpoint").toOption,
              cursor.get[String]("jwks_uri").toOption
            ) match {
              case (Some(issuer), Some(authorization), Some(token), Some(jwks)) =>
                Discovered(issuer, authorization, token, jwks).asRight[KuiError]
              case _ =>
                malformed(
                  "the discovery document is missing one of issuer, authorization_endpoint, " +
                    "token_endpoint or jwks_uri"
                ).asLeft[Discovered]
            }
          }
      })

    // ---------------------------------------------------------------------------------------------
    // The authorization request
    // ---------------------------------------------------------------------------------------------

    private def authorizationUrl(
        provider: Discovered,
        state: String,
        nonce: String,
        challenge: String
    ): String = {
      val parameters = List(
        "response_type" -> "code",
        "client_id" -> config.clientId,
        "redirect_uri" -> config.redirectUri,
        "scope" -> config.scopes.mkString(" "),
        "state" -> state,
        "nonce" -> nonce,
        "code_challenge" -> challenge,
        "code_challenge_method" -> "S256"
      )

      val query = parameters.map((name, value) => s"$name=${encode(value)}").mkString("&")
      val separator = if provider.authorizationEndpoint.contains("?") then "&" else "?"

      s"${provider.authorizationEndpoint}$separator$query"
    }

    // ---------------------------------------------------------------------------------------------
    // The token exchange
    // ---------------------------------------------------------------------------------------------

    /** Exchanges the code for an ID token.
      *
      * The client secret goes in the body rather than in an `Authorization: Basic` header because the two are
      * equivalent to a compliant provider and the body form works with providers that never implemented the
      * header one. Either way it goes over TLS to the address the provider itself published.
      */
    private def exchange(
        provider: Discovered,
        code: String,
        pending: PendingLogin
    ): F[Either[KuiError, String]] = {
      val form = Map(
        "grant_type" -> "authorization_code",
        "code" -> code,
        "redirect_uri" -> config.redirectUri,
        "client_id" -> config.clientId,
        "client_secret" -> config.clientSecret.value,
        "code_verifier" -> pending.codeVerifier.value
      )

      Uri.parse(provider.tokenEndpoint) match {
        case Left(_) => malformed("the provider's token endpoint is not a URL").asLeft[String].pure[F]
        case Right(uri) =>
          basicRequest
            .post(uri)
            .body(form)
            .contentType(MediaType.ApplicationXWwwFormUrlencoded)
            .readTimeout(CallTimeout)
            .send(backend)
            .map { response =>
              response.body match {
                case Left(_) =>
                  // The provider's error body is deliberately not read into the answer: it routinely
                  // echoes the request, client id included, and this string reaches a browser.
                  InfrastructureError
                    .Upstream(config.label, response.code.code)
                    .asLeft[String]
                case Right(body) =>
                  parser
                    .parse(body)
                    .toOption
                    .flatMap(_.hcursor.get[String]("id_token").toOption)
                    .toRight(malformed("the provider's token response carried no id_token"))
              }
            }
            .handleErrorWith(error =>
              logger
                .error(error)("identity: the OIDC token exchange failed")
                .as(unreachable.asLeft[String])
            )
      }
    }

    // ---------------------------------------------------------------------------------------------
    // ID token validation
    // ---------------------------------------------------------------------------------------------

    private def keys(provider: Discovered): F[Either[KuiError, JWKSet]] =
      get(provider.jwksUri).map(_.flatMap { body =>
        Either
          .catchNonFatal(JWKSet.parse(body))
          .leftMap(_ => malformed("the provider's key set could not be read"))
      })

    /** Signature, issuer, audience, expiry and nonce — all five, or no identity. */
    private def verify(
        keySet: JWKSet,
        idToken: String,
        nonce: String
    ): Either[KuiError, Identity] =
      Either
        .catchNonFatal {
          val processor = new DefaultJWTProcessor[SecurityContext]()
          processor.setJWSKeySelector(
            new JWSVerificationKeySelector[SecurityContext](
              // RS256 is what essentially every provider signs with; a provider that signs with something
              // else is refused rather than trusted, because an algorithm this process did not expect is
              // the shape of the classic "alg: none" family of attacks.
              JWSAlgorithm.RS256,
              new ImmutableJWKSet[SecurityContext](keySet)
            )
          )
          // Nimbus's `SecurityContext` is its hook for callers that select keys per request; this process
          // selects one fixed key set, so there is nothing to pass and the library's own documented value
          // for "no context" is `null`. There is no Option-shaped overload to use instead.
          // scalafix:off DisableSyntax.null
          processor.process(idToken, null)
          // scalafix:on DisableSyntax.null
        }
        .leftMap(_ => OidcRelyingParty.Invalid)
        .flatMap(claims => checkClaims(claims, nonce))

    private def checkClaims(
        claims: JWTClaimsSet,
        nonce: String
    ): Either[KuiError, Identity] = {
      val audiences = Option(claims.getAudience).map(_.toArray.toList.map(String.valueOf)).getOrElse(Nil)
      val expiry = Option(claims.getExpirationTime).map(_.toInstant)
      val presentedNonce = Option(claims.getStringClaim("nonce"))

      // The configured issuer, not the discovered one: discovery is trusted only once it has agreed with it.
      if Option(claims.getIssuer).forall(_ != config.issuer) then OidcRelyingParty.Invalid.asLeft
      else if !audiences.contains(config.clientId) then OidcRelyingParty.Invalid.asLeft
      else if expiry.forall(_.isBefore(java.time.Instant.now())) then OidcRelyingParty.Invalid.asLeft
      else if !presentedNonce.contains(nonce) then OidcRelyingParty.Invalid.asLeft
      else
        Option(claims.getStringClaim(config.usernameClaim))
          .filter(_.trim.nonEmpty)
          .toRight(
            ApplicationError.Unauthenticated(
              s"the provider's token carries no '${config.usernameClaim}' claim to use as a username"
            )
          )
          .flatMap(name => groupsOf(claims).map(groups => Identity(UserName.unsafe(name), groups)))
    }

    /** The groups claim, decoded from the raw claim value rather than through nimbus's typed getters, which
      * throw on every shape but the one each of them expects.
      *
      * Three shapes are accepted, the ones providers actually emit: a JSON array of strings, a single string,
      * and a comma-separated string. An absent claim and an empty array both yield no groups, which is a
      * deployment whose roles name users rather than groups — not an error. Anything else — a number, an
      * object, an array holding something other than strings — refuses the sign-in with a message naming the
      * claim, because guessing at the groups of a token we cannot read is guessing at somebody's permissions.
      */
    private def groupsOf(claims: JWTClaimsSet): Either[KuiError, Set[String]] =
      config.groupsClaim.fold(Set.empty[String].asRight[KuiError]) { claim =>
        val refused: KuiError = ApplicationError.Unauthenticated(
          s"the provider's '$claim' claim is neither a list of group names nor a comma-separated string"
        )

        val names: Either[KuiError, List[String]] = Option(claims.getClaim(claim)) match {
          case None => Nil.asRight
          case Some(single: String) => single.split(',').toList.asRight
          case Some(listed: java.util.List[?]) =>
            listed.asScala.toList.traverse {
              case name: String => name.asRight[KuiError]
              case _ => refused.asLeft[String]
            }
          case Some(_) => refused.asLeft
        }

        names.map(_.map(_.trim).filter(_.nonEmpty).toSet)
      }

    // ---------------------------------------------------------------------------------------------

    private def get(url: String): F[Either[KuiError, String]] =
      Uri.parse(url) match {
        case Left(_) =>
          malformed(s"the provider published an address KUI cannot parse").asLeft[String].pure[F]
        case Right(uri) =>
          basicRequest
            .get(uri)
            .readTimeout(CallTimeout)
            .send(backend)
            .map(response =>
              response.body.leftMap(_ => InfrastructureError.Upstream(config.label, response.code.code))
            )
            .handleErrorWith(error =>
              logger
                .error(error)(s"identity: $url could not be read")
                .as(unreachable.asLeft[String])
            )
      }

    private def randomValue: F[String] =
      Sync[F].delay {
        val bytes = new Array[Byte](32)
        random.nextBytes(bytes)
        Base64.getUrlEncoder.withoutPadding.encodeToString(bytes)
      }

    private def malformed(what: String): KuiError =
      InfrastructureError.Unreachable(config.label, what)

    private def unreachable: KuiError =
      InfrastructureError.Unreachable(config.label, "the provider could not be reached")
  }

  /** What every failed ID-token check answers with.
    *
    * One value for a bad signature, a wrong issuer, a wrong audience, an expired token and a mismatched
    * nonce, because telling a caller which of the five failed tells an attacker which part of a forged token
    * to fix — the same reasoning `PrincipalError` applies to the signed principal header.
    */
  val Invalid: KuiError =
    ApplicationError.Unauthenticated("that sign-in could not be verified; start again")

  /** The PKCE code challenge: base64url of the SHA-256 of the verifier, unpadded (RFC 7636 §4.2). */
  def codeChallenge(verifier: String): String =
    Base64.getUrlEncoder.withoutPadding
      .encodeToString(
        MessageDigest.getInstance("SHA-256").digest(verifier.getBytes(StandardCharsets.UTF_8))
      )

  private def encode(value: String): String = URLEncoder.encode(value, StandardCharsets.UTF_8)
}

/** The port a deployment with no provider configured gets.
  *
  * It exists so that the OIDC use case is built in every deployment rather than in some of them: one code
  * path, and a refusal that says what is missing rather than a `NullPointerException` or an endpoint that is
  * absent from half the deployments' route lists.
  */
object UnconfiguredOidcProvider {

  val NotConfigured: KuiError =
    ApplicationError.Unsupported("signing in with an identity provider, because kui.auth.oidc is not set")

  def apply[F[_]: Sync]: OidcProviderPort[F] = new OidcProviderPort[F] {

    def start(state: String): F[Either[KuiError, (String, PendingLogin)]] =
      Sync[F].pure(NotConfigured.asLeft)

    def complete(code: String, pending: PendingLogin): F[Either[KuiError, Identity]] =
      Sync[F].pure(NotConfigured.asLeft)
  }
}
