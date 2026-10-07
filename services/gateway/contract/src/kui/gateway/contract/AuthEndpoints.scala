package kui.gateway.contract

import sttp.model.StatusCode
import sttp.model.headers.CookieValueWithMeta
import sttp.tapir.*
import sttp.tapir.json.circe.jsonBody

import kui.contracts.ErrorEnvelope
import kui.gateway.contract.dto.AuthMeResponse
import kui.identity.contract.dto.{
  AuthSettingsDto,
  ChangePasswordRequest,
  LoginRequest,
  LoginResponse,
  OidcStartResponse
}

/** The gateway's own authentication surface: what kind of sign-in this is, how to do it, who you are, and how
  * to stop being them (ADR-015, ADR-019).
  *
  * ==Why these live at the gateway and not in the identity service's contract==
  *
  * Every other service's contract becomes a public route by being proxied. These cannot be, because a login
  * is the moment a browser is given a session — a new id, a new CSRF secret, a `Set-Cookie` — and sessions
  * live here. A proxied `/api/v1/identity/login` would answer with a principal in a JSON body and set no
  * cookie, which is a login that logs nobody in. So the gateway serves these itself and calls the identity
  * service one hop inward, through the very same contract values (ADR-041 rule A4), which is also why the
  * request and response shapes below are the identity service's own types rather than copies of them.
  *
  * ==No principal header, and no CSRF header either==
  *
  * None of these carries `X-Kui-Principal`: that header is what the gateway *emits* on its way to a service
  * and never something it accepts from a browser (ADR-040). None of them declares `X-Kui-Csrf` as an input
  * either, and that is not an omission — `SessionMiddleware`'s CSRF interceptor applies the check to every
  * mutating request before any endpoint's logic runs, so declaring the header here would only change a `403`
  * with a reason into a `400` about a missing header.
  */
object AuthEndpoints {

  /** The session cookie's name, as ADR-019 fixes it.
    *
    * It is declared here, in the contract, because two of the endpoints below name it as an output and the
    * session middleware names it when it reads one — `SessionMiddleware.CookieName` is defined as this value,
    * so there is one name in one place.
    *
    * **It has to come before the endpoints that use it, and that is not a style preference.** Scala
    * initialises an object's `val`s in source order, so an endpoint declared above this line would be built
    * while this string was still `null` and would emit a cookie literally named `null` — which is exactly
    * what happened, and what `SignInSeamSuite` caught: the browser was handed `null=<id>`, the session
    * middleware then stamped the *old* cookie beside it because it saw no `kui_session=` header of its own,
    * and signing in silently left the operator in the session they arrived with. Session fixation, from a
    * declaration order.
    */
  val SessionCookie: String = "kui_session"

  /** The base for a sign-in endpoint: the gateway's usual envelope, plus the status.
    *
    * The status is part of the error output for the same reason `SecuredRoutes` puts it there on every
    * internal endpoint: `ErrorEnvelope.statusOf` is the single code-to-status table in the system (ADR-034),
    * and these are the gateway's only own endpoints that can fail with a business error — a refused password
    * is a `401`, a change with nowhere to be saved is a `501`, and answering both with the endpoint's default
    * `400` would be a second, wrong mapping.
    */
  private val failing: PublicEndpoint[Unit, (ErrorEnvelope, StatusCode), Unit, Any] =
    GatewayEndpoints.base.errorOut(statusCode)

  val me: PublicEndpoint[Unit, ErrorEnvelope, AuthMeResponse, Any] =
    GatewayEndpoints.base.get
      .in("auth" / "me")
      .out(jsonBody[AuthMeResponse])
      .name("gateway.auth.me")
      .summary("The caller's identity and the CSRF token to use for mutations")
      .tag("auth")

  /** What kind of sign-in this deployment uses, for the screen that has to draw one.
    *
    * Answered from the gateway's own configuration rather than by asking the identity service, and that is
    * deliberate: a login screen that cannot render because the service behind the login is down is a product
    * that gives an operator nothing to look at during exactly the outage they need to see. The three fields
    * are all the browser needs and none of them can carry a credential.
    */
  val settings: PublicEndpoint[Unit, ErrorEnvelope, AuthSettingsDto, Any] =
    GatewayEndpoints.base.get
      .in("auth" / "settings")
      .out(jsonBody[AuthSettingsDto])
      .name("gateway.auth.settings")
      .summary("Which kind of sign-in this deployment uses")
      .tag("auth")

  /** Signing in with a username and a password, **as a client sees it**.
    *
    * ==Why the `Set-Cookie` is not on this value==
    *
    * The response really does carry one — signing in *replaces* the session, id and CSRF secret and all
    * (ADR-019's session-fixation defence), so the cookie that has to reach the browser is not the one the
    * request arrived with, and the server therefore has to declare it rather than leave it to the session
    * middleware. It is declared on [[loginWithSession]] below, which is what the server is built from.
    *
    * It is not on the value a *client* is built from, because a browser can never satisfy it. `Set-Cookie` is
    * a forbidden response header: `fetch` strips it before any script can see it, on purpose, since a script
    * that could read one could read an `HttpOnly` session cookie. A Tapir client built from an endpoint that
    * declares a `Set-Cookie` output therefore fails to decode **every** response, including the successful
    * ones.
    *
    * That is not hypothetical. The sign-in screen was built against the cookie-bearing value, and a correct
    * sign-in — `200`, session issued, cookie stored by the browser exactly as it should be — was reported to
    * the user as "The server sent something KUI could not read", with no way in. The browser was already
    * holding the cookie it was complaining it could not see.
    */
  val login: PublicEndpoint[LoginRequest, (ErrorEnvelope, StatusCode), LoginResponse, Any] =
    failing.post
      .in("auth" / "login")
      .in(jsonBody[LoginRequest])
      .out(jsonBody[LoginResponse])
      .name("gateway.auth.login")
      .summary("Sign in with a username and a password")
      .tag("auth")

  /** [[login]] with the session cookie the server actually emits. Server-side and documentation only.
    *
    * Derived from [[login]] rather than written out again, so the path, the body and the error shape have
    * exactly one definition and cannot drift apart.
    */
  val loginWithSession: PublicEndpoint[
    LoginRequest,
    (ErrorEnvelope, StatusCode),
    (LoginResponse, Option[CookieValueWithMeta]),
    Any
  ] =
    login.out(setCookieOpt(SessionCookie))

  /** Completing a required password change. It grants no session on its own: the caller signs in again with
    * the new password, which is one flow rather than two ways to obtain a session.
    */
  val changePassword: PublicEndpoint[ChangePasswordRequest, (ErrorEnvelope, StatusCode), Unit, Any] =
    failing.post
      .in("auth" / "password")
      .in(jsonBody[ChangePasswordRequest])
      .name("gateway.auth.password")
      .summary("Set a new password, completing a required change")
      .tag("auth")

  /** Where to send the browser to sign in with the configured provider. */
  val oidcStart: PublicEndpoint[Unit, (ErrorEnvelope, StatusCode), OidcStartResponse, Any] =
    failing.post
      .in("auth" / "oidc" / "start")
      .out(jsonBody[OidcStartResponse])
      .name("gateway.auth.oidc.start")
      .summary("Begin a sign-in with the configured identity provider")
      .tag("auth")

  /** Where the provider sends the browser back to.
    *
    * A `GET`, because a provider redirects rather than posts, and therefore outside the CSRF check — which is
    * correct and not a gap: the `state` parameter is this flow's own single-use anti-forgery token, issued by
    * the identity service and refused if it was not (ADR-019, and `OidcLoginUseCase`'s own note).
    *
    * It answers with a redirect rather than a document, because the thing at the other end of it is a browser
    * that was navigated here, not a script that called an API.
    */
  val oidcCallback: PublicEndpoint[
    (String, String),
    (ErrorEnvelope, StatusCode),
    (StatusCode, String, CookieValueWithMeta),
    Any
  ] =
    failing.get
      .in("auth" / "oidc" / "callback")
      .in(query[String]("code"))
      .in(query[String]("state"))
      .out(statusCode.description(StatusCode.Found, "the browser is sent back to the interface"))
      .out(header[String]("Location"))
      .out(setCookie(SessionCookie))
      .name("gateway.auth.oidc.callback")
      .summary("Complete a sign-in with the configured identity provider")
      .tag("auth")

  val logout: PublicEndpoint[Unit, ErrorEnvelope, Unit, Any] =
    GatewayEndpoints.base.post
      .in("auth" / "logout")
      .name("gateway.auth.logout")
      .summary("Clears the session cookie")
      .tag("auth")

  /** For the published OpenAPI document, which describes what the *server* sends — so the cookie-bearing
    * value is the one listed. `oidcCallback` keeps its cookie output on the single value it has, because
    * nothing decodes that response in a script: it is a redirect the browser follows.
    */
  val all: List[AnyEndpoint] =
    List(me, settings, loginWithSession, changePassword, oidcStart, oidcCallback, logout)
}
