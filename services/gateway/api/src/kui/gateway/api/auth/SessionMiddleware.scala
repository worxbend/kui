package kui.gateway.api.auth

import java.time.Instant

import scala.concurrent.duration.DurationInt

import cats.effect.kernel.{Clock, Sync}
import cats.syntax.all.*
import org.typelevel.log4cats.StructuredLogger
import sttp.model.headers.{Cookie, CookieWithMeta}
import sttp.model.{Header, StatusCode}
import sttp.monad.MonadError
import sttp.tapir.json.circe.jsonBody
import sttp.tapir.model.ServerRequest
import sttp.tapir.server.interceptor.{
  DecodeFailureContext,
  DecodeSuccessContext,
  EndpointHandler,
  EndpointInterceptor,
  Interceptor,
  RequestInterceptor,
  RequestResult,
  Responder,
  SecurityFailureContext
}
import sttp.tapir.server.interpreter.BodyListener
import sttp.tapir.server.model.ValuedEndpointOutput
import sttp.tapir.{statusCode, AttributeKey, EndpointOutput}

import kui.contracts.ErrorEnvelope.given
import kui.contracts.{ErrorEnvelope, HttpHeaders}
import kui.gateway.application.session.{Session, SessionId, SessionRef, SessionStore}
import kui.gateway.contract.GatewayEndpoints
import kui.http.BasePath
import kui.kernel.error.ErrorCode
import kui.observability.Correlation
import kui.security.{Principal, PrincipalKind}

/** The session and CSRF boundary every request passes through (ADR-019).
  *
  * Three interceptors, in the order [[interceptors]] returns them, mirroring the split `EdgeHeaders` already
  * uses and for the identical reason: a request has to be attached to a session *before* Tapir decodes any
  * endpoint's inputs, or an endpoint that reads the request via `sttp.tapir.extractFromRequest` — which
  * `AuthRoutes` does — captures the pre-attachment version and never sees it. That rules out doing this as a
  * single [[EndpointInterceptor]], which only runs *after* decoding:
  *
  *   1. **Attach a session** ([[RequestInterceptor.transformServerRequest]]). The cookie the request carries
  *      is looked up; a missing, unrecognised or expired one gets a fresh anonymous session instead of a
  *      failure, except for recently retired pre-login ids, which cannot issue a cookie over a login's
  *      replacement — anonymous mode issues a session to every browser precisely so the CSRF machinery is
  *      exercised in CI for six milestones before login exists. That applies to the gateway's own API only: a
  *      static asset or a health probe carries no session unless it presented a valid cookie of its own, and
  *      [[ensureSession]] says why. The session, when there is one, is attached to the request as an
  *      attribute ([[SessionMiddleware.Attribute]]), which both later steps and `AuthRoutes` read.
  *   1. **Enforce authentication and CSRF** (an [[EndpointInterceptor]], which alone has a [[Responder]] in
  *      hand to answer with something other than the endpoint's own logic). [[CsrfCheck.verdict]] is applied
  *      once an endpoint has matched; a `Denied` verdict answers `403 KUI-FORBIDDEN` immediately, without
  *      that endpoint's logic ever running, and is logged at `WARN` — a CSRF rejection is a signal worth
  *      alerting on, not a client error to note and move past.
  *   1. **Stamp `Set-Cookie`** ([[RequestInterceptor.transformResultEffect]]), reading the same attribute
  *      back off the request once a response exists, only when attachment minted a new session. Existing
  *      sessions are never restamped: a delayed response must not overwrite a later sign-in's rotation.
  *      Sign-in routes explicitly output their rotated cookie.
  *
  * Installed as a block, after `EdgeHeaders` and before `KuiInterceptors`, in `GatewayWiring`'s chain:
  * `Cookie` is not an `X-Kui-*` header so the edge strip does not touch it, and this decision belongs beside
  * authentication, ahead of tracing and metrics recording an endpoint that never ran.
  */
object SessionMiddleware {

  /** The cookie name ADR-019 fixes. */
  val CookieName: String = "kui_session"

  /** The header a non-`GET` cookie-authenticated request must echo the session's CSRF secret in.
    *
    * The name itself lives in `kui.contracts.HttpHeaders`, which the browser's `ApiClient` compiles against
    * too, so that the two halves cannot drift apart the way they once did. `HttpHeaders.Csrf` explains why
    * the name is deliberately outside the `X-Kui-*` family.
    */
  val CsrfHeaderName: String = HttpHeaders.Csrf

  /** Where the resolved session lives for the rest of the request pipeline to read. */
  val Attribute: AttributeKey[Session] = new AttributeKey[Session]("kui.gateway.session")

  private val RetiredAttribute: AttributeKey[Boolean] =
    new AttributeKey[Boolean]("kui.gateway.retired-session")

  private enum Resolution {
    case Active(session: Session)
    case Absent, Retired
  }

  /** The three interceptors, in the order `GatewayWiring` installs them. */
  def interceptors[F[_]: Sync](
      store: SessionStore[F],
      logger: StructuredLogger[F],
      basePath: String,
      secureCookies: Boolean,
      authenticationRequired: Boolean = false,
      trustedProxies: Set[String] = Set.empty
  ): List[Interceptor[F]] = {
    val creation = AuthRoutes.RateLimiter[F]
    List(
      attachInterceptor[F](store, basePath, creation, trustedProxies),
      CsrfInterceptor[F](logger, basePath, authenticationRequired),
      stampCookieInterceptor[F](basePath, secureCookies)
    )
  }

  /** The session this request should carry, if it should carry one at all.
    *
    * A valid cookie always resolves to its session, whatever the request is for. A request with no usable
    * cookie (unless recently retired) gets a *new* anonymous session only when it is addressed to the
    * gateway's own API, and that restriction is doing two jobs.
    *
    * The first is cache poisoning. Every response the gateway produces used to have `Set-Cookie` appended to
    * it, static assets included -- and a hashed asset is served `Cache-Control: public, max-age=31536000,
    * immutable`. A response that is both publicly cacheable for a year and carries a per-user credential is a
    * session-fixation bug waiting for a shared cache or CDN to store it, after which every visitor is handed
    * the first visitor's session. KUI supports running behind a reverse proxy (`server.basePath` exists for
    * it), so that topology is not hypothetical. A request that never gets a session never gets a
    * `Set-Cookie`, whatever its cache headers say.
    *
    * The second is allocation pressure. Static assets and health probes need no session and do not create
    * one. API creation is rate-limited, and the store isolates anonymous and authenticated capacity.
    */
  private def ensureSession[F[_]: Sync](
      store: SessionStore[F],
      request: ServerRequest,
      basePath: String,
      now: Instant,
      creationBudget: Option[F[Boolean]]
  ): F[Resolution] =
    cookieValue(request) match {
      case None => mint[F](store, request, basePath, now, creationBudget).map(resolve)
      case Some(raw) =>
        store.get(SessionId.unsafe(raw), now).flatMap {
          case Some(session) => (Resolution.Active(session): Resolution).pure[F]
          case None =>
            store.isRetired(SessionId.unsafe(raw), now).flatMap {
              case true => (Resolution.Retired: Resolution).pure[F]
              case false => mint[F](store, request, basePath, now, creationBudget).map(resolve)
            }
        }
    }

  private def resolve(session: Option[Session]): Resolution =
    session.fold[Resolution](Resolution.Absent)(Resolution.Active(_))

  private def mint[F[_]: Sync](
      store: SessionStore[F],
      request: ServerRequest,
      basePath: String,
      now: Instant,
      creationBudget: Option[F[Boolean]]
  ): F[Option[Session]] =
    if needsSession(request, basePath) then
      creationBudget.getOrElse(true.pure[F]).flatMap {
        case true => store.create(Principal.Anonymous, now).map(_.some)
        case false => none[Session].pure[F]
      }
    else none[Session].pure[F]

  /** Whether a request with no session of its own should be given one.
    *
    * The gateway's own API, and nothing else. Health probes are excluded even though they live under the same
    * prefix: they are called by an orchestrator on a timer, forever, and have no user behind them.
    */
  def needsSession(request: ServerRequest, basePath: String): Boolean =
    needsSession(request.uri.path.toList.filter(_.nonEmpty), basePath)

  /** The same decision over a path that has already been split into segments.
    *
    * It exists so that the rule can be checked against the *route table* rather than against one request at a
    * time, which is what makes the CSRF check's `session.fold(PrincipalKind.Anonymous)(...)` safe: that fold
    * is a fail-open the moment the gateway serves a non-safe method outside the set of paths that mint a
    * session, and whether it does is a property of the endpoint list. `EveryMutatingRouteMintsASession` in
    * `SessionMiddlewareSuite` is the case, and it needs this shape because a `ServerRequest` cannot be
    * conjured out of an endpoint's path template.
    */
  def needsSession(pathSegments: List[String], basePath: String): Boolean =
    pathSegments.drop(BasePath.segments(basePath).size) match {
      case api if api.startsWith(ApiSegments) => !api.drop(ApiSegments.size).startsWith(List("health"))
      case _ => false
    }

  private val ApiSegments: List[String] = BasePath.segments(GatewayEndpoints.ApiPrefix)

  private def needsAuthentication(request: ServerRequest, basePath: String): Boolean = {
    val path = request.uri.path.toList.filter(_.nonEmpty).drop(BasePath.segments(basePath).size)
    val publicPaths = Set(
      List("info"),
      List("health", "live"),
      List("health", "ready"),
      List("auth", "me"),
      List("auth", "settings"),
      List("auth", "login"),
      List("auth", "password"),
      List("auth", "logout"),
      List("auth", "oidc", "start"),
      List("auth", "oidc", "callback")
    )
    path.startsWith(ApiSegments) && !publicPaths.contains(path.drop(ApiSegments.size))
  }

  /** The `kui_session` cookie's value from the request's `Cookie` header, if present and well-formed.
    *
    * Parsed with `sttp.model.headers.Cookie.parse` rather than read as a raw substring: a `Cookie` header can
    * carry several cookies, `;`-separated, and a substring search would find the wrong one if a proxy or
    * another application on the same domain set a cookie whose name happened to contain `kui_session`.
    */
  def cookieValue(request: ServerRequest): Option[String] =
    request.header("Cookie").flatMap { raw =>
      Cookie.parse(raw).toOption.flatMap(_.find(_.name == CookieName)).map(_.value)
    }

  /** The `Set-Cookie` value for a session, exactly as ADR-019 specifies it.
    *
    * `secure` defaults to `true`; the one escape hatch is `devInsecureCookies` (`GatewayWiring`), meant for
    * `localhost` development over plain HTTP, and refused outright by CFG-001 when the deployment's
    * configured base URL is `https` — a flag meant for a laptop must not be reachable in a configuration that
    * could also describe production.
    */
  def setCookie(session: Session, basePath: String, secure: Boolean): CookieWithMeta =
    CookieWithMeta.unsafeApply(
      name = CookieName,
      value = session.id.value,
      path = Some(if basePath.isEmpty then "/" else s"$basePath/"),
      secure = secure,
      httpOnly = true,
      sameSite = Some(Cookie.SameSite.Lax)
    )

  // -----------------------------------------------------------------------------------------------
  // 1. Attach a session, ahead of any endpoint's own input decoding.
  // -----------------------------------------------------------------------------------------------

  private def attachInterceptor[F[_]: Sync](
      store: SessionStore[F],
      basePath: String,
      creation: AuthRoutes.RateLimiter[F],
      trustedProxies: Set[String]
  ): RequestInterceptor[F] =
    RequestInterceptor.transformServerRequest[F] { request =>
      for {
        now <- Clock[F].realTimeInstant
        session <- ensureSession[F](
          store,
          request,
          basePath,
          now,
          Some(
            // A sender already blocked by its own budget must not spend everybody else's allowance.
            creation.tryAcquire(ClientAddress.of(request, trustedProxies), 30, 1.minute).flatMap {
              case false => false.pure[F]
              case true => creation.tryAcquire("all", 600, 1.minute)
            }
          )
        )
      } yield session match {
        case Resolution.Active(active) => request.attribute(Attribute, active)
        case Resolution.Retired => request.attribute(RetiredAttribute, true)
        case Resolution.Absent => request
      }
    }

  // -----------------------------------------------------------------------------------------------
  // 2. CSRF: an EndpointInterceptor, because only this extension point hands over a Responder capable
  //    of answering with a whole different response before the endpoint's own logic runs.
  // -----------------------------------------------------------------------------------------------

  final private class CsrfInterceptor[F[_]: Sync](
      logger: StructuredLogger[F],
      basePath: String,
      authenticationRequired: Boolean
  ) extends EndpointInterceptor[F] {

    def apply[B](responder: Responder[F, B], delegate: EndpointHandler[F, B]): EndpointHandler[F, B] =
      new EndpointHandler[F, B] {

        def onDecodeSuccess[A, U, I](ctx: DecodeSuccessContext[F, A, U, I])(using
            monad: MonadError[F],
            bodyListener: BodyListener[F, B]
        ): F[sttp.tapir.server.model.ServerResponse[B]] = {
          // A request can legitimately arrive with no session at all: static assets and health probes
          // are never given one. `CsrfCheck` already has the right answer for that -- a safe method is
          // allowed, and a mutation with no session to check the token against is denied. Passing the
          // absence through rather than special-casing it keeps every CSRF decision in the one function
          // whose table is the specification, and keeps the fail-closed direction for a mutation.
          val session = ctx.request.attribute(Attribute)
          val verdict = CsrfCheck.verdict(
            method = ctx.request.method.method,
            authKind = session.fold(PrincipalKind.Anonymous)(_.principal.kind),
            headerToken = ctx.request.header(CsrfHeaderName),
            sessionSecret = session.map(_.csrfSecret.value),
            secFetchSite = ctx.request.header("Sec-Fetch-Site")
          )
          if ctx.request.attribute(RetiredAttribute).contains(true) && needsSession(ctx.request, basePath)
          then
            CsrfInterceptor.respond[F, B](
              responder,
              ctx.request,
              StatusCode.Unauthorized,
              "KUI-UNAUTHENTICATED",
              "this session was replaced; retry with the current session"
            )
          else if session.isEmpty && needsSession(ctx.request, basePath) then
            CsrfInterceptor.respond[F, B](
              responder,
              ctx.request,
              StatusCode.TooManyRequests,
              "KUI-AUTH-RATE-LIMITED",
              "too many new sessions; try again later"
            )
          else if authenticationRequired && needsAuthentication(ctx.request, basePath) &&
            session.forall(_.principal.kind == PrincipalKind.Anonymous)
          then
            CsrfInterceptor.respond[F, B](
              responder,
              ctx.request,
              StatusCode.Unauthorized,
              "KUI-UNAUTHENTICATED",
              "sign in to access this resource"
            )
          else
            verdict match {
              case CsrfCheck.Verdict.Allowed => delegate.onDecodeSuccess(ctx)
              case CsrfCheck.Verdict.Denied(reason) =>
                CsrfInterceptor.forbidden[F, B](responder, logger, ctx.request, session, reason)
            }
        }

        def onSecurityFailure[A](ctx: SecurityFailureContext[F, A])(using
            monad: MonadError[F],
            bodyListener: BodyListener[F, B]
        ): F[sttp.tapir.server.model.ServerResponse[B]] =
          delegate.onSecurityFailure(ctx)

        def onDecodeFailure(ctx: DecodeFailureContext)(using
            monad: MonadError[F],
            bodyListener: BodyListener[F, B]
        ): F[Option[sttp.tapir.server.model.ServerResponse[B]]] =
          delegate.onDecodeFailure(ctx)
      }
  }

  private object CsrfInterceptor {

    private val forbiddenOutput: EndpointOutput[(StatusCode, ErrorEnvelope)] =
      statusCode.and(jsonBody[ErrorEnvelope])

    def forbidden[F[_]: Sync, B](
        responder: Responder[F, B],
        logger: StructuredLogger[F],
        request: ServerRequest,
        session: Option[Session],
        reason: String
    ): F[sttp.tapir.server.model.ServerResponse[B]] =
      for {
        _ <- logger.warn(
          Map(
            "path" -> request.uri.path.mkString("/", "/", ""),
            "secFetchSite" -> request.header("Sec-Fetch-Site").getOrElse("absent"),
            "session.ref" -> session.fold("-")(active => SessionRef.of(active.id).value)
          )
        )(s"CSRF rejected: $reason")
        response <- respond[F, B](responder, request, StatusCode.Forbidden, ErrorCode.Forbidden.wire, reason)
      } yield response

    def respond[F[_]: Sync, B](
        responder: Responder[F, B],
        request: ServerRequest,
        status: StatusCode,
        code: String,
        message: String
    ): F[sttp.tapir.server.model.ServerResponse[B]] =
      for {
        correlationId <- request.header(Correlation.HeaderName).getOrElse("").pure[F]
        at <- Clock[F].realTimeInstant
        body = ErrorEnvelope(code, message, Nil, correlationId, at, retryable = false)
        response <- responder.apply(request, ValuedEndpointOutput(forbiddenOutput, (status, body)))
      } yield response
  }

  // -----------------------------------------------------------------------------------------------
  // 3. Stamp Set-Cookie on whatever response came back, for every endpoint at once.
  // -----------------------------------------------------------------------------------------------

  private def stampCookieInterceptor[F[_]: Sync](
      basePath: String,
      secureCookies: Boolean
  ): RequestInterceptor[F] =
    RequestInterceptor.transformResultEffect[F](
      new RequestInterceptor.RequestResultEffectTransform[F] {
        def apply[B](request: ServerRequest, result: F[RequestResult[B]]): F[RequestResult[B]] =
          result.map { requestResult =>
            request.attribute(Attribute) match {
              case None => requestResult
              case Some(session) if !cookieValue(request).contains(session.id.value) =>
                requestResult match {
                  case RequestResult.Response(response, source) =>
                    RequestResult.Response(stampCookie(response, session, basePath, secureCookies), source)
                  case failure => failure
                }
              case Some(_) => requestResult
            }
          }
      }
    )

  /** Appends the session cookie, unless the route already set one of its own.
    *
    * The exception is the sign-in routes. Signing in *replaces* the session — a new id and a new CSRF secret,
    * which is ADR-019's session-fixation defence — so the cookie that has to reach the browser is not the one
    * this request arrived with, and that is the only one this interceptor knows about. `AuthRoutes` therefore
    * declares the cookie as an output of those endpoints and this step stands aside, because two `Set-Cookie`
    * headers for one name is a browser-dependent coin toss over which session the operator ends up in.
    */
  private def stampCookie[B](
      response: sttp.tapir.server.model.ServerResponse[B],
      session: Session,
      basePath: String,
      secure: Boolean
  ): sttp.tapir.server.model.ServerResponse[B] =
    if alreadyCarriesSessionCookie(response) then response
    else
      response.copy(headers =
        response.headers :+ Header("Set-Cookie", setCookie(session, basePath, secure).toString)
      )

  /** Whether the route already put a session cookie on this response.
    *
    * `private[auth]` rather than `private` so that the rule above it can be asserted directly: this is the
    * whole session-fixation defence on the login path, and the only route that sets its own cookie is one
    * that needs an identity service to reach, so a case going through a server could not exercise it.
    */
  private[auth] def alreadyCarriesSessionCookie[B](
      response: sttp.tapir.server.model.ServerResponse[B]
  ): Boolean =
    response.headers.exists(header =>
      header.name.equalsIgnoreCase("Set-Cookie") && header.value.startsWith(s"$CookieName=")
    )
}
