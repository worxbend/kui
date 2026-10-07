package kui.gateway.api.auth

import scala.concurrent.duration.{DurationInt, FiniteDuration}

import cats.effect.kernel.{Clock, Ref, Sync}
import cats.syntax.all.*
import sttp.model.StatusCode
import sttp.model.headers.CookieValueWithMeta
import sttp.tapir.model.ServerRequest
import sttp.tapir.server.ServerEndpoint

import kui.config.AuthConfig
import kui.contracts.ErrorEnvelope
import kui.gateway.application.client.{CallContext, ServiceClient}
import kui.gateway.application.session.{Session, SessionStore}
import kui.gateway.contract.AuthEndpoints
import kui.gateway.contract.dto.{AuthMeResponse, PermissionDto, PrincipalDto}
import kui.http.ErrorInterceptor
import kui.identity.contract.IdentityEndpoints
import kui.identity.contract.dto.*
import kui.kernel.error.{ApplicationError, KuiError}
import kui.kernel.{CorrelationId, RoleName, UserName}
import kui.observability.Correlation
import kui.security.rbac.{ClusterPermission, ClusterScope, Rbac, RbacPolicy}
import kui.security.{Principal, PrincipalKind}

/** The gateway's whole authentication surface: `me`, `settings`, `login`, `password`, the OpenID Connect pair
  * and `logout`.
  *
  * ==Where the session is, and where the decision is==
  *
  * The split is the point of this file. The *decision* — is this the right password, what does this ID token
  * say, which roles does that give — belongs to the identity service and is reached through its published
  * contract. The *session* — a cookie, a CSRF secret, an id that is replaced when privileges change — belongs
  * here, because there is one session mechanism in the product and it has been here since Milestone 0.
  *
  * ==Signing in replaces the session, rather than editing it==
  *
  * [[signIn]] retires the session the request arrived on and creates a new one for the authenticated
  * principal. That is the session-fixation defence ADR-019 requires: an attacker who can get a victim to use
  * a session id of their choosing — through a link, a subdomain, an XSS — holds a useless value the moment
  * the victim signs in, because the id changed and the CSRF secret changed with it.
  *
  * The new cookie is an *output of the login endpoint* rather than something the session middleware stamps,
  * because the middleware stamps the session the request came in with, which by then is the one that was just
  * thrown away. `SessionMiddleware`'s stamp step stands aside when a response already carries a session
  * cookie, and says so.
  *
  * ==Reading `me` and `settings` never calls anything==
  *
  * Both are answered from the gateway's own state and configuration. An operator whose identity service is
  * down must still be able to load the interface and see why: a login screen that cannot render during an
  * authentication outage is a product that disappears exactly when it is needed.
  */
object AuthRoutes {

  private val request: sttp.tapir.EndpointInput[ServerRequest] =
    sttp.tapir.extractFromRequest(identity)

  /** @param policy
    *   the deployment's roles. A disabled policy is not an empty answer: it grants everything, over every
    *   cluster, because a deployment that has configured no roles has not asked for authorization.
    * @param auth
    *   what kind of sign-in this deployment uses, which is what `settings` answers and what `me` reports.
    * @param identity
    *   the identity service, when this deployment has one. Without it the sign-in routes answer "this
    *   deployment has no identity service" rather than disappearing: a `404` on `/auth/login` in a deployment
    *   configured for `form` would send an operator hunting through the browser's network tab, while a
    *   sentence names the missing upstream.
    */
  def apply[F[_]: Sync](
      store: SessionStore[F],
      policy: RbacPolicy = RbacPolicy.Disabled,
      auth: AuthConfig = AuthConfig.Default,
      identity: Option[ServiceClient[F]] = None,
      basePath: String = "",
      secureCookies: Boolean = true
  ): List[ServerEndpoint[Any, F]] = {
    // One limiter for the whole gateway instance, shared by every request `apply` ever answers, because
    // the point of throttling a login is to remember attempts *across* requests. See `RateLimiter` below:
    // the identity service's own contract leans on the edge to do this, and until now nothing here did.
    val loginLimiter: RateLimiter[F] = RateLimiter[F]
    val oidcStates = new BrowserOidcState[F]

    List(
      me[F](policy, auth),
      settings[F](auth, policy),
      login[F](store, identity, basePath, secureCookies, loginLimiter, auth.trustedProxies),
      changePassword[F](identity, loginLimiter, auth.trustedProxies),
      oidcStart[F](identity, oidcStates),
      oidcCallback[F](store, identity, basePath, secureCookies, oidcStates),
      logout[F](store)
    )
  }

  // -----------------------------------------------------------------------------------------------
  // Reads, answered from this process alone
  // -----------------------------------------------------------------------------------------------

  private def me[F[_]: Sync](policy: RbacPolicy, auth: AuthConfig): ServerEndpoint[Any, F] =
    AuthEndpoints.me
      .in(request)
      .serverLogicSuccess[F](req => sessionOf[F](req).map(toResponse(policy, auth, _)))

  private def settings[F[_]: Sync](auth: AuthConfig, policy: RbacPolicy): ServerEndpoint[Any, F] =
    AuthEndpoints.settings.serverLogicSuccess[F](_ => Sync[F].pure(settingsOf(auth, policy)))

  /** What the login screen is told.
    *
    * Three fields, and the reason there are only three is in `research/scala/security-research.md`: one
    * reference product serves its own configuration — Kafka credentials included — from an endpoint like this
    * one. The defence that works is a type that cannot hold a credential, so the provider is reduced to the
    * label on its button before it ever gets near a response.
    */
  private[auth] def settingsOf(auth: AuthConfig, policy: RbacPolicy): AuthSettingsDto =
    AuthSettingsDto(
      authType = auth.authType.wire,
      providerLabel = auth.oidc.map(_.label),
      // Whether this deployment has configured any roles at all — not what the caller may do, which is
      // `me`'s answer and needs a principal. The browser uses it to tell "you may do everything because
      // nobody asked for authorization" apart from "you may do everything because your role says so".
      rbacEnabled = policy.enabled
    )

  // -----------------------------------------------------------------------------------------------
  // Signing in
  // -----------------------------------------------------------------------------------------------

  private def login[F[_]: Sync](
      store: SessionStore[F],
      identity: Option[ServiceClient[F]],
      basePath: String,
      secureCookies: Boolean,
      limiter: RateLimiter[F],
      trustedProxies: Set[String]
  ): ServerEndpoint[Any, F] =
    AuthEndpoints.loginWithSession
      .in(request)
      .serverLogic[F] { (credentials, req) =>
        withinRateLimit[F, (LoginResponse, Option[CookieValueWithMeta])](
          limiter,
          req,
          List(
            s"login:ip:${ClientAddress.of(req, trustedProxies)}" -> MaxLoginAttemptsPerIp,
            s"login:user:${kui.security.AccountName.canonical(credentials.username)}" -> MaxLoginAttemptsPerUser
          )
        ) {
          answering[F, (LoginResponse, Option[CookieValueWithMeta])](req) {
            withIdentity(identity) { client =>
              call(client, req)(IdentityEndpoints.login, credentials).flatMap {
                case Left(error) => error.asLeft[(LoginResponse, Option[CookieValueWithMeta])].pure[F]

                case Right(LoginResponse.SignedIn(who)) =>
                  signIn[F](store, req, principalOf(who), basePath, secureCookies)
                    .map(cookie => (LoginResponse.SignedIn(who), Some(cookie)).asRight[KuiError])

                // A required password change grants no session at all, so the session the request arrived
                // on is left exactly as it was — anonymous. Handing out a cookie here would be handing out
                // a session to somebody the server has just decided may not have one.
                case Right(change @ LoginResponse.PasswordChangeRequired(_)) =>
                  (change, none[CookieValueWithMeta]).asRight[KuiError].pure[F]
              }
            }
          }
        }
      }

  private def changePassword[F[_]: Sync](
      identity: Option[ServiceClient[F]],
      limiter: RateLimiter[F],
      trustedProxies: Set[String]
  ): ServerEndpoint[Any, F] =
    AuthEndpoints.changePassword
      .in(request)
      .serverLogic[F] { (change, req) =>
        withinRateLimit[F, Unit](
          limiter,
          req,
          List(s"password:ip:${ClientAddress.of(req, trustedProxies)}" -> MaxPasswordChangeAttemptsPerIp)
        ) {
          answering[F, Unit](req) {
            withIdentity(identity)(client => call(client, req)(IdentityEndpoints.changePassword, change))
          }
        }
      }

  private def oidcStart[F[_]: Sync](
      identity: Option[ServiceClient[F]],
      states: BrowserOidcState[F]
  ): ServerEndpoint[Any, F] =
    AuthEndpoints.oidcStart
      .in(request)
      .serverLogic[F] { req =>
        answering[F, OidcStartResponse](req) {
          withIdentity(identity) { client =>
            call(client, req)(IdentityEndpoints.oidcStart, ()).flatMap {
              case Left(error) => error.asLeft[OidcStartResponse].pure[F]
              case Right(started) =>
                sessionOf[F](req).flatMap(session => states.bind(session.id, started.state)).map {
                  case true => Right(started)
                  case false =>
                    Left(ApplicationError.Unauthenticated("too many pending sign-ins; try again later"))
                }
            }
          }
        }
      }

  /** The provider sent the browser back. Exchange the code, and — only if that worked — issue a session. */
  private def oidcCallback[F[_]: Sync](
      store: SessionStore[F],
      identity: Option[ServiceClient[F]],
      basePath: String,
      secureCookies: Boolean,
      states: BrowserOidcState[F]
  ): ServerEndpoint[Any, F] =
    AuthEndpoints.oidcCallback
      .in(request)
      .serverLogic[F] { (code, state, req) =>
        answering[F, (StatusCode, String, CookieValueWithMeta)](req) {
          withIdentity(identity) { client =>
            sessionOf[F](req).flatMap(session => states.consume(session.id, state)).flatMap {
              case false =>
                ApplicationError
                  .Unauthenticated("invalid or expired browser sign-in state")
                  .asLeft[(StatusCode, String, CookieValueWithMeta)]
                  .pure[F]
              case true =>
                call(client, req)(IdentityEndpoints.oidcCallback, OidcCallbackRequest(code, state)).flatMap {
                  case Left(error) => error.asLeft[(StatusCode, String, CookieValueWithMeta)].pure[F]
                  case Right(LoginResponse.SignedIn(who)) =>
                    signIn[F](store, req, principalOf(who), basePath, secureCookies)
                      .map(cookie => (StatusCode.Found, landingPage(basePath), cookie).asRight[KuiError])
                  case Right(LoginResponse.PasswordChangeRequired(_)) =>
                    // A provider sign-in cannot produce this: there is no KUI password behind it to change.
                    // Answering with the same refusal as an unusable callback keeps the failure honest rather
                    // than redirecting a browser into a flow that has no next step.
                    NoPasswordChangeForProvider
                      .asLeft[(StatusCode, String, CookieValueWithMeta)]
                      .pure[F]
                }
            }
          }
        }
      }

  private def logout[F[_]: Sync](store: SessionStore[F]): ServerEndpoint[Any, F] =
    AuthEndpoints.logout.in(request).serverLogicSuccess[F] { req =>
      sessionOf[F](req).flatMap(session => store.delete(session.id))
    }

  // -----------------------------------------------------------------------------------------------

  /** Turns the `KuiError` a use case returned into the envelope and the status the contract declares.
    *
    * `ErrorInterceptor.render` is the single code-to-status table (ADR-034), so a refused password is a `401`
    * and a change with nowhere to be saved is a `501` without this file knowing either number.
    */
  private def answering[F[_]: Sync, A](req: ServerRequest)(
      result: F[Either[KuiError, A]]
  ): F[Either[(ErrorEnvelope, StatusCode), A]] =
    result.flatMap {
      case Right(value) => value.asRight[(ErrorEnvelope, StatusCode)].pure[F]
      case Left(error) =>
        for {
          correlationId <- correlationOf[F](req)
          now <- Clock[F].realTimeInstant
          (status, envelope) = ErrorInterceptor.render(error, correlationId, now)
        } yield (envelope, StatusCode(status)).asLeft[A]
    }

  /** How many attempts a single window tolerates, and how long a window lasts, for each throttled key.
    *
    * Per-IP is the wider bucket, catching one caller trying many usernames; per-username is the narrower one,
    * catching many callers guessing one account's password. Both cover PBKDF2's own cost: 210k iterations is
    * expensive enough on its own, and unlimited attempts multiplies that cost by whatever an attacker is
    * willing to spend.
    */
  private val RateLimitWindow: FiniteDuration = 1.minute
  private val MaxLoginAttemptsPerIp: Int = 20
  private val MaxLoginAttemptsPerUser: Int = 5
  private val MaxPasswordChangeAttemptsPerIp: Int = 10

  /** Runs `action` only while every key in `limits` is still under its own budget for this window, and
    * answers `429 KUI-AUTH-RATE-LIMITED` the moment one of them is not. Each key is charged in order and the
    * chain stops at the first refusal, so a caller who is already over one budget does not also spend a token
    * out of the other.
    */
  private def withinRateLimit[F[_]: Sync, A](
      limiter: RateLimiter[F],
      req: ServerRequest,
      limits: List[(String, Int)]
  )(action: => F[Either[(ErrorEnvelope, StatusCode), A]]): F[Either[(ErrorEnvelope, StatusCode), A]] =
    limits
      .foldLeft(true.pure[F]) { (acc, limit) =>
        acc.flatMap {
          case false => false.pure[F]
          case true => limiter.tryAcquire(limit._1, limit._2, RateLimitWindow)
        }
      }
      .flatMap {
        case true => action
        case false => tooManyRequests[F, A](req)
      }

  private def tooManyRequests[F[_]: Sync, A](req: ServerRequest): F[Either[(ErrorEnvelope, StatusCode), A]] =
    for {
      correlationId <- correlationOf[F](req)
      now <- Clock[F].realTimeInstant
    } yield (
      ErrorEnvelope(
        code = "KUI-AUTH-RATE-LIMITED",
        message = "too many attempts; wait before trying again",
        details = Nil,
        correlationId = correlationId.value,
        timestamp = now,
        retryable = true
      ),
      StatusCode.TooManyRequests
    ).asLeft[A]

  /** An in-memory fixed-window counter, one per gateway process. It is not a `KuiError` case because it never
    * reaches business logic — a refused request stops here, at the edge, exactly where the identity service's
    * own contract says the throttle has to live.
    *
    * Fixed-window rather than a sliding one: a login screen does not need the precision, and a window that
    * resets on a clean boundary is a counter and an instant, not a log of every attempt's timestamp. Expired
    * windows are dropped on every call so that a process fielding attempts from many distinct addresses does
    * not grow this map without bound.
    */
  final private[auth] class RateLimiter[F[_]: Sync] private (state: Ref[F, Map[String, RateLimiter.Window]]) {

    /** `true` when `key` still had budget left in its current window, which this call then spends one unit
      * of; `false` when it did not, in which case nothing is spent.
      */
    def tryAcquire(key: String, maxAttempts: Int, window: FiniteDuration): F[Boolean] =
      Clock[F].realTimeInstant.flatMap { now =>
        state.modify { attempts =>
          val live = attempts.filterNot { case (_, w) => RateLimiter.expired(w, now, window) }
          live.get(key) match {
            case Some(RateLimiter.Window(count, _)) if count >= maxAttempts => (live, false)
            case Some(w @ RateLimiter.Window(count, _)) =>
              (live.updated(key, w.copy(count = count + 1)), true)
            case None => (live.updated(key, RateLimiter.Window(1, now)), true)
          }
        }
      }
  }

  private[auth] object RateLimiter {
    final case class Window(count: Int, start: java.time.Instant)

    private def expired(window: Window, now: java.time.Instant, ttl: FiniteDuration): Boolean =
      java.time.Duration.between(window.start, now).toMillis >= ttl.toMillis

    def apply[F[_]: Sync]: RateLimiter[F] = new RateLimiter[F](Ref.unsafe(Map.empty))
  }

  /** Replaces the session with a new one for `principal`, and answers with the cookie for it.
    *
    * Retire then create, rather than editing the session in place: the id and the CSRF secret both have to
    * change, because both are values an attacker may already hold. Retirement prevents late requests from
    * minting an anonymous cookie over this new one.
    */
  private def signIn[F[_]: Sync](
      store: SessionStore[F],
      req: ServerRequest,
      principal: Principal,
      basePath: String,
      secureCookies: Boolean
  ): F[CookieValueWithMeta] =
    sessionOf[F](req)
      .flatMap(previous => replaceSession[F](store, previous, principal))
      .map(cookieOf(_, basePath, secureCookies))

  /** Retire, then create. The id and the CSRF secret both have to change, because both are values an attacker
    * may already hold — which is what makes a session id they planted on the victim worthless the moment that
    * victim signs in.
    *
    * `private[auth]` rather than `private`, and separated from [[signIn]], so that the rule can be asserted
    * against a real session store without a `ServerRequest` and without an identity service to sign in to:
    * the whole sign-in path is unreachable in a deployment that has configured no identity service, which is
    * every deployment this project's suites build.
    */
  private[auth] def replaceSession[F[_]: Sync](
      store: SessionStore[F],
      previous: Session,
      principal: Principal
  ): F[Session] =
    for {
      now <- Clock[F].realTimeInstant
      _ <- store.retire(previous.id, now)
      session <- store.create(principal, now)
    } yield session

  private def cookieOf(session: Session, basePath: String, secure: Boolean): CookieValueWithMeta =
    SessionMiddleware.setCookie(session, basePath, secure).valueWithMeta

  /** Where a completed provider sign-in sends the browser. The interface's own root, under whatever path this
    * deployment is mounted at.
    */
  private[auth] def landingPage(basePath: String): String =
    if basePath.isEmpty then "/ui/" else s"$basePath/ui/"

  /** One call to the identity service, carrying this request's correlation id and the session's principal.
    *
    * The principal is the *current* one, which during a login is anonymous. That is correct and is what the
    * identity service expects: the header proves the call came from the gateway and is bound to these exact
    * bytes (ADR-020), and it is not a claim that anybody has signed in.
    */
  private def call[F[_]: Sync, I, O](client: ServiceClient[F], req: ServerRequest)(
      endpoint: sttp.tapir.Endpoint[kui.security.SignedPrincipal, I, kui.contracts.ErrorEnvelope, O, Any],
      input: I
  ): F[Either[KuiError, O]] =
    for {
      session <- sessionOf[F](req)
      correlationId <- correlationOf[F](req)
      result <- client.call(endpoint, input)(CallContext(session.principal, correlationId, None))
    } yield result

  private def correlationOf[F[_]: Sync](req: ServerRequest): F[CorrelationId] =
    req.header(Correlation.HeaderName).flatMap(Correlation.accept) match {
      case Some(id) => id.pure[F]
      case None => Correlation.newRandom[F]
    }

  /** What every sign-in route answers when this deployment has no identity service configured.
    *
    * A sentence rather than a missing route. A `404` on `/auth/login` in a deployment whose configuration
    * says `type: form` sends an operator into the browser's network tab; this sends them to
    * `kui.gateway.services.identity`.
    */
  private def withIdentity[F[_]: Sync, A](
      identity: Option[ServiceClient[F]]
  )(use: ServiceClient[F] => F[Either[KuiError, A]]): F[Either[KuiError, A]] =
    identity match {
      case Some(client) => use(client)
      case None => Sync[F].pure(NotConfigured.asLeft[A])
    }

  val NotConfigured: KuiError =
    ApplicationError.Unsupported(
      "signing in, because this deployment has no identity service (set kui.gateway.services.identity)"
    )

  val NoPasswordChangeForProvider: KuiError =
    ApplicationError.InvalidState("a provider sign-in cannot require a KUI password change")

  /** Reads the session `SessionMiddleware` attached. In practice it is always present — the middleware runs
    * on every request before any endpoint's own logic does — but a route is written defensively rather than
    * with `.get`: a future interceptor-ordering mistake should surface as an internal error with a clear
    * cause, not a `NoSuchElementException` three stack frames from anything that explains it.
    */
  private def sessionOf[F[_]: Sync](req: ServerRequest): F[Session] =
    req.attribute(SessionMiddleware.Attribute) match {
      case Some(session) => session.pure[F]
      case None =>
        Sync[F].raiseError(
          new IllegalStateException(
            "no session attached to the request; SessionMiddleware must run before any route's own logic"
          )
        )
    }

  /** The principal the identity service decided on, as this process's own type.
    *
    * The roles come across the wire and are trusted *because of where they came from*: an internal call over
    * a signed contract, from the one service that resolves them. `PrincipalKind.Session` is set here rather
    * than read from the wire for the same reason the gateway never accepts `X-Kui-Principal` from a browser —
    * how KUI came to believe an identity is this process's conclusion, not a caller's assertion.
    */
  private[auth] def principalOf(who: IdentityPrincipalDto): Principal =
    Principal(
      UserName.unsafe(who.name),
      who.roles.map(RoleName.unsafe).toSet,
      PrincipalKind.Session
    )

  private def toResponse(policy: RbacPolicy, auth: AuthConfig, session: Session): AuthMeResponse =
    AuthMeResponse(
      principal = toDto(session.principal),
      csrfToken = session.csrfSecret.value,
      authType = auth.authType.wire,
      // Computed here rather than proxied from a service, because this is the one answer that has to be
      // the same for every screen in the product: four microfrontends gating the same write control
      // against four different sources is four ways for them to disagree.
      permissions = Rbac.grants(policy, session.principal).map(toDto)
    )

  private def toDto(principal: Principal): PrincipalDto =
    PrincipalDto(principal.name.value, principal.roles.map(_.value).toList.sorted, principal.kind.wire)

  /** One grant, on the wire.
    *
    * Sorted, both lists, so that two responses describing the same permissions are byte-identical. An
    * unsorted set here would make the response change from request to request for no reason, which defeats
    * every cache and makes a golden-file test impossible to write.
    */
  private def toDto(granted: ClusterPermission): PermissionDto =
    PermissionDto(
      clusters = granted.clusters match {
        case ClusterScope.Every => List(ClusterScope.EveryWire)
        case ClusterScope.Named(clusters) => clusters.map(_.value).toList.sorted
      },
      resource = granted.permission.resource.wire,
      value = granted.permission.value.map(_.raw),
      actions = granted.permission.actions.map(_.wire).toList.sorted,
      defaultRole = granted.defaultRole
    )
}
