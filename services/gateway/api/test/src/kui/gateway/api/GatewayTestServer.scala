package kui.gateway.api

import scala.concurrent.duration.DurationInt

import cats.effect.IO
import cats.effect.kernel.Resource
import sttp.capabilities.fs2.Fs2Streams
import sttp.client4.*
import sttp.client4.httpclient.fs2.HttpClientFs2Backend
import sttp.model.Uri
import sttp.tapir.server.ServerEndpoint

import kui.config.{AuthConfig, GatewayConfig, ServerConfig}
import kui.gateway.api.auth.SessionMiddleware
import kui.gateway.application.client.ServiceClient
import kui.gateway.application.session.{InMemorySessionStore, SessionConfig, SessionStore}
import kui.http.health.ReadinessCheck
import kui.http.{ErrorInterceptor, KuiServer}
import kui.kernel.{Host, Port}
import kui.observability.Telemetry
import kui.security.rbac.RbacPolicy
import kui.testkit.fakes.FakeStructuredLogger

/** A gateway on a real port, for the assertions that only a real server can make.
  *
  * A stub interpreter would be faster and would prove less than nothing here. The whole subject of these
  * suites is the wiring: whether a forged header is really removed before a handler runs, whether the
  * correlation id really appears in a response *header*, whether an unmatched path really reaches the reject
  * handler. A stub would assert the wiring it replaced.
  *
  * It is written here rather than reused from `libs/http` because that module's own `TestServer` belongs to
  * its test module, and a test module is not a published artefact for other modules to build on.
  */
object GatewayTestServer {

  /** One bound gateway, plus everything a test needs to talk to it and to read what it logged. */
  final case class Running(
      binding: KuiServer.ServerBinding,
      // Typed as a *stream* backend so that the SSE suites can read a live response body rather than
      // waiting for one that never ends. Every other suite uses it as a plain backend, which it also is.
      backend: StreamBackend[IO, Fs2Streams[IO]],
      logger: FakeStructuredLogger[IO],
      sessions: SessionStore[IO]
  ) {

    def at(path: String): Uri = Uri.unsafeParse(s"http://localhost:${binding.port}$path")

    /** A `GET`, with any headers a test wants to forge. */
    def get(path: String, headers: Map[String, String] = Map.empty): IO[Response[String]] =
      request(basicRequest.get(at(path)), headers)

    def post(path: String, headers: Map[String, String] = Map.empty): IO[Response[String]] =
      request(basicRequest.post(at(path)), headers)

    /** A `GET` that stops at the redirect instead of following it.
      *
      * [[get]] is built from `basicRequest`, which follows redirects by default — so a case written against
      * [[get]] never sees a `302`, its `Location` or its `Set-Cookie`: it sees whatever is at the other end.
      * The OpenID Connect callback is the one route in this gateway whose entire successful answer *is* a
      * redirect, and a case asserting `302` through [[get]] silently asserts the static SPA fallback instead.
      */
    def getWithoutFollowing(
        path: String,
        headers: Map[String, String] = Map.empty
    ): IO[Response[String]] =
      request(basicRequest.get(at(path)).followRedirects(false), headers)

    /** A `POST` carrying a JSON body, which is what every sign-in route takes.
      *
      * Separate from [[post]] rather than a defaulted parameter on it, so that the bodyless form keeps
      * sending no `Content-Type` at all — three suites assert what the gateway does with a request that
      * declares none, and a default would quietly give them one.
      */
    def postJson(
        path: String,
        body: String,
        headers: Map[String, String] = Map.empty
    ): IO[Response[String]] =
      request(basicRequest.post(at(path)).body(body).contentType("application/json"), headers)

    private def request(
        builder: Request[Either[String, String]],
        headers: Map[String, String]
    ): IO[Response[String]] =
      headers
        .foldLeft(builder)((request, header) => request.header(header._1, header._2))
        .response(asStringAlways)
        .send(backend)
  }

  /** The real gateway: the real routes, the real interceptor chain, on a port the operating system picks.
    *
    * Port `0` means "any free port", so suites never collide with each other or with a gateway a developer
    * happens to be running.
    */
  def resource(
      basePath: String = "/",
      extraRoutes: List[ServerEndpoint[Fs2Streams[IO], IO]] = Nil,
      devInsecureCookies: Boolean = true,
      // The identity service, when a case wants the sign-in routes to do something other than refuse.
      //
      // W10-A1 filed this as the seam it could not open: every suite in this tree built a gateway with
      // `identity = None`, so `/auth/login` answered `KUI-UNSUPPORTED` before `signIn` ran, and the whole
      // sign-in surface — the password-change path's deliberate absence of a new cookie, the OIDC
      // callback's refusal of a `PasswordChangeRequired`, the "this deployment has no identity service"
      // sentence — had no behavioural case at all. It is a parameter rather than a second constructor so
      // that the twenty-nine suites that want no identity service keep getting one word: nothing.
      identity: Option[ServiceClient[IO]] = None,
      // Which kind of sign-in this deployment says it uses, for `/auth/settings` and `/auth/me`.
      auth: AuthConfig = AuthConfig.Default,
      rbac: RbacPolicy = RbacPolicy.Disabled,
      // The session store's tunables, so that a case can build a deployment whose sessions expire inside the
      // lifetime of a test.
      //
      // Filed as V2 by this wave's verification pass over W12-03 and the seam it said it could not open:
      // every suite in this tree built a gateway on `SessionConfig.Default`, whose idle timeout is thirty
      // minutes and whose absolute timeout is twelve hours, so no case anywhere could watch a session
      // expire on the REQUEST path. `store.get(id, now)` in `SessionMiddleware.ensureSession` is the only
      // expiry check a browser request ever meets, and with it neutered — `now <- Clock[F].realTimeInstant`
      // replaced by `Sync[F].pure(Instant.EPOCH)` — `./mill --no-daemon services.gateway.api.test` was
      // 1270/1270 SUCCESS over all 303 cases. A captured cookie would then be valid for the life of the
      // process and ADR-019's session lifetime would be configuration nothing measures.
      sessionConfig: SessionConfig = SessionConfig.Default
  ): Resource[IO, Running] =
    for {
      logger <- Resource.eval(FakeStructuredLogger[IO])
      sessions <- InMemorySessionStore.resource[IO](sessionConfig)
      readiness = List(ReadinessCheck.always[IO]("process"))
      routes =
        GatewayApi.routes[IO](configView(basePath, auth, rbac), readiness, sessions, extraRoutes, identity)
      interceptors = EdgeHeaders.interceptors[IO] ++
        SessionMiddleware.interceptors[IO](
          sessions,
          logger,
          basePath,
          secureCookies = !devInsecureCookies,
          authenticationRequired = auth.authType != kui.config.AuthType.Disabled,
          trustedProxies = auth.trustedProxies
        ) ++
        ErrorInterceptor.interceptors[IO](logger)
      config = ServerConfig(Host.unsafe("localhost"), Port.unsafe(0), basePath)
      binding <- KuiServer.resource[IO](config, routes, interceptors, logger, gracefulShutdown = 10.millis)
      backend <- HttpClientFs2Backend.resource[IO]()
    } yield Running(binding, backend, logger, sessions)

  /** The configuration the routes read: this deployment's server settings, and no upstream services.
    *
    * `private[api]` rather than `private` so that a suite in this package can build the same view with a
    * different `AuthConfig` — the sign-in routes answer `/auth/settings` out of it — without assembling the
    * whole route list itself.
    */
  private[api] def configView(
      basePath: String,
      auth: AuthConfig = AuthConfig.Default,
      rbac: RbacPolicy = RbacPolicy.Disabled
  ): GatewayServiceConfigView =
    GatewayServiceConfigView(
      ServerConfig(Host.unsafe("localhost"), Port.unsafe(0), basePath),
      GatewayConfig.Default,
      auth,
      secureCookies = true,
      rbac
    )

  /** The telemetry a suite uses when it needs one at all: records nothing, costs nothing. */
  val noTelemetry: Telemetry[IO] = Telemetry.noop[IO]
}
