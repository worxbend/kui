package kui.gateway.api

import cats.Parallel
import cats.effect.kernel.Async
import sttp.capabilities.fs2.Fs2Streams
import sttp.tapir.server.ServerEndpoint

import kui.config.{AuthConfig, GatewayConfig, ServerConfig}
import kui.gateway.api.auth.AuthRoutes
import kui.gateway.api.static.{BootstrapConfig, StaticRoutes}
import kui.gateway.application.Gateway
import kui.gateway.application.client.ServiceClient
import kui.gateway.application.session.SessionStore
import kui.gateway.contract.GatewayEndpoints
import kui.http.BasePath
import kui.http.health.{HealthEndpoints, ReadinessCheck}
import kui.security.rbac.RbacPolicy

/** Every route the gateway serves, assembled in one place.
  *
  * The list is short in M0 and it grows by task: build info (GW-010), the session and CSRF endpoints
  * (GW-009), the capability registry (GW-003), the static assets and the SPA fallback (GW-008), the proxied
  * service routes (GW-006) and the OpenAPI document (GW-007). Keeping the assembly in one function rather
  * than spreading it through a composition root means the answer to "what does this process actually serve"
  * is one file, and the order routes are matched in is visible rather than emergent.
  */
object GatewayApi {

  /** The gateway's own service id, and the `service.name` it reports itself under. Both are re-exported from
    * the application layer, which owns them, so that a route can name the gateway without every caller having
    * to know which layer the identity lives in.
    */
  export Gateway.{Id, ServiceName}

  /** The routes, in the order the router tries them.
    *
    * @param readiness
    *   what this process checks before saying it can serve. The gateway has **no mandatory upstream**: it is
    *   Core tier (PLAN §15), so if it will not start, the browser gets the full-screen "cannot reach gateway"
    *   page and nothing else works at all. A gateway that refused to start because the schema service was
    *   down would turn one service's outage into a total one, and would do it at exactly the moment an
    *   operator most needs a working UI to diagnose it.
    * @param config
    *   the sections the routes read: where this deployment is mounted, and which services it names
    * @param extra
    *   the routes the composition root builds because they need components this module must not construct:
    *   the capability endpoints over the registry (GW-005), the contract-derived proxy routes (GW-006) and
    *   the merged documentation (GW-007).
    *
    * The gateway does **not** serve `/api/v1/capabilities` as its own self-report the way every other service
    * does. On the gateway that path means something different and more useful -- what the whole product can
    * do -- and two documents behind one URL is a collision nobody sees in a route list.
    */
  def routes[F[_]: {Async, Parallel}](
      config: GatewayServiceConfigView,
      readiness: List[ReadinessCheck[F]],
      sessions: SessionStore[F],
      extra: List[ServerEndpoint[Fs2Streams[F], F]] = Nil,
      // The identity service, when this deployment has one. It is an `Option` for the same reason every
      // other upstream is: the gateway starts and serves whatever it can reach, and a missing identity
      // service becomes a sentence on the sign-in routes rather than a process that will not boot.
      identity: Option[ServiceClient[F]] = None
  ): List[ServerEndpoint[Fs2Streams[F], F]] =
    // Two kinds of route, prefixed differently, and the difference is worth stating rather than
    // discovering. The gateway's own endpoints are built from `GatewayEndpoints.base`, which already
    // carries `/api/v1` — that is deliberate, because a contract module is what the OpenAPI document and
    // the browser's client are generated from, and both need the real public path. The health endpoints
    // come from `libs/http` and are shared with all eleven services, which serve them at the root of their
    // own `/internal/v1`-shaped world, so the gateway adds the prefix to those.
    //
    // `libs/http` types them as `ServerEndpoint[Any, F]` — "needs no capability from the server" — while
    // the gateway's list is typed on `Fs2Streams`, because some of its routes do need streaming: the
    // capability stream (GW-005) and the re-streamed message browser. `ServerEndpoint` is contravariant in
    // that parameter, so an endpoint requiring nothing fits wherever one that may require streaming does.
    BasePath.prefixAll(GatewayEndpoints.ApiPrefix, HealthEndpoints.probes[F](readiness)) ++
      InfoRoutes[F](config.server, config.gateway, config.auth.authType) ++
      AuthRoutes[F](
        sessions,
        config.rbac,
        config.auth,
        identity,
        BasePath.normalize(config.server.basePath),
        config.secureCookies
      ) ++
      // The capability, proxy and documentation routes the composition root builds, which need
      // components (the registry, the service clients) that this module must not construct itself.
      extra ++
      // Matched last: the static routes' `/ui/**` fallback answers `index.html` for any path that reaches
      // it, so any API route that had to come after it would never be seen at all.
      StaticRoutes[F](BasePath.normalize(config.server.basePath), bootstrapOf(config))
}

/** The two configuration sections the route list needs, and no more.
  *
  * The composition root's `GatewayServiceConfig` lives in the `app` module, which depends on this one, so the
  * dependency cannot point the other way. Naming exactly the two sections the routes read is better than
  * inventing a way around that anyway: a reader can see from the signature that assembling the routes
  * involves the server settings and the upstream list and nothing else — not the telemetry configuration, not
  * the signing keys.
  */
final case class GatewayServiceConfigView(
    server: ServerConfig,
    gateway: GatewayConfig,
    // Which kind of sign-in this deployment uses, which `GET /auth/settings` answers and `GET /auth/me`
    // reports. The gateway answers both from its own configuration rather than by asking the identity
    // service: a login screen that cannot render while the service behind the login is down is a product
    // that disappears exactly when an operator needs to see why.
    auth: AuthConfig,
    // Whether the session cookie is marked `Secure`. It follows `kui.server.devInsecureCookies`, and the
    // sign-in routes need it because they issue the cookie themselves rather than letting the session
    // middleware stamp it.
    secureCookies: Boolean,
    // What `GET /auth/me` answers with, so that the browser hides exactly the controls the edge would
    // refuse. It is the same `RbacPolicy` value the edge's own check reads, from the same configuration:
    // a second policy here would be a second answer to "may I", and the interface would be wrong in
    // whichever direction the two had drifted.
    rbac: RbacPolicy
)

/** The bootstrap block `StaticRoutes` embeds in `index.html`, derived from the same configuration and build
  * the rest of the process reports (GW-010) — one source of truth for "which build, which base path, which
  * API prefix" rather than three places that could drift apart.
  */
private def bootstrapOf(config: GatewayServiceConfigView): BootstrapConfig =
  BootstrapConfig(
    basePath = BasePath.normalize(config.server.basePath),
    apiBase = s"${BasePath.normalize(config.server.basePath)}${GatewayEndpoints.ApiPrefix}",
    buildVersion = InfoRoutes.buildInfo.version
  )
