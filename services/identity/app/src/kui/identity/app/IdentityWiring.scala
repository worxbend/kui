package kui.identity.app

import cats.Parallel
import cats.effect.kernel.{Async, Resource}
import cats.syntax.all.*
import fs2.io.file.Files
import org.typelevel.log4cats.{LoggerFactory, StructuredLogger}
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.interceptor.Interceptor

import kui.config.store.{ConfigStore, ConfigStoreResource}
import kui.config.{AuthConfig, AuthType, HttpTlsConfig, StoreConfig, UrlPolicy}
import kui.contracts.capability.ServiceCapabilities
import kui.http.ProcessLoggerFactory
import kui.http.health.ReadinessCheck
import kui.http.principal.PrincipalVerification
import kui.http.upstream.HttpTls
import kui.identity.api.IdentityApi
import kui.identity.application.*
import kui.identity.domain.{AuthMode, UserDirectory}
import kui.identity.infrastructure.{
  ConfiguredUserDirectory,
  OidcRelyingParty,
  Pbkdf2PasswordHasher,
  StoredUserDirectory,
  UnconfiguredOidcProvider
}
import kui.kernel.UserName
import kui.observability.Telemetry
import kui.observability.audit.LoggingAuthAuditSink
import kui.security.PrincipalCodec
import kui.security.rbac.RbacPolicy

/** Everything the identity service needs in order to be served, with no listener started.
  *
  * The same shape as every other service's (ADR-010): stopping one step short of a running server is what
  * lets the all-in-one deployment take these routes, add every other service's, and start one listener over
  * the lot.
  */
final case class IdentityServer[F[_]](
    routes: List[ServerEndpoint[Any, F]],
    interceptors: List[Interceptor[F]],
    readiness: List[ReadinessCheck[F]],
    capabilities: F[ServiceCapabilities]
)

/** The identity service's composition root.
  *
  * ==What it contacts, and when==
  *
  * Nothing, unless a deployment configured something. With `kui.auth.type: disabled` — the default, and the
  * demonstration environment — it holds no accounts, opens no HTTP client, contacts no provider, and answers
  * `settings` with `disabled`. A KUI that has not been asked to authenticate anybody must start exactly as
  * fast, and fail exactly as rarely, as it did before this service existed.
  *
  * ==Where each mode's pieces come from==
  *
  *   - **form**: the accounts are `kui.auth.users[]`, laid under whatever the metadata store holds for
  *     accounts whose password has since been changed. Passwords are PBKDF2 (ADR-015 Amendment 1);
  *   - **oidc**: an HTTP relying party over the configured provider, or — when the mode is on and the
  *     provider block is absent, which the configuration loader already refuses — a port that says so;
  *   - **disabled**: use cases still refuse modes they are not in, but no password store is acquired. Only
  *     form authentication owns password overrides; OIDC also has no dependency on their store.
  */
object IdentityWiring {

  /** The instrumentation scope this service's tracer and meter are named after. */
  val Instrumentation: String = "kui.identity"

  def make[F[_]: {Async, Parallel}](
      auth: AuthConfig,
      rbac: RbacPolicy,
      store: StoreConfig,
      telemetry: Telemetry[F],
      principals: PrincipalCodec[F],
      logger: StructuredLogger[F]
  ): Resource[F, IdentityServer[F]] = {
    // The metadata store's own components ask for a logger through log4cats' factory rather than taking
    // one as a parameter, so the process's logger is adapted into one here — the same three lines, and the
    // same reason, as `ClusterWiring`.
    given LoggerFactory[F] = ProcessLoggerFactory.of(logger)

    for {
      rejections <- Resource.eval(
        telemetry.meter(Instrumentation).flatMap(PrincipalVerification.rejectionCounter[F])
      )
      config = configOf(auth, rbac)
      _ <- Resource.eval(announce[F](config, logger))
      audit = LoggingAuthAuditSink.make[F](logger)
      hasher <- Resource.eval(Pbkdf2PasswordHasher.make[F])
      configured <- Resource.eval(ConfiguredUserDirectory.make[F](auth.users, logger))
      users <- auth.authType match {
        case AuthType.Form =>
          configStoreOf[F](store, logger).map(metadata =>
            StoredUserDirectory.make[F](configured, metadata, logger)
          )
        case AuthType.Disabled | AuthType.Oidc => Resource.pure[F, UserDirectory[F]](configured)
      }
      challenges <- Resource.eval(SingleUseTokens.make[F, UserName]())
      pending <- Resource.eval(SingleUseTokens.make[F, PendingLogin]())
      oidc <- oidcOf[F](auth, logger)
      interceptors <- Resource.eval(IdentityApi.interceptors[F](telemetry, rejections, logger))
    } yield IdentityServer(
      routes = IdentityApi.routes[F](
        settings = SettingsUseCase[F](config),
        login = new LoginUseCase[F](config, users, hasher, challenges, audit, logger),
        changePassword = new ChangePasswordUseCase[F](config, users, hasher, challenges, audit),
        permissions = PermissionsUseCase[F](config),
        oidc = new OidcLoginUseCase[F](config, oidc, pending, audit, logger),
        readiness = readinessChecks[F],
        principals = principals,
        rejections = rejections,
        logger = logger
      ),
      interceptors = interceptors,
      readiness = readinessChecks[F],
      capabilities = IdentityApi.capabilityDocument[F]
    )
  }

  /** The configuration's vocabulary, as the application layer's.
    *
    * Three lines, and they are what keeps `kui.identity.domain` free of the configuration loader (ADR-041
    * rule A1) — the loader drags a YAML parser, ciris and a Kafka client behind it, none of which a rule
    * about who may sign in should be able to see.
    */
  def configOf(auth: AuthConfig, rbac: RbacPolicy): IdentityConfig =
    IdentityConfig(
      mode = auth.authType match {
        case AuthType.Disabled => AuthMode.Disabled
        case AuthType.Form => AuthMode.Form
        case AuthType.Oidc => AuthMode.Oidc
      },
      provider = auth.oidc.map(provider => ProviderSummary(provider.label)),
      policy = rbac
    )

  /** One line at start-up saying what this deployment will do, because "why does KUI not ask me to log in" is
    * otherwise answered by reading a YAML file on a machine somebody else deployed.
    *
    * `private[app]` for the reason `SchemaWiring.upstreamConfig` is: the alternative is a suite that builds
    * the whole composition root, which needs a metadata store and a provider, and this module had no suite at
    * all until then — so the sentence an operator relies on was carried by nothing.
    */
  private[app] def announce[F[_]](config: IdentityConfig, logger: StructuredLogger[F]): F[Unit] =
    logger.info(
      Map(
        "auth.type" -> config.mode.wire,
        "rbac.enabled" -> config.policy.enabled.toString,
        "rbac.roles" -> config.policy.roles.size.toString
      )
    )(
      config.mode match {
        case AuthMode.Disabled =>
          "identity: authentication is disabled; every request is anonymous"
        case AuthMode.Form => "identity: sign-in is by username and password"
        case AuthMode.Oidc => "identity: sign-in is through the configured OpenID Connect provider"
      }
    )

  /** Password overrides use the same bootstrapped, encrypted store lifecycle as other metadata owners. */
  private[app] def configStoreOf[F[_]: {Async, Parallel, LoggerFactory}](
      store: StoreConfig,
      logger: StructuredLogger[F]
  ): Resource[F, ConfigStore[F]] = {
    given Files[F] = Files.forAsync[F]
    ConfigStoreResource.resource[F](store, "kui-identity", logger)
  }

  private def oidcOf[F[_]: Async](
      auth: AuthConfig,
      logger: StructuredLogger[F]
  ): Resource[F, OidcProviderPort[F]] =
    auth.oidc match {
      case Some(provider) =>
        // The HTTP client exists only in a deployment that configured a provider. A KUI with
        // authentication disabled — the default — opens no connection pool it will never use.
        Resource
          .eval(Async[F].delay(UrlPolicy.fromEnv(sys.env)))
          .flatMap(policy =>
            HttpTls
              .resource[F](HttpTlsConfig.Default, policy)
          )
          .flatMap(backend => OidcRelyingParty.resource[F](provider, backend, logger))
      case None => Resource.pure[F, OidcProviderPort[F]](UnconfiguredOidcProvider[F])
    }

  /** The identity service is ready as soon as it is built.
    *
    * It has no upstream to wait for: the accounts were read at start-up and a provider is contacted only when
    * somebody signs in. A readiness check that pinged the provider would make an outage there into a failing
    * pod, which is precisely the wrong trade — a KUI whose OIDC provider is down should still serve
    * everything an already-signed-in operator is doing.
    */
  private def readinessChecks[F[_]: Async]: List[ReadinessCheck[F]] =
    List(ReadinessCheck.always[F]("identity"))
}
