package kui.config

import cats.effect.kernel.Sync
import cats.syntax.all.*

import kui.config.ConfigReader.{Indices, Lookup, Problems}

/** Reading `kui.auth` — the section that decides whether anybody has to sign in (ADR-015, AU-001).
  *
  * ==Two phases, for the reason every secret-bearing section has two==
  *
  * [[decode]] is pure and produces a draft in which a password hash and a client secret are still
  * [[SecretRef]]s — `env:KUI_ADMIN_HASH`, `file:/run/secrets/oidc`, or a literal. [[resolve]] then follows
  * those references. The split matters because decoding accumulates *every* problem in the file, while
  * resolving can only report the ones it reaches; keeping the parse pure means a typo in the OIDC issuer and
  * an unset environment variable are reported together, in one restart.
  *
  * ==What is deliberately absent==
  *
  * There is no `kui.auth.ldap`. ADR-015 names UnboundID for LDAP and Active Directory, and it is the one
  * authentication mode this milestone does not ship: it needs a new third-party dependency, a directory
  * server to test against, and nested-group resolution that has no honest fake. Saying so here, in the file
  * an operator reads, is better than a half-parsed section that fails at login.
  */
object AuthConfigSection {

  val Prefix: String = "kui.auth"

  private val UsersPrefix: String = s"$Prefix.users"
  private val OidcPrefix: String = s"$Prefix.oidc"

  /** Every key this section understands, as `UnknownKeys` compares them. */
  val keys: List[List[String]] =
    List(
      List("kui", "auth", "type"),
      List("kui", "auth", "trustedProxies"),
      List("kui", "auth", "trustedProxies", "*"),
      List("kui", "auth", "users", "*", "name"),
      List("kui", "auth", "users", "*", "passwordHash"),
      List("kui", "auth", "users", "*", "groups"),
      List("kui", "auth", "users", "*", "groups", "*"),
      List("kui", "auth", "users", "*", "mustChangePassword"),
      List("kui", "auth", "oidc", "issuer"),
      List("kui", "auth", "oidc", "clientId"),
      List("kui", "auth", "oidc", "clientSecret"),
      List("kui", "auth", "oidc", "redirectUri"),
      List("kui", "auth", "oidc", "scopes"),
      List("kui", "auth", "oidc", "scopes", "*"),
      List("kui", "auth", "oidc", "usernameClaim"),
      List("kui", "auth", "oidc", "groupsClaim"),
      List("kui", "auth", "oidc", "label")
    )

  /** `kui.auth` with its secrets still unresolved. */
  final case class Draft(
      authType: AuthType,
      users: List[UserDraft],
      oidc: Option[OidcDraft],
      trustedProxies: Set[String] = Set.empty
  )

  final case class UserDraft(
      name: String,
      passwordHash: SecretRef,
      groups: Set[String],
      mustChangePassword: Boolean
  )

  final case class OidcDraft(
      issuer: String,
      clientId: String,
      clientSecret: SecretRef,
      redirectUri: String,
      scopes: List[String],
      usernameClaim: String,
      groupsClaim: Option[String],
      label: String
  )

  /** Reads the section, reporting every problem in it. */
  def decode(lookup: Lookup, indices: Indices): Problems[Draft] = {
    val userIndices = indices(UsersPrefix)

    val raw: String = ConfigReader.withDefault(lookup, s"$Prefix.type", AuthType.Disabled.wire)

    val authType: Problems[AuthType] =
      AuthType.fromWire(raw) match {
        case Some(value) => value.validNel
        case None if raw.equalsIgnoreCase("ldap") =>
          ConfigReader
            .problem(
              lookup,
              s"$Prefix.type",
              "LDAP and Active Directory are not implemented yet; the supported types are " +
                AuthType.values.map(_.wire).mkString(", ")
            )
            .invalidNel
        case None =>
          ConfigReader
            .problem(
              lookup,
              s"$Prefix.type",
              s"'$raw' is not an authentication type; expected one of " +
                AuthType.values.map(_.wire).mkString(", ")
            )
            .invalidNel
      }

    val users = (
      ConfigReader.denseIndices(UsersPrefix, userIndices),
      ConfigReader.all(userIndices.map(index => user(lookup, index)))
    ).mapN((_, decoded) => decoded)

    val trusted = ConfigReader
      .all(ConfigReader.list(lookup, s"$Prefix.trustedProxies").map { raw =>
        IpLiteral.canonical(raw) match {
          case Some(address) => address.validNel
          case None =>
            ConfigReader
              .problem(
                lookup,
                s"$Prefix.trustedProxies",
                "must contain only literal IPv4 or IPv6 addresses (no hostnames or networks)"
              )
              .invalidNel
        }
      })
      .map(_.toSet)
    (authType, users, oidc(lookup), trusted)
      .mapN(Draft.apply)
      .andThen(draft => checkTypeIsUsable(lookup, draft))
  }

  /** A deployment that asks for a mode it has not configured is told at start-up rather than at the first
    * login attempt.
    *
    * `type: form` with no users is the mistake that matters: KUI starts, the browser shows a login screen,
    * and no password on earth works. There is no bootstrap account invented in its place, because an account
    * invented by a server and printed into a log is an account somebody will still be able to use in six
    * months.
    */
  private def checkTypeIsUsable(lookup: Lookup, draft: Draft): Problems[Draft] =
    draft.authType match {
      case AuthType.Form if draft.users.isEmpty =>
        ConfigReader
          .problem(
            lookup,
            s"$UsersPrefix",
            "is required when kui.auth.type is 'form'; configure at least one account, whose hash comes " +
              "from './mill services.identity.app.runMain kui.identity.app.HashPassword'"
          )
          .invalidNel
      case AuthType.Oidc if draft.oidc.isEmpty =>
        ConfigReader
          .problem(
            lookup,
            s"$OidcPrefix",
            "is required when kui.auth.type is 'oidc'; configure the issuer, clientId, clientSecret and " +
              "redirectUri of the provider"
          )
          .invalidNel
      case _ => draft.validNel
    }

  private def user(lookup: Lookup, index: Int): Problems[UserDraft] = {
    val prefix = s"$UsersPrefix.$index"
    (
      ConfigReader.required(lookup, s"$prefix.name", "a login name"),
      ConfigReader
        .required(
          lookup,
          s"$prefix.passwordHash",
          "an encoded password hash, or env:NAME / file:PATH pointing at one"
        )
        .map(SecretRef.parse),
      ConfigReader.boolean(lookup, s"$prefix.mustChangePassword", fallback = false)
    ).mapN((name, hash, mustChange) =>
      UserDraft(name, hash, ConfigReader.list(lookup, s"$prefix.groups").toSet, mustChange)
    )
  }

  /** The OIDC block, which is absent for most deployments and complete for the rest.
    *
    * "Present" is decided by the issuer, because that is the one key an operator cannot omit and cannot have
    * written by accident. A block with an issuer and no client id is a half-finished edit and is reported as
    * such, rather than being ignored because one field was missing.
    */
  private def oidc(lookup: Lookup): Problems[Option[OidcDraft]] =
    ConfigReader.optional(lookup, s"$OidcPrefix.issuer") match {
      case None => none[OidcDraft].validNel
      case Some(issuer) =>
        (
          ConfigReader.required(
            lookup,
            s"$OidcPrefix.clientId",
            "the client id registered with the provider"
          ),
          ConfigReader
            .required(
              lookup,
              s"$OidcPrefix.clientSecret",
              "the client secret, normally as env:NAME or file:PATH"
            )
            .map(SecretRef.parse),
          ConfigReader.required(
            lookup,
            s"$OidcPrefix.redirectUri",
            "the address this deployment serves the callback on, registered with the provider"
          )
        ).mapN { (clientId, clientSecret, redirectUri) =>
          val configured = ConfigReader.list(lookup, s"$OidcPrefix.scopes") match {
            case Nil => OidcConfig.DefaultScopes
            case listed => listed
          }
          OidcDraft(
            issuer = issuer,
            clientId = clientId,
            clientSecret = clientSecret,
            redirectUri = redirectUri,
            // `openid` is added rather than demanded: a provider asked for a plain OAuth 2 flow returns no
            // ID token, KUI then has no verified identity, and the operator's mistake would surface as a
            // login that succeeds at the provider and fails here.
            scopes =
              if configured.contains(OidcConfig.RequiredScope) then configured
              else OidcConfig.RequiredScope :: configured,
            usernameClaim =
              ConfigReader.withDefault(lookup, s"$OidcPrefix.usernameClaim", OidcConfig.DefaultUsernameClaim),
            groupsClaim = ConfigReader.optional(lookup, s"$OidcPrefix.groupsClaim"),
            label = ConfigReader.withDefault(lookup, s"$OidcPrefix.label", "your identity provider")
          )
        }.map(_.some)
    }

  /** Follows the `env:` and `file:` references the draft carries.
    *
    * A failure names the reference and never its value — `references environment variable X, which is not
    * set` — which is [[SecretRef.resolve]]'s guarantee and the reason a password hash travels as a
    * [[SecretRef]] this far instead of as a string somebody could log on the way.
    */
  def resolve[F[_]: Sync](draft: Draft, env: Map[String, String]): F[Problems[AuthConfig]] =
    for {
      users <- draft.users.zipWithIndex.traverse { (user, index) =>
        SecretRef
          .resolve[F](user.passwordHash, env)
          .map {
            case Right(secret) =>
              FormUserConfig(user.name, secret, user.groups, user.mustChangePassword).validNel
            case Left(reason) =>
              ConfigProblem(
                s"$UsersPrefix.$index.passwordHash",
                reason,
                ConfigSourceName.Default
              ).invalidNel
          }
      }
      oidcConfig <- draft.oidc.traverse { provider =>
        SecretRef
          .resolve[F](provider.clientSecret, env)
          .map {
            case Right(secret) =>
              OidcConfig(
                issuer = provider.issuer,
                clientId = provider.clientId,
                clientSecret = secret,
                redirectUri = provider.redirectUri,
                scopes = provider.scopes,
                usernameClaim = provider.usernameClaim,
                groupsClaim = provider.groupsClaim,
                label = provider.label
              ).validNel
            case Left(reason) =>
              ConfigProblem(s"$OidcPrefix.clientSecret", reason, ConfigSourceName.Default).invalidNel
          }
      }
    } yield (ConfigReader.all(users), oidcConfig.sequence).mapN((accounts, provider) =>
      AuthConfig(draft.authType, accounts, provider, draft.trustedProxies)
    )
}
