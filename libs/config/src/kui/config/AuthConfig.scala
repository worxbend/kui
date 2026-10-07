package kui.config

import java.util.Locale

import kui.kernel.Secret

/** How a person proves who they are to KUI itself (ADR-015).
  *
  * This is KUI's *own* login, and it has nothing to do with how KUI authenticates to a Kafka cluster — that
  * is `kui.clusters.<n>.security`, a completely separate concern with a completely separate configuration
  * (ADR-022). Conflating the two is the mistake that makes a product hand a browser its broker password, and
  * keeping them in different sections of the file is the cheapest defence against it.
  *
  * `wire` is the spelling in `kui.auth.type`, written out per case so that renaming a case can never change
  * what a deployment's configuration file means.
  */
enum AuthType(val wire: String) {

  /** Nobody signs in; every request is [[kui.security.Principal.Anonymous]].
    *
    * **The default, and it must stay the default.** The demonstration environment depends on it, and a
    * newcomer who meets a login screen before they have seen a topic list has not seen the product. It is
    * also the only mode in which KUI has run at all until now, so keeping it working is a compatibility
    * requirement rather than a convenience.
    */
  case Disabled extends AuthType("disabled")

  /** A username and password checked against `kui.auth.users[]`. */
  case Form extends AuthType("form")

  /** OpenID Connect: the browser is sent to an external provider and comes back with a code. */
  case Oidc extends AuthType("oidc")
}

object AuthType {

  def fromWire(raw: String): Option[AuthType] = values.find(_.wire == raw.trim.toLowerCase(Locale.ROOT))

  given CanEqual[AuthType, AuthType] = CanEqual.derived
}

/** One account in `kui.auth.users[]` — the smallest credible deployment (AU-002): a shared instance with
  * three named people and no directory server to ask.
  *
  * @param name
  *   the login name, compared case-insensitively, because a person who signs in as `Admin` on Monday and
  *   `admin` on Tuesday is the same person and any other answer is a support ticket.
  * @param passwordHash
  *   the encoded hash, never a password. `Secret` so that a configuration dump, a startup error or a log line
  *   cannot print it: a password hash is not a password, but it is the input to an offline cracking attempt,
  *   and there is no reason for it to appear anywhere. Written by `./mill services.identity.app.runMain
  *   kui.identity.app.HashPassword`.
  * @param groups
  *   the groups this account is in, which is what `kui.rbac.roles[].subjects` with `kind: group` and
  *   `provider: FORM` match against. Roles are not written on the account itself, so that the same role file
  *   describes a form deployment and an OIDC one.
  * @param mustChangePassword
  *   whether this person is forced through a password change before anything else is allowed. The
  *   first-launch bootstrap account sets it; see `kui.identity.domain.UserRecord`.
  */
final case class FormUserConfig(
    name: String,
    passwordHash: Secret[String],
    groups: Set[String],
    mustChangePassword: Boolean
)

object FormUserConfig {
  given CanEqual[FormUserConfig, FormUserConfig] = CanEqual.derived
}

/** `kui.auth.oidc` — one OpenID Connect provider, which is as many as one KUI deployment has (ADR-015).
  *
  * @param issuer
  *   the provider's issuer URL. Discovery reads `<issuer>/.well-known/openid-configuration`, so this is the
  *   only address an operator normally has to write down.
  * @param clientId
  *   the relying party's id, as registered with the provider. Not a secret: it travels in the authorization
  *   URL the browser follows, so hiding it would be theatre.
  * @param clientSecret
  *   the relying party's secret. A `Secret`, and it never leaves this process — in particular it is never
  *   part of any answer to a browser, which is one of the two mistakes `research/scala/security-research.md`
  *   records the reference products making.
  * @param redirectUri
  *   where the provider sends the browser back to. It must be registered with the provider and it must be an
  *   address this deployment actually serves, which in practice is
  *   `<public base url>/api/v1/auth/oidc/callback`.
  * @param scopes
  *   what to ask the provider for. `openid` is mandatory and is added if the operator forgets it.
  * @param usernameClaim
  *   which ID-token claim holds the name to show and to audit. `preferred_username` for Keycloak, `email` for
  *   Google.
  * @param groupsClaim
  *   which claim holds the person's groups, when the provider puts them in the token at all. Absent means
  *   this deployment gets no groups from OIDC, which is honest rather than an error: role subjects can still
  *   name the user.
  * @param label
  *   what the login screen's button says. `"Sign in with Google"` is built from it.
  */
final case class OidcConfig(
    issuer: String,
    clientId: String,
    clientSecret: Secret[String],
    redirectUri: String,
    scopes: List[String],
    usernameClaim: String,
    groupsClaim: Option[String],
    label: String
)

object OidcConfig {

  /** The scope every OpenID Connect request must carry; without it the provider runs a plain OAuth 2 flow and
    * returns no ID token, which is the one thing KUI actually needs.
    */
  val RequiredScope: String = "openid"

  val DefaultScopes: List[String] = List(RequiredScope, "profile", "email")

  val DefaultUsernameClaim: String = "preferred_username"

  given CanEqual[OidcConfig, OidcConfig] = CanEqual.derived
}

/** The whole of `kui.auth`.
  *
  * The list of users and the OIDC block are read whatever the type is, and are simply unused by the modes
  * that do not need them. That is deliberate: an operator switching `type: form` to `type: oidc` and back
  * while they get a provider working should not have to delete and retype their accounts, and a section that
  * is only *parsed* when it is also *used* is a section whose typos appear a week later.
  */
final case class AuthConfig(
    authType: AuthType,
    users: List[FormUserConfig],
    oidc: Option[OidcConfig],
    trustedProxies: Set[String] = Set.empty
)

object AuthConfig {

  /** What a deployment that has said nothing about authentication gets: none. */
  val Default: AuthConfig = AuthConfig(AuthType.Disabled, Nil, None)

  given CanEqual[AuthConfig, AuthConfig] = CanEqual.derived
}
