package kui.identity.contract.dto

import io.circe.syntax.*
import io.circe.{Codec, DecodingFailure, HCursor, Json}
import sttp.tapir.Schema

/** What this deployment's authentication looks like, for the screen that has to draw a login form.
  *
  * ==What is deliberately not in here==
  *
  * The issuer URL, the client id, the client secret, the token endpoint, the accounts list, and every Kafka
  * setting KUI holds. `research/scala/security-research.md` records a reference product serving its own
  * connection configuration — Kafka credentials included — from an endpoint like this one. The defence that
  * works is not care; it is that this type has three fields and none of them can hold a credential.
  *
  * @param authType
  *   `disabled`, `form` or `oidc`, spelled as `kui.auth.type` spells it
  * @param providerLabel
  *   what to write on the sign-in button, when there is a provider at all
  * @param rbacEnabled
  *   whether this deployment has configured any roles. It distinguishes "you may do everything because nobody
  *   asked for authorization" from "you may do everything because your role says so", which look identical in
  *   a permission list and are very different things to tell an operator
  */
final case class AuthSettingsDto(authType: String, providerLabel: Option[String], rbacEnabled: Boolean)

object AuthSettingsDto {

  given Codec[AuthSettingsDto] = Codec.from(
    (cursor: HCursor) =>
      for {
        authType <- cursor.get[String]("authType")
        providerLabel <- cursor.get[Option[String]]("providerLabel")
        rbacEnabled <- cursor.get[Boolean]("rbacEnabled")
      } yield AuthSettingsDto(authType, providerLabel, rbacEnabled),
    (dto: AuthSettingsDto) =>
      Json.obj(
        "authType" -> dto.authType.asJson,
        "providerLabel" -> dto.providerLabel.asJson,
        "rbacEnabled" -> dto.rbacEnabled.asJson
      )
  )

  given Schema[AuthSettingsDto] = Schema.derived[AuthSettingsDto]

  given CanEqual[AuthSettingsDto, AuthSettingsDto] = CanEqual.derived
}

/** A username and a password, on their way in. It exists only as a request body and never as a response. */
final case class LoginRequest(username: String, password: String)

object LoginRequest {

  given Codec[LoginRequest] = Codec.from(
    (cursor: HCursor) =>
      for {
        username <- cursor.get[String]("username")
        password <- cursor.get[String]("password")
      } yield LoginRequest(username, password),
    (dto: LoginRequest) => Json.obj("username" -> dto.username.asJson, "password" -> dto.password.asJson)
  )

  given Schema[LoginRequest] = Schema.derived[LoginRequest]

  given CanEqual[LoginRequest, LoginRequest] = CanEqual.derived
}

/** Who somebody turned out to be, on the wire.
  *
  * It mirrors `kui.security.Principal` field for field and is a separate type because a contract module may
  * not depend on a service's domain or application layer (ADR-041 rule A2) — and because the day `Principal`
  * grows a field that should not leave the server, the wire shape should not grow it too.
  */
final case class IdentityPrincipalDto(name: String, roles: List[String], kind: String)

object IdentityPrincipalDto {

  given Codec[IdentityPrincipalDto] = Codec.from(
    (cursor: HCursor) =>
      for {
        name <- cursor.get[String]("name")
        roles <- cursor.get[List[String]]("roles")
        kind <- cursor.get[String]("kind")
      } yield IdentityPrincipalDto(name, roles, kind),
    (dto: IdentityPrincipalDto) =>
      Json.obj("name" -> dto.name.asJson, "roles" -> dto.roles.asJson, "kind" -> dto.kind.asJson)
  )

  given Schema[IdentityPrincipalDto] = Schema.derived[IdentityPrincipalDto]

  given CanEqual[IdentityPrincipalDto, IdentityPrincipalDto] = CanEqual.derived
}

/** How a sign-in ended.
  *
  * Two outcomes, discriminated by `status`, because there genuinely are two: signed in, or "the password was
  * right and you may not have a session until you have set a new one". Modelling the second as a success with
  * a flag beside it is exactly how a forced password change becomes something only the browser enforces — and
  * a rule only the browser enforces is not a rule.
  *
  * A *failed* sign-in is not in here at all. It is an error response with the standard envelope
  * (`KUI-UNAUTHENTICATED`), because it is a failure, and because sharing one shape with successes invites a
  * client to treat a rejection as a login with no principal in it.
  */
enum LoginResponse {

  /** Signed in, with roles already resolved. */
  case SignedIn(principal: IdentityPrincipalDto)

  /** A new password is required first.
    *
    * @param challenge
    *   the single-use, few-minute token that the change must be presented with. It is the *only* thing the
    *   caller gets: no principal, no roles, no permissions.
    */
  case PasswordChangeRequired(challenge: String)
}

object LoginResponse {

  val SignedInStatus: String = "signed_in"
  val PasswordChangeStatus: String = "password_change_required"

  /** Written out rather than derived (ADR-007). A discriminated union is a contract whose shape a reader
    * should be able to see in one screen, and a derivation would put the discriminator's spelling in a
    * compiler plugin's hands.
    */
  given Codec[LoginResponse] = Codec.from(
    (cursor: HCursor) =>
      cursor.get[String]("status").flatMap {
        case SignedInStatus => cursor.get[IdentityPrincipalDto]("principal").map(SignedIn.apply)
        case PasswordChangeStatus => cursor.get[String]("challenge").map(PasswordChangeRequired.apply)
        case other =>
          Left(DecodingFailure(s"'$other' is not a login outcome KUI knows about", cursor.history))
      },
    {
      case SignedIn(principal) =>
        Json.obj("status" -> SignedInStatus.asJson, "principal" -> principal.asJson)
      case PasswordChangeRequired(challenge) =>
        Json.obj("status" -> PasswordChangeStatus.asJson, "challenge" -> challenge.asJson)
    }
  )

  given Schema[LoginResponse] = Schema.derived[LoginResponse]

  given CanEqual[LoginResponse, LoginResponse] = CanEqual.derived
}

/** Finishing a forced password change.
  *
  * It carries the challenge rather than the old password, because the challenge *is* the proof that the old
  * password was verified moments ago. Accepting a username and a password here would make this a second login
  * endpoint with its own logging and its own rate limiting.
  */
final case class ChangePasswordRequest(challenge: String, newPassword: String)

object ChangePasswordRequest {

  given Codec[ChangePasswordRequest] = Codec.from(
    (cursor: HCursor) =>
      for {
        challenge <- cursor.get[String]("challenge")
        newPassword <- cursor.get[String]("newPassword")
      } yield ChangePasswordRequest(challenge, newPassword),
    (dto: ChangePasswordRequest) =>
      Json.obj("challenge" -> dto.challenge.asJson, "newPassword" -> dto.newPassword.asJson)
  )

  given Schema[ChangePasswordRequest] = Schema.derived[ChangePasswordRequest]

  given CanEqual[ChangePasswordRequest, ChangePasswordRequest] = CanEqual.derived
}

/** One grant, expanded, on the wire.
  *
  * It is structurally identical to the gateway's `PermissionDto`, and that repetition is the layering rule
  * rather than an oversight: a contract module is a service's *published* shape, and two services sharing one
  * type means one of them owns the other's wire format. The gateway maps between them in one function
  * (ADR-033), which is also the place where a future divergence would be visible.
  *
  * `actions` is already closed under the action dependencies: a grant of `DELETE` carries `VIEW` too, because
  * the server expanded it when it read the role. The browser therefore never re-derives the closure, which is
  * the whole point — two implementations of "granting delete also grants view" is two implementations that
  * can disagree, and the wrong one is the one a user sees.
  */
final case class GrantDto(
    clusters: List[String],
    resource: String,
    value: Option[String],
    actions: List[String],
    defaultRole: Boolean = false
)

object GrantDto {

  given Codec[GrantDto] = Codec.from(
    (cursor: HCursor) =>
      for {
        clusters <- cursor.get[List[String]]("clusters")
        resource <- cursor.get[String]("resource")
        value <- cursor.get[Option[String]]("value")
        actions <- cursor.get[List[String]]("actions")
        defaultRole <- cursor.getOrElse[Boolean]("defaultRole")(false)
      } yield GrantDto(clusters, resource, value, actions, defaultRole),
    (dto: GrantDto) =>
      Json.obj(
        "clusters" -> dto.clusters.asJson,
        "resource" -> dto.resource.asJson,
        "value" -> dto.value.asJson,
        "actions" -> dto.actions.asJson,
        "defaultRole" -> dto.defaultRole.asJson
      )
  )

  given Schema[GrantDto] = Schema.derived[GrantDto]

  given CanEqual[GrantDto, GrantDto] = CanEqual.derived
}

/** Everything the caller may do. */
final case class PermissionsResponse(permissions: List[GrantDto])

object PermissionsResponse {

  given Codec[PermissionsResponse] = Codec.from(
    (cursor: HCursor) => cursor.get[List[GrantDto]]("permissions").map(PermissionsResponse.apply),
    (dto: PermissionsResponse) => Json.obj("permissions" -> dto.permissions.asJson)
  )

  given Schema[PermissionsResponse] = Schema.derived[PermissionsResponse]

  given CanEqual[PermissionsResponse, PermissionsResponse] = CanEqual.derived
}

/** Where to send the browser to sign in with the external provider, and the value that has to come back.
  *
  * The `state` is handed to the caller because the caller — the gateway — has to be able to recognise the
  * callback as belonging to this flow. It is single-use and short-lived, and the provider never interprets
  * it.
  */
final case class OidcStartResponse(authorizationUrl: String, state: String)

object OidcStartResponse {

  given Codec[OidcStartResponse] = Codec.from(
    (cursor: HCursor) =>
      for {
        url <- cursor.get[String]("authorizationUrl")
        state <- cursor.get[String]("state")
      } yield OidcStartResponse(url, state),
    (dto: OidcStartResponse) =>
      Json.obj("authorizationUrl" -> dto.authorizationUrl.asJson, "state" -> dto.state.asJson)
  )

  given Schema[OidcStartResponse] = Schema.derived[OidcStartResponse]

  given CanEqual[OidcStartResponse, OidcStartResponse] = CanEqual.derived
}

/** What the provider sent the browser back with. */
final case class OidcCallbackRequest(code: String, state: String)

object OidcCallbackRequest {

  given Codec[OidcCallbackRequest] = Codec.from(
    (cursor: HCursor) =>
      for {
        code <- cursor.get[String]("code")
        state <- cursor.get[String]("state")
      } yield OidcCallbackRequest(code, state),
    (dto: OidcCallbackRequest) => Json.obj("code" -> dto.code.asJson, "state" -> dto.state.asJson)
  )

  given Schema[OidcCallbackRequest] = Schema.derived[OidcCallbackRequest]

  given CanEqual[OidcCallbackRequest, OidcCallbackRequest] = CanEqual.derived
}
