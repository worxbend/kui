package kui.gateway.contract.dto

import io.circe.syntax.*
import io.circe.{Codec, HCursor, Json}
import sttp.tapir.Schema

/** The caller's identity, as the wire states it — never `kui.security.Principal` itself, which is a domain
  * type the gateway's `application` layer owns and which `ARCHITECTURE.md` §3 forbids a contract module from
  * depending on (ADR-041 rule A2). `AuthRoutes` maps one to the other (ADR-033).
  */
final case class PrincipalDto(name: String, roles: List[String], kind: String)

object PrincipalDto {

  given Codec[PrincipalDto] = Codec.from(
    (cursor: HCursor) =>
      for {
        name <- cursor.get[String]("name")
        roles <- cursor.get[List[String]]("roles")
        kind <- cursor.get[String]("kind")
      } yield PrincipalDto(name, roles, kind),
    (dto: PrincipalDto) =>
      Json.obj("name" -> dto.name.asJson, "roles" -> dto.roles.asJson, "kind" -> dto.kind.asJson)
  )

  given Schema[PrincipalDto] = Schema.derived[PrincipalDto]

  given CanEqual[PrincipalDto, PrincipalDto] = CanEqual.derived
}

/** One grant, as the wire states it: these actions, on this resource, on the resources whose name matches
  * this pattern, on these clusters.
  *
  * It mirrors `kui.security.rbac.ClusterPermission` field for field, and it is a separate type for the reason
  * [[PrincipalDto]] is: a contract module may not depend on the domain types the application layer owns
  * (ADR-041 rule A2). `AuthRoutes` maps one to the other.
  *
  * The `actions` are **already expanded**: if a role grants `DELETE` on a topic, `VIEW` is in this list too,
  * because the server expanded the dependency when it built the policy. The browser therefore never
  * re-derives the closure, which is the whole reason the expansion happens on the server — two
  * implementations of "granting delete also grants view" is two implementations that can disagree, and the
  * one that is wrong is the one the user sees.
  *
  * @param clusters
  *   the cluster ids this grant applies on. A role names its clusters, so the scoping lives here rather than
  *   on the permission.
  * @param resource
  *   the configuration spelling: `TOPIC`, `CONSUMER`, `SCHEMA`, …
  * @param value
  *   the resource-name pattern, as the regular expression an operator wrote. Absent for a resource that has
  *   no name — the audit trail, ksqlDB, the ACL list.
  * @param actions
  *   the configuration spellings: `VIEW`, `DELETE`, `MESSAGES_PRODUCE`, …
  */
final case class PermissionDto(
    clusters: List[String],
    resource: String,
    value: Option[String],
    actions: List[String],
    defaultRole: Boolean = false
)

object PermissionDto {

  given Codec[PermissionDto] = Codec.from(
    (cursor: HCursor) =>
      for {
        clusters <- cursor.get[List[String]]("clusters")
        resource <- cursor.get[String]("resource")
        value <- cursor.get[Option[String]]("value")
        actions <- cursor.get[List[String]]("actions")
        defaultRole <- cursor.getOrElse[Boolean]("defaultRole")(false)
      } yield PermissionDto(clusters, resource, value, actions, defaultRole),
    (dto: PermissionDto) =>
      Json.obj(
        "clusters" -> dto.clusters.asJson,
        "resource" -> dto.resource.asJson,
        "value" -> dto.value.asJson,
        "actions" -> dto.actions.asJson,
        "defaultRole" -> dto.defaultRole.asJson
      )
  )

  given Schema[PermissionDto] = Schema.derived[PermissionDto]

  given CanEqual[PermissionDto, PermissionDto] = CanEqual.derived
}

/** What `GET /api/v1/auth/me` answers with — the endpoint the frontend's `ApiClient` (UI-005) reads its CSRF
  * token from before it sends its first mutating request.
  *
  * @param principal
  *   who the caller is. `PrincipalDto("anonymous", Nil, "anonymous")` for every M0 request.
  * @param csrfToken
  *   the value to echo back in `X-Kui-Csrf` on every non-`GET` request. Safe to expose to JavaScript — it is
  *   read from the response body, not a cookie — which is exactly the double-submit property CSRF protection
  *   depends on: a page on another origin can make the browser attach a cookie automatically, but it cannot
  *   read this response to learn the token to put beside it.
  * @param authType
  *   `"disabled"` in M0; the value M6's login form checks to decide what to show
  * @param permissions
  *   everything this caller may do, expanded, so the interface can hide a control rather than offer it and
  *   have the server refuse it. A button that is offered and then refused teaches an operator that KUI's
  *   refusals are bugs, and the next real refusal is not believed.
  *
  * While no roles are configured this is one wildcard grant per resource over every cluster, not an empty
  * list. The two are very different: an empty list means "you may do nothing", and answering that in a
  * deployment which has asked for no authorization at all would hide every write control in the quickstart.
  */
final case class AuthMeResponse(
    principal: PrincipalDto,
    csrfToken: String,
    authType: String,
    permissions: List[PermissionDto]
)

object AuthMeResponse {

  given Codec[AuthMeResponse] = Codec.from(
    (cursor: HCursor) =>
      for {
        principal <- cursor.get[PrincipalDto]("principal")
        csrfToken <- cursor.get[String]("csrfToken")
        authType <- cursor.get[String]("authType")
        // Tolerated as absent so that an older gateway does not stop a newer browser from starting.
        // A response with no `permissions` field is one from before E4, and the honest reading of it
        // is "this server has nothing to say about permissions", not "you may do nothing".
        permissions <- cursor.getOrElse[List[PermissionDto]]("permissions")(Nil)
      } yield AuthMeResponse(principal, csrfToken, authType, permissions),
    (response: AuthMeResponse) =>
      Json.obj(
        "principal" -> response.principal.asJson,
        "csrfToken" -> response.csrfToken.asJson,
        "authType" -> response.authType.asJson,
        "permissions" -> response.permissions.asJson
      )
  )

  given Schema[AuthMeResponse] = Schema.derived[AuthMeResponse]

  given CanEqual[AuthMeResponse, AuthMeResponse] = CanEqual.derived
}
