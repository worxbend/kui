package kui.identity.api

import kui.identity.application.LoginResult
import kui.identity.contract.dto.*
import kui.security.Principal
import kui.security.rbac.{ClusterPermission, ClusterScope}

/** Application types to wire types, in one place (ADR-033).
  *
  * It is a handful of one-line functions and it still earns its own file, because it is the only place in the
  * service where a domain value becomes something a browser can read — which makes it the only place a field
  * that should not leave the server could accidentally start leaving it.
  */
object IdentityMapping {

  def principal(value: Principal): IdentityPrincipalDto =
    IdentityPrincipalDto(
      name = value.name.value,
      // Sorted so that two responses about the same principal are byte-identical. An unsorted set here
      // makes the response change from request to request for no reason, which defeats every cache and
      // makes a golden-file test impossible to write.
      roles = value.roles.map(_.value).toList.sorted,
      kind = value.kind.wire
    )

  def login(result: LoginResult): LoginResponse = result match {
    case LoginResult.SignedIn(who) => LoginResponse.SignedIn(principal(who))
    case LoginResult.MustChangePassword(challenge) =>
      LoginResponse.PasswordChangeRequired(challenge.value)
  }

  /** One grant, with both lists sorted for the reason [[principal]] sorts its roles. */
  def grant(granted: ClusterPermission): GrantDto =
    GrantDto(
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
