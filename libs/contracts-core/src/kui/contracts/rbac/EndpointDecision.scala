package kui.contracts.rbac

import sttp.tapir.AnyEndpoint

import kui.kernel.ClusterId
import kui.security.Principal
import kui.security.rbac.{ClusterFlags, Decision, DenialReason, Rbac, RbacPolicy}

/** May this principal call this endpoint? — the whole question, in one pure function.
  *
  * ==Why it is here and not at each enforcement point==
  *
  * Three places have to answer it and they must never disagree: the gateway, before the call leaves the edge;
  * the service, because a service that trusts its caller is open to anyone who can reach it directly; and the
  * browser, so a control the server would refuse is never drawn as a button. This file compiles for the JVM
  * and for Scala.js and all three call [[decide]].
  *
  * ==What the browser's copy is worth==
  *
  * Nothing, as a security measure, and that is fine — its job is to hide a control, not to protect anything.
  * The two server-side calls are the enforcement, and the second one is not redundant with the first.
  */
object EndpointDecision {

  /** The verdict, together with what could not be answered.
    *
    * `Left` means the question could not be formed at all — the endpoint carries no declaration, or names its
    * resource with a path parameter that this request's path does not have. It is deliberately not folded
    * into `Denied`: a denial is a fact about the caller and belongs in the audit trail as one, while this is
    * a fact about the code, belongs in the log at error level, and should wake somebody. Both refuse the
    * request; only one of them is a bug.
    *
    * @param bodyNames
    *   the names in the decoded body, by [[NameSource.RequestBody]] field — given by the service, which has
    *   decoded it, and absent at the edge, which has not. See `EndpointAuthorization.accessRequest`
    */
  def decide(
      policy: RbacPolicy,
      principal: Principal,
      flags: ClusterFlags,
      endpoint: AnyEndpoint,
      cluster: Option[ClusterId],
      requestSegments: List[String],
      bodyNames: Option[Map[String, List[String]]] = None
  ): Either[String, Decision] =
    EndpointAuthorization.accessRequest(endpoint, cluster, requestSegments, bodyNames).map { request =>
      Rbac.decide(policy, principal, flags, request) match {
        case denied: Decision.Denied => denied
        case Decision.Allowed => bodyNamedGate(policy, principal, flags, endpoint, cluster)
      }
    }

  /** The coarse half of the check for a resource whose name is only in the request body.
    *
    * `Rbac.decide` above has already applied the cluster gate, and the read-only gate over every requirement
    * whose name this side of the hop can see. At the edge, what it could not consider is a requirement named
    * [[NameSource.RequestBody]] — a topic create names its topic in the body — so those are handled here, and
    * exactly two things are asserted about them:
    *
    *   1. **read-only still refuses.** A create on a read-only cluster is refused at the edge, which is the
    *      whole point of the flag and does not depend on knowing the name.
    *   1. **the principal holds the action on *some* pattern of that resource.** Somebody with no topic
    *      permissions at all cannot reach the create endpoint. Somebody whose grant is `payments\..*` can,
    *      and if they then ask to create `orders` the service refuses them with the name in hand: it passes
    *      the body's names as `bodyNames`, and `Rbac.decide` above has then already checked each one exactly.
    *
    * This is stated as a deliberate weakening rather than hidden as an implementation detail, because a
    * reader has to be able to see where the exact check lives. The alternative — the gateway decoding every
    * service's request bodies to authorize them — would put a copy of eleven services' DTOs in the one module
    * that is supposed to know none of them.
    */
  private def bodyNamedGate(
      policy: RbacPolicy,
      principal: Principal,
      flags: ClusterFlags,
      endpoint: AnyEndpoint,
      cluster: Option[ClusterId]
  ): Decision = {
    val bodyNamed = EndpointAuthorization
      .of(endpoint)
      .toList
      .flatMap(_.requirements)
      .filter(requirement =>
        requirement.name match {
          case NameSource.RequestBody(_) => true
          case _ => false
        }
      )

    if bodyNamed.isEmpty then Decision.Allowed
    else {
      val altering = bodyNamed.flatMap(_.actions).filter(_.isAlter).toSet

      cluster match {
        case Some(id) if flags.readOnly && altering.nonEmpty =>
          Decision.Denied(DenialReason.ReadOnlyCluster(id, altering))

        case _ if !policy.enabled => Decision.Allowed

        case _ =>
          val granted = Rbac.effectivePermissions(policy, principal, cluster)

          bodyNamed
            .flatMap(requirement =>
              requirement.actions.toList
                .filterNot(action =>
                  granted.exists(permission =>
                    permission.resource == requirement.resource &&
                      permission.value.isDefined &&
                      permission.actions.contains(action)
                  )
                )
                .map(action => (requirement, action))
            )
            .headOption
            .fold(Decision.Allowed) { case (requirement, _) =>
              Decision.Denied(
                DenialReason.MissingActions(requirement.resource, None, requirement.actions)
              )
            }
      }
    }
  }
}
