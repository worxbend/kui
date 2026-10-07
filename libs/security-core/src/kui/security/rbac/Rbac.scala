package kui.security.rbac

import kui.kernel.{ClusterId, RoleName}
import kui.security.Principal

/** The name of the operation being authorized, for the audit record and the log line.
  *
  * `deleteTopic`, `resetConsumerGroupOffsets`. It carries no authority of its own — nothing branches on it —
  * and exists so that an audit entry and an access decision cannot disagree about what was being attempted,
  * because both are derived from the same [[AccessRequest]].
  */
final case class OperationName(value: String)

object OperationName {
  given CanEqual[OperationName, OperationName] = CanEqual.derived
}

/** One thing a request needs to be allowed to do.
  *
  * @param name
  *   the resource's name, where it has one: a topic name, a group id, `payments/sink`. `None` for the
  *   resources that are singletons — the audit trail, the ACL list, ksqlDB.
  * @param fallback
  *   a second access to try when this one is denied. Exactly one thing uses it: a connector access falls back
  *   to the same action on its parent connect cluster, so that granting `EDIT` on the connect cluster
  *   `payments` does not have to be repeated for each of its forty connectors. [[ResourceAccess.connector]]
  *   builds the pair; nothing else should build a fallback by hand.
  */
final case class ResourceAccess(
    resource: Resource,
    name: Option[String],
    actions: Set[Action],
    fallback: Option[ResourceAccess]
)

object ResourceAccess {

  /** An access to a named resource — a topic, a group, a subject. */
  def named(resource: Resource, name: String, actions: Action*): ResourceAccess =
    ResourceAccess(resource, Some(name), actions.toSet, None)

  /** An access to a resource that has no name: the audit trail, ksqlDB, the ACL list. */
  def unnamed(resource: Resource, actions: Action*): ResourceAccess =
    ResourceAccess(resource, None, actions.toSet, None)

  /** A connector access, with the fallback to its parent connect cluster attached.
    *
    * The connector is named `"<connect>/<connector>"` because that is the name Kafbat's patterns are written
    * against; the fallback is named with the connect cluster alone.
    */
  def connector(connect: String, connector: String, actions: Action*): ResourceAccess = {
    val onConnect = actions.flatMap(_.onParentConnect).toSet

    ResourceAccess(
      resource = Resource.Connector,
      name = Some(s"$connect/$connector"),
      actions = actions.toSet,
      fallback =
        if onConnect.isEmpty then None
        else Some(ResourceAccess(Resource.Connect, Some(connect), onConnect, None))
    )
  }

  given CanEqual[ResourceAccess, ResourceAccess] = CanEqual.derived
}

/** One authorization question, complete.
  *
  * Every resource in `resources` must be allowed: an operation that reads one topic and writes another is
  * refused unless both are permitted, and it is refused before either happens.
  */
final case class AccessRequest(
    cluster: Option[ClusterId],
    resources: List[ResourceAccess],
    operation: OperationName
)

object AccessRequest {

  def apply(cluster: ClusterId, operation: String, resources: ResourceAccess*): AccessRequest =
    AccessRequest(Some(cluster), resources.toList, OperationName(operation))

  /** A request about KUI itself rather than about a cluster: the application configuration, the audit viewer.
    */
  def global(operation: String, resources: ResourceAccess*): AccessRequest =
    AccessRequest(None, resources.toList, OperationName(operation))

  given CanEqual[AccessRequest, AccessRequest] = CanEqual.derived
}

/** Why a request was refused.
  *
  * It is a value rather than a message so that the audit record, the log line and the 403's `message` are
  * three renderings of one fact instead of three strings that can disagree. None of these reaches an
  * unauthenticated caller: the HTTP layer turns every one of them into the same `403`.
  */
enum DenialReason {

  /** The principal holds no role that covers this cluster, and there is no default role. */
  case NoClusterAccess(cluster: ClusterId)

  /** The cluster is configured read-only and the request would change it (ADR-047). */
  case ReadOnlyCluster(cluster: ClusterId, actions: Set[Action])

  /** The principal's permissions do not cover these actions on this resource. */
  case MissingActions(resource: Resource, name: Option[String], actions: Set[Action])

  /** A one-line rendering for a log or an audit record. It names the resource and the actions and never the
    * principal, because the caller of this already knows who it asked about.
    */
  def describe: String = this match {
    case NoClusterAccess(cluster) => s"no role grants access to cluster ${cluster.value}"
    case ReadOnlyCluster(cluster, actions) =>
      s"cluster ${cluster.value} is read-only, so ${render(actions)} is not accepted"
    case MissingActions(resource, name, actions) =>
      val on = name.fold(resource.wire)(value => s"${resource.wire} $value")
      s"${render(actions)} on $on is not granted"
  }

  private def render(actions: Set[Action]): String =
    actions.map(_.wire).toList.sorted.mkString(", ")
}

object DenialReason {
  given CanEqual[DenialReason, DenialReason] = CanEqual.derived
}

/** The answer. */
enum Decision {
  case Allowed
  case Denied(reason: DenialReason)

  def isAllowed: Boolean = this match {
    case Allowed => true
    case Denied(_) => false
  }
}

object Decision {
  given CanEqual[Decision, Decision] = CanEqual.derived
}

/** Which clusters a grant applies on.
  *
  * `Every` is a case of its own rather than "the set of all cluster ids", because the thing that computes a
  * grant does not always know what all the cluster ids are. The gateway serves `/auth/me` and holds no
  * cluster list — the cluster service does — so a default role, or a deployment with no roles configured at
  * all, has to be able to say "everywhere" without enumerating anywhere.
  */
enum ClusterScope {

  /** Every cluster this deployment has, including ones added after this answer was computed. */
  case Every

  /** Exactly these, as a role named them. */
  case Named(clusters: Set[ClusterId])

  def includes(cluster: ClusterId): Boolean = this match {
    case Every => true
    case Named(clusters) => clusters.contains(cluster)
  }
}

object ClusterScope {

  /** How `Every` is spelled on the wire.
    *
    * A cluster id is a lowercase slug, so `*` cannot collide with one — which is what makes it safe to put in
    * the same list as real cluster ids rather than adding a second field beside it.
    */
  val EveryWire: String = "*"

  given CanEqual[ClusterScope, ClusterScope] = CanEqual.derived
}

/** A permission together with the clusters it applies on.
  *
  * This is the shape `/auth/me` sends to the browser and the shape the browser's own gate reads. A
  * `Permission` alone is not enough there, because the browser has to answer "may I delete *this* topic on
  * *this* cluster" and the cluster scoping lives on the role rather than on the permission.
  */
final case class ClusterPermission(
    clusters: ClusterScope,
    permission: Permission,
    defaultRole: Boolean = false
)

object ClusterPermission {
  given CanEqual[ClusterPermission, ClusterPermission] = CanEqual.derived
}

/** Deciding what a principal may do — the whole of it, as pure functions.
  *
  * ==Why this is pure, and why it is cross-compiled==
  *
  * Every write control in the interface has to be hidden when the person looking at it may not use it. A
  * button that is offered and then refused by the server teaches an operator that KUI's refusals are bugs,
  * and the next real refusal is not believed. The only way for the interface's answer and the server's to
  * agree in every case is for them to run the same code, so this file compiles for the JVM and for the
  * browser and both sides call [[decide]].
  *
  * The browser's copy is advisory. A service always re-runs [[decide]] from the principal the gateway signed
  * (ADR-021, ADR-020), because a check that only ran in the browser is not a check.
  *
  * ==The order of the gates==
  *
  * Read [[decide]] top to bottom and the order is the specification:
  *
  *   1. RBAC off → allowed, except that read-only still refuses. Read-only is not an authorization rule; it
  *      is a fact about the cluster, and it applies to a deployment with no roles exactly as it does to one
  *      with a hundred.
  *   1. the cluster gate — the principal must hold a role naming this cluster, or a default role must exist;
  *   1. the read-only gate — a request containing any [[Action.isAlter]] action is refused on a read-only
  *      cluster before any resource is considered, so that an operator is told the cluster is read-only
  *      rather than that they lack a permission they may well have;
  *   1. the resource gate — every [[ResourceAccess]] must be covered, with a connector falling back to its
  *      parent connect cluster.
  */
object Rbac {

  /** The roles an identity is in. Runs once, at login; the result goes into the signed principal. */
  def resolveRoles(policy: RbacPolicy, identity: Map[Provider, IdentityAttributes]): Set[RoleName] =
    policy.rolesFor(identity)

  /** Every permission that applies to this principal on this cluster, with the actions already expanded.
    *
    * The default role applies when the principal's roles yield no permissions *on this cluster*, which
    * includes the case of someone who holds roles for other clusters only. That is Kafbat's behaviour and it
    * is the one that makes a default role useful: it is the floor, not the fallback for the role-less.
    */
  def effectivePermissions(
      policy: RbacPolicy,
      principal: Principal,
      cluster: Option[ClusterId]
  ): List[Permission] = {
    val fromRoles = policy
      .held(principal.roles)
      .filter(role => cluster.forall(role.clusters.contains))
      .flatMap(_.permissions)

    if fromRoles.nonEmpty then fromRoles
    else policy.defaultRole.map(_.permissions).getOrElse(List.empty)
  }

  /** May this principal do this?
    *
    * @param flags
    *   what is true of the cluster itself. `ClusterFlags.Writable` for a request that names no cluster.
    */
  def decide(
      policy: RbacPolicy,
      principal: Principal,
      flags: ClusterFlags,
      request: AccessRequest
  ): Decision = {
    val altering = request.resources.flatMap(_.actions).filter(_.isAlter).toSet

    val readOnlyGate: Option[Decision.Denied] = request.cluster match {
      case Some(cluster) if flags.readOnly && altering.nonEmpty =>
        Some(Decision.Denied(DenialReason.ReadOnlyCluster(cluster, altering)))
      case _ => None
    }

    if !policy.enabled then readOnlyGate.getOrElse(Decision.Allowed)
    else
      clusterGate(policy, principal, request.cluster) match {
        case Some(denied) => denied
        case None =>
          readOnlyGate match {
            case Some(denied) => denied
            case None =>
              val granted = effectivePermissions(policy, principal, request.cluster)
              request.resources
                .map(access => resourceGate(granted, access))
                .collectFirst { case denied: Decision.Denied => denied }
                .getOrElse(Decision.Allowed)
          }
      }
  }

  /** The subset of `items` the principal may see, for a list endpoint.
    *
    * List endpoints filter rather than refuse, which is Kafbat's rule and the right one: an operator who may
    * see three of a hundred topics wants to see three topics, not a 403. A denial there would also leak the
    * existence of the other ninety-seven.
    *
    * @param action
    *   the action that constitutes "seeing" this kind of thing — normally the resource's `VIEW`.
    */
  def visible[A](
      policy: RbacPolicy,
      principal: Principal,
      flags: ClusterFlags,
      cluster: Option[ClusterId],
      resource: Resource,
      action: Action
  )(items: List[A])(nameOf: A => String): List[A] =
    if !policy.enabled then items
    else
      items.filter(item =>
        decide(
          policy,
          principal,
          flags,
          AccessRequest(
            cluster,
            List(ResourceAccess(resource, Some(nameOf(item)), Set(action), None)),
            OperationName(s"list:${resource.wire}")
          )
        ).isAllowed
      )

  /** Everything this principal may do, across every cluster.
    *
    * This is what `/auth/me` answers with and what the browser's permission store holds (E4). Every
    * permission arrives with its actions already expanded, so the browser's gate is a lookup and never a
    * second implementation of [[Action.closure]]. Default-role grants are marked fallback-only. Consumers
    * must first scope all grants to the requested cluster, then discard defaults whenever any non-default
    * grant remains, before checking resources.
    *
    * When RBAC is off the answer is a single wildcard grant per resource over every cluster, rather than an
    * empty list. An empty list and "no restrictions" would otherwise be indistinguishable in the browser, and
    * the interface would hide every write control in a deployment that has asked for no authorization at all
    * — which is the quickstart.
    *
    * The answer is advisory and always was: it says what the *server* will allow, so the interface can hide
    * what it would refuse. It is not a capability, and holding it grants nothing.
    */
  def grants(policy: RbacPolicy, principal: Principal): List[ClusterPermission] =
    if !policy.enabled then
      Resource.values.toList.map(resource =>
        ClusterPermission(
          ClusterScope.Every,
          RbacPolicy.allPermission(resource, Some(ResourcePattern.Everything))
        )
      )
    else {
      val fromRoles = policy
        .held(principal.roles)
        .flatMap(role => role.permissions.map(ClusterPermission(ClusterScope.Named(role.clusters), _)))

      val fromDefault = policy.defaultRole.toList
        .flatMap(_.permissions)
        .map(permission => ClusterPermission(ClusterScope.Every, permission, defaultRole = true))

      fromRoles ++ fromDefault
    }

  // -----------------------------------------------------------------------------------------------

  /** The cluster gate: hold a role naming this cluster, or fall through to the default role. */
  private def clusterGate(
      policy: RbacPolicy,
      principal: Principal,
      cluster: Option[ClusterId]
  ): Option[Decision.Denied] = cluster match {
    case None => None
    case Some(id) =>
      val covered = policy.held(principal.roles).exists(_.clusters.contains(id))
      if covered || policy.defaultRole.isDefined then None
      else Some(Decision.Denied(DenialReason.NoClusterAccess(id)))
  }

  /** The resource gate for one access, including its fallback. */
  private def resourceGate(granted: List[Permission], access: ResourceAccess): Decision = {
    val held = granted
      .filter(_.covers(access.resource, access.name))
      .flatMap(_.actions)
      .toSet

    val missing = access.actions.diff(held)

    if missing.isEmpty then Decision.Allowed
    else
      access.fallback match {
        case Some(parent) => resourceGate(granted, parent)
        case None =>
          Decision.Denied(DenialReason.MissingActions(access.resource, access.name, missing))
      }
  }
}
