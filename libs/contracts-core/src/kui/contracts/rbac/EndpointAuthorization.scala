package kui.contracts.rbac

import cats.syntax.all.*
import sttp.tapir.{AnyEndpoint, AttributeKey, EndpointIO, EndpointInput}

import kui.kernel.ClusterId
import kui.security.rbac.{AccessRequest, Action, OperationName, Resource, ResourceAccess}

/** Where the name of the thing being accessed comes from.
  *
  * A permission is granted over a *pattern* — `payments\..*` — so a decision needs the name of the topic, the
  * group or the subject the call is about. That name is almost always already in the endpoint's path, and
  * saying which path parameter holds it is enough for anyone to recover it from a request without decoding
  * the endpoint's erased input types.
  */
enum NameSource {

  /** The resource has no name: the audit trail, ksqlDB, the ACL list, a cluster's own configuration. */
  case Unnamed

  /** The name is the value of this path parameter, spelled exactly as the endpoint declared it. */
  case PathParam(param: String)

  /** The name is fixed by the endpoint itself, whatever the request says. */
  case Fixed(value: String)

  /** The name is in the request body — the topic a create names, the destination a resend copies into, the
    * topics a track reads — and so is not visible at the edge.
    *
    * The gateway does not decode a service's request bodies; it proxies them, and `ContractRouting` works
    * over endpoints whose input types Tapir has erased. So a requirement named this way is **not** checked
    * against a pattern at the gateway. The cluster gate and the read-only gate still apply, which is what
    * stops an unauthorized caller reaching the service at all, and the owning service — which has the decoded
    * body in hand — runs the exact check: its route hands the names in this field to
    * `SecuredRoutes.withBody`, and [[EndpointAuthorization.accessRequest]] turns each into an access over
    * that exact name. A route that hands over no names for the field is refused, never waved through.
    *
    * It is spelled out as its own case rather than left as `Unnamed` because the two mean opposite things: an
    * unnamed resource is one that genuinely has no name, and treating a body-named topic as unnamed would
    * make `Permission.covers` refuse every create in any deployment with RBAC on. Naming the gap is also what
    * lets `EndpointAuthorizationSuite` list exactly which endpoints depend on their service for the second
    * half of their check.
    *
    * @param field
    *   the body field the name is in, for the reader and for the service's own check. The field may hold one
    *   name or a list of them; every name in it must be permitted
    */
  case RequestBody(field: String)
}

object NameSource {
  given CanEqual[NameSource, NameSource] = CanEqual.derived
}

/** One thing an endpoint needs permission for. */
final case class ResourceRequirement(resource: Resource, name: NameSource, actions: Set[Action])

object ResourceRequirement {

  def named(resource: Resource, param: String, actions: Action*): ResourceRequirement =
    ResourceRequirement(resource, NameSource.PathParam(param), actions.toSet)

  def unnamed(resource: Resource, actions: Action*): ResourceRequirement =
    ResourceRequirement(resource, NameSource.Unnamed, actions.toSet)

  /** A resource whose name only the owning service can see. See [[NameSource.RequestBody]]. */
  def inBody(resource: Resource, field: String, actions: Action*): ResourceRequirement =
    ResourceRequirement(resource, NameSource.RequestBody(field), actions.toSet)

  given CanEqual[ResourceRequirement, ResourceRequirement] = CanEqual.derived
}

/** What an endpoint declares it needs permission for, attached to the endpoint itself.
  *
  * ==Why this lives on the endpoint and not in a table==
  *
  * The gateway must refuse a call before it leaves the edge, and the service behind it must refuse the same
  * call again, because a service that trusts its caller is a service that is open to anyone who reaches it
  * directly. Two enforcement points need the same rule. A lookup table on either side would be a second
  * spelling of that rule, and the one thing this project has learned the hard way is that a rule written
  * twice is a rule that disagrees with itself — usually in the direction of allowing something.
  *
  * So the rule is written once, by the team that owns the contract, on the endpoint. The gateway reads it off
  * the published `AnyEndpoint` it is already proxying; the service reads it off the very same value when it
  * binds its route. Neither can drift, because there is nothing to drift from.
  *
  * ==Why it is a declaration and not a function==
  *
  * A `Principal => Boolean` on the endpoint would be more flexible and could not be inspected. This shape can
  * be enumerated: `EndpointAuthorizationSuite` in each contract module walks every endpoint the service
  * publishes and fails if one carries no declaration. An endpoint added next year without a permission is a
  * build failure rather than a hole nobody notices, which is exactly the treatment `KuiEndpoint.MutationKey`
  * already gets for mutations.
  *
  * @param operation
  *   the stable operation name for the audit record and the log line. It matches the endpoint's own
  *   `.name(...)` so that "who was refused what" and "which route" are one string, not two
  * @param requirements
  *   every resource the call touches. All of them must be permitted: an operation that reads one topic and
  *   writes another is refused unless both are allowed, and it is refused before either happens
  */
final case class EndpointAuthorization(operation: String, requirements: List[ResourceRequirement])

object EndpointAuthorization {

  /** The attribute an endpoint carries its authorization declaration in. */
  val Key: AttributeKey[EndpointAuthorization] = AttributeKey[EndpointAuthorization]

  /** A declaration that needs nothing beyond access to the cluster in the path.
    *
    * This is what a *list* endpoint declares, and it is a real declaration rather than an absent one.
    * Kafbat's rule, kept by ADR-021, is that a list filters rather than refuses: an operator who may see
    * three of a hundred topics wants to see three topics, not a 403 — and a 403 would leak that the other
    * ninety-seven exist. So the endpoint is reachable by anyone who may reach the cluster, and `Rbac.visible`
    * removes the rows they may not see, inside the service that has the rows.
    */
  def clusterScoped(operation: String): EndpointAuthorization = EndpointAuthorization(operation, List.empty)

  /** A declaration over one resource, which is what almost every endpoint needs. */
  def one(operation: String, requirement: ResourceRequirement): EndpointAuthorization =
    EndpointAuthorization(operation, List(requirement))

  /** What this endpoint declared, if anything. */
  def of(endpoint: AnyEndpoint): Option[EndpointAuthorization] = endpoint.attribute(Key)

  /** One path slot of an endpoint: a literal segment, or a captured parameter. */
  private enum Slot {
    case Fixed(segment: String)
    case Capture(param: Option[String])
  }

  /** The endpoint's path, slot by slot, in the order a URL spells them.
    *
    * Both a fixed segment and a captured parameter consume exactly one segment of the request path, so the
    * position of a parameter in this list is its position in the request — which is the only fact needed to
    * read its value back out of a concrete request.
    */
  private def slots(input: EndpointInput[?]): List[Slot] =
    leaves(input).collect {
      case EndpointInput.FixedPath(segment, _, _) => Slot.Fixed(segment)
      case EndpointInput.PathCapture(name, _, _) => Slot.Capture(name)
    }

  private def leaves(input: EndpointInput[?]): List[EndpointInput[?]] =
    input match {
      case EndpointInput.Pair(left, right, _, _) => leaves(left) ++ leaves(right)
      case EndpointIO.Pair(left, right, _, _) => leaves(left) ++ leaves(right)
      case EndpointInput.MappedPair(wrapped, _) => leaves(wrapped)
      case EndpointIO.MappedPair(wrapped, _) => leaves(wrapped)
      case leaf => List(leaf)
    }

  /** How many path segments this endpoint's own path is made of. */
  def pathLength(endpoint: AnyEndpoint): Int = slots(endpoint.input).size

  /** The value of one of the endpoint's path parameters, read out of a concrete request's path.
    *
    * The request's segments are aligned with the endpoint's **from the end**, not from the start. A gateway
    * request arrives under `/api/v1/...` where the endpoint says `/internal/v1/...`, and a deployment served
    * under a base path — the Compose stack serves KUI under `/kui` — carries segments in front of even that.
    * Counting from the end is immune to both, because nothing is ever appended after an endpoint's declared
    * path.
    *
    * Returns `None` when the endpoint has no such parameter or the request is shorter than the endpoint's own
    * path. A caller must treat that as a refusal, never as "no name required": a decision taken over the
    * wrong name, or over none, is the one outcome worse than a wrong refusal.
    */
  def pathValue(endpoint: AnyEndpoint, param: String, requestSegments: List[String]): Option[String] = {
    val declared = slots(endpoint.input)
    val index = declared.indexWhere {
      case Slot.Capture(Some(name)) => name == param
      case _ => false
    }

    if index < 0 || requestSegments.sizeIs < declared.size then None
    else requestSegments.drop(requestSegments.size - declared.size).lift(index)
  }

  /** The authorization question this request asks, ready for `Rbac.decide`.
    *
    * `Left` carries the reason it could not be built, and every one of those reasons is a refusal rather than
    * a pass: an endpoint that declared nothing, a name that is not in the path where the declaration said it
    * would be, or a body field whose names the service was not given. All three mean the same thing
    * operationally — this call cannot be authorized — and answering "allowed" because the question could not
    * be formed is how authorization bypasses happen.
    *
    * @param bodyNames
    *   the names each [[NameSource.RequestBody]] field of the decoded body holds, keyed by field. `None` is
    *   the edge, which has not decoded the body: those requirements are left to the coarse gate in
    *   `EndpointDecision` and to the service. `Some` is the service, which has: every name becomes an access
    *   over that exact name, and a body-named field missing from the map is a `Left`
    */
  def accessRequest(
      endpoint: AnyEndpoint,
      cluster: Option[ClusterId],
      requestSegments: List[String],
      bodyNames: Option[Map[String, List[String]]] = None
  ): Either[String, AccessRequest] =
    of(endpoint) match {
      case None =>
        Left(
          s"${endpoint.info.name.getOrElse(endpoint.showShort)} declares no authorization requirement, " +
            "so no permission check can be run for it"
        )

      case Some(declared) =>
        declared.requirements
          .traverse(requirement => access(endpoint, requirement, requestSegments, bodyNames))
          .map(resources => AccessRequest(cluster, resources.flatten, OperationName(declared.operation)))
    }

  private def access(
      endpoint: AnyEndpoint,
      requirement: ResourceRequirement,
      requestSegments: List[String],
      bodyNames: Option[Map[String, List[String]]]
  ): Either[String, List[ResourceAccess]] =
    requirement.name match {
      case NameSource.Unnamed =>
        Right(List(ResourceAccess(requirement.resource, None, requirement.actions, None)))

      case NameSource.Fixed(value) =>
        Right(List(ResourceAccess(requirement.resource, Some(value), requirement.actions, None)))

      case NameSource.RequestBody(field) =>
        bodyNames match {
          // The edge, by construction: the gateway has no decoded body. Dropping it from the request leaves
          // the cluster and read-only gates in force and hands the name check to the service.
          case None => Right(Nil)

          // The service, which decoded the body: one access per name, each over that exact name.
          case Some(names) =>
            names.get(field) match {
              case Some(values) =>
                Right(
                  values.map(value =>
                    ResourceAccess(requirement.resource, Some(value), requirement.actions, None)
                  )
                )
              case None =>
                Left(
                  s"${endpoint.info.name.getOrElse(endpoint.showShort)} names its resource with the body " +
                    s"field '$field', whose names the route did not hand to the permission check"
                )
            }
        }

      case NameSource.PathParam(param) =>
        pathValue(endpoint, param, requestSegments) match {
          case Some(value) =>
            Right(List(ResourceAccess(requirement.resource, Some(value), requirement.actions, None)))
          case None =>
            Left(
              s"${endpoint.info.name.getOrElse(endpoint.showShort)} names its resource with the path " +
                s"parameter '$param', which this request's path does not carry"
            )
        }
    }

  given CanEqual[EndpointAuthorization, EndpointAuthorization] = CanEqual.derived
}
