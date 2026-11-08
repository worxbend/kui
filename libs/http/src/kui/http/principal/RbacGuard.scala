package kui.http.principal

import cats.Applicative
import cats.effect.kernel.Sync
import cats.syntax.all.*
import org.typelevel.log4cats.StructuredLogger
import sttp.tapir.AnyEndpoint

import kui.contracts.rbac.{EndpointAuthorization, EndpointDecision}
import kui.kernel.ClusterId
import kui.kernel.error.{ApplicationError, KuiError}
import kui.security.Principal
import kui.security.rbac.{ClusterFlags, Decision, RbacPolicy}

/** A service's own permission check, run on every route it serves.
  *
  * ==Why a second check==
  *
  * The gateway already refused this call if the caller had no permission for it, and that check is the one
  * that saves the work. This one is the one that is load-bearing. A KUI service listens on its own port, in
  * its own container, and anything that can reach that port and mint a principal the service will verify can
  * call it — the gateway is not a wall, it is a door. ADR-021 says every service re-runs the decision from
  * the signed principal for exactly that reason, and a service that skips it is open to whatever else shares
  * its network.
  *
  * It is not a copy of the gateway's rule. Both call `EndpointDecision.decide` over the same declaration the
  * endpoint carries, so there is one rule with two enforcement points, which is the only arrangement in which
  * they cannot disagree.
  *
  * ==What it can check that the gateway cannot==
  *
  * The cluster's name and the resource's name, from the same path — and the names that are only in the
  * request body, which the gateway does not decode and so checks only coarsely. `SecuredRoutes.withBody` has
  * the decoded input in hand and passes those names here as `bodyNames`, so every one of them is checked
  * against the caller's patterns exactly. The endpoints in that position are enumerated by the gateway's
  * `EndpointAuthorizationSuite` rather than left to be discovered.
  */
trait RbacGuard[F[_]] {

  /** @param requestPath
    *   the request's path as it arrived, `/internal/v1/clusters/local/topics/orders`
    * @param bodyNames
    *   the names the decoded request body holds, keyed by the field a `NameSource.RequestBody` requirement
    *   declares. A body-named requirement whose field is not in this map cannot be decided and is refused, so
    *   a route that forgets to hand its names over fails closed
    */
  def authorize(
      principal: Principal,
      endpoint: AnyEndpoint,
      requestPath: String,
      bodyNames: Map[String, List[String]] = Map.empty
  ): F[Either[KuiError, Unit]]
}

object RbacGuard {

  /** The path parameter every cluster-scoped endpoint names its cluster with.
    *
    * A service reads the cluster off the path by parameter name rather than by position, because unlike the
    * gateway it is not restricted to erased `AnyEndpoint`s in a list — and a name survives a path being
    * re-arranged, which a position does not.
    */
  val ClusterIdParam: String = "clusterId"

  /** Allows everything. For a suite that is testing something else, and for a composition root that has not
    * been given a policy yet — the guard is then the gateway's alone, which is a weaker deployment and should
    * not be the default anywhere it can be avoided.
    */
  def allowAll[F[_]: Applicative]: RbacGuard[F] =
    new RbacGuard[F] {
      def authorize(
          principal: Principal,
          endpoint: AnyEndpoint,
          requestPath: String,
          bodyNames: Map[String, List[String]]
      ): F[Either[KuiError, Unit]] = ().asRight[KuiError].pure[F]
    }

  /** The real guard: this deployment's roles, and what it knows about each cluster.
    *
    * @param flagsFor
    *   whether a cluster is read-only. A service knows this from its own configuration, so the refusal does
    *   not depend on the gateway having been asked first
    */
  def fromPolicy[F[_]: Sync](
      policy: RbacPolicy,
      flagsFor: ClusterId => ClusterFlags,
      logger: StructuredLogger[F]
  ): RbacGuard[F] =
    new RbacGuard[F] {

      private val refusal: KuiError =
        ApplicationError.Forbidden("You do not have permission to do that")

      def authorize(
          principal: Principal,
          endpoint: AnyEndpoint,
          requestPath: String,
          bodyNames: Map[String, List[String]]
      ): F[Either[KuiError, Unit]] = {
        val segments = requestPath.split('/').iterator.filter(_.nonEmpty).toList

        val cluster = EndpointAuthorization
          .pathValue(endpoint, ClusterIdParam, segments)
          .flatMap(ClusterId.from(_).toOption)

        val flags = cluster.fold(ClusterFlags.Writable)(flagsFor)

        EndpointDecision.decide(
          policy,
          principal,
          flags,
          endpoint,
          cluster,
          segments,
          Some(bodyNames)
        ) match {
          case Right(Decision.Allowed) => ().asRight[KuiError].pure[F]

          case Right(Decision.Denied(reason)) =>
            logger
              .warn(
                Map(
                  "principal" -> principal.name.value,
                  "path" -> requestPath,
                  "cluster" -> cluster.fold("-")(_.value),
                  "reason" -> reason.describe
                )
              )("a request was refused by this service's own permission check")
              .as(refusal.asLeft[Unit])

          case Left(problem) =>
            logger
              .error(Map("path" -> requestPath))(
                s"$problem; the request was refused because it could not be authorized"
              )
              .as(refusal.asLeft[Unit])
        }
      }
    }
}
