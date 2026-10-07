package kui.cluster.contract

import sttp.model.StatusCode
import sttp.tapir.*
import sttp.tapir.json.circe.jsonBody

import kui.cluster.contract.dto.{ClusterProfileDto, ProfileResult}
import kui.contracts.KernelSchemas.given
import kui.contracts.rbac.EndpointAuthorization
import kui.contracts.{ErrorEnvelope, KuiEndpoint}
import kui.kernel.{ClusterId, ServiceId}
import kui.security.SignedPrincipal

/** How another KUI service learns a cluster's connection settings, and how it is told they changed.
  *
  * This is the server half of ADR-036's distribution sentence: resolved profiles at
  * `GET /internal/v1/clusters/{id}/profile` with an `ETag`, change notifications on an SSE stream, and
  * consumers that keep the last profile they saw, poll as a fallback, and rebuild their clients when the
  * version moves. It is built now, in M1, while there is exactly one producer and no consumer — the cheapest
  * possible moment to get a distribution mechanism wrong and fix it.
  *
  * The stream endpoint is not here but in the `api` module (`ClusterStreamEndpoint`), for the same reason
  * `CapabilityRoutes.streamEndpoint` is: describing an event stream needs `fs2`, which has no business in a
  * module that must link for the browser.
  */
object ProfileEndpoints {

  /** The audience a caller signs its principal for (ADR-020).
    *
    * It lives in the contract rather than in the service's `application` layer because both sides need it and
    * only one of them may see that layer: `services/cluster/client` signs tokens for this audience and rule
    * A11 keeps it out of `services.cluster.application`, where the same string is `ClusterService.Id`.
    * `ClusterApiSuite` asserts the two are equal, which is the only place in the build that can see both.
    */
  val Audience: ServiceId = ServiceId.unsafe("cluster")

  val ProfileSegment: String = "profile"
  val StreamSegment: String = "stream"
  val EventName: String = "clusters"

  /** The conditional-request header a consumer sends back the ETag in. */
  val IfNoneMatchHeader: String = "If-None-Match"

  val ETagHeader: String = "ETag"

  /** The wildcard `If-None-Match` value. A client sending it is asking for the profile whatever it holds. */
  val AnyEtag: String = "*"

  /** `GET /internal/v1/clusters/{clusterId}/profile`.
    *
    * Two outcomes, modelled as a `oneOf` rather than an optional body: 200 with the profile and its `ETag`,
    * or 304 with the `ETag` and nothing else. A generated client and the generated document therefore both
    * know that 304 is a normal answer rather than an error, which is the difference between a consumer that
    * polls cheaply and one that logs a failure every minute.
    */
  val profile: Endpoint[SignedPrincipal, (ClusterId, Option[String]), ErrorEnvelope, ProfileResult, Any] =
    KuiEndpoint.internal.get
      .in(
        "internal" / "v1" / ClusterEndpoints.ClustersSegment /
          path[ClusterId](ClusterEndpoints.ClusterIdParam)
            .description("The configured cluster's slug id") / ProfileSegment
      )
      .in(
        header[Option[String]](IfNoneMatchHeader)
          .description("The ETag the caller already holds; '*' always fetches")
      )
      .out(
        oneOf[ProfileResult](
          oneOfVariant(
            statusCode(StatusCode.Ok)
              .and(header[String](ETagHeader))
              .and(jsonBody[ClusterProfileDto])
              .mapTo[ProfileResult.Current]
          ),
          oneOfVariant(
            statusCode(StatusCode.NotModified)
              .and(header[String](ETagHeader))
              .mapTo[ProfileResult.NotModified]
          )
        )
      )
      .name("cluster.profile")
      .attribute(EndpointAuthorization.Key, EndpointAuthorization.clusterScoped("cluster.profile"))
      .summary("A cluster's resolved connection settings, for another KUI service")
      .description(
        "The ETag is the profile's store version. A caller keeps the last profile it saw and re-fetches " +
          "with If-None-Match; an unchanged profile answers 304 with no body. This document carries the " +
          "cluster's credentials, because a consuming service builds a Kafka client from it (ADR-046). " +
          "It is served on /internal/v1 only, to a caller with a signed principal; /internal/v1 must not " +
          "be exposed outside the deployment network."
      )
      .tag("cluster")

  /** Every endpoint declared in this file. */
  val all: List[AnyEndpoint] = List(profile)
}
