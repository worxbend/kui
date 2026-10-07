package kui.cluster.api

import fs2.Stream
import sttp.capabilities.fs2.Fs2Streams
import sttp.tapir.{stringToPath, AnyEndpoint, Endpoint}

import kui.cluster.contract.{ClusterEndpoints, ProfileEndpoints}
import kui.contracts.rbac.EndpointAuthorization
import kui.contracts.{ErrorEnvelope, KuiEndpoint}
import kui.http.sse.Sse
import kui.security.SignedPrincipal

/** The change notifications half of ADR-036's profile distribution.
  *
  * A consumer subscribes here, keeps the last profile it fetched, and re-fetches when it sees a version it
  * does not hold. Polling `{id}/profile` every sixty seconds is the fallback, so a missed frame costs one
  * poll interval of staleness rather than a wrong client — which is why this stream has no `Last-Event-ID`
  * resume and no per-subscriber backlog: a reconnecting consumer runs exactly the code it runs at startup.
  *
  * It lives in the `api` module rather than in the cross-compiled contract for the same reason
  * `CapabilityRoutes.streamEndpoint` does: describing an event-stream body needs `fs2`, and the contract
  * module has to link for the browser.
  */
object ClusterStreamEndpoint {

  /** The SSE event name a consumer registers a listener for. */
  val EventName: String = ProfileEndpoints.EventName

  val StreamPath: String =
    s"/internal/v1/${ClusterEndpoints.ClustersSegment}/${ProfileEndpoints.StreamSegment}"

  def endpoint[F[_]]: Endpoint[SignedPrincipal, Unit, ErrorEnvelope, Stream[F, Byte], Fs2Streams[F]] =
    KuiEndpoint.internal.get
      .in("internal" / "v1" / ClusterEndpoints.ClustersSegment / ProfileEndpoints.StreamSegment)
      .out(Sse.body[F])
      .name("cluster.stream")
      // Like the cluster list, the subscription filters each item by the caller's scope in ProfileRoutes.
      .attribute(EndpointAuthorization.Key, EndpointAuthorization.clusterScoped("cluster.stream"))
      .summary("One event whenever a cluster's profile changes or a cluster is removed")
      .description(
        "Named events: 'clusters' carries {id, version, change, at}; 'heartbeat' keeps proxies from " +
          "closing an idle connection; 'error' carries the standard envelope. A frame carries no " +
          "profile - a consumer that sees an unfamiliar version fetches it, which keeps credentials off " +
          "every subscriber's socket and makes a dropped frame cost a fetch rather than a stale client."
      )
      .tag("cluster")

  /** Every endpoint of this file, so CLAPI-010's OpenAPI merge documents the stream even though the
    * cross-compiled contract cannot describe it.
    */
  def endpoints[F[_]]: List[AnyEndpoint] = List(endpoint[F])
}
