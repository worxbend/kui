package kui.allinone

import cats.effect.kernel.{Async, Resource}
import org.typelevel.log4cats.StructuredLogger
import sttp.capabilities.fs2.Fs2Streams
import sttp.client4.impl.cats.implicits.*
import sttp.client4.testing.StreamBackendStub
import sttp.model.Uri
import sttp.tapir.server.stub4.TapirStreamStubInterpreter

import kui.cluster.api.ClusterApi
import kui.cluster.app.ClusterServer
import kui.cluster.client.{ClusterProfiles, ClusterProfilesConfig, HttpClusterProfiles}
import kui.kernel.UserName
import kui.observability.Telemetry
import kui.security.PrincipalCodec

/** The established profile protocol over the cluster service's real routes, with no loopback socket. It
  * shares the transport strategy of InProcessServiceClient and the profile client's event/reconnect lifecycle
  * with standalone services, rather than inventing another registry or refresh loop.
  */
private[allinone] object InProcessClusterProfiles {
  def resource[F[_]: Async](
      cluster: ClusterServer[F],
      telemetry: Telemetry[F],
      principals: PrincipalCodec[F],
      logger: StructuredLogger[F]
  ): Resource[F, ClusterProfiles[F]] = {
    val backend = TapirStreamStubInterpreter[F, Fs2Streams[F]](
      cluster.interceptors,
      StreamBackendStub[F, Fs2Streams[F]](summon)
    ).whenServerEndpointsRunLogic(cluster.routes).backend()
    HttpClusterProfiles.resource[F](
      Uri.unsafeParse(InProcessServiceClient.baseUrlFor(ClusterApi.Id)),
      backend,
      principals,
      UserName.unsafe("kui-collectors"),
      ClusterProfilesConfig.default,
      telemetry,
      logger
    )
  }
}
