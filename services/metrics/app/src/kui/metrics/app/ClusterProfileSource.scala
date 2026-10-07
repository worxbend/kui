package kui.metrics.app

import cats.effect.kernel.{Async, Resource}
import cats.effect.std.Queue
import cats.syntax.all.*
import fs2.Stream
import org.typelevel.log4cats.StructuredLogger
import sttp.model.Uri

import kui.cluster.client.{ClusterProfiles, ClusterProfilesConfig, HttpClusterProfiles}
import kui.config.{ClusterConfig, HttpTlsConfig, KuiConfig, UrlPolicy}
import kui.http.upstream.HttpTls
import kui.kernel.{ServiceId, UserName}
import kui.observability.Telemetry
import kui.security.PrincipalCodec

/** Deployment-owned adapter of the shared profile client. No independent refresh timer. */
private[app] object ClusterProfileSource {
  def snapshot[F[_]: Async](profiles: ClusterProfiles[F]): F[List[ClusterConfig]] =
    profiles.all.map(_.values.toList.sortBy(_.id.value).map { profile =>
      val connection = profile.connection
      ClusterConfig(
        profile.id,
        profile.name,
        connection.bootstrapServers,
        connection.security,
        connection.overrides,
        profile.readOnly,
        connection.admin
      )
    })

  /** Register before reading current state; enqueue invalidations, not stale snapshots. The finalizer removes
    * the callback before the client and its transport are released.
    */
  def changes[F[_]: Async](profiles: ClusterProfiles[F]): Stream[F, List[ClusterConfig]] =
    Stream.eval(Queue.bounded[F, Unit](1)).flatMap { wake =>
      Stream.resource(Resource.make(profiles.onChange(_ => wake.tryOffer(()).void))(identity)).flatMap { _ =>
        Stream.eval(snapshot(profiles)) ++ Stream.fromQueueUnterminated(wake).evalMap(_ => snapshot(profiles))
      }
    }

  def remote[F[_]: Async](
      config: KuiConfig,
      telemetry: Telemetry[F],
      principals: PrincipalCodec[F],
      logger: StructuredLogger[F],
      policy: UrlPolicy
  ): Resource[F, ClusterProfiles[F]] = {
    val configured = config.topics.clusterProfiles
    val url =
      configured.map(_.url).orElse(config.gateway.services.get(ServiceId.unsafe("cluster")).map(_.url))
    val settings = configured.fold(ClusterProfilesConfig.default)(value =>
      ClusterProfilesConfig(
        value.pollInterval,
        value.requestTimeout,
        value.reconnectBackoff,
        value.maxReconnectBackoff
      )
    )
    for {
      target <- Resource.eval(
        Async[F].fromEither(
          url.toRight(
            new IllegalArgumentException(
              "the standalone metrics service requires kui.clusterProfiles.url (or kui.gateway.services.cluster.url) for live cluster profiles"
            )
          )
        )
      )
      backend <- HttpTls.resource[F](HttpTlsConfig.Default, policy)
      profiles <- HttpClusterProfiles.resource[F](
        Uri.unsafeParse(target.value),
        backend,
        principals,
        UserName.unsafe("kui-metrics"),
        settings,
        telemetry,
        logger
      )
    } yield profiles
  }
}
