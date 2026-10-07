package kui.cluster.api

import java.time.Instant

import cats.effect.kernel.{Async, Clock}
import cats.syntax.all.*
import fs2.Stream
import io.circe.syntax.*
import org.typelevel.log4cats.StructuredLogger
import org.typelevel.otel4s.metrics.Counter
import sttp.capabilities.fs2.Fs2Streams
import sttp.tapir.server.ServerEndpoint

import kui.cluster.application.{ClusterRegistry, RegistrySnapshot}
import kui.cluster.contract.ProfileEndpoints
import kui.cluster.contract.dto.{ClusterChangeDto, ProfileResult}
import kui.http.principal.RbacGuard
import kui.http.sse.{Sse, SseConfig, SseEvent}
import kui.kernel.error.KuiError
import kui.observability.Telemetry
import kui.security.PrincipalCodec

/** How another KUI service reads a cluster's settings, and how it hears that they changed.
  *
  * The two routes are one mechanism. A consumer fetches a profile, keeps it with its `ETag`, subscribes to
  * the stream, and re-fetches whenever it sees a version it does not hold — polling the profile every sixty
  * seconds as a fallback, so a dropped frame costs a poll interval of staleness rather than a client built
  * from settings an operator has revoked.
  */
object ProfileRoutes {

  def apply[F[_]: Async](
      registry: ClusterRegistry[F],
      principals: PrincipalCodec[F],
      rejections: Counter[F, Long],
      telemetry: Telemetry[F],
      logger: StructuredLogger[F],
      guard: RbacGuard[F],
      sse: SseConfig = SseConfig.default
  ): List[ServerEndpoint[Fs2Streams[F], F]] =
    List(
      profileRoute[F](registry, principals, rejections, logger, guard),
      streamRoute[F](registry, principals, rejections, telemetry, logger, guard, sse)
    )

  /** `GET /internal/v1/clusters/{clusterId}/profile`.
    *
    * A 304 is recorded like any other answer — same span, same metric — because a caller that has silently
    * stopped receiving updates should still be visible as traffic rather than disappearing from the graphs
    * the moment it becomes efficient.
    */
  private def profileRoute[F[_]: Async](
      registry: ClusterRegistry[F],
      principals: PrincipalCodec[F],
      rejections: Counter[F, Long],
      logger: StructuredLogger[F],
      guard: RbacGuard[F]
  ): ServerEndpoint[Any, F] =
    ClusterApi.Securing[F](principals, rejections, logger, guard)(ProfileEndpoints.profile) { _ =>
      { case (id, ifNoneMatch) =>
        registry.resolve(id).flatMap {
          case Left(error) => error.asLeft[ProfileResult].pure[F]
          case Right(profile) =>
            val version = profile.version.value

            if ProfileResult.isCurrent(ifNoneMatch, version) then
              ProfileResult.notModified(version).asRight[KuiError].pure[F]
            else
              Clock[F].realTimeInstant
                .map(now => ProfileResult.current(ClusterMapping.profile(profile, now)).asRight[KuiError])
        }
      }
    }

  /** `GET /internal/v1/clusters/stream`. */
  private def streamRoute[F[_]: Async](
      registry: ClusterRegistry[F],
      principals: PrincipalCodec[F],
      rejections: Counter[F, Long],
      telemetry: Telemetry[F],
      logger: StructuredLogger[F],
      guard: RbacGuard[F],
      config: SseConfig
  ): ServerEndpoint[Fs2Streams[F], F] =
    ClusterApi.Securing[F](principals, rejections, logger, guard).stream(ClusterStreamEndpoint.endpoint[F]) {
      principal => _ => _ =>
        Sse
          .encode(
            Sse.stream(
              visibleChanges[F](registry, principal, guard),
              config,
              ClusterStreamEndpoint.EventName,
              telemetry,
              logger
            )
          )
          .asRight[KuiError]
          .pure[F]
    }

  private[api] def visibleChanges[F[_]: Async](
      registry: ClusterRegistry[F],
      principal: kui.security.Principal,
      guard: RbacGuard[F]
  ): Stream[F, SseEvent] =
    changes[F](registry).evalFilter(event =>
      event.data.hcursor.get[String]("id") match {
        case Left(_) => false.pure[F]
        case Right(id) =>
          guard
            .authorize(principal, ProfileEndpoints.profile, s"/internal/v1/clusters/$id/profile")
            .map(_.isRight)
      }
    )

  /** The registry's snapshots, as the differences between them.
    *
    * The registry publishes whole snapshots; a consumer wants to know what moved. Comparing consecutive
    * snapshots here rather than in every consumer means the comparison rule — a version that increased, or a
    * cluster that disappeared — is written once. The first snapshot produces no events: it is the state a
    * consumer already fetched, not a change to it.
    */
  def changes[F[_]: Async](registry: ClusterRegistry[F]): Stream[F, SseEvent] =
    registry.changes.zipWithPrevious
      .flatMap((previous, current) =>
        Stream
          .eval(Clock[F].realTimeInstant)
          .flatMap(now => Stream.emits(previous.toList.flatMap(diff(_, current, now))))
      )

  /** What changed between two registry snapshots, in cluster id order.
    *
    * A removal is its own event rather than an absence for a consumer to infer: a cluster an operator deleted
    * must make every consumer drop its clients, and "I have heard nothing about it" is indistinguishable from
    * a healthy quiet cluster.
    */
  def diff(previous: RegistrySnapshot, current: RegistrySnapshot, at: Instant): List[SseEvent] = {
    val updated = current.profiles.toList.collect {
      case (id, profile) if !previous.profiles.get(id).exists(_.version == profile.version) =>
        ClusterChangeDto(id, profile.version.value, ClusterChangeDto.Updated, at)
    }

    val removed = previous.profiles.toList.collect {
      case (id, profile) if !current.profiles.contains(id) =>
        ClusterChangeDto(id, profile.version.value, ClusterChangeDto.Removed, at)
    }

    (updated ++ removed)
      .sortBy(change => change.id.value)
      .map(change => SseEvent.data(ClusterStreamEndpoint.EventName, change.asJson))
  }
}
