package kui.gateway.api

import java.time.Instant

import cats.effect.kernel.{Async, Clock}
import cats.syntax.all.*
import fs2.Stream
import io.circe.syntax.*
import org.typelevel.log4cats.StructuredLogger
import sttp.capabilities.fs2.Fs2Streams
import sttp.tapir.*
import sttp.tapir.server.ServerEndpoint

import kui.contracts.ErrorEnvelope
import kui.contracts.capability.{CapabilityChange, CapabilityEntry, CapabilityKey, CapabilitySnapshot}
import kui.contracts.sse.SseEventName
import kui.gateway.api.auth.SessionMiddleware
import kui.gateway.application.capability.{CapabilityRegistry, Trigger}
import kui.gateway.contract.{CapabilityEndpoints, GatewayEndpoints}
import kui.http.sse.{Sse, SseConfig, SseEvent}
import kui.kernel.error.ApplicationError
import kui.kernel.{CorrelationId, ServiceId}
import kui.observability.{Correlation, Telemetry}
import kui.security.Principal
import kui.security.rbac.{AccessRequest, ClusterFlags, Rbac, RbacPolicy}

/** The three routes that let a browser find out what works, and be told when that changes.
  *
  * This is what makes the milestone's headline demo work: the sidebar dims within seconds of a service dying,
  * without the page polling and without the user reloading.
  */
object CapabilityRoutes {

  /** The event name every capability frame carries. One name for both the opening snapshot and each later
    * change, so a client registers one listener rather than two and cannot forget the second.
    */
  val EventName: String = SseEventName.Capabilities

  /** The streaming endpoint. It lives here rather than in the contract module because describing it needs
    * `fs2` and a server-side event-stream body, and the contract has to link for the browser.
    */
  def streamEndpoint[F[_]]: PublicEndpoint[Unit, ErrorEnvelope, Stream[F, Byte], Fs2Streams[F]] =
    GatewayEndpoints.base.get
      .in("capabilities" / "stream")
      .out(Sse.body[F])
      .name("gateway.capabilities.stream")
      .summary("A snapshot, then every capability change as it happens")
      .description(
        "The first event is always a full snapshot, so a client that has just connected -- or " +
          "reconnected -- never has to guess what it missed. There is no cursor and no terminal event: " +
          "the stream ends when the client disconnects. A heartbeat every fifteen seconds keeps " +
          "proxies from closing an idle connection."
      )
      .tag("capabilities")

  /** Every capability endpoint, in the order they are documented. Used by GW-007's OpenAPI merge, which must
    * see the stream too even though the contract module cannot describe it.
    */
  def endpoints[F[_]]: List[AnyEndpoint] =
    List(CapabilityEndpoints.snapshot, streamEndpoint[F], CapabilityEndpoints.probe)

  def apply[F[_]: Async](
      registry: CapabilityRegistry[F],
      trigger: Trigger[F],
      telemetry: Telemetry[F],
      logger: StructuredLogger[F],
      sse: SseConfig = SseConfig.default,
      rbac: RbacPolicy = RbacPolicy.Disabled
  ): List[ServerEndpoint[Fs2Streams[F], F]] =
    List(
      snapshotRoute[F](registry, rbac),
      streamRoute[F](registry, telemetry, logger, sse, rbac),
      probeRoute[F](registry, trigger)
    )

  /** Everything known right now, including services that have not been checked yet. */
  def snapshotOf[F[_]: Async](registry: CapabilityRegistry[F]): F[CapabilitySnapshot] =
    (registry.entries, Clock[F].realTimeInstant).mapN(CapabilitySnapshot.apply)

  private def snapshotRoute[F[_]: Async](
      registry: CapabilityRegistry[F],
      rbac: RbacPolicy
  ): ServerEndpoint[Any, F] =
    CapabilityEndpoints.snapshot
      .in(caller)
      .serverLogicSuccess[F](principal =>
        snapshotOf(registry).map(snapshot =>
          snapshot.copy(entries = snapshot.entries.filter(entry => visible(rbac, principal)(entry.key)))
        )
      )

  private val caller = extractFromRequest[Principal](request =>
    request.attribute(SessionMiddleware.Attribute).map(_.principal).getOrElse(Principal.Anonymous)
  )

  private[api] def visible(policy: RbacPolicy, principal: Principal)(key: CapabilityKey): Boolean =
    key.cluster.forall(id =>
      Rbac.decide(policy, principal, ClusterFlags.Writable, AccessRequest(id, "capabilities.read")).isAllowed
    )

  /** The snapshot as an event, followed by one event per change.
    *
    * The subscription is opened *before* the snapshot is read, and that order is the whole correctness
    * argument: a change that happens between the two is delivered as a delta rather than lost, so replaying
    * the deltas onto the snapshot always reproduces the registry. The other order leaves a window in which a
    * browser's sidebar silently drifts from reality and nothing ever tells it.
    */
  def snapshotThenChanges[F[_]: Async](
      registry: CapabilityRegistry[F],
      allowed: CapabilityKey => Boolean = _ => true
  ): Stream[F, SseEvent] =
    Stream.resource(registry.subscribe).flatMap { deltas =>
      Stream
        .eval(snapshotOf(registry))
        .map(snapshot =>
          snapshotEvent(snapshot.copy(entries = snapshot.entries.filter(entry => allowed(entry.key))))
        ) ++
        deltas.filter(change => allowed(change.entry.key)).map(changeEvent)
    }

  private def streamRoute[F[_]: Async](
      registry: CapabilityRegistry[F],
      telemetry: Telemetry[F],
      logger: StructuredLogger[F],
      config: SseConfig,
      rbac: RbacPolicy
  ): ServerEndpoint[Fs2Streams[F], F] =
    streamEndpoint[F].in(caller).serverLogicSuccess[F] { principal =>
      // The correlation id is minted per connection rather than read from the request, because the
      // failure this guards against happens long after the request headers were handled and the id has
      // to be the one that identifies *this* stream in the logs.
      Correlation.newRandom[F].map { correlationId =>
        Sse.encode(
          Sse.stream(
            // Without this, a failure raised after the response headers are already on the wire reaches
            // the browser as a truncated body: `EventSource` reconnects, re-runs the same failing
            // subscription and loops, with nothing on screen or in the client's hands to say why. ADR-035
            // requires exactly one terminal `done` or `error` event, and this is the `error` half.
            Sse.withErrorEvent(snapshotThenChanges(registry, visible(rbac, principal)), correlationId),
            config,
            EventName,
            telemetry,
            logger
          )
        )
      }
    }

  private def probeRoute[F[_]: Async](
      registry: CapabilityRegistry[F],
      trigger: Trigger[F]
  ): ServerEndpoint[Any, F] =
    CapabilityEndpoints.probe.serverLogic[F](raw => probeOne(registry, trigger, raw))

  /** Re-checks one service and answers with its recomputed state.
    *
    * The probe waits for the check rather than accepting it and returning: a "Retry now" button that returned
    * before it knew the answer would show the user the state they were already looking at, and they would
    * press it again.
    */
  def probeOne[F[_]: Async](
      registry: CapabilityRegistry[F],
      trigger: Trigger[F],
      raw: String
  ): F[Either[ErrorEnvelope, CapabilityEntry]] =
    for {
      correlationId <- Correlation.newRandom[F]
      now <- Clock[F].realTimeInstant
      known <- registry.snapshot.map(_.keySet.map(_.service))
      result <- ServiceId.from(raw).toOption.filter(known.contains) match {
        case None => Async[F].pure(Left(unknownService(raw, correlationId, now)))
        case Some(service) =>
          val key = CapabilityKey(service, None)
          trigger.probe(service) *>
            registry.entries.map(entries =>
              Right(
                entries
                  .find(_.key == key)
                  .getOrElse(
                    CapabilityEntry(key, kui.contracts.capability.CapabilityState.NotConfigured, now)
                  )
              )
            )
      }
    } yield result

  /** An unknown service id is a bad request, not a missing page.
    *
    * The task sketch asked for a 404. `ErrorEnvelope.statusOf` is the single code-to-status table in the
    * whole system and maps `KUI-VALIDATION` to 400; adding a second mapping so that one route could answer
    * differently is exactly what that rule exists to prevent. The code the client reads is the one the sketch
    * named, and it is the code the UI branches on.
    */
  private def unknownService(raw: String, correlationId: CorrelationId, at: Instant): ErrorEnvelope =
    ErrorEnvelope.of(
      ApplicationError.Invalid(
        s"'$raw' is not a service this deployment is configured with",
        List(kui.kernel.error.FieldError.of("service", "one of the configured service ids"))
      ),
      correlationId,
      at
    )

  /** One capability frame. Both the opening snapshot and each change use the same event name. */
  def snapshotEvent(snapshot: CapabilitySnapshot): SseEvent =
    SseEvent(EventName, snapshot.asJson)

  def changeEvent(change: CapabilityChange): SseEvent =
    SseEvent(EventName, change.asJson)
}
