package kui.alerts.api

import java.time.Instant

import scala.concurrent.duration.{DurationInt, FiniteDuration}

import cats.data.NonEmptyList
import cats.effect.kernel.Resource
import cats.effect.{IO, Ref}
import cats.syntax.all.*
import fs2.Stream
import io.circe.Json
import io.circe.parser.parse
import org.typelevel.otel4s.metrics.MeterProvider
import sttp.client4.*
import sttp.client4.impl.cats.implicits.*
import sttp.client4.testing.BackendStub
import sttp.model.Uri
import sttp.tapir.server.stub4.TapirStubInterpreter

import kui.alerts.application.*
import kui.alerts.domain.*
import kui.contracts.KuiEndpoint
import kui.http.health.HealthEndpoints
import kui.http.principal.{PrincipalVerification, RbacGuard}
import kui.kernel.error.{ApplicationError, ErrorCode, KuiError}
import kui.kernel.{ClusterId, RoleName, Secret, UserName}
import kui.observability.Telemetry
import kui.security.*
import kui.security.rbac.{ClusterFlags, RbacPolicy}
import kui.testkit.fakes.FakeStructuredLogger

/** This service's routes over Tapir's stub interpreter: the real interceptor chain, the real principal
  * verification and the real permission guard, with no socket.
  *
  * A bound port would add seconds per case and prove only that Netty works, which `libs/http` proves once for
  * every service. What cannot be proved anywhere else is the *order* — that a caller without
  * `ALERTS:ACKNOWLEDGE` is refused before the store is asked to write — and the order only exists once the
  * guard, the route and the use case are assembled the way the composition root assembles them.
  */
object AlertsTestServer {

  val cluster: ClusterId = ClusterId.unsafe("prod-eu")
  val readOnly: ClusterId = ClusterId.unsafe("prod-us")
  val unknown: ClusterId = ClusterId.unsafe("nowhere")

  val at: Instant = Instant.parse("2020-01-01T00:00:00Z")

  val operator: RoleName = RoleName.unsafe("operator")

  /** A store that records every acknowledgement it was **asked** to make, refused or not, and the `markRead`
    * argument of every read it was asked for.
    *
    * That counter is the whole point of this fixture: a stub that answered a constant could not tell a caller
    * who was refused from one who was never allowed to reach it, and "refused before the store is written" is
    * a statement about which of those happened. `reads` is there for the same kind of claim about the read:
    * `markRead` is decoded, validated and documented, and the only way to see that the route passed it on is
    * to ask the thing on the other side of the route what it was given.
    */
  final class CountingStore(
      state: Ref[IO, List[AlertEvent]],
      val attempts: Ref[IO, List[AlertEventId]],
      val reads: Ref[IO, List[Option[Instant]]],
      reports: List[RuleReport],
      evaluatedAt: Option[Instant]
  ) extends AlertStore[IO] {

    def feed(id: ClusterId, principal: Principal, limit: Int, markRead: Option[Instant]): IO[AlertFeed] =
      reads.update(_ :+ markRead) >> state.get.map { events =>
        val ordered = events.sorted

        AlertFeed(
          events = ordered.take(limit),
          total = ordered.size,
          openCount = ordered.count(_.isOpen),
          openByRule = ordered.filter(_.isOpen).groupBy(_.key.rule).view.mapValues(_.size).toMap,
          unreadCount = ordered.size,
          lastReadAt = None,
          evaluatedAt = evaluatedAt,
          reports = reports
        )
      }

    def acknowledge(
        id: ClusterId,
        event: AlertEventId,
        when: Instant,
        by: String
    ): IO[Either[KuiError, AcknowledgedAlert]] =
      attempts.update(_ :+ event) >> state.modify { events =>
        events.indexWhere(_.id == event) match {
          case -1 =>
            (
              events,
              ApplicationError.Conflict(s"no open alert event '${event.value}'").asLeft[AcknowledgedAlert]
            )
          case index if !events(index).isOpen =>
            (
              events,
              ApplicationError.Conflict(s"'${event.value}' is already closed").asLeft[AcknowledgedAlert]
            )
          case index =>
            val closed = events(index).resolvedBy(when, AlertResolutionKind.Acknowledged, Some(by))
            val next = events.updated(index, closed)
            (next, AcknowledgedAlert(closed, next.count(_.isOpen)).asRight[KuiError])
        }
      }

    def record(id: ClusterId, evaluation: Evaluation, when: Instant): IO[Unit] = IO.unit

    def ruleState(id: ClusterId): IO[AlertRuleState] = IO.pure(AlertRuleState.empty)

    def changes: Stream[IO, ClusterId] = Stream.empty
  }

  final class Profiles extends ClusterProfileSource[IO] {

    private val views = List(
      ClusterProfileView(cluster, "Production EU", readOnly = false),
      ClusterProfileView(readOnly, "Production US", readOnly = true)
    )

    def profileOf(id: ClusterId): IO[Either[KuiError, ClusterProfileView]] =
      IO.pure(
        views
          .find(_.cluster == id)
          .toRight(ApplicationError.NotFound("cluster", id.value, ErrorCode.ClusterNotFound): KuiError)
      )

    def all: IO[List[ClusterProfileView]] = IO.pure(views)
  }

  /** Thirty-two bytes, which is the shortest key HS256 accepts. */
  private val key: SigningKey =
    SigningKey("test-1", Secret(Array.fill[Byte](32)(7)), Instant.parse("2020-01-01T00:00:00Z"))

  val codec: PrincipalCodec[IO] =
    JwsPrincipalCodec
      .make[IO](NonEmptyList.of(key), "kui-gateway")
      .getOrElse(throw new IllegalStateException("the test signing key is too short for HS256"))

  final case class Rig(backend: Backend[IO], store: CountingStore)

  /** The service, with the deployment's policy and the events it starts holding. */
  def resource(
      rbac: RbacPolicy = RbacPolicy.Disabled,
      events: List[AlertEvent] = Nil,
      reports: List[RuleReport] = AlertRule.All.map(RuleReport(_, RuleOutcome.evaluated)),
      evaluatedAt: Option[Instant] = Some(at)
  ): Resource[IO, Rig] =
    Resource.eval(
      for {
        logger <- FakeStructuredLogger[IO]
        meter <- MeterProvider.noop[IO].get("kui.alerts")
        rejections <- PrincipalVerification.rejectionCounter[IO](meter)
        interceptors <- AlertsApi.interceptors[IO](Telemetry.noop[IO], rejections, logger)
        held <- Ref.of[IO, List[AlertEvent]](events)
        attempts <- Ref.of[IO, List[AlertEventId]](Nil)
        reads <- Ref.of[IO, List[Option[Instant]]](Nil)
        store = new CountingStore(held, attempts, reads, reports, evaluatedAt)
        profiles = new Profiles
        guard = MutationGuard.make[IO](profiles, AcknowledgementSink.noop[IO], logger)
        useCases = AlertUseCases.make[IO](profiles, store, guard)
        permissions = RbacGuard.fromPolicy[IO](
          rbac,
          id => ClusterFlags(id == readOnly),
          logger
        )
        // The JSON half of `AlertsApi.routes`, assembled the way that method assembles it. The change
        // stream is left out because Tapir's stub backend carries no capability, and a stub typed on
        // `Fs2Streams` would need a bound socket — which is what `CapabilityRoutesSuite` spends a real
        // server on one service over. That the composition *does* serve the stream is asserted directly
        // in `AlertsCapabilitiesSuite` rather than left to this omission.
        secured = AlertsApi.Securing[IO](codec, rejections, logger, permissions)
        routes = HealthEndpoints.make[IO](
          Nil,
          AlertsApi.capabilityDocument[IO](AlertsCapabilities.make[IO](profiles), logger)
        ) ++ AlertsRoutes[IO](useCases, secured)
      } yield Rig(
        TapirStubInterpreter(interceptors, BackendStub[IO](summon))
          .whenServerEndpointsRunLogic(routes)
          .backend(),
        store
      )
    )

  /** Exactly what the composition root serves, including the change stream.
    *
    * `resource` above drives only the JSON half, because Tapir's stub backend carries no streaming
    * capability. This builds the whole list so that a case can assert the stream is in it — the shape of
    * orphan this project has shipped once per wave for three waves.
    */
  def compositionRoutes
      : IO[List[sttp.tapir.server.ServerEndpoint[sttp.capabilities.fs2.Fs2Streams[IO], IO]]] =
    for {
      logger <- FakeStructuredLogger[IO]
      meter <- MeterProvider.noop[IO].get("kui.alerts")
      rejections <- PrincipalVerification.rejectionCounter[IO](meter)
      held <- Ref.of[IO, List[AlertEvent]](Nil)
      attempts <- Ref.of[IO, List[AlertEventId]](Nil)
      reads <- Ref.of[IO, List[Option[Instant]]](Nil)
      store = new CountingStore(held, attempts, reads, Nil, None)
      profiles = new Profiles
      guard = MutationGuard.make[IO](profiles, AcknowledgementSink.noop[IO], logger)
    } yield AlertsApi.routes[IO](
      AlertUseCases.make[IO](profiles, store, guard),
      store,
      Nil,
      AlertsCapabilities.make[IO](profiles),
      codec,
      rejections,
      Telemetry.noop[IO],
      logger,
      RbacGuard.allowAll[IO]
    )

  def event(rule: AlertRule, subject: String, openedAt: Instant = at): AlertEvent =
    AlertEvent.open(
      AlertKey(rule, subject),
      AlertSeverity.Warning,
      openedAt,
      s"${rule.wire} on $subject",
      "detail"
    )

  def feedPath(id: ClusterId): String = s"/internal/v1/clusters/${id.value}/alerts/events"

  def acknowledgePath(id: ClusterId, event: String): String =
    s"/internal/v1/clusters/${id.value}/alerts/events/$event/acknowledgement"

  /** `http://alerts<path>`, parsed rather than interpolated: sttp's `uri` interpolator escapes an embedded
    * string as one segment, which would turn every path here into a single literal and every request into a
    * 404 that had nothing to do with the case.
    */
  def address(path: String): Uri = Uri.unsafeParse(s"http://alerts$path")

  /** A token for one request line. The digest covers the method and the *path*; a query string is outside it
    * (ADR-020), which is why `?limit=` can vary between cases while the token does not.
    */
  def token(
      method: String,
      path: String,
      roles: Set[RoleName] = Set.empty,
      validFor: FiniteDuration = 60.seconds
  ): IO[SignedPrincipal] =
    IO.realTimeInstant.flatMap(now =>
      codec.sign(
        PrincipalClaims(
          subject = UserName.unsafe("alice"),
          roles = roles,
          kind = PrincipalKind.Session,
          sessionRef = None,
          issuedAt = now,
          expiresAt = now.plusSeconds(validFor.toSeconds),
          audience = AlertsApi.Id,
          requestDigest = RequestDigest.ofRequestLine(method, path.takeWhile(_ != '?'))
        )
      )
    )

  def get(rig: Rig, path: String, roles: Set[RoleName] = Set.empty): IO[Response[String]] =
    token("GET", path, roles).flatMap(signed =>
      basicRequest
        .get(address(path))
        .header(KuiEndpoint.PrincipalHeader, signed.value)
        .response(asStringAlways)
        .send(rig.backend)
    )

  def post(rig: Rig, path: String, roles: Set[RoleName] = Set.empty): IO[Response[String]] =
    token("POST", path, roles).flatMap(signed =>
      basicRequest
        .post(address(path))
        .header(KuiEndpoint.PrincipalHeader, signed.value)
        .header(kui.contracts.HttpHeaders.Csrf, "test-csrf")
        .response(asStringAlways)
        .send(rig.backend)
    )

  def body(response: Response[String]): Json =
    parse(response.body)
      .fold(failure => sys.error(s"not JSON: ${failure.message} in ${response.body}"), identity)
}
