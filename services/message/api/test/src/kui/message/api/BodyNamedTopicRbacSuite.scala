package kui.message.api

import java.nio.charset.StandardCharsets
import java.time.Instant

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.kernel.Ref
import fs2.Stream
import io.circe.Encoder
import io.circe.syntax.*
import org.typelevel.otel4s.metrics.MeterProvider
import sttp.client4.*
import sttp.client4.impl.cats.implicits.*
import sttp.client4.testing.BackendStub
import sttp.tapir.server.stub4.TapirStubInterpreter

import kui.contracts.{HttpHeaders, KuiEndpoint}
import kui.http.principal.{PrincipalVerification, RbacGuard}
import kui.kernel.browse.PollBudget
import kui.kernel.{ClusterId, Offset, PartitionId, RoleName, Secret, TopicName, UserName}
import kui.message.application.produce.{ProduceUseCase, ResendUseCase}
import kui.message.application.purge.PurgeUseCase
import kui.message.application.{TrackEvent, TrackUseCase}
import kui.message.contract.{OffsetRangeDto, ResendRequestDto, TrackMatchDto, TrackQueryDto}
import kui.message.domain.*
import kui.observability.Telemetry
import kui.security.*
import kui.security.rbac.*
import kui.testkit.KuiIOSuite
import kui.testkit.fakes.FakeStructuredLogger

/** The topics a request names in its body, checked by this service against the caller's patterns.
  *
  * A track names the topics it reads in its body and a resend names the topic it writes into there, so the
  * gateway — which does not decode bodies — cannot check either name against a pattern. This service has the
  * decoded body, and is therefore the only place the exact check can happen. Every request below goes
  * straight to this service's routes with a valid principal, which is the call the check exists for.
  *
  * The policy is the one an operator writes to scope a team to its own topics: messages may be read and
  * produced on `public\..*`, and read (only) on `orders`.
  */
final class BodyNamedTopicRbacSuite extends KuiIOSuite {

  private val cluster = ClusterId.unsafe("local")
  private val team = RoleName.unsafe("team")
  private val trackPath = s"/internal/v1/clusters/${cluster.value}/messages/track"
  private val resendPath = s"/internal/v1/clusters/${cluster.value}/topics/orders/messages/resend"

  private val codec = JwsPrincipalCodec
    .make[IO](
      NonEmptyList.of(SigningKey("test", Secret(Array.fill[Byte](32)(7)), Instant.EPOCH)),
      "kui-gateway"
    )
    .toOption
    .get

  private def pattern(raw: String): ResourcePattern =
    ResourcePattern.compile(raw).getOrElse(fail(s"'$raw' does not compile"))

  private val scoped: RbacPolicy =
    RbacPolicy(
      roles = List(
        Role(
          name = team,
          clusters = Set(cluster),
          subjects = Nil,
          permissions = List(
            RbacPolicy.permission(
              Resource.Topic,
              Some(pattern("public\\..*")),
              Set(Action.TopicMessagesRead, Action.TopicMessagesProduce)
            ),
            RbacPolicy.permission(Resource.Topic, Some(pattern("orders")), Set(Action.TopicMessagesRead))
          )
        )
      ),
      defaultRole = None
    )

  /** What the routes handed to the application layer, which for a refused call must be nothing. */
  final case class Reached(tracked: List[List[TopicName]], resent: List[TopicName])

  private def trackBody(topics: String*): TrackQueryDto =
    TrackQueryDto(
      topics = topics.toList.map(TopicName.unsafe),
      `match` = TrackMatchDto(TrackMatchDto.Source.Value, None, TrackMatchDto.Operator.Contains, "4711"),
      from = Instant.parse("2026-11-01T00:00:00Z"),
      to = Instant.parse("2026-11-02T00:00:00Z"),
      limit = None
    )

  private def resendBody(toTopic: String): ResendRequestDto =
    ResendRequestDto(
      TopicName.unsafe(toTopic),
      List(OffsetRangeDto(PartitionId.unsafe(0), Offset.unsafe(0), Offset.unsafe(3)))
    )

  private def call[A: Encoder](
      rbac: RbacPolicy,
      path: String,
      document: A,
      mutation: Boolean
  ): IO[(Response[String], Reached)] =
    for {
      tracked <- Ref.of[IO, List[List[TopicName]]](Nil)
      resent <- Ref.of[IO, List[TopicName]](Nil)
      logger <- FakeStructuredLogger[IO]
      meter <- MeterProvider.noop[IO].get("kui.message")
      rejections <- PrincipalVerification.rejectionCounter[IO](meter)
      interceptors <- MessageApi.interceptors[IO](Telemetry.noop[IO], rejections, logger)
      track = new TrackUseCase[IO] {
        def track(query: TrackQuery, budget: PollBudget): Stream[IO, TrackEvent] =
          Stream.exec(tracked.update(_ :+ query.topics.toList)) ++
            Stream.emit(TrackEvent.Finished(0L, 0L, truncated = false))
      }
      resend = new ResendUseCase[IO] {
        def resend(principal: Principal, request: ResendRequest) =
          resent.update(_ :+ request.destination.topic).as(Right(ResendResult(3L, 3L, Nil)))
      }
      produce = new ProduceUseCase[IO] {
        def produce(principal: Principal, request: ProduceRequest) =
          IO.raiseError(new AssertionError("produce"))
      }
      purge = new PurgeUseCase[IO] {
        def plan(cluster: ClusterId, topic: TopicName) = IO.raiseError(new AssertionError("plan"))
        def apply(principal: Principal, cluster: ClusterId, topic: TopicName, token: String) =
          IO.raiseError(new AssertionError("purge"))
      }
      guard = RbacGuard.fromPolicy[IO](rbac, _ => ClusterFlags.Writable, logger)
      secured = MessageApi.Securing[IO](codec, rejections, logger, guard)
      backend = TapirStubInterpreter(interceptors, BackendStub[IO](summon))
        .whenServerEndpointsRunLogic(
          MessageMutationRoutes[IO](produce, resend, purge, secured) ++ TrackRoutes[IO](track, secured)
        )
        .backend()
      body = document.asJson.noSpaces
      now <- IO.realTimeInstant
      signed <- codec.sign(
        PrincipalClaims(
          subject = UserName.unsafe("alice"),
          roles = Set(team),
          kind = PrincipalKind.Session,
          sessionRef = None,
          issuedAt = now,
          expiresAt = now.plusSeconds(60),
          audience = MessageApi.Id,
          requestDigest = RequestDigests.of("POST", path, body.getBytes(StandardCharsets.UTF_8))
        )
      )
      base = basicRequest
        .post(uri"${s"http://message$path"}")
        .header(KuiEndpoint.PrincipalHeader, signed.value)
        .contentType("application/json")
        .body(body)
        .response(asStringAlways)
      response <- (if mutation then base.header(HttpHeaders.Csrf, "test-csrf") else base).send(backend)
      reachedTrack <- tracked.get
      reachedResend <- resent.get
    } yield (response, Reached(reachedTrack, reachedResend))

  // -- track: every topic in the body must be readable -----------------------------------------------

  test("a track naming a topic the caller may not read is refused before anything is scanned") {
    call(scoped, trackPath, trackBody("payments.cards"), mutation = false).map { (response, reached) =>
      assertEquals(response.code.code, 403, response.body)
      assert(response.body.contains("KUI-FORBIDDEN"), response.body)
      assertEquals(reached.tracked, Nil, "the scan ran for a topic the caller may not read")
    }
  }

  test("one unreadable topic refuses the whole track, not just its own share of the hits") {
    call(scoped, trackPath, trackBody("public.events", "payments.cards"), mutation = false).map {
      (response, reached) =>
        assertEquals(response.code.code, 403, response.body)
        assertEquals(reached.tracked, Nil)
    }
  }

  test("a track over topics the caller may read is scanned") {
    call(scoped, trackPath, trackBody("public.events", "orders"), mutation = false).map {
      (response, reached) =>
        assertEquals(response.code.code, 200, response.body)
        assertEquals(
          reached.tracked,
          List(List(TopicName.unsafe("public.events"), TopicName.unsafe("orders")))
        )
    }
  }

  test("a track is unaffected in a deployment with no roles configured") {
    call(RbacPolicy.Disabled, trackPath, trackBody("payments.cards"), mutation = false).map {
      (response, reached) =>
        assertEquals(response.code.code, 200, response.body)
        assertEquals(reached.tracked, List(List(TopicName.unsafe("payments.cards"))))
    }
  }

  // -- resend: the destination in the body must be producible ----------------------------------------

  test("a resend into a topic the caller may not produce to is refused before anything is copied") {
    call(scoped, resendPath, resendBody("payments.commands"), mutation = true).map { (response, reached) =>
      assertEquals(response.code.code, 403, response.body)
      assert(response.body.contains("KUI-FORBIDDEN"), response.body)
      assertEquals(reached.resent, Nil, "the copy ran into a topic the caller may not produce to")
    }
  }

  test("a resend into a topic the caller may produce to is copied") {
    call(scoped, resendPath, resendBody("public.replay"), mutation = true).map { (response, reached) =>
      assertEquals(response.code.code, 200, response.body)
      assertEquals(reached.resent, List(TopicName.unsafe("public.replay")))
    }
  }

  test("a resend is unaffected in a deployment with no roles configured") {
    call(RbacPolicy.Disabled, resendPath, resendBody("payments.commands"), mutation = true).map {
      (response, reached) =>
        assertEquals(response.code.code, 200, response.body)
        assertEquals(reached.resent, List(TopicName.unsafe("payments.commands")))
    }
  }
}
