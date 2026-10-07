package kui.message.api

import java.nio.charset.StandardCharsets
import java.time.Instant

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.kernel.Ref
import io.circe.parser.decode
import io.circe.syntax.*
import org.typelevel.otel4s.metrics.MeterProvider
import sttp.client4.*
import sttp.client4.impl.cats.implicits.*
import sttp.client4.testing.BackendStub
import sttp.tapir.server.stub4.TapirStubInterpreter

import kui.contracts.{ErrorEnvelope, HttpHeaders, KuiEndpoint}
import kui.http.principal.{PrincipalVerification, RbacGuard}
import kui.kernel.error.{ApplicationError, ErrorCode, FieldError, KuiError}
import kui.kernel.{ClusterId, Offset, PartitionId, Secret, TopicName, UserName}
import kui.message.application.produce.{ProduceUseCase, ResendUseCase}
import kui.message.application.purge.PurgeUseCase
import kui.message.contract.{OffsetRangeDto, ResendRequestDto, ResendResultDto}
import kui.message.domain.*
import kui.observability.Telemetry
import kui.security.*
import kui.testkit.KuiIOSuite
import kui.testkit.fakes.FakeStructuredLogger

/** Run the real endpoint, body binding and error mapping; only the application port is scripted. */
final class ResendRoutesSuite extends KuiIOSuite {
  private val destination = TopicName.unsafe("destination")
  private val path = "/internal/v1/clusters/local/topics/source/messages/resend"
  private val codec = JwsPrincipalCodec
    .make[IO](
      NonEmptyList.of(SigningKey("test", Secret(Array.fill[Byte](32)(7)), Instant.EPOCH)),
      "kui-gateway"
    )
    .toOption
    .get

  private def run(
      outcomes: List[Either[KuiError, ResendResult]]
  ): IO[(Response[String], List[Int])] =
    for {
      attempted <- Ref.of[IO, List[Int]](Nil)
      logger <- FakeStructuredLogger[IO]
      meter <- MeterProvider.noop[IO].get("kui.message")
      rejections <- PrincipalVerification.rejectionCounter[IO](meter)
      interceptors <- MessageApi.interceptors[IO](Telemetry.noop[IO], rejections, logger)
      resend = new ResendUseCase[IO] {
        def resend(principal: Principal, request: ResendRequest) =
          attempted.update(_ :+ request.source.partition.value).as(outcomes(request.source.partition.value))
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
      secured = MessageApi.Securing[IO](codec, rejections, logger, RbacGuard.allowAll[IO])
      backend = TapirStubInterpreter(interceptors, BackendStub[IO](summon))
        .whenServerEndpointsRunLogic(MessageMutationRoutes[IO](produce, resend, purge, secured))
        .backend()
      request = ResendRequestDto(
        destination,
        outcomes.indices.toList.map(index =>
          OffsetRangeDto(PartitionId.unsafe(index), Offset.unsafe(10), Offset.unsafe(13))
        )
      )
      body = request.asJson.noSpaces
      now <- IO.realTimeInstant
      signed <- codec.sign(
        PrincipalClaims(
          subject = UserName.unsafe("alice"),
          roles = Set.empty,
          kind = PrincipalKind.Session,
          sessionRef = None,
          issuedAt = now,
          expiresAt = now.plusSeconds(60),
          audience = MessageApi.Id,
          requestDigest = RequestDigests.of("POST", path, body.getBytes(StandardCharsets.UTF_8))
        )
      )
      response <- basicRequest
        .post(uri"${s"http://message$path"}")
        .header(KuiEndpoint.PrincipalHeader, signed.value)
        .header(HttpHeaders.Csrf, "test-csrf")
        .contentType("application/json")
        .body(body)
        .response(asStringAlways)
        .send(backend)
      calls <- attempted.get
    } yield (response, calls)

  test("a wholly refused resend preserves the KuiError HTTP status and envelope") {
    val error = ApplicationError.Refused(ErrorCode.ReadOnly, "cluster is read-only")
    run(List(Left(error))).map { (response, calls) =>
      assertEquals(calls, List(0), response.body)
      assertEquals(response.code.code, error.code.httpStatus, response.body)
      val envelope = decode[ErrorEnvelope](response.body).toOption.get
      assertEquals(envelope.code, error.code.wire)
      assertEquals(envelope.message, error.message)
    }
  }

  private val refusals: List[KuiError] = List(
    ApplicationError.Refused(ErrorCode.ReadOnly, "read-only cluster"),
    ApplicationError.NotFound("topic", destination.value, ErrorCode.TopicNotFound),
    ApplicationError.Refused(ErrorCode.InvalidState, "source scan did not finish; nothing sent"),
    ApplicationError.Invalid("resend exceeds limits", List(FieldError.of("ranges", "smaller range")))
  )

  for {
    error <- refusals
    count <- List(1, 3)
  } test(s"$count wholly refused ranges retain ${error.code.wire} and details") {
    val later = ApplicationError.Refused(ErrorCode.InvalidState, "later refusal")
    run(Left(error) :: List.fill(count - 1)(Left(later))).map { (response, calls) =>
      assertEquals(calls, (0 until count).toList)
      assertEquals(response.code.code, error.code.httpStatus, response.body)
      val envelope = decode[ErrorEnvelope](response.body).toOption.get
      assertEquals(envelope.code, error.code.wire)
      assertEquals(envelope.message, error.message)
      assertEquals(
        envelope.details.map(d => (d.field, d.restrictions)),
        error.details.map(d => (d.field, d.restrictions))
      )
    }
  }

  for (completedFirst <- List(true, false))
    test(s"partial receipt keeps committed writes and all failures; completedFirst=$completedFirst") {
      val failure = ApplicationError.Refused(ErrorCode.InvalidState, "record not acknowledged")
      val refusal = ApplicationError.NotFound("topic", destination.value, ErrorCode.TopicNotFound)
      val partial = Right(ResendResult(3L, 2L, List(ResendFailure(Offset.unsafe(11), failure))))
      val outcomes = if completedFirst then List(partial, Left(refusal), Right(ResendResult(3L, 3L, Nil)))
      else List(Left(refusal), partial, Right(ResendResult(3L, 3L, Nil)))
      run(outcomes).map { (response, calls) =>
        assertEquals(response.code.code, 200, response.body)
        assertEquals(calls, List(0, 1, 2))
        val receipt = decode[ResendResultDto](response.body).toOption.get
        assertEquals(receipt.toTopic, destination)
        assertEquals(receipt.read, 6L)
        assertEquals(receipt.written, 5L)
        val completedPartition = PartitionId.unsafe(if completedFirst then 0 else 1)
        val refusedPartition = PartitionId.unsafe(if completedFirst then 1 else 0)
        assertEquals(
          receipt.failures.map(f => (f.partition, f.offset, f.code, f.error)),
          List((completedPartition, Offset.unsafe(11), failure.code.wire, failure.message))
        )
        assertEquals(
          receipt.rangeFailures.map(f => (f.range, f.code, f.error)),
          List(
            (
              OffsetRangeDto(refusedPartition, Offset.unsafe(10), Offset.unsafe(13)),
              refusal.code.wire,
              refusal.message
            )
          )
        )
      }
    }

  for (empty <- List(true, false))
    test(s"zero written is not all-refused when a range returned a receipt; empty=$empty") {
      val error = ApplicationError.Refused(ErrorCode.InvalidState, "not written")
      val done = if empty then ResendResult(0L, 0L, Nil)
      else
        ResendResult(3L, 0L, List(10L, 11L, 12L).map(offset => ResendFailure(Offset.unsafe(offset), error)))
      run(List(Right(done), Left(error))).map { (response, calls) =>
        assertEquals(response.code.code, 200, response.body)
        assertEquals(calls, List(0, 1))
        val receipt = decode[ResendResultDto](response.body).toOption.get
        assertEquals(receipt.read, done.read)
        assertEquals(receipt.written, 0L)
        assertEquals(receipt.failures.map(_.offset), done.failures.map(_.sourceOffset))
        assertEquals(receipt.rangeFailures.map(_.code), List(error.code.wire))
      }
    }
}
