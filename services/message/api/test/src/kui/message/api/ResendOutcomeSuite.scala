package kui.message.api

import io.circe.syntax.*
import munit.FunSuite

import kui.kernel.error.{ApplicationError, ErrorCode}
import kui.kernel.{ClusterId, Offset, OffsetRange, PartitionId, TopicName}
import kui.message.contract.ResendResultDto
import kui.message.domain.{Destination, ResendFailure, ResendRequest, ResendResult, SourceRange}

final class ResendOutcomeSuite extends FunSuite {
  private val destination = TopicName.unsafe("destination")
  private val partition = PartitionId.unsafe(3)
  private val from = Offset.unsafe(10)
  private val until = Offset.unsafe(13)
  private val request = ResendRequest
    .of(
      ClusterId.unsafe("local"),
      SourceRange(TopicName.unsafe("source"), partition, OffsetRange.from(from, until).toOption.get),
      Destination(destination, None),
      keepHeaders = true
    )
    .toOption
    .get
  private val failure = ApplicationError.Refused(ErrorCode.InvalidState, "write refused")

  test("partial and total resend failures retain source partition offset and error on the wire") {
    List(0L, 2L).foreach { written =>
      val result = MessageMutationRoutes.resendOutcome(
        ResendResultDto(destination, 0L, 0L),
        request,
        Right(ResendResult(3L, written, List(ResendFailure(from, failure))))
      )
      val decoded = result.asJson.as[ResendResultDto].toOption.get
      assertEquals(decoded.written, written)
      assertEquals(
        decoded.failures.map(f => (f.partition, f.offset, f.code, f.error)),
        List((partition, from, failure.code.wire, failure.message))
      )
    }
  }

  test("a later range error retains prior writes and per-record failures") {
    val partial = MessageMutationRoutes.resendOutcome(
      ResendResultDto(destination, 0L, 0L),
      request,
      Right(ResendResult(3L, 2L, List(ResendFailure(from, failure))))
    )
    val result = MessageMutationRoutes.resendOutcome(partial, request, Left(failure))
    assertEquals(result.written, 2L)
    assertEquals(result.failures.size, 1)
    assertEquals(
      result.rangeFailures.map(f => (f.range.partition, f.range.from, f.range.until, f.error)),
      List((partition, from, until, failure.message))
    )
  }
}
