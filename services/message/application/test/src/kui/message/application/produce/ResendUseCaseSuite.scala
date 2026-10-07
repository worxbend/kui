package kui.message.application.produce

import java.time.Instant

import cats.effect.IO
import munit.CatsEffectSuite

import kui.kernel.browse.PollBudget
import kui.kernel.error.ErrorCode
import kui.kernel.{Offset, OffsetRange, PartitionId}
import kui.message.application.{RawHeader, RawRecord}
import kui.message.domain.{Destination, ResendRequest, SourceRange, TimestampType}
import kui.security.audit.{MutationKind, MutationOutcome}

/** Copying records into another topic.
  *
  * The feature's whole claim is **byte for byte**, so that is what most of these assert: the bytes that come
  * out of the source are the objects that go into the producer, headers included, and nothing decodes
  * anything on the way. The last of those is asserted the only way it can be — with a serde source that has a
  * counter on it, so that "never deserializes" is a number rather than a promise in a comment.
  */
final class ResendUseCaseSuite extends CatsEffectSuite {

  import ProduceRig.*

  private val budget: PollBudget =
    PollBudget.unsafe(
      maxRecords = 1000,
      maxBytes = 1024L * 1024L,
      deadline = scala.concurrent.duration.Duration(30, "seconds")
    )

  /** Bytes that are not valid UTF-8. A resend has to carry these unchanged, and it is the case that fails the
    * moment anybody puts a `new String(...)` on this path.
    */
  private val invalidUtf8: Array[Byte] = Array[Byte](0xc3.toByte, 0x28.toByte, 0xa0.toByte)

  private def record(offset: Long, value: Array[Byte] = invalidUtf8): RawRecord =
    RawRecord(
      partition = PartitionId.unsafe(0),
      offset = Offset.unsafe(offset),
      timestamp = Instant.parse("2026-09-04T09:00:00Z"),
      timestampType = TimestampType.CreateTime,
      key = Some(Array[Byte](1, 2, 3)),
      value = Some(value),
      headers = List(RawHeader("trace", Some(Array[Byte](9, 9)))),
      keySize = 3,
      valueSize = value.length,
      headersSize = 7
    )

  private def requestOf(
      from: Long = 10L,
      until: Long = 13L,
      keepHeaders: Boolean = true
  ): ResendRequest =
    ResendRequest
      .of(
        cluster = Cluster,
        source = SourceRange(
          Topic,
          PartitionId.unsafe(0),
          OffsetRange.from(Offset.unsafe(from), Offset.unsafe(until)).getOrElse(fail("bad fixture range"))
        ),
        destination = Destination(ReplayTopic, None),
        keepHeaders = keepHeaders
      )
      .getOrElse(fail("the fixture request is not valid"))

  private def rig(
      readOnly: Boolean = false,
      source: List[RawRecord] = List(record(10L), record(11L), record(12L)),
      limits: ResendLimits = ResendLimits.Default,
      scanBudget: PollBudget = budget
  ): IO[(ResendUseCase[IO], FakeProducers, RecordingAudit, FakeSerdes)] =
    for {
      producers <- FakeProducers.make()
      serdes <- FakeSerdes.make()
      audit <- RecordingAudit.make
      guard <- guardFor(new Profiles(readOnly), audit)
    } yield (
      ResendUseCase.make[IO](producers, new FakeRecords(source), guard, scanBudget, limits),
      producers,
      audit,
      serdes
    )

  // -----------------------------------------------------------------------------------------------

  test("an incomplete source scan writes nothing") {
    for {
      (resend, producers, _, _) <- rig(scanBudget = budget.consume(999, 0))
      answer <- resend.resend(ProduceRig.Caller, requestOf())
      written <- producers.sent.get
    } yield {
      assert(answer.isLeft)
      assertEquals(written, Nil)
    }
  }

  test("byte-budget exhaustion refuses the copy before any write") {
    for {
      (resend, producers, _, _) <- rig(scanBudget = budget.consume(0, budget.bytesLeft - 1L))
      answer <- resend.resend(ProduceRig.Caller, requestOf())
      written <- producers.sent.get
    } yield {
      assert(answer.isLeft)
      assertEquals(written, Nil)
    }
  }

  test("a complete sparse range at the record budget remains copyable") {
    for {
      (resend, producers, _, _) <- rig(
        source = List(record(10L), record(12L)),
        scanBudget = budget.consume(998, 0)
      )
      answer <- resend.resend(ProduceRig.Caller, requestOf())
      written <- producers.sent.get
    } yield {
      assertEquals(answer.map(_.produced), Right(2L))
      assertEquals(written.size, 2)
    }
  }

  test("copiesTheBytesExactly") {
    for {
      (resend, producers, _, _) <- rig()
      answer <- resend.resend(ProduceRig.Caller, requestOf())
      written <- producers.sent.get
    } yield {
      assertEquals(answer.map(_.produced), Right(3L))
      assertEquals(written.length, 3)
      assert(
        written.forall(one => java.util.Arrays.equals(one.value.orNull, invalidUtf8)),
        "the payload changed on the way through; a resend that re-encodes is not a resend"
      )
      assert(
        written.forall(one => java.util.Arrays.equals(one.key.orNull, Array[Byte](1, 2, 3))),
        "the key changed on the way through"
      )
      assertEquals(written.map(_.topic), List.fill(3)(ReplayTopic))
    }
  }

  test("headersTravelWithTheRecord") {
    for {
      (resend, producers, _, _) <- rig()
      _ <- resend.resend(ProduceRig.Caller, requestOf())
      written <- producers.sent.get
    } yield {
      assertEquals(written.head.headers.map(_.key), List("trace"))
      assert(java.util.Arrays.equals(written.head.headers.head.value.orNull, Array[Byte](9, 9)))
    }
  }

  test("headersAreDroppedWhenTheRequestSaysSo") {
    // A real choice an operator makes: a replayed record often carries the retry counters and dead-letter
    // stamps of the machinery that failed it, and feeding those back in is how a record loops forever.
    for {
      (resend, producers, _, _) <- rig()
      _ <- resend.resend(ProduceRig.Caller, requestOf(keepHeaders = false))
      written <- producers.sent.get
    } yield assertEquals(written.head.headers, Nil)
  }

  test("nothingIsEverDeserialized") {
    // The serde source is handed in and never called. That is what makes a topic KUI has no serde for
    // still copyable, and it is what makes masking structurally impossible on this path (ADR-023).
    for {
      (resend, _, _, serdes) <- rig()
      _ <- resend.resend(ProduceRig.Caller, requestOf())
      decodes <- serdes.decodes.get
    } yield assertEquals(decodes, 0)
  }

  test("recordsOutsideTheRequestedWindowAreNotCopied") {
    // The read is bounded by a limit, not by an offset, so retention or a chatty partition can hand back
    // more than was asked for. Copying those would replay records the operator did not name.
    for {
      (resend, producers, _, _) <- rig(source = List(record(9L), record(10L), record(11L), record(99L)))
      answer <- resend.resend(ProduceRig.Caller, requestOf(from = 10L, until = 12L))
      written <- producers.sent.get
    } yield {
      assertEquals(answer.map(_.produced), Right(2L))
      assertEquals(written.length, 2)
    }
  }

  test("readAndWrittenAreReportedSeparately") {
    // They differ whenever retention removed part of the source under the copy. Reporting only the second
    // would make "there was nothing left to copy" and "the copy did nothing" the same sentence.
    for {
      (resend, _, _, _) <- rig(source = Nil)
      answer <- resend.resend(ProduceRig.Caller, requestOf())
    } yield assertEquals(answer, Right(kui.message.domain.ResendResult(0L, 0L, Nil)))
  }

  test("aRangeAboveTheCeilingIsRefusedAndSaysTheNumber") {
    for {
      (resend, producers, _, _) <- rig(limits = ResendLimits(maxRecords = 2L))
      answer <- resend.resend(ProduceRig.Caller, requestOf(from = 0L, until = 500L))
      opened <- producers.opened.get
    } yield {
      assertEquals(answer.left.map(_.code), Left(ErrorCode.Validation))
      assert(answer.swap.exists(_.message.contains("2")), s"the ceiling is not in the message: $answer")
      // And no producer was opened, so a refused resend costs a connection to nothing.
      assertEquals(opened, 0)
    }
  }

  test("readOnlyClusterIsRefusedBeforeAnythingIsReadOrOpened") {
    for {
      (resend, producers, audit, _) <- rig(readOnly = true)
      answer <- resend.resend(ProduceRig.Caller, requestOf())
      opened <- producers.opened.get
      written <- producers.sent.get
      entries <- audit.entries.get
    } yield {
      assertEquals(answer.left.map(_.code), Left(ErrorCode.ReadOnly))
      assertEquals(opened, 0)
      assertEquals(written, Nil)
      assertEquals(entries.map(_.outcome), List(MutationOutcome.Refused))
      assertEquals(entries.map(_.kind), List(MutationKind.Resend))
    }
  }

  test("theAuditRecordNamesTheSourcePartitionAndTheDestination") {
    for {
      (resend, _, audit, _) <- rig()
      _ <- resend.resend(ProduceRig.Caller, requestOf())
      entries <- audit.entries.get
    } yield {
      assertEquals(entries.length, 1)
      assertEquals(entries.head.resource, s"${Topic.value}:0")
      assertEquals(entries.head.detail.get("destination"), Some(ReplayTopic.value))
      assertEquals(entries.head.detail.get("from"), Some("10"))
      assertEquals(entries.head.detail.get("until"), Some("13"))
    }
  }

  test("copyingAPartitionOntoItselfIsRefusedByTheDomain") {
    // It would append every record it reads and then read what it appended. The domain refuses it, so no
    // use case can be written that does it by accident.
    val same = ResendRequest.of(
      cluster = Cluster,
      source = SourceRange(
        Topic,
        PartitionId.unsafe(0),
        OffsetRange.from(Offset.unsafe(0L), Offset.unsafe(5L)).getOrElse(fail("bad fixture range"))
      ),
      destination = Destination(Topic, Some(PartitionId.unsafe(0))),
      keepHeaders = true
    )

    assert(same.isLeft, s"copying a partition onto itself must be refused: $same")
  }
}
