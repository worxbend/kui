package kui.ksql.application

import scala.concurrent.duration.DurationInt

import cats.effect.IO
import munit.CatsEffectSuite

import kui.kernel.error.{ApplicationError, ErrorCode, InfrastructureError, KuiError}
import kui.ksql.domain.KsqlStatement
import kui.security.audit.MutationOutcome

/** The only way a statement runs in this service (ADR-047), driven directly.
  *
  * Directly, and not through a route, because two of the rules below are about states a route cannot produce:
  * a cancelled operation, and a read-only refusal in a deployment whose composition root wired
  * `RbacGuard.allowAll` and therefore has no first refusal at all. `KsqlRoutesSuite` drives the wired pair;
  * this drives the guard on its own, which is what makes both of its refusals reachable rather than
  * decorative.
  */
final class MutationGuardSuite extends CatsEffectSuite {

  import KsqlRig.*

  private val statement: KsqlStatement =
    KsqlStatement.parse("DROP STREAM ORDERS DELETE TOPIC;").getOrElse(fail("the fixture did not parse"))

  private def guardWith(sink: KsqlStatementSink[IO]): IO[MutationGuard[IO]] =
    kui.testkit.fakes
      .FakeStructuredLogger[IO]
      .map(logger => MutationGuard.make[IO](new Source(Map.empty), sink, logger))

  private def recording: IO[(MutationGuard[IO], RecordingSink)] =
    for {
      entries <- cats.effect.Ref.of[IO, List[KsqlStatementRecord]](Nil)
      sink = new RecordingSink(entries)
      guard <- guardWith(sink)
    } yield (guard, sink)

  test("a read-only cluster is refused without the operation being run at all") {
    recording.flatMap { (guard, sink) =>
      for {
        ran <- cats.effect.Ref.of[IO, Int](0)
        answer <- guard.guard(alice, readOnly, statement, "ksql.statement")(
          ran.update(_ + 1).as(Right(()): Either[KuiError, Unit])
        )
        count <- ran.get
        entries <- sink.entries.get
      } yield {
        assertEquals(answer.left.toOption.map(_.code), Some(ErrorCode.ReadOnly))
        // The body is a by-name parameter and must not be evaluated: an attempt to change a read-only
        // cluster has to be refused, not attempted and then reported.
        assertEquals(count, 0)
        assertEquals(entries.map(_.outcome), List(MutationOutcome.Refused))
      }
    }
  }

  test("a cancelled statement is audited as Unknown and never as Failed") {
    // Five services implement this classification and they used to disagree about this one case. A ksqlDB
    // statement is written to the command topic before the HTTP response is composed, so a cancellation
    // that lands in between has very often already created the stream — and a record saying `Failed` would
    // tell an incident review that a topic was never dropped when it may well have been. The topic and
    // message services wrote `Failed` here and W6-A1 repaired both; this is not a third.
    recording.flatMap { (guard, sink) =>
      for {
        fiber <- guard
          .guard(alice, cluster, statement, "ksql.statement")(
            IO.sleep(1.minute).as(Right(()): Either[KuiError, Unit])
          )
          .start
        _ <- IO.sleep(50.millis)
        _ <- fiber.cancel
        entries <- sink.entries.get
      } yield {
        assertEquals(entries.map(_.outcome), List(MutationOutcome.Unknown))
        assertNotEquals(entries.map(_.outcome), List(MutationOutcome.Failed))
        assert(clue(entries.flatMap(_.detail.values)).exists(_.contains("cancelled")))
      }
    }
  }

  test("a call timeout is Unknown and a bulkhead rejection is Refused") {
    // The timeout fires after the statement was sent. ksqlDB writes the command before it answers, so a
    // record saying Refused would tell an incident review the statement never ran. A bulkhead rejection is
    // the same error with a zero wait, and the call never left the process.
    val timedOut: Either[KuiError, Unit] = Left(InfrastructureError.Timeout("ksqldb", 10000))
    val bulkhead: Either[KuiError, Unit] = Left(InfrastructureError.Timeout("ksqldb (bulkhead full)", 0))
    val open: Either[KuiError, Unit] =
      Left(InfrastructureError.CircuitOpen("ksqldb", java.time.Instant.EPOCH))

    recording.flatMap { (guard, sink) =>
      for {
        _ <- guard.guard(alice, cluster, statement, "ksql.statement")(IO.pure(timedOut))
        _ <- guard.guard(alice, cluster, statement, "ksql.statement")(IO.pure(bulkhead))
        _ <- guard.guard(alice, cluster, statement, "ksql.statement")(IO.pure(open))
        entries <- sink.entries.get
      } yield assertEquals(
        entries.map(_.outcome),
        List(MutationOutcome.Unknown, MutationOutcome.Refused, MutationOutcome.Refused)
      )
    }
  }

  test("a refusal below 500 is Refused and a failure at or above it is Failed") {
    // The split an audit reader depends on: "ksqlDB said no" and "KUI could not reach ksqlDB" are
    // different incidents, and a trail that filed both as failures cannot tell them apart.
    val refusal: Either[KuiError, Unit] = Left(ApplicationError.Invalid("line 1:8: mismatched input", Nil))
    val outage: Either[KuiError, Unit] = Left(InfrastructureError.Unreachable("ksqldb", "refused"))

    recording.flatMap { (guard, sink) =>
      for {
        _ <- guard.guard(alice, cluster, statement, "ksql.statement")(IO.pure(refusal))
        _ <- guard.guard(alice, cluster, statement, "ksql.statement")(IO.pure(outage))
        entries <- sink.entries.get
      } yield assertEquals(entries.map(_.outcome), List(MutationOutcome.Refused, MutationOutcome.Failed))
    }
  }

  test("a thrown failure is audited as Failed and the throw is not swallowed") {
    recording.flatMap { (guard, sink) =>
      for {
        thrown <- guard
          .guard(alice, cluster, statement, "ksql.statement")(
            IO.raiseError[Either[KuiError, Unit]](new RuntimeException("boom"))
          )
          .attempt
        entries <- sink.entries.get
      } yield {
        assert(thrown.isLeft, clue = thrown)
        assertEquals(entries.map(_.outcome), List(MutationOutcome.Failed))
      }
    }
  }

  test("a success is audited once, with the statement and its shape") {
    recording.flatMap { (guard, sink) =>
      for {
        answer <- guard.guard(alice, cluster, statement, "ksql.statement")(
          IO.pure(Right(()): Either[KuiError, Unit])
        )
        entries <- sink.entries.get
      } yield {
        assertEquals(answer, Right(()))
        assertEquals(entries.size, 1)
        assertEquals(entries.map(_.statement), List("DROP STREAM ORDERS DELETE TOPIC;"))
        assert(entries.forall(_.destructive))
        assertEquals(entries.map(_.resource), List(s"ksql/${cluster.value}"))
      }
    }
  }

  test("an audit sink that fails does not fail the statement it is describing") {
    // `LoggingAuditSink`'s rule: a sink that could refuse would make the audit trail an availability
    // dependency of running a statement, and an operator who cannot create a stream because a log disk is
    // full is worse off than one whose bookkeeping is a minute late.
    guardWith(failingSink).flatMap(guard =>
      guard
        .guard(alice, cluster, statement, "ksql.statement")(IO.pure(Right(()): Either[KuiError, Unit]))
        .map(answer => assertEquals(answer, Right(())))
    )
  }

  test("a cluster KUI has never heard of is audited as Failed and refused") {
    recording.flatMap { (guard, sink) =>
      for {
        answer <- guard.guard(alice, kui.kernel.ClusterId.unsafe("nope"), statement, "ksql.statement")(
          IO.pure(Right(()): Either[KuiError, Unit])
        )
        entries <- sink.entries.get
      } yield {
        assertEquals(answer.left.toOption.map(_.code), Some(ErrorCode.ClusterNotFound))
        assertEquals(entries.map(_.outcome), List(MutationOutcome.Failed))
      }
    }
  }
}
