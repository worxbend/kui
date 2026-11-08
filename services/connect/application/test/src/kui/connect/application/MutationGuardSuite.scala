package kui.connect.application

import scala.concurrent.duration.DurationInt

import cats.effect.IO
import cats.syntax.all.*

import kui.connect.domain.ConnectorOperation
import kui.kernel.error.{ApplicationError, ErrorCode, InfrastructureError, KuiError}
import kui.security.audit.MutationOutcome
import kui.testkit.KuiIOSuite
import kui.testkit.fakes.FakeStructuredLogger

/** What the audit trail says about an operation, including the ones that did not finish.
  *
  * The classification is `AuditSink`'s and the disagreement it settles is a real one: three services once
  * wrote `Failed` for a cancelled mutation and W6-A1 repaired two of them. This suite is what stops this
  * service becoming the third.
  */
final class MutationGuardSuite extends KuiIOSuite {

  import ConnectRig.*

  private def guard(sink: ConnectorOperationSink[IO]): IO[MutationGuard[IO]] =
    FakeStructuredLogger[IO].map(logger => MutationGuard.make[IO](new Source(Map.empty), sink, logger))

  test("a cancelled restart is audited as Unknown, because the worker may well have accepted it") {
    // The required case. A cancellation lands between the request going out and the 202 coming back, and
    // Connect applies the operation asynchronously: a record saying `Failed` would tell an incident review
    // that a connector was never restarted when it very likely was.
    for {
      sink <- RecordingSink.create
      mutations <- guard(sink)
      fibre <- mutations
        .guard(caller, cluster, payments, elastic, ConnectorOperation.Restart)(
          IO.never[Either[KuiError, Unit]]
        )
        .start
      _ <- IO.sleep(50.millis)
      _ <- fibre.cancel
      written <- sink.written.get
    } yield {
      assertEquals(written.map(_.outcome.label), List("unknown"))
      assertEquals(written.map(_.operation), List(ConnectorOperation.Restart))
      assert(clue(written.head.detail.getOrElse("reason", "")).contains("cancelled"))
    }
  }

  test("a succeeded operation is audited once, with the connector's full name") {
    for {
      sink <- RecordingSink.create
      mutations <- guard(sink)
      _ <- mutations.guard(caller, cluster, payments, elastic, ConnectorOperation.Pause)(
        IO.pure(().asRight[KuiError])
      )
      written <- sink.written.get
    } yield {
      assertEquals(written.map(_.outcome.label), List("succeeded"))
      assertEquals(written.map(_.resource), List("payments/elastic-sink"))
      assertEquals(written.head.detail, Map.empty[String, String])
    }
  }

  test("a call timeout is Unknown and a bulkhead rejection is Refused") {
    // A restart that times out may already have been accepted by the worker. Recording Refused claims it
    // was not. The bulkhead case never sent the request, and an open circuit did not either.
    for {
      sink <- RecordingSink.create
      mutations <- guard(sink)
      _ <- mutations.guard(caller, cluster, payments, elastic, ConnectorOperation.Restart)(
        IO.pure(InfrastructureError.Timeout("connect", 10000).asLeft[Unit])
      )
      _ <- mutations.guard(caller, cluster, payments, elastic, ConnectorOperation.Restart)(
        IO.pure(InfrastructureError.Timeout("connect (bulkhead full)", 0).asLeft[Unit])
      )
      _ <- mutations.guard(caller, cluster, payments, elastic, ConnectorOperation.Restart)(
        IO.pure(InfrastructureError.CircuitOpen("connect", java.time.Instant.EPOCH).asLeft[Unit])
      )
      written <- sink.written.get
    } yield assertEquals(
      written.map(_.outcome),
      List(MutationOutcome.Unknown, MutationOutcome.Refused, MutationOutcome.Refused)
    )
  }

  test("a refusal under 500 is Refused and a failure at or over it is Failed") {
    // The split ADR-047 asks for: a caller who was told no, against a system that broke. `KUI-UNSUPPORTED`
    // is a 501 and is deliberately on the failure side of it.
    for {
      sink <- RecordingSink.create
      mutations <- guard(sink)
      _ <- mutations.guard(caller, cluster, payments, elastic, ConnectorOperation.Pause)(
        IO.pure(ApplicationError.Conflict("no such connector").asLeft[Unit])
      )
      _ <- mutations.guard(caller, cluster, payments, elastic, ConnectorOperation.Pause)(
        IO.pure(ApplicationError.Unsupported("kafka connect").asLeft[Unit])
      )
      written <- sink.written.get
    } yield assertEquals(written.map(_.outcome.label), List("refused", "failed"))
  }

  test("an operation that threw is audited as Failed and the failure still reaches the caller") {
    for {
      sink <- RecordingSink.create
      mutations <- guard(sink)
      attempt <- mutations
        .guard(caller, cluster, payments, elastic, ConnectorOperation.Resume)(
          IO.raiseError[Either[KuiError, Unit]](new RuntimeException("the pool is closed"))
        )
        .attempt
      written <- sink.written.get
    } yield {
      assert(attempt.isLeft)
      assertEquals(written.map(_.outcome.label), List("failed"))
    }
  }

  test("a sink that fails does not fail the operation it is recording") {
    // A sink that could refuse would make the audit trail an availability dependency of restarting a dead
    // connector.
    val broken = new ConnectorOperationSink[IO] {
      def record(entry: ConnectorOperationRecord): IO[Unit] =
        IO.raiseError(new RuntimeException("the log disk is full"))
    }

    guard(broken)
      .flatMap(
        _.guard(caller, cluster, payments, elastic, ConnectorOperation.Pause)(IO.pure(().asRight[KuiError]))
      )
      .assertEquals(Right(()))
  }

  test("a cluster KUI has never heard of is audited as a failure and never reaches the operation") {
    for {
      sink <- RecordingSink.create
      mutations <- guard(sink)
      answer <- mutations.guard(
        caller,
        kui.kernel.ClusterId.unsafe("nowhere"),
        payments,
        elastic,
        ConnectorOperation.Restart
      )(IO.raiseError[Either[KuiError, Unit]](new AssertionError("the operation ran")))
      written <- sink.written.get
    } yield {
      assertEquals(answer.left.map(_.code), Left(ErrorCode.ClusterNotFound))
      assertEquals(written.map(_.outcome.label), List("failed"))
    }
  }
}
