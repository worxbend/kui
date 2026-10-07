package kui.ksql.infrastructure

import scala.concurrent.duration.*

import cats.effect.{IO, Ref}
import sttp.capabilities.fs2.Fs2Streams
import sttp.client4.impl.cats.implicits.*
import sttp.client4.testing.{BackendStub, ResponseStub, StreamBackendStub, StubBody}
import sttp.model.StatusCode

import kui.config.SafeUrl
import kui.kernel.error.KuiError
import kui.kernel.{ClusterId, Secret}
import kui.ksql.application.*
import kui.security.Principal
import kui.security.audit.MutationOutcome
import kui.testkit.KuiIOSuite
import kui.testkit.fakes.FakeStructuredLogger

final class KsqlExecutionBoundarySuite extends KuiIOSuite {
  private val cluster = ClusterId.unsafe("prod")

  List(
    ("ERROR", None, MutationOutcome.Failed),
    ("QUEUED", Some("pending"), MutationOutcome.Unknown),
    ("PARSING", Some("pending"), MutationOutcome.Unknown),
    ("EXECUTING", Some("pending"), MutationOutcome.Unknown),
    ("SUCCESS", Some("status"), MutationOutcome.Succeeded),
    ("RUNNING", Some("pending"), MutationOutcome.Unknown),
    ("TERMINATED", Some("status"), MutationOutcome.Succeeded)
  ).foreach { case (status, expected, auditOutcome) =>
    test(s"HTTP-200 $status propagates through the service and its audit boundary") {
      val body =
        s"""[{"@type":"currentStatus","commandId":"stream/X/create","commandStatus":{"status":"$status","message":"server result"}}]"""
      execute(body, "CREATE STREAM X (ID STRING);").map { (result, written) =>
        assertEquals(result.toOption.map(_.outcome.wire), expected)
        result.toOption.foreach { executed =>
          executed.outcome match {
            case kui.ksql.domain.StatementOutcome.Status(message, id) =>
              assertEquals(message, "server result")
              assertEquals(id, Some("stream/X/create"))
            case kui.ksql.domain.StatementOutcome.Pending(message, id) =>
              assertEquals(message, "server result")
              assertEquals(id, Some("stream/X/create"))
            case other => fail(s"unexpected outcome: $other")
          }
        }
        assertEquals(written.map(_.outcome), List(auditOutcome))
      }
    }
  }

  List(
    """{"commandId":"stream/X/create"}""",
    """{"commandId":"stream/X/create","commandStatus":{}}""",
    """{"commandId":null}""",
    """{"commandSequenceNumber":10}""",
    """{"@type":"streams","streams":[],"commandId":"stream/X/create"}"""
  ).foreach { entity =>
    test(s"command-shaped response without status is not an audited success: $entity") {
      execute(s"[$entity]", "CREATE STREAM X (ID STRING);").map { (result, written) =>
        assert(result.isLeft, result)
        assertEquals(written.map(_.outcome), List(MutationOutcome.Failed))
      }
    }
  }

  List("""{"@type":"streams","streams":[]}""", """{"streams":[]}""").foreach { listing =>
    test(s"a legitimate SHOW listing remains completed through the audit boundary: $listing") {
      execute(s"[$listing]", "SHOW STREAMS;").map { (result, written) =>
        assertEquals(result.toOption.map(_.outcome.wire), Some("status"))
        assertEquals(written.map(_.outcome), List(MutationOutcome.Succeeded))
      }
    }
  }

  private def execute(
      body: String,
      sql: String
  ): IO[(Either[KuiError, ExecutedStatement], List[KsqlStatementRecord])] = {
    val backend = BackendStub[IO](summon[sttp.monad.MonadError[IO]]).whenAnyRequest
      .thenRespondF(_ => IO.pure(ResponseStub.adjust(body, StatusCode.Ok): sttp.client4.Response[StubBody]))
    val streaming = StreamBackendStub[IO, Fs2Streams[IO]](summon[sttp.monad.MonadError[IO]])
    val httpClient = new KsqlHttp[IO](
      backend,
      streaming,
      SafeUrl.unsafe("http://ksql:8088"),
      30.seconds,
      KsqlCredentials.anonymous[IO]
    )
    val profiles = new ClusterKsqlSource[IO] {
      private val profile = KsqlProfileView(cluster, "Prod", readOnly = false, configured = true)
      def profileOf(id: ClusterId) = IO.pure(Right(profile))
      def all = IO.pure(List(profile))
      def client(id: ClusterId) = IO.pure(Some(httpClient))
    }
    for {
      records <- Ref.of[IO, List[KsqlStatementRecord]](Nil)
      logger <- FakeStructuredLogger[IO]
      sink = new KsqlStatementSink[IO] {
        def record(entry: KsqlStatementRecord) = records.update(_ :+ entry)
      }
      guard = MutationGuard.make[IO](profiles, sink, logger)
      useCases = KsqlUseCases
        .make[IO](profiles, KsqlPlanToken.make[IO](Secret(Array.fill[Byte](32)(1))), guard)
      result <- useCases.execute(Principal.Anonymous, cluster, sql, None)
      written <- records.get
    } yield (result, written)
  }
}
