package kui.schema.infrastructure

import cats.effect.{IO, Ref}
import sttp.client4.impl.cats.implicits.*
import sttp.client4.testing.{BackendStub, ResponseStub, StubBody}
import sttp.model.StatusCode

import kui.config.SafeUrl
import kui.kernel.{ClusterId, Subject}
import kui.schema.application.*
import kui.schema.domain.CompatibilityLevel
import kui.security.Principal
import kui.security.audit.{AuditSink, MutationOutcome, MutationRecord}
import kui.testkit.KuiIOSuite
import kui.testkit.fakes.FakeStructuredLogger

final class CompatibilityWriteBoundarySuite extends KuiIOSuite {
  test("registry PUT 404 fails the service mutation and is audited as failed") {
    val cluster = ClusterId.unsafe("prod")
    val backend = BackendStub[IO](summon[sttp.monad.MonadError[IO]]).whenAnyRequest
      .thenRespondF(_ =>
        IO.pure(ResponseStub.adjust("", StatusCode.NotFound): sttp.client4.Response[StubBody])
      )
    val http =
      new RegistryHttp[IO](backend, SafeUrl.unsafe("http://registry:8081"), RegistryCredentials.anonymous[IO])
    val registries = new ClusterRegistries[IO] {
      private val view = RegistryProfile(cluster, "Prod", hasRegistry = true, readOnly = false)
      def all = IO.pure(List(view))
      def profile(id: ClusterId) = IO.pure(Some(view))
      def registry(id: ClusterId) = IO.pure(Some(http))
    }
    for {
      records <- Ref.of[IO, List[MutationRecord]](Nil)
      logger <- FakeStructuredLogger[IO]
      audit = new AuditSink[IO] {
        def record(entry: MutationRecord) = records.update(_ :+ entry)
      }
      service = SetCompatibilityUseCase.make[IO](registries, audit, logger)
      global <- service.setGlobal(Principal.Anonymous, cluster, CompatibilityLevel.Full)
      subject <- service.setForSubject(
        Principal.Anonymous,
        cluster,
        Subject.unsafe("orders"),
        CompatibilityLevel.Full
      )
      written <- records.get
    } yield {
      assert(global.isLeft, clue = global)
      assert(subject.isLeft, clue = subject)
      assertEquals(written.map(_.outcome), List(MutationOutcome.Failed, MutationOutcome.Failed))
    }
  }
}
