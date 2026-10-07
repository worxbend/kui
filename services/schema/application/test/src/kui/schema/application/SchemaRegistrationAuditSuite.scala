package kui.schema.application

import cats.effect.{Deferred, IO, Ref}

import kui.kernel.Subject
import kui.kernel.error.KuiError
import kui.schema.domain.*
import kui.security.Principal
import kui.security.audit.{MutationOutcome, MutationRecord}
import kui.testkit.KuiIOSuite

final class SchemaRegistrationAuditSuite extends KuiIOSuite {
  private val subject = Subject.unsafe("orders-value")
  private val proposed = ProposedSchema(SchemaFormat.Avro, "\"string\"", Nil)

  private def rig(work: Option[IO[Either[KuiError, RegisteredVersion]]] = None) =
    for {
      base <- SchemaRig.registry()
      records <- Ref.of[IO, List[MutationRecord]](Nil)
      logger <- SchemaRig.logger
      port = new SchemaRegistryPort[IO] {
        export base.{
          subjects,
          summary,
          versions,
          schema,
          globalCompatibility,
          subjectCompatibility,
          setGlobalCompatibility,
          setSubjectCompatibility,
          checkCompatibility
        }
        def register(subject: Subject, proposed: ProposedSchema) =
          work.getOrElse(base.register(subject, proposed))
      }
      registries = new FakeRegistries(SchemaRig.profiles, Map(SchemaRig.WithRegistry -> port))
      useCase = RegisterSchemaUseCase.make[IO](registries, new RecordingAudit(records), logger)
    } yield (useCase, records)

  test("successful registration records structured identity and result, never schema text") {
    rig().flatMap { case (useCase, records) =>
      for {
        result <- useCase.register(Principal.Anonymous, SchemaRig.WithRegistry, subject, proposed)
        written <- records.get
      } yield {
        assert(result.isRight)
        assertEquals(written.map(_.outcome), List(MutationOutcome.Succeeded))
        assertEquals(written.map(_.kind.operation), List(RegisterSchemaUseCase.Operation))
        assertEquals(written.map(_.resource), List(subject.value))
        assertEquals(written.map(_.principal), List(Principal.Anonymous))
        assert(written.head.after.exists(_.contains(SchemaRig.RegisteredId.toString)))
        assert(!written.head.toString.contains(proposed.definition))
      }
    }
  }

  test("read-only and validation refusals each record once") {
    rig().flatMap { case (useCase, records) =>
      for {
        _ <- useCase.register(Principal.Anonymous, SchemaRig.ReadOnly, subject, proposed)
        _ <- useCase.register(
          Principal.Anonymous,
          SchemaRig.WithRegistry,
          subject,
          proposed.copy(definition = "")
        )
        written <- records.get
      } yield assertEquals(written.map(_.outcome), List(MutationOutcome.Refused, MutationOutcome.Refused))
    }
  }

  test("a typed upstream rejection is audited as failed") {
    rig(Some(IO.pure(Left(SchemaRig.unreachable)))).flatMap { case (useCase, records) =>
      for {
        result <- useCase.register(Principal.Anonymous, SchemaRig.WithRegistry, subject, proposed)
        written <- records.get
      } yield {
        assertEquals(result, Left(SchemaRig.unreachable))
        assertEquals(written.map(_.outcome), List(MutationOutcome.Failed))
      }
    }
  }

  test("a raised registration failure is audited once and preserved") {
    rig(Some(IO.raiseError(new RuntimeException("registry failed")))).flatMap { case (useCase, records) =>
      for {
        result <- useCase.register(Principal.Anonymous, SchemaRig.WithRegistry, subject, proposed).attempt
        written <- records.get
      } yield {
        assert(result.isLeft)
        assertEquals(written.map(_.outcome), List(MutationOutcome.Failed))
      }
    }
  }

  test("cancellation after dispatch is unknown, not success or failure") {
    for {
      entered <- Deferred[IO, Unit]
      setup <- rig(Some(entered.complete(()).flatMap(_ => IO.never)))
      (useCase, records) = setup
      fiber <- useCase.register(Principal.Anonymous, SchemaRig.WithRegistry, subject, proposed).start
      _ <- entered.get
      _ <- fiber.cancel
      written <- records.get
    } yield assertEquals(written.map(_.outcome), List(MutationOutcome.Unknown))
  }
}
