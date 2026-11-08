package kui.schema.application

import cats.effect.IO
import cats.effect.kernel.{Deferred, Ref}

import kui.kernel.Subject
import kui.kernel.error.{ErrorCode, InfrastructureError}
import kui.schema.domain.*
import kui.security.Principal
import kui.security.audit.{AuditSink, MutationKind, MutationOutcome, MutationRecord}
import kui.testkit.KuiIOSuite
import kui.testkit.fakes.FakeStructuredLogger

/** The three-way answer every read in this service gives, and the fact the whole service is shaped around.
  *
  * "There is no such cluster", "this cluster has no registry" and "the registry did not answer" are three
  * different things. Collapsing any pair of them produces a screen that lies:
  *
  *   - unknown as not-configured hides a broken link behind a feature that looks switched off;
  *   - not-configured as unreachable puts a permanently red panel on a deployment where nothing is wrong;
  *   - unreachable as an empty list claims the registry holds nothing, which sends an operator looking for a
  *     schema somebody deleted instead of at a registry that is down.
  */
final class RegistryAbsenceSuite extends KuiIOSuite {

  private val orders = Subject.unsafe("orders-value")

  test("a cluster KUI has never heard of is a cluster-not-found, not a missing registry") {
    for {
      registry <- SchemaRig.registry()
      logger <- SchemaRig.logger
      useCase = SubjectListUseCase.make[IO](SchemaRig.registries(registry), logger)
      result <- useCase.list(SchemaRig.Unknown, SubjectQuery.Default)
    } yield assertEquals(result.left.map(_.code), Left(ErrorCode.ClusterNotFound))
  }

  test("a configured cluster with no registry is unsupported, and the message names the key to set") {
    for {
      registry <- SchemaRig.registry()
      logger <- SchemaRig.logger
      useCase = SubjectListUseCase.make[IO](SchemaRig.registries(registry), logger)
      result <- useCase.list(SchemaRig.WithoutRegistry, SubjectQuery.Default)
    } yield {
      assertEquals(result.left.map(_.code), Left(ErrorCode.Unsupported))
      assert(
        clue(result.swap.toOption.map(_.message)).exists(_.contains("kui.clusters.<n>.schemaRegistry.url")),
        "the refusal has to say what to configure, or it reads as a KUI limitation"
      )
    }
  }

  test("a registry that does not answer fails; it never becomes an empty subject list") {
    for {
      registry <- SchemaRig.registry(failure = Some(SchemaRig.unreachable))
      logger <- SchemaRig.logger
      useCase = SubjectListUseCase.make[IO](SchemaRig.registries(registry), logger)
      result <- useCase.list(SchemaRig.WithRegistry, SubjectQuery.Default)
    } yield assertEquals(result, Left(SchemaRig.unreachable))
  }

  test("a subject the registry does not hold is a schema-not-found") {
    for {
      registry <- SchemaRig.registry(subjects = Map("orders-value" -> List(1, 2)))
      useCase = SubjectVersionsUseCase.make[IO](SchemaRig.registries(registry))
      missing <- useCase.versions(SchemaRig.WithRegistry, Subject.unsafe("nope-value"))
      found <- useCase.versions(SchemaRig.WithRegistry, orders)
    } yield {
      assertEquals(missing.left.map(_.code), Left(ErrorCode.SchemaNotFound))
      assertEquals(found.map(_.map(_.value)), Right(List(1, 2)))
    }
  }
}

/** Where a subject's compatibility level comes from, which is a fact a screen must not flatten. */
final class CompatibilityReadSuite extends KuiIOSuite {

  private val orders = Subject.unsafe("orders-value")

  test("a subject with its own level reports it as its own") {
    for {
      logger <- SchemaRig.logger
      registry <- SchemaRig.registry(subjectLevels = Map("orders-value" -> CompatibilityLevel.Full))
      useCase = CompatibilityReadUseCase.make[IO](SchemaRig.registries(registry), logger)
      result <- useCase.forSubject(SchemaRig.WithRegistry, orders)
    } yield assertEquals(result, Right(SubjectCompatibility.own(CompatibilityLevel.Full)))
  }

  test("a subject with no level of its own reports the global one, marked as inherited") {
    for {
      logger <- SchemaRig.logger
      registry <- SchemaRig.registry(globalLevel = CompatibilityLevel.ForwardTransitive)
      useCase = CompatibilityReadUseCase.make[IO](SchemaRig.registries(registry), logger)
      result <- useCase.forSubject(SchemaRig.WithRegistry, orders)
    } yield {
      // Both halves matter. Without the level the screen has nothing to show; without the flag,
      // "confirming" what is displayed writes an override the operator never intended.
      assertEquals(result.map(_.level), Right(CompatibilityLevel.ForwardTransitive))
      assertEquals(result.map(_.inheritedFromGlobal), Right(true))
    }
  }

  test("an inherited level that cannot be read fails rather than reporting the registry's default") {
    for {
      logger <- SchemaRig.logger
      registry <- SchemaRig.registry(failure = Some(SchemaRig.unreachable))
      useCase = CompatibilityReadUseCase.make[IO](SchemaRig.registries(registry), logger)
      result <- useCase.forSubject(SchemaRig.WithRegistry, orders)
    } yield assertEquals(result, Left(SchemaRig.unreachable))
  }
}

/** A registry that accepts the write and then never answers, so a fibre can be cancelled mid-call.
  *
  * The `before` read succeeds, because the rule under test is about what the record says once the PUT has
  * left KUI: everything before that point is already covered by the refusal cases.
  */
final private class Hanging(inFlight: Deferred[IO, Unit]) extends SchemaRegistryPort[IO] {

  def setGlobalCompatibility(level: CompatibilityLevel): IO[Either[kui.kernel.error.KuiError, Unit]] =
    inFlight.complete(()) *> IO.never

  def globalCompatibility: IO[Either[kui.kernel.error.KuiError, CompatibilityLevel]] =
    IO.pure(Right(CompatibilityLevel.Full))

  def subjects = IO.pure(Right(Nil))
  def summary(subject: Subject) = IO.pure(Right(None))
  def versions(subject: Subject) = IO.pure(Right(None))
  def schema(subject: Subject, version: VersionSelector) = IO.pure(Right(None))
  def subjectCompatibility(subject: Subject) = IO.pure(Right(None))
  def register(subject: Subject, proposed: ProposedSchema) = IO.never
  def setSubjectCompatibility(subject: Subject, level: CompatibilityLevel) = IO.never
  def checkCompatibility(subject: Subject, version: VersionSelector, proposed: ProposedSchema) =
    IO.pure(Right(None))
}

/** The one mutation: refused on a read-only cluster, audited either way. */
final class SetCompatibilitySuite extends KuiIOSuite {

  private val orders = Subject.unsafe("orders-value")
  private val who: Principal = Principal.Anonymous

  private def rig(
      registry: FakeRegistry
  ): IO[(SetCompatibilityUseCase[IO], RecordingAudit)] =
    for {
      logger <- SchemaRig.logger
      records <- Ref.of[IO, List[kui.security.audit.MutationRecord]](Nil)
      audit = new RecordingAudit(records)
    } yield (SetCompatibilityUseCase.make[IO](SchemaRig.registries(registry), audit, logger), audit)

  test("a read-only cluster is refused, and the registry is never contacted") {
    for {
      registry <- SchemaRig.registry()
      pair <- rig(registry)
      (useCase, audit) = pair
      result <- useCase.setGlobal(who, SchemaRig.ReadOnly, CompatibilityLevel.None)
      writes <- registry.writes.get
      records <- audit.records.get
    } yield {
      assertEquals(result.left.map(_.code), Left(ErrorCode.ReadOnly))
      // Not contacting the registry is the assertion, not an optimisation: a refused write that still
      // reached the upstream is a call an operator's proxy logs and then has to explain.
      assertEquals(writes, Nil)
      assertEquals(records.map(_.outcome), List(MutationOutcome.Refused))
    }
  }

  test("a successful write is audited with the level that was in force before it") {
    for {
      registry <- SchemaRig.registry(globalLevel = CompatibilityLevel.Full)
      pair <- rig(registry)
      (useCase, audit) = pair
      result <- useCase.setGlobal(who, SchemaRig.WithRegistry, CompatibilityLevel.None)
      writes <- registry.writes.get
      records <- audit.records.get
    } yield {
      assertEquals(result, Right(CompatibilityLevel.None))
      assertEquals(writes, List("global" -> CompatibilityLevel.None))
      assertEquals(records.map(_.kind), List(MutationKind.SetGlobalCompatibility))
      assertEquals(records.map(_.outcome), List(MutationOutcome.Succeeded))
      // "Somebody set NONE" is not the record an incident review needs; "somebody replaced FULL with
      // NONE" is.
      assertEquals(records.flatMap(_.before), List("FULL"))
      assertEquals(records.flatMap(_.after), List("NONE"))
    }
  }

  test("a subject that was inheriting says so in the audit record, and stops inheriting afterwards") {
    for {
      registry <- SchemaRig.registry(globalLevel = CompatibilityLevel.Backward)
      pair <- rig(registry)
      (useCase, audit) = pair
      result <- useCase.setForSubject(who, SchemaRig.WithRegistry, orders, CompatibilityLevel.Full)
      records <- audit.records.get
    } yield {
      assertEquals(result, Right(SubjectCompatibility.own(CompatibilityLevel.Full)))
      assertEquals(records.flatMap(_.before), List("inherited from the global level"))
    }
  }

  test("a cluster with no registry is refused and the attempt is still recorded") {
    for {
      registry <- SchemaRig.registry()
      pair <- rig(registry)
      (useCase, audit) = pair
      result <- useCase.setGlobal(who, SchemaRig.WithoutRegistry, CompatibilityLevel.Full)
      records <- audit.records.get
    } yield {
      assertEquals(result.left.map(_.code), Left(ErrorCode.Unsupported))
      assertEquals(records.map(_.outcome), List(MutationOutcome.Failed))
    }
  }

  test("a registry call timeout is audited as unknown and a bulkhead rejection as refused") {
    // The PUT may already have been applied when the call budget runs out. A bulkhead rejection is the
    // same timeout type with a zero wait, and that request never left.
    val timedOut = InfrastructureError.Timeout("schema-registry", 10000)
    val bulkhead = InfrastructureError.Timeout("schema-registry (bulkhead full)", 0)
    for {
      slow <- SchemaRig.registry(failure = Some(timedOut))
      timedPair <- rig(slow)
      (timedUseCase, timedAudit) = timedPair
      timed <- timedUseCase.setGlobal(who, SchemaRig.WithRegistry, CompatibilityLevel.None)
      timedRecords <- timedAudit.records.get
      full <- SchemaRig.registry(failure = Some(bulkhead))
      refusedPair <- rig(full)
      (refusedUseCase, refusedAudit) = refusedPair
      refused <- refusedUseCase.setGlobal(who, SchemaRig.WithRegistry, CompatibilityLevel.None)
      refusedRecords <- refusedAudit.records.get
    } yield {
      assertEquals(timed, Left(timedOut))
      assertEquals(timedRecords.map(_.outcome), List(MutationOutcome.Unknown))
      assertEquals(refused, Left(bulkhead))
      assertEquals(refusedRecords.map(_.outcome), List(MutationOutcome.Refused))
    }
  }

  test("a previous level that could not be read is recorded as unknown, never as a blank") {
    // `before.orElse(Some(UnknownBefore))` can be reduced to `before` with every other case in this
    // service green, and the record then carries no `before` at all. The file's own header says why that
    // matters: a blank `before` and an unknown `before` mean different things to an incident review, and
    // only one of them is true when the read failed.
    for {
      // The whole registry refuses, so the `before` read fails and the write fails after it.
      registry <- SchemaRig.registry(failure = Some(SchemaRig.unreachable))
      pair <- rig(registry)
      (useCase, audit) = pair
      result <- useCase.setGlobal(who, SchemaRig.WithRegistry, CompatibilityLevel.None)
      records <- audit.records.get
    } yield {
      assertEquals(result, Left(SchemaRig.unreachable))
      assertEquals(records.map(_.outcome), List(MutationOutcome.Failed))
      assertEquals(records.map(_.before), List(Some(SetCompatibilityUseCase.UnknownBefore)))
      assert(
        clue(SetCompatibilityUseCase.UnknownBefore).contains("unknown"),
        "the marker has to read as an absence of knowledge rather than as a level"
      )
    }
  }

  test("a cancelled write is audited as Unknown, because it may still have been applied") {
    // The outcome that tells an operator to go and look. `MutationOutcome.Unknown` can be changed to
    // `Succeeded` in the `guaranteeCase` block with every other case in this service green — and a record
    // claiming a cancelled PUT succeeded is the one an incident review would trust and should not. The
    // registry hangs *after* accepting the request, which is the only state this rule is about: the PUT
    // is on the wire and nobody knows whether it landed.
    for {
      inFlight <- Deferred[IO, Unit]
      records <- Ref.of[IO, List[MutationRecord]](Nil)
      logger <- SchemaRig.logger
      audit = new RecordingAudit(records)
      registries = new FakeRegistries(SchemaRig.profiles, Map(SchemaRig.WithRegistry -> Hanging(inFlight)))
      useCase = SetCompatibilityUseCase.make[IO](registries, audit, logger)
      fibre <- useCase.setGlobal(who, SchemaRig.WithRegistry, CompatibilityLevel.None).start
      _ <- inFlight.get
      _ <- fibre.cancel
      written <- records.get
    } yield {
      assertEquals(written.map(_.outcome), List(MutationOutcome.Unknown))
      assert(
        clue(written.flatMap(_.detail.get("reason"))).exists(_.contains("cancelled")),
        "the record has to say the operation was cut off after the request was sent"
      )
    }
  }

  test("an audit sink that cannot write never fails the mutation it was recording") {
    // Every other guard in KUI makes the same trade and states it: an operator's registry matters more
    // than KUI's bookkeeping. Delete the `handleErrorWith` around `audit.record` and the write starts
    // failing on a broken sink, with every other case in this service green.
    val broken = new AuditSink[IO] {
      def record(entry: MutationRecord): IO[Unit] =
        IO.raiseError(new RuntimeException("the audit log is full"))
    }

    for {
      registry <- SchemaRig.registry()
      logger <- FakeStructuredLogger[IO]
      useCase = SetCompatibilityUseCase.make[IO](SchemaRig.registries(registry), broken, logger)
      result <- useCase.setGlobal(who, SchemaRig.WithRegistry, CompatibilityLevel.Full)
      writes <- registry.writes.get
      entries <- logger.entriesWith("operation")
    } yield {
      assertEquals(result, Right(CompatibilityLevel.Full))
      assertEquals(writes, List("global" -> CompatibilityLevel.Full))
      // And it is not silent: the record that could not be written is the thing somebody has to know is
      // missing, so the failure is logged at error with the same context the record would have carried.
      assertEquals(entries.map(_.level), List("error"))
    }
  }
}

/** The check: a read that carries a body, answered on a read-only cluster. */
final class CompatibilityCheckSuite extends KuiIOSuite {

  private val orders = Subject.unsafe("orders-value")

  private def proposal(text: String): ProposedSchema = ProposedSchema(SchemaFormat.Avro, text, Nil)

  test("a read-only cluster still gets an answer") {
    for {
      registry <- SchemaRig.registry(subjects = Map("orders-value" -> List(1)))
      useCase = CompatibilityCheckUseCase.make[IO](SchemaRig.registries(registry))
      result <- useCase.check(SchemaRig.ReadOnly, orders, VersionSelector.Latest, proposal("compatible"))
    } yield assertEquals(result.map(_.compatible), Right(true))
  }

  test("an empty schema is refused before the registry is asked") {
    for {
      registry <- SchemaRig.registry(subjects = Map("orders-value" -> List(1)))
      useCase = CompatibilityCheckUseCase.make[IO](SchemaRig.registries(registry))
      result <- useCase.check(SchemaRig.WithRegistry, orders, VersionSelector.Latest, proposal("   "))
    } yield assertEquals(result.left.map(_.code), Left(ErrorCode.Validation))
  }

  test("a document larger than the bound is refused, and the case does not read the bound to build it") {
    // `"x" * (MaxDefinitionBytes + 1)` was the input here until wave 5, which is the constant asserted
    // against itself: multiply `MaxDefinitionBytes` by 1024 and this case still passes, while a 1 GiB
    // document is buffered in this process and posted to a single-writer registry. The literal below is
    // the mebibyte the constant's own scaladoc argues for.
    assertEquals(CompatibilityCheckUseCase.MaxDefinitionBytes, SchemaRegistrationSuite.OneMebibyte)

    val huge = "x" * (SchemaRegistrationSuite.OneMebibyte + 1)

    for {
      registry <- SchemaRig.registry(subjects = Map("orders-value" -> List(1)))
      useCase = CompatibilityCheckUseCase.make[IO](SchemaRig.registries(registry))
      result <- useCase.check(SchemaRig.WithRegistry, orders, VersionSelector.Latest, proposal(huge))
    } yield {
      assertEquals(result.left.map(_.code), Left(ErrorCode.Validation))
      assert(clue(result.swap.toOption.map(_.message)).exists(_.contains("the limit is")))
    }
  }

  test("a document of exactly the bound is checked, so the refusal is not a refusal of everything") {
    val atTheLimit = "x" * SchemaRegistrationSuite.OneMebibyte

    for {
      registry <- SchemaRig.registry(subjects = Map("orders-value" -> List(1)))
      useCase = CompatibilityCheckUseCase.make[IO](SchemaRig.registries(registry))
      result <- useCase.check(SchemaRig.WithRegistry, orders, VersionSelector.Latest, proposal(atTheLimit))
      // The fake answers `compatible` when the text contains that word, so a document of x's is a
      // verdict of `false` — a verdict, which is what proves the call was made rather than refused.
    } yield assertEquals(result.map(_.compatible), Right(false))
  }

  test("an unknown subject is a schema-not-found rather than a fabricated 'compatible'") {
    for {
      registry <- SchemaRig.registry()
      useCase = CompatibilityCheckUseCase.make[IO](SchemaRig.registries(registry))
      result <- useCase.check(SchemaRig.WithRegistry, orders, VersionSelector.Latest, proposal("anything"))
    } yield assertEquals(result.left.map(_.code), Left(ErrorCode.SchemaNotFound))
  }
}
