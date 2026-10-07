package kui.schema.application

import cats.effect.IO

import kui.kernel.error.ErrorCode
import kui.kernel.{RoleName, Subject, UserName}
import kui.schema.domain.*
import kui.security.{Principal, PrincipalKind}
import kui.testkit.KuiIOSuite
import kui.testkit.fakes.FakeStructuredLogger

/** Registering a schema: the four answers the use case gives before the registry sees anything, and the one
  * it gives afterwards.
  *
  * The read-only case is the one with teeth, and it asserts the *absence of a request* rather than the code
  * of the refusal. A refusal that still contacted the registry is a write in somebody's proxy log that KUI
  * then has to explain, and no status code can show whether it happened — only the count of what the registry
  * was asked.
  */
final class SchemaRegistrationSuite extends KuiIOSuite {

  import SchemaRegistrationSuite.OneMebibyte

  private val orders = Subject.unsafe("orders-value")
  private val who: Principal = Principal.Anonymous

  private val avro = ProposedSchema(
    format = SchemaFormat.Avro,
    definition = """{"type":"record","name":"Order","fields":[]}""",
    references = Nil
  )

  private def useCase(registry: FakeRegistry): IO[RegisterSchemaUseCase[IO]] =
    SchemaRig.logger.map(logger =>
      RegisterSchemaUseCase
        .make[IO](SchemaRig.registries(registry), kui.security.audit.AuditSink.noop[IO], logger)
    )

  test("a valid schema registers and the answer carries the version") {
    for {
      registry <- SchemaRig.registry(subjects = Map("orders-value" -> List(1, 2)))
      run <- useCase(registry)
      result <- run.register(who, SchemaRig.WithRegistry, orders, avro)
      registrations <- registry.registrations.get
    } yield {
      assertEquals(result.map(_.id.value), Right(SchemaRig.RegisteredId))
      assertEquals(result.map(_.version.map(_.value)), Right(Some(3)))
      assertEquals(registrations.map(_._1), List("orders-value"))
      // The document reaches the registry unchanged. A use case that reformatted it would send the
      // registry something the operator never wrote, and the registry stores text verbatim.
      assertEquals(registrations.map(_._2.definition), List(avro.definition))
    }
  }

  test("a read-only cluster refuses with KUI-READ-ONLY and the registry is never contacted") {
    for {
      registry <- SchemaRig.registry()
      run <- useCase(registry)
      result <- run.register(who, SchemaRig.ReadOnly, orders, avro)
      registrations <- registry.registrations.get
    } yield {
      assertEquals(result.left.map(_.code), Left(ErrorCode.ReadOnly))
      assertEquals(registrations, Nil, "a refused registration still reached the registry")
    }
  }

  test("a registry rejection travels with the registry's own sentence rather than being summarised") {
    for {
      registry <- SchemaRig.registry(rejects = Set("orders-value"))
      run <- useCase(registry)
      result <- run.register(who, SchemaRig.WithRegistry, orders, avro)
    } yield {
      assertEquals(result.left.map(_.code), Left(ErrorCode.Validation))
      assertEquals(
        result.swap.toOption.toList.flatMap(_.details).flatMap(_.restrictions),
        List("Schema being registered is incompatible with an earlier schema for subject 'orders-value'")
      )
    }
  }

  test("an unknown cluster is a cluster-not-found and a cluster with no registry is unsupported") {
    for {
      registry <- SchemaRig.registry()
      run <- useCase(registry)
      unknown <- run.register(who, SchemaRig.Unknown, orders, avro)
      bare <- run.register(who, SchemaRig.WithoutRegistry, orders, avro)
      registrations <- registry.registrations.get
    } yield {
      assertEquals(unknown.left.map(_.code), Left(ErrorCode.ClusterNotFound))
      assertEquals(bare.left.map(_.code), Left(ErrorCode.Unsupported))
      assertEquals(registrations, Nil)
    }
  }

  test("an empty schema is refused here rather than forwarded, because the registry answers 500 to it") {
    for {
      registry <- SchemaRig.registry()
      run <- useCase(registry)
      result <- run.register(who, SchemaRig.WithRegistry, orders, avro.copy(definition = "   "))
      registrations <- registry.registrations.get
    } yield {
      assertEquals(result.left.map(_.code), Left(ErrorCode.Validation))
      assert(clue(result.swap.toOption.map(_.message)).exists(_.contains("paste the schema text")))
      assertEquals(registrations, Nil)
    }
  }

  test("the operational log names who, which cluster and which subject alongside the audit record") {
    val alice = Principal(UserName.unsafe("alice"), Set(RoleName.unsafe("registrar")), PrincipalKind.Session)

    for {
      registry <- SchemaRig.registry()
      logger <- FakeStructuredLogger[IO]
      run = RegisterSchemaUseCase
        .make[IO](SchemaRig.registries(registry), kui.security.audit.AuditSink.noop[IO], logger)
      _ <- run.register(alice, SchemaRig.WithRegistry, orders, avro)
      entries <- logger.entriesWith("operation")
    } yield {
      assertEquals(entries.map(_.level), List("info"))
      assertEquals(entries.flatMap(_.context.get("principal")), List("alice"))
      assertEquals(entries.flatMap(_.context.get("cluster.id")), List(SchemaRig.WithRegistry.value))
      assertEquals(entries.flatMap(_.context.get("resource")), List(orders.value))
      assertEquals(entries.flatMap(_.context.get("operation")), List(RegisterSchemaUseCase.Operation))
    }
  }

  test("a refused registration is logged too, because what somebody tried is the interesting half") {
    val alice = Principal(UserName.unsafe("alice"), Set.empty, PrincipalKind.Session)

    for {
      registry <- SchemaRig.registry(rejects = Set("orders-value"))
      logger <- FakeStructuredLogger[IO]
      run = RegisterSchemaUseCase
        .make[IO](SchemaRig.registries(registry), kui.security.audit.AuditSink.noop[IO], logger)
      _ <- run.register(alice, SchemaRig.WithRegistry, orders, avro)
      readOnly <- run.register(alice, SchemaRig.ReadOnly, orders, avro)
      entries <- logger.entriesWith("operation")
    } yield {
      assertEquals(readOnly.left.map(_.code), Left(ErrorCode.ReadOnly))
      assertEquals(entries.map(_.level), List("warn", "info"))
      assertEquals(entries.flatMap(_.context.get("principal")), List("alice", "alice"))
    }
  }

  test("a schema document larger than the bound is refused by a case that does not compute its input") {
    // The input is a literal, and that is the whole point of this case. Until wave 5 it read
    // `"x" * (RegisterSchemaUseCase.MaxDefinitionBytes + 1)`, which is the bound measured against itself:
    // `MaxDefinitionBytes` could be multiplied by 1024 and every case in this service stayed green, and a
    // 1 GiB schema document would then be buffered here and forwarded to a single-writer registry JVM.
    // One mebibyte is written out below so that raising the constant reddens this case instead.
    assertEquals(
      RegisterSchemaUseCase.MaxDefinitionBytes,
      OneMebibyte,
      "the bound this endpoint forwards operator text under is a mebibyte, written out and not derived"
    )
    // One bound for the two endpoints that forward operator text to a registry. A document KUI will check
    // and then refuse to register is a difference an operator finds by hitting it.
    assertEquals(RegisterSchemaUseCase.MaxDefinitionBytes, CompatibilityCheckUseCase.MaxDefinitionBytes)

    for {
      registry <- SchemaRig.registry()
      run <- useCase(registry)
      oversized = avro.copy(definition = "x" * (OneMebibyte + 1))
      result <- run.register(who, SchemaRig.WithRegistry, orders, oversized)
      registrations <- registry.registrations.get
    } yield {
      assertEquals(result.left.map(_.code), Left(ErrorCode.Validation))
      assert(clue(result.swap.toOption.map(_.message)).exists(_.contains("the limit is")))
      // Refused here, so nothing that size is ever buffered into a request to the registry.
      assertEquals(registrations, Nil, "an oversized document still reached the registry")
    }
  }

  test("a schema document of exactly the bound is registered, so the refusal refuses something") {
    // The other half of the bound. A case that only asserts a refusal is satisfied by a use case that
    // refuses every document, which is house rule 6 in one sentence.
    for {
      registry <- SchemaRig.registry()
      run <- useCase(registry)
      atTheLimit = avro.copy(definition = "x" * OneMebibyte)
      result <- run.register(who, SchemaRig.WithRegistry, orders, atTheLimit)
      registrations <- registry.registrations.get
    } yield {
      assertEquals(result.map(_.id.value), Right(SchemaRig.RegisteredId))
      assertEquals(registrations.map(_._2.definition.length), List(OneMebibyte))
    }
  }

  test("an oversized document at a cluster KUI has never heard of is a 404 and not a complaint about it") {
    // The order the file's header states, which the code did not follow until wave 5: validation ran
    // first, so the answer to a link pointing at nothing was "the schema is 1048577 characters". That is
    // the sentence an operator would have acted on, and the cluster is the thing that is actually wrong.
    for {
      registry <- SchemaRig.registry()
      run <- useCase(registry)
      oversized = avro.copy(definition = "x" * (OneMebibyte + 1))
      unknown <- run.register(who, SchemaRig.Unknown, orders, oversized)
      empty <- run.register(who, SchemaRig.Unknown, orders, avro.copy(definition = "   "))
    } yield {
      assertEquals(unknown.left.map(_.code), Left(ErrorCode.ClusterNotFound))
      assertEquals(empty.left.map(_.code), Left(ErrorCode.ClusterNotFound))
    }
  }

  test("an oversized document at a read-only cluster is refused as read-only, not as a bad document") {
    // Same ordering rule one step further in. ADR-047 §2's refusal is about the deployment's link to the
    // cluster and it is the answer that tells an operator why the button will never work here; "your
    // schema is too big" invites them to paste a smaller one and try again.
    for {
      registry <- SchemaRig.registry()
      run <- useCase(registry)
      oversized = avro.copy(definition = "x" * (OneMebibyte + 1))
      result <- run.register(who, SchemaRig.ReadOnly, orders, oversized)
      registrations <- registry.registrations.get
    } yield {
      assertEquals(result.left.map(_.code), Left(ErrorCode.ReadOnly))
      assertEquals(registrations, Nil)
    }
  }
}

object SchemaRegistrationSuite {

  /** 1024 * 1024, written out, because the point of the case that reads it is not to read the constant it is
    * checking. See `CompatibilityCheckUseCase.MaxDefinitionBytes` for why the bound exists.
    */
  val OneMebibyte: Int = 1048576
}
