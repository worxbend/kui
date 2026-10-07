package kui.schema.infrastructure

import cats.effect.IO
import sttp.client4.Backend
import sttp.client4.impl.cats.implicits.*
import sttp.client4.testing.{BackendStub, ResponseStub, StubBody}
import sttp.model.StatusCode

import kui.config.SafeUrl
import kui.kernel.Subject
import kui.kernel.error.{ErrorCode, InfrastructureError}
import kui.schema.domain.*
import kui.testkit.KuiIOSuite

/** What the client does with each answer a Schema Registry can give.
  *
  * A stub rather than a running registry: every promise here is a promise about a *response*, and a real
  * registry is the slowest possible way to produce one — and cannot be made to produce most of them at all.
  * The registry KUI has to survive is the one answering a proxy's HTML error page, a 404 that means three
  * different things depending on the path, and a compatibility level it has never heard of.
  */
final class RegistryHttpSuite extends KuiIOSuite {

  private val base: SafeUrl = SafeUrl.unsafe("http://registry:8081")
  private val orders = Subject.unsafe("orders-value")

  private def registry(respond: PartialFunction[String, (StatusCode, String)]): RegistryHttp[IO] = {
    val backend: Backend[IO] = BackendStub[IO](summon[sttp.monad.MonadError[IO]]).whenAnyRequest
      .thenRespondF { request =>
        val path = "/" + request.uri.path.mkString("/")
        val (status, body) = respond.applyOrElse(path, (_: String) => (StatusCode.NotFound, ""))
        IO.pure(ResponseStub.adjust(body, status): sttp.client4.Response[StubBody])
      }
    new RegistryHttp[IO](backend, base, RegistryCredentials.anonymous[IO])
  }

  private val nothing: PartialFunction[String, (StatusCode, String)] = { case "/never" =>
    (StatusCode.Ok, "")
  }

  test("requests are built relative to the root, because failover puts the base path back on") {
    // The defect this pins. `Failover.rebase` replaces the scheme and authority of each request and
    // *prefixes the base URL's own path*, so a client that also built its requests against the full
    // configured URL had that path applied twice. Apicurio serves the Confluent-compatible API at
    // `/apis/ccompat/v7` — it is the registry the quickstart runs — and every schema screen reported
    // "the configured address does not look like a Schema Registry", because the request had gone to
    // `/apis/ccompat/v7/apis/ccompat/v7/subjects` and honestly received a 404.
    //
    // The suite could not see it: every case here used a base with no path at all.
    var asked: String = ""
    val backend: Backend[IO] = BackendStub[IO](summon[sttp.monad.MonadError[IO]]).whenAnyRequest
      .thenRespondF { request =>
        asked = "/" + request.uri.path.mkString("/")
        IO.pure(ResponseStub.adjust("[]", StatusCode.Ok): sttp.client4.Response[StubBody])
      }
    val subPath = SafeUrl.unsafe("http://registry:8081/apis/ccompat/v7")

    new RegistryHttp[IO](backend, subPath, RegistryCredentials.anonymous[IO]).subjects
      .map(_ => assertEquals(asked, "/subjects"))
  }

  test("the subject list decodes into subjects") {
    registry { case "/subjects" => (StatusCode.Ok, """["orders-value","payments-value"]""") }.subjects
      .map(_.map(_.map(_.value)))
      .assertEquals(Right(List("orders-value", "payments-value")))
  }

  test("a 404 on the subject list is not 'no subjects'; it is an address that is not a registry") {
    // This is the difference between an empty Schemas screen and a message telling the operator their
    // ingress points at the wrong service, and it is the only 404 in this client that is a failure.
    registry(nothing).subjects.map {
      case Left(error) =>
        assertEquals(error.code, ErrorCode.UpstreamUnavailable)
        assert(clue(error.message).contains("does not look like a Schema Registry"))
      case Right(found) => fail(s"expected a failure, got $found")
    }
  }

  test("a subject that does not exist is an absence rather than a failure") {
    registry(nothing).versions(orders).assertEquals(Right(None))
  }

  test("a version list arrives sorted, and a nonsense version number is dropped") {
    registry { case "/subjects/orders-value/versions" => (StatusCode.Ok, "[3,1,0,2]") }
      .versions(orders)
      .map(_.map(_.map(_.map(_.value))))
      .assertEquals(Right(Some(List(1, 2, 3))))
  }

  test("a schema keeps its text verbatim and reads its references") {
    val body =
      """{"subject":"orders-value","version":2,"id":11,"schemaType":"JSON",
        |"schema":"{\"type\":\"object\"}",
        |"references":[{"name":"Address","subject":"address-value","version":1}]}""".stripMargin

    registry { case "/subjects/orders-value/versions/2" => (StatusCode.Ok, body) }
      .schema(orders, VersionSelector.Numbered(SchemaVersion.unsafe(2)))
      .map {
        case Right(Some(schema)) =>
          assertEquals(schema.definition, """{"type":"object"}""")
          assertEquals(schema.format, SchemaFormat.Json)
          assertEquals(schema.version.value, 2)
          assertEquals(schema.id.value, 11)
          assertEquals(schema.references.map(_.name), List("Address"))
        case other => fail(s"expected a schema, got $other")
      }
  }

  test("a schema with no schemaType is Avro, because that is what omitting it means") {
    registry { case "/subjects/orders-value/versions/latest" =>
      (StatusCode.Ok, """{"version":1,"id":1,"schema":"\"string\""}""")
    }.schema(orders, VersionSelector.Latest)
      .map(_.map(_.map(_.format)))
      .assertEquals(Right(Some(SchemaFormat.Avro)))
  }

  test("an answer that is not JSON says so, and quotes nothing the registry sent") {
    registry { case "/subjects/orders-value/versions/latest" =>
      (StatusCode.Ok, "<html><body>502 Bad Gateway from squid</body></html>")
    }.schema(orders, VersionSelector.Latest)
      .map {
        case Left(error) =>
          assert(clue(error.message).contains("could not be understood"))
          assert(!error.message.contains("squid"), "an upstream body must never be echoed (ADR-034)")
        case Right(found) => fail(s"expected a failure, got $found")
      }
  }

  test("no global level configured is the registry's own default, because that is what it will apply") {
    registry(nothing).globalCompatibility.assertEquals(Right(CompatibilityLevel.Backward))
  }

  test("both spellings of the level field are read") {
    registry { case "/config" =>
      (StatusCode.Ok, """{"compatibilityLevel":"FULL_TRANSITIVE"}""")
    }.globalCompatibility
      .assertEquals(Right(CompatibilityLevel.FullTransitive)) *>
      registry { case "/config" => (StatusCode.Ok, """{"compatibility":"NONE"}""") }.globalCompatibility
        .assertEquals(Right(CompatibilityLevel.None))
  }

  test("a subject with no level of its own answers None rather than failing") {
    registry(nothing).subjectCompatibility(orders).assertEquals(Right(None))
  }

  test("compatibility PUT 404 is a rejected write, not successful absence") {
    for {
      global <- registry(nothing).setGlobalCompatibility(CompatibilityLevel.Full)
      subject <- registry(nothing).setSubjectCompatibility(orders, CompatibilityLevel.Full)
    } yield {
      assert(global.isLeft, clue = global)
      assert(subject.isLeft, clue = subject)
    }
  }

  test("a level KUI does not know is a failure naming the seven it does") {
    registry { case "/config" =>
      (StatusCode.Ok, """{"compatibilityLevel":"SIDEWAYS"}""")
    }.globalCompatibility
      .map {
        case Left(error) => assert(clue(error.message).contains("BACKWARD"))
        case Right(found) => fail(s"expected a failure, got $found")
      }
  }

  test("a 422 from the registry is a validation failure carrying the registry's own explanation") {
    registry { case "/config" =>
      (StatusCode.UnprocessableEntity, """{"error_code":42203,"message":"Invalid compatibility level"}""")
    }.setGlobalCompatibility(CompatibilityLevel.Full)
      .map {
        case Left(error) =>
          assertEquals(error.code, ErrorCode.Validation)
          assert(clue(error.message).contains("Invalid compatibility level"))
        case Right(_) => fail("expected a failure")
      }
  }

  test("a registration answers the id the registry stored and the version the lookup reports") {
    // Two requests, because the Confluent API's registration response is `{"id": N}` and carries no
    // version. The second is the registry's "which version is *this* schema" lookup and not
    // `versions/latest`, which would be a race: somebody else's registration in between would hand this
    // operator back a version that is not theirs.
    var asked: List[String] = Nil
    val client = registry {
      case "/subjects/orders-value/versions" =>
        asked = asked :+ "/subjects/orders-value/versions"
        (StatusCode.Ok, """{"id":41}""")
      case "/subjects/orders-value" =>
        asked = asked :+ "/subjects/orders-value"
        (StatusCode.Ok, """{"subject":"orders-value","id":41,"version":3,"schema":"{}"}""")
    }

    client.register(orders, ProposedSchema(SchemaFormat.Avro, """{"type":"record"}""", Nil)).map {
      case Right(registered) =>
        assertEquals(registered.id.value, 41)
        assertEquals(registered.version.map(_.value), Some(3))
        assertEquals(registered.subject.value, "orders-value")
        assertEquals(asked, List("/subjects/orders-value/versions", "/subjects/orders-value"))
      case Left(error) => fail(s"expected a registration, got $error")
    }
  }

  test("a registry that rejects the schema says so as a validation failure beside the field") {
    // The whole point of the endpoint. A 409 from a Schema Registry means "incompatible with what this
    // subject already holds", and the sentence after it names the field that broke the rule — which is the
    // only part of the answer an operator can act on. It must not arrive as a 500 and must not be
    // swallowed into "the upstream is unavailable", both of which send them to the wrong person.
    val explanation =
      "Schema being registered is incompatible with an earlier schema for subject 'orders-value'"

    registry { case "/subjects/orders-value/versions" =>
      (StatusCode.Conflict, s"""{"error_code":409,"message":"$explanation"}""")
    }.register(orders, ProposedSchema(SchemaFormat.Avro, "{}", Nil)).map {
      case Left(error) =>
        assertEquals(error.code, ErrorCode.Validation)
        assert(clue(error.message).contains(explanation))
        // `details[0]` is where a form reads the text it puts beside the input somebody typed into.
        assertEquals(error.details.map(_.field), List(Some("definition")))
        assertEquals(error.details.flatMap(_.restrictions), List(explanation))
      case Right(registered) => fail(s"expected a refusal, got $registered")
    }
  }

  test("a registration whose version lookup does not answer is still a registration, with no version") {
    // The schema is in the registry by the time the second call is made. Answering `Left` here would tell
    // an operator to register it again, and inventing "the previous latest plus one" would print a version
    // number that may not exist. Absent is the true third answer.
    registry { case "/subjects/orders-value/versions" =>
      (StatusCode.Ok, """{"id":41}""")
    }.register(orders, ProposedSchema(SchemaFormat.Avro, "{}", Nil)).map {
      case Right(registered) =>
        assertEquals(registered.id.value, 41)
        assertEquals(registered.version, None)
      case Left(error) => fail(s"expected a registration, got $error")
    }
  }

  test("a 404 on the registration itself is an address that is not a registry") {
    // A subject that does not exist is *created* by this call, so there is nothing here that a 404 could
    // honestly mean an absence of. It is the same misconfigured ingress `GET /subjects` reports.
    registry(nothing).register(orders, ProposedSchema(SchemaFormat.Avro, "{}", Nil)).map {
      case Left(error) =>
        assertEquals(error.code, ErrorCode.UpstreamUnavailable)
        assert(clue(error.message).contains("does not look like a Schema Registry"))
      case Right(registered) => fail(s"expected a failure, got $registered")
    }
  }

  test("a 401 is an authentication failure and not a generic upstream error") {
    registry { case "/subjects" => (StatusCode.Unauthorized, "nope") }.subjects.map {
      case Left(error) => assertEquals(error.code, ErrorCode.UpstreamAuth)
      case Right(found) => fail(s"expected a failure, got $found")
    }
  }

  test("a 503 becomes an upstream failure rather than an empty answer") {
    registry { case "/subjects" => (StatusCode.ServiceUnavailable, "") }.subjects.map {
      case Left(error) => assertEquals(error.code, ErrorCode.UpstreamUnavailable)
      case Right(found) => fail(s"expected a failure, got $found")
    }
  }

  test("the compatibility verdict carries the registry's messages, and an unknown subject is an absence") {
    val client = registry { case "/compatibility/subjects/orders-value/versions/latest" =>
      (StatusCode.Ok, """{"is_compatible":false,"messages":["field 'total' has no default"]}""")
    }

    client
      .checkCompatibility(orders, VersionSelector.Latest, ProposedSchema(SchemaFormat.Avro, "{}", Nil))
      .assertEquals(Right(Some(CompatibilityVerdict(false, List("field 'total' has no default"))))) *>
      registry(nothing)
        .checkCompatibility(orders, VersionSelector.Latest, ProposedSchema(SchemaFormat.Avro, "{}", Nil))
        .assertEquals(Right(None))
  }

  test("an incompatible verdict with no messages is a real state, not a decode failure") {
    registry { case "/compatibility/subjects/orders-value/versions/latest" =>
      (StatusCode.Ok, """{"is_compatible":false}""")
    }.checkCompatibility(orders, VersionSelector.Latest, ProposedSchema(SchemaFormat.Avro, "{}", Nil))
      .assertEquals(Right(Some(CompatibilityVerdict(false, Nil))))
  }

  test("a summary counts the version list rather than reading the latest version's number") {
    registry {
      case "/subjects/orders-value/versions" => (StatusCode.Ok, "[1,2,7]")
      case "/subjects/orders-value/versions/latest" =>
        (StatusCode.Ok, """{"version":7,"id":11,"schemaType":"PROTOBUF","schema":"syntax = \"proto3\";"}""")
      case "/config/orders-value" => (StatusCode.Ok, """{"compatibilityLevel":"FULL"}""")
    }.summary(orders).map {
      case Right(Some(summary)) =>
        // Versions 3 to 6 have been deleted. The latest is 7 and there are three of them, and a row
        // reading "7 versions" would be printing a number the registry never offered as a count.
        assertEquals(summary.versionCount, Some(3))
        assertEquals(summary.format, Some(SchemaFormat.Protobuf))
        assertEquals(summary.compatibility, Some(SubjectCompatibility.own(CompatibilityLevel.Full)))
      case other => fail(s"expected a summary, got $other")
    }
  }

  test("a subject with no level of its own leaves the level absent for the caller to inherit") {
    // The registry answers 404 to `/config/{subject}` for the overwhelming majority of subjects, and
    // resolving that into the global level here would be one extra request per row of every page.
    registry {
      case "/subjects/orders-value/versions" => (StatusCode.Ok, "[1]")
      case "/subjects/orders-value/versions/latest" =>
        (StatusCode.Ok, """{"version":1,"id":1,"schema":"\"string\""}""")
    }.summary(orders).map(_.map(_.flatMap(_.compatibility))).assertEquals(Right(None))
  }

  test("a subject that has gone is an absence, and the two decorating requests are never sent") {
    var asked = List.empty[String]
    val backend: Backend[IO] = BackendStub[IO](summon[sttp.monad.MonadError[IO]]).whenAnyRequest
      .thenRespondF { request =>
        asked = asked :+ ("/" + request.uri.path.mkString("/"))
        IO.pure(ResponseStub.adjust("", StatusCode.NotFound): sttp.client4.Response[StubBody])
      }

    new RegistryHttp[IO](backend, base, RegistryCredentials.anonymous[IO]).summary(orders).map { result =>
      assertEquals(result, Right(None))
      // Not three requests for a subject that is no longer there. The version call already answered
      // the only question the other two were going to decorate.
      assertEquals(asked, List("/subjects/orders-value/versions"))
    }
  }

  test("a decorating request that fails refuses the whole row rather than half of it") {
    registry {
      case "/subjects/orders-value/versions" => (StatusCode.Ok, "[1]")
      case "/subjects/orders-value/versions/latest" =>
        (StatusCode.Ok, """{"version":1,"id":1,"schema":"\"string\""}""")
      case "/config/orders-value" => (StatusCode.ServiceUnavailable, "")
    }.summary(orders).map {
      case Left(error) => assertEquals(error.code, ErrorCode.UpstreamUnavailable)
      case Right(found) => fail(s"expected a failure, got $found")
    }
  }

  test("a 409 on a read is not reported as a validation failure") {
    // Wave 4 added `StatusCode.Conflict` to the branch that produces `KUI-VALIDATION`, and `errorFrom` is
    // shared by every call this file makes — so a 409 answering `GET /subjects` became a 400 telling the
    // operator their request was invalid, with `details[0].field` null and no form anywhere on the screen.
    // A read sends no body: there is nothing it could have got wrong. A registry mid-election, mid-migration
    // or behind a confused proxy is an upstream problem and says so, which is what a caller can act on.
    val readIsUpstream = registry { case "/subjects" =>
      (StatusCode.Conflict, """{"error_code":40901,"message":"leader election in progress"}""")
    }.subjects.map {
      case Left(error) =>
        assertEquals(error.code, ErrorCode.UpstreamUnavailable)
        assertEquals(error.details, Nil, "a read has no field a refusal could belong beside")
      case Right(found) => fail(s"expected a failure, got $found")
    }

    val configReadIsUpstream = registry { case "/config" =>
      (StatusCode.Conflict, """{"message":"leader election in progress"}""")
    }.globalCompatibility.map {
      case Left(error) => assertEquals(error.code, ErrorCode.UpstreamUnavailable)
      case Right(found) => fail(s"expected a failure, got $found")
    }

    // The other half, and it is the half that must not move: the registration's own 409 is the refusal an
    // operator acts on, and it stays `KUI-VALIDATION` beside the field they typed into.
    val writeStaysValidation = registry { case "/subjects/orders-value/versions" =>
      (StatusCode.Conflict, """{"error_code":409,"message":"incompatible with an earlier schema"}""")
    }.register(orders, ProposedSchema(SchemaFormat.Avro, "{}", Nil)).map {
      case Left(error) =>
        assertEquals(error.code, ErrorCode.Validation)
        assertEquals(error.details.map(_.field), List(Some("definition")))
      case Right(registered) => fail(s"expected a refusal, got $registered")
    }

    readIsUpstream *> configReadIsUpstream *> writeStaysValidation
  }

  test("a refusal with no field of its own still carries the registry's sentence, keyed to nothing") {
    // `PUT /config` with a level the registry does not know. There is no form input to mark — the whole
    // request is the level — so `details[0].field` is null, which is the shape ADR-034 gives a refusal
    // about the request rather than about one of its fields. The case that covered this asserted only
    // `error.message`, so the `details` array could be emptied with every case in this service green, and
    // a browser reading `details[0]` for the registry's own words would have found nothing there.
    registry { case "/config" =>
      (StatusCode.UnprocessableEntity, """{"error_code":42203,"message":"Invalid compatibility level"}""")
    }.setGlobalCompatibility(CompatibilityLevel.Full)
      .map {
        case Left(error) =>
          assertEquals(error.code, ErrorCode.Validation)
          assertEquals(error.details.map(_.field), List(None))
          assertEquals(error.details.flatMap(_.restrictions), List("Invalid compatibility level"))
        case Right(_) => fail("expected a failure")
      }
  }

  test("a refusal quotes the registry's message and nothing else it sent") {
    // ADR-034 forbids echoing an upstream body: it routinely carries another system's internals. The
    // registry's `message` field is written for a human and is the exception; every other field it sent
    // — a stack trace, an internal host name, a `details` object — is read and thrown away.
    val body =
      """{"error_code":409,"message":"incompatible with an earlier schema",
        |"stack":"at io.confluent.kafka.Internal(Secret.java:41)",
        |"upstream":"http://user:hunter2@registry-internal:8081"}""".stripMargin

    registry { case "/subjects/orders-value/versions" => (StatusCode.Conflict, body) }
      .register(orders, ProposedSchema(SchemaFormat.Avro, "{}", Nil))
      .map {
        case Left(error) =>
          assert(clue(error.message).contains("incompatible with an earlier schema"))
          assert(!error.message.contains("hunter2"), "an upstream body must never be echoed (ADR-034)")
          assert(!error.message.contains("Secret.java"), "an upstream body must never be echoed (ADR-034)")
          assertEquals(error.details.flatMap(_.restrictions).size, 1)
        case Right(registered) => fail(s"expected a refusal, got $registered")
      }
  }

  test("a connection failure retains its class but never its potentially sensitive message") {
    val canary = "https://user:registry-secret@registry-internal:8081"
    val backend: Backend[IO] = BackendStub[IO](summon[sttp.monad.MonadError[IO]]).whenAnyRequest
      .thenRespondF { _ =>
        IO.raiseError(
          new RuntimeException(
            canary,
            new java.net.ConnectException(canary)
          )
        )
      }

    new RegistryHttp[IO](backend, base, RegistryCredentials.anonymous[IO]).subjects.map {
      case Left(InfrastructureError.Unreachable(upstream, cause)) =>
        assertEquals(upstream, RegistryHttp.UpstreamName)
        assertEquals(cause, "ConnectException")
        assert(!cause.contains("registry-secret"), cause)
      case other => fail(s"expected an unreachable upstream, got $other")
    }
  }

  test("a 403 is an authentication failure, exactly as a 401 is") {
    // Both, because a registry behind an authorising proxy answers 403 where the registry itself answers
    // 401, and "your credentials were refused" is the same sentence and the same screen for either.
    registry { case "/subjects" => (StatusCode.Forbidden, "nope") }.subjects.map {
      case Left(error) => assertEquals(error.code, ErrorCode.UpstreamAuth)
      case Right(found) => fail(s"expected a failure, got $found")
    }
  }

  test("the compatibility check asks the registry for its reasons") {
    // Without `verbose=true` the registry answers a bare `{"is_compatible": false}`, and the screen tells
    // an operator "no" with no reason — the least useful possible answer to "why will this not register".
    // The query string is the only place that request differs, so only the asked-for URI can show it.
    var asked: String = ""
    val backend: Backend[IO] = BackendStub[IO](summon[sttp.monad.MonadError[IO]]).whenAnyRequest
      .thenRespondF { request =>
        asked = request.uri.toString
        IO.pure(ResponseStub.adjust("""{"is_compatible":true}""", StatusCode.Ok))
      }

    new RegistryHttp[IO](backend, base, RegistryCredentials.anonymous[IO])
      .checkCompatibility(orders, VersionSelector.Latest, ProposedSchema(SchemaFormat.Avro, "{}", Nil))
      .map(_ => assert(clue(asked).contains("verbose=true")))
  }

  test("the subject a schema names is the registry's own, not the one that was asked for") {
    // Confluent's compatibility layers answer `GET /subjects/{s}/versions/{v}` with the subject the schema
    // is actually stored under, and an ingress or an alias can make those two differ. Printing the
    // requested name over the stored one is a panel claiming a schema belongs to a subject it does not.
    registry { case "/subjects/orders-value/versions/1" =>
      (StatusCode.Ok, """{"subject":"orders-value-v2","version":1,"id":9,"schema":"\"string\""}""")
    }.schema(orders, VersionSelector.Numbered(SchemaVersion.unsafe(1)))
      .map(_.map(_.map(_.subject.value)))
      .assertEquals(Right(Some("orders-value-v2")))
  }

  test("a reference pinned to a version that is not a version is dropped, never carried as itself") {
    // Registry versions start at 1 and `-1` is its spelling of "latest", so neither is a number a
    // reference can pin. Carrying one through would put a dependency on screen that nobody can look up;
    // dropping it leaves a schema whose references are the ones that exist.
    val body =
      """{"subject":"orders-value","version":2,"id":11,"schema":"{}",
        |"references":[{"name":"Address","subject":"address-value","version":1},
        |{"name":"Latest","subject":"legacy-value","version":-1},
        |{"name":"Zero","subject":"legacy-value","version":0}]}""".stripMargin

    registry { case "/subjects/orders-value/versions/2" => (StatusCode.Ok, body) }
      .schema(orders, VersionSelector.Numbered(SchemaVersion.unsafe(2)))
      .map {
        case Right(Some(schema)) =>
          assertEquals(schema.references.map(_.name), List("Address"))
          assertEquals(schema.references.map(_.version.value), List(1))
        case other => fail(s"expected a schema, got $other")
      }
  }

  test("KUI sends the vendor content type and accepts plain JSON as well") {
    // Two differences between the registries that speak this API, and both are handled rather than
    // assumed away: several implementations only ever *send* `application/json`, so a strict `Accept`
    // gets a 406 from them, while the vendor type is what the documented API says to send.
    var accept: String = ""
    var contentType: String = ""
    val backend: Backend[IO] = BackendStub[IO](summon[sttp.monad.MonadError[IO]]).whenAnyRequest
      .thenRespondF { request =>
        accept = request.header("Accept").getOrElse("")
        contentType = request.header("Content-Type").getOrElse("")
        IO.pure(ResponseStub.adjust("""{"id":41}""", StatusCode.Ok))
      }

    new RegistryHttp[IO](backend, base, RegistryCredentials.anonymous[IO])
      .register(orders, ProposedSchema(SchemaFormat.Avro, "{}", Nil))
      .map { _ =>
        assert(clue(accept).contains("application/vnd.schemaregistry.v1+json"))
        assert(clue(accept).contains("application/json"), "a registry that only sends JSON answers 406")
        assert(clue(contentType).startsWith("application/vnd.schemaregistry.v1+json"))
      }
  }
}
