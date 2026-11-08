package kui.schema.api

import java.time.Instant

import scala.concurrent.duration.DurationInt

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.kernel.Resource
import io.circe.Json
import io.circe.parser.parse
import org.typelevel.otel4s.metrics.MeterProvider
import sttp.client4.*
import sttp.client4.impl.cats.implicits.*
import sttp.client4.testing.BackendStub
import sttp.model.Uri
import sttp.tapir.server.stub4.TapirStubInterpreter

import kui.contracts.KuiEndpoint
import kui.http.principal.{PrincipalVerification, RbacGuard}
import kui.kernel.error.KuiError
import kui.kernel.{ClusterId, SchemaId, Secret, Subject, UserName}
import kui.observability.Telemetry
import kui.schema.application.*
import kui.schema.domain.*
import kui.security.*
import kui.testkit.KuiIOSuite
import kui.testkit.fakes.FakeStructuredLogger

/** The four read routes `SubjectListRoutesSuite` does not drive, and the mapping underneath them.
  *
  * ==Why this file exists==
  *
  * `SchemaMapping` is nine methods and, until wave 5, two of them were reachable from a route test — the
  * subject list's and the registration's. The other seven could be reduced to constants with the whole
  * service green: the schema panel's `references` emptied, an inherited compatibility level reported as the
  * subject's own, the registry-wide level marked as inherited from itself, a version list truncated, a
  * nonsense version segment answered with the latest schema. Every one of those is a wrong answer on a screen
  * and none of them is visible below the route, because `SchemaMapping` is the module that turns an
  * application type into the bytes a browser reads.
  *
  * The registry is a fake with fixed contents and everything between it and the socket is real: the path
  * codec, the route order, the mapping, the principal check and `ErrorEnvelope.statusOf`.
  */
final class SchemaReadRoutesSuite extends KuiIOSuite {

  private val cluster = ClusterId.unsafe("local")
  private val orders = Subject.unsafe("orders-value")

  /** One subject with three versions, a Protobuf latest, a level of its own, and one reference. */
  private val ordersLatest: RegisteredSchema = RegisteredSchema(
    subject = orders,
    version = SchemaVersion.unsafe(7),
    id = SchemaId.unsafe(11),
    format = SchemaFormat.Protobuf,
    definition = """syntax = "proto3"; import "address.proto"; message Order { Address to = 1; }""",
    references =
      List(SchemaReference("address.proto", Subject.unsafe("address-value"), SchemaVersion.unsafe(2)))
  )

  final private class Registry extends SchemaRegistryPort[IO] {

    def subjects: IO[Either[KuiError, List[Subject]]] =
      IO.pure(Right(List(orders, Subject.unsafe("payments-value"))))

    def summary(subject: Subject): IO[Either[KuiError, Option[SubjectSummary]]] =
      IO.pure(
        Right(
          Some(
            SubjectSummary(
              subject = subject,
              format = Some(SchemaFormat.Protobuf),
              versionCount = Some(3),
              // Only `orders-value` is pinned; `payments-value` inherits, which is the pair of rows that
              // tells a flattened compatibility field from an honest one.
              compatibility =
                Option.when(subject == orders)(SubjectCompatibility.own(CompatibilityLevel.Full))
            )
          )
        )
      )

    def versions(subject: Subject): IO[Either[KuiError, Option[List[SchemaVersion]]]] =
      IO.pure(Right(Some(List(1, 2, 7).map(SchemaVersion.unsafe))))

    def schema(subject: Subject, version: VersionSelector): IO[Either[KuiError, Option[RegisteredSchema]]] =
      IO.pure(Right(Some(ordersLatest)))

    def globalCompatibility: IO[Either[KuiError, CompatibilityLevel]] =
      IO.pure(Right(CompatibilityLevel.ForwardTransitive))

    def subjectCompatibility(subject: Subject): IO[Either[KuiError, Option[CompatibilityLevel]]] =
      IO.pure(Right(Option.when(subject == orders)(CompatibilityLevel.Full)))

    def register(subject: Subject, proposed: ProposedSchema) =
      IO.pure(Right(RegisteredVersion(subject, SchemaId.unsafe(1), None)))

    def setGlobalCompatibility(level: CompatibilityLevel) = IO.pure(Right(()))
    def setSubjectCompatibility(subject: Subject, level: CompatibilityLevel) = IO.pure(Right(()))

    def checkCompatibility(subject: Subject, version: VersionSelector, proposed: ProposedSchema) =
      IO.pure(Right(None))
  }

  private def server: Resource[IO, Backend[IO]] =
    Resource.eval(
      for {
        logger <- FakeStructuredLogger[IO]
        meter <- MeterProvider.noop[IO].get("kui.schema")
        rejections <- PrincipalVerification.rejectionCounter[IO](meter)
        interceptors <- SchemaApi.interceptors[IO](Telemetry.noop[IO], rejections, logger)
      } yield {
        val port = new Registry

        val registries = new ClusterRegistries[IO] {
          private val profiles =
            List(RegistryProfile(cluster, "Local", hasRegistry = true, readOnly = false))
          def all = IO.pure(profiles)
          def profile(id: ClusterId) = IO.pure(profiles.find(_.cluster == id))
          def registry(id: ClusterId) = IO.pure(Option.when(id == cluster)(port))
        }

        val routes = SchemaRoutes[IO](
          SubjectListUseCase.make[IO](registries, logger),
          SubjectVersionsUseCase.make[IO](registries),
          SchemaVersionUseCase.make[IO](registries),
          CompatibilityReadUseCase.make[IO](registries, logger),
          SchemaApi.Securing[IO](codec, rejections, logger, RbacGuard.allowAll[IO])
        )

        TapirStubInterpreter(interceptors, BackendStub[IO](summon))
          .whenServerEndpointsRunLogic(routes)
          .backend()
      }
    )

  // -----------------------------------------------------------------------------------------------

  private val key: SigningKey =
    SigningKey("test-1", Secret(Array.fill[Byte](32)(7)), Instant.parse("2020-01-01T00:00:00Z"))

  private val codec: PrincipalCodec[IO] =
    JwsPrincipalCodec
      .make[IO](NonEmptyList.of(key), "kui-gateway")
      .getOrElse(throw new IllegalStateException("the test signing key is too short for HS256"))

  private def base(on: ClusterId = cluster): String = s"/internal/v1/clusters/${on.value}/schemas"

  private def get(backend: Backend[IO], path: String): IO[Response[String]] =
    IO.realTimeInstant
      .flatMap(now =>
        codec.sign(
          PrincipalClaims(
            subject = UserName.unsafe("alice"),
            roles = Set.empty,
            kind = PrincipalKind.Session,
            sessionRef = None,
            issuedAt = now,
            expiresAt = now.plusSeconds(60.seconds.toSeconds),
            audience = SchemaApi.Id,
            requestDigest = RequestDigest.ofRequestLine("GET", path.takeWhile(_ != '?'))
          )
        )
      )
      .flatMap(signed =>
        basicRequest
          .get(Uri.unsafeParse(s"http://schema$path"))
          .header(KuiEndpoint.PrincipalHeader, signed.value)
          .response(asStringAlways)
          .send(backend)
      )

  private def body(response: Response[String]): Json =
    parse(response.body).fold(failure => fail(s"not JSON: ${failure.message} in ${response.body}"), identity)

  // -----------------------------------------------------------------------------------------------

  test("the schema panel carries the schemas a schema depends on, by name, subject and version") {
    // `SchemaMapping.schema` can answer `references = Nil` with everything else in this service green,
    // and the panel then shows a Protobuf schema whose imports have vanished — which is a document that
    // does not compile and an operator diffing it against their registry's own screen for nothing.
    server.use { backend =>
      get(backend, s"${base()}/subjects/${orders.value}/versions/7").map { response =>
        val json = body(response).hcursor

        assertEquals(response.code.code, 200, response.body)
        assertEquals(json.get[String]("schemaType"), Right("PROTOBUF"))
        // Verbatim, down to the whitespace: an operator diffs this against the registry's own screen.
        assertEquals(json.get[String]("definition"), Right(ordersLatest.definition))

        val reference = json.downField("references").downN(0)
        assertEquals(json.downField("references").values.map(_.size), Some(1))
        assertEquals(reference.get[String]("name"), Right("address.proto"))
        assertEquals(reference.get[String]("subject"), Right("address-value"))
        assertEquals(reference.get[Int]("version"), Right(2))
      }
    }
  }

  test("a version segment that is neither a number nor 'latest' is refused, never quietly the latest") {
    // `SchemaMapping.version` can be made to fall back to `VersionSelector.Latest` with every case in
    // this service green — `VersionSelector.parse` is gated in the domain suite and the mapping's *use*
    // of it was not. A typo that returns the newest schema shows an operator the wrong document with
    // nothing anywhere saying so, which is the exact failure the parser's own comment argues against.
    server.use { backend =>
      get(backend, s"${base()}/subjects/${orders.value}/versions/lastest").map { response =>
        assertEquals(response.code.code, 400, response.body)
        assertEquals(body(response).hcursor.get[String]("code"), Right("KUI-VALIDATION"))
        assert(clue(response.body).contains("latest"), "the refusal names both spellings it accepts")
      }
    }
  }

  test("a subject's version list arrives whole and ascending") {
    server.use { backend =>
      get(backend, s"${base()}/subjects/${orders.value}/versions").map { response =>
        val json = body(response).hcursor

        assertEquals(response.code.code, 200, response.body)
        assertEquals(json.get[String]("subject"), Right(orders.value))
        assertEquals(json.get[List[Int]]("versions"), Right(List(1, 2, 7)))
      }
    }
  }

  test("the registry-wide level is nobody's inheritance, and a subject's inherited one says so") {
    // Three answers from one `CompatibilityDto`, and `inheritedFromGlobal` is the field that stops the
    // screen from lying. It can be hard-coded either way in `SchemaMapping` with everything green: false
    // everywhere makes an inherited level look pinned, so pressing Save writes an override the operator
    // never intended; true on the global level claims the registry inherits from itself.
    server.use { backend =>
      for {
        global <- get(backend, s"${base()}/compatibility")
        pinned <- get(backend, s"${base()}/subjects/${orders.value}/compatibility")
        inheriting <- get(backend, s"${base()}/subjects/payments-value/compatibility")
      } yield {
        assertEquals(global.code.code, 200, global.body)
        assertEquals(body(global).hcursor.get[String]("level"), Right("FORWARD_TRANSITIVE"))
        assertEquals(body(global).hcursor.get[Boolean]("inheritedFromGlobal"), Right(false))

        assertEquals(body(pinned).hcursor.get[String]("level"), Right("FULL"))
        assertEquals(body(pinned).hcursor.get[Boolean]("inheritedFromGlobal"), Right(false))

        assertEquals(body(inheriting).hcursor.get[String]("level"), Right("FORWARD_TRANSITIVE"))
        assertEquals(body(inheriting).hcursor.get[Boolean]("inheritedFromGlobal"), Right(true))
      }
    }
  }

  test("/schemas/compatibility is the registry's level and not a subject called 'compatibility'") {
    // `SchemaRoutes.apply` says the order of its five routes is load bearing, because a router that tried
    // the subject routes first would answer this request with a lookup of a subject named
    // "compatibility". Measured in wave 5, that is not true of the paths as published: `/schemas/subjects`
    // and `/schemas/subjects/{s}/compatibility` are two and four segments and neither can match
    // `/schemas/compatibility`, so swapping the two lines leaves this case and every other one green.
    // What is asserted here is therefore the answer itself and not the ordering — a `CompatibilityDto`
    // rather than a subject-shaped one — and the ordering claim is recorded as unfounded rather than
    // defended by a case that cannot fail.
    server.use { backend =>
      get(backend, s"${base()}/compatibility").map { response =>
        val json = body(response).hcursor

        assertEquals(response.code.code, 200, response.body)
        // A `CompatibilityDto` and not a `SubjectSummaryDto`: the subject route would have answered with
        // the subject's own shape, which has no `level` at all.
        assertEquals(json.get[String]("level"), Right("FORWARD_TRANSITIVE"))
        assertEquals(json.downField("subject").focus, None)
      }
    }
  }

  test("a subject list row carries the compatibility level the registry reported for it") {
    // `SchemaMapping.summary` can answer `compatibility = None` for every row with the whole service
    // green: the list still renders, with the caption every row is built from silently missing.
    server.use { backend =>
      get(backend, s"${base()}/subjects").map { response =>
        val rows = body(response).hcursor.downField("items")

        assertEquals(response.code.code, 200, response.body)
        assertEquals(rows.downN(0).get[String]("subject"), Right(orders.value))
        assertEquals(rows.downN(0).get[String]("format"), Right("PROTOBUF"))
        assertEquals(rows.downN(0).get[Int]("versionCount"), Right(3))
        assertEquals(rows.downN(0).downField("compatibility").get[String]("level"), Right("FULL"))
        assertEquals(
          rows.downN(0).downField("compatibility").get[Boolean]("inheritedFromGlobal"),
          Right(false)
        )
        // And the row that has no level of its own carries the global one, marked as inherited — the
        // same distinction the single-subject route makes, applied once per page rather than per row.
        assertEquals(
          rows.downN(1).downField("compatibility").get[Boolean]("inheritedFromGlobal"),
          Right(true)
        )
      }
    }
  }

  test("a blank search is no search at all, and does not filter the list down to nothing") {
    // `SchemaMapping.query` trims `q` and drops it when it is empty, and so does `SubjectCatalog.page`
    // — the same expression, one layer in. Measured in wave 5, removing either one alone leaves this
    // case green, because the other still applies the rule; that is defence in depth rather than an
    // ungated rule, and it is why this case drives the whole route instead of the mapping. What it pins
    // is the behaviour a browser sees: `?q=` — which every search form sends before anybody types — is
    // not a search for the empty string.
    server.use { backend =>
      for {
        blank <- get(backend, s"${base()}/subjects?q=")
        spaces <- get(backend, s"${base()}/subjects?q=%20%20")
        real <- get(backend, s"${base()}/subjects?q=payments")
      } yield {
        def total(response: Response[String]): Either[io.circe.DecodingFailure, Option[Long]] =
          body(response).hcursor.downField("page").get[Option[Long]]("totalItems")

        assertEquals(total(blank), Right(Some(2L)))
        assertEquals(total(spaces), Right(Some(2L)))
        // And the search still searches, or "no search" would be satisfied by ignoring `q` entirely.
        assertEquals(total(real), Right(Some(1L)))
      }
    }
  }
}
