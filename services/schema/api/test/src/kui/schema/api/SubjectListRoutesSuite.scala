package kui.schema.api

import java.time.Instant

import scala.concurrent.duration.{DurationInt, FiniteDuration}

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.kernel.{Ref, Resource}
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
import kui.kernel.error.{InfrastructureError, KuiError}
import kui.kernel.{ClusterId, SchemaId, Secret, Subject, UserName}
import kui.observability.Telemetry
import kui.schema.application.*
import kui.schema.contract.SchemaEndpoints
import kui.schema.domain.*
import kui.security.*
import kui.testkit.KuiIOSuite
import kui.testkit.fakes.FakeStructuredLogger

/** What the subject list answers over HTTP, and what it costs the registry to answer it.
  *
  * Everything asserted here was argued for at the use-case layer and asserted nowhere a client can see. Two
  * of the three claims are about a *status code*, which the layer below has no way to have an opinion about:
  *
  *   - **an enrichment that fails is still a 200 with the row in it.** W2-09 shipped that rule and tested it
  *     with `result.isRight` at the use-case layer, which is one `handleError` in a route away from being a
  *     500 on a screen. A subject list that fails because one subject's format could not be read is the
  *     failure this service's whole enrichment design exists to prevent.
  *   - **the page size is this endpoint's, not the kernel's.** `PageSize.Max` is 500 and a row here is three
  *     requests to a single-writer registry, so the ceiling is `SchemaEndpoints.MaxPageSize`. The clamp is in
  *     `SchemaMapping.query`, on the far side of the query-string codec, and the answer's own `pageSize` is
  *     the only place a caller can read what they actually got.
  *   - **a count-only request costs one registry call.** The drawer's schema badge reads `totalItems` and
  *     draws no rows; asking for a page of one to get it costs five.
  *
  * The registry is a counting fake and everything between it and the socket is real: the query-string codec,
  * the mapping's clamp, the use case's fan-out, the error envelope and the principal check. It runs through
  * Tapir's stub interpreter rather than a bound port for the reason `MetricsRoutesSuite` gives — a socket
  * would prove Netty works, which `libs/http` proves once for every service.
  */
final class SubjectListRoutesSuite extends KuiIOSuite {

  private val cluster = ClusterId.unsafe("local")

  /** Two hundred and fifty subjects, which is more than this endpoint's page ceiling and half the kernel's,
    * so the two ceilings cannot both be satisfied by the same answer.
    */
  private val many: List[String] = (0 until 250).map(n => f"orders-$n%03d-value").toList

  private val unreachable: KuiError =
    InfrastructureError.Unreachable("schema-registry", "connection refused")

  // -----------------------------------------------------------------------------------------------
  // A registry that counts what it was asked
  // -----------------------------------------------------------------------------------------------

  /** Every call the list page makes, recorded.
    *
    * `versions`, `schema` and `subjectCompatibility` are the three requests one enriched row costs, and they
    * are counted as one `summary` here because `RegistryHttp.summary` is what the port publishes and what the
    * use case calls. The registry-wide level is counted separately: it is the call an empty page must not
    * make, and no number of rows can show whether it was made.
    */
  final private class CountingRegistry(
      names: List[String],
      broken: Set[String],
      val enriched: Ref[IO, List[String]],
      val globalReads: Ref[IO, Int]
  ) extends SchemaRegistryPort[IO] {

    def subjects: IO[Either[KuiError, List[Subject]]] =
      IO.pure(Right(names.map(Subject.unsafe)))

    def summary(subject: Subject): IO[Either[KuiError, Option[SubjectSummary]]] =
      enriched.update(_ :+ subject.value) *> IO.pure(
        if broken.contains(subject.value) then Left(unreachable)
        else
          Right(
            Some(
              SubjectSummary(
                subject = subject,
                format = Some(SchemaFormat.Avro),
                versionCount = Some(3),
                compatibility = None
              )
            )
          )
      )

    def versions(subject: Subject) = IO.pure(Right(None))
    def schema(subject: Subject, version: VersionSelector) = IO.pure(Right(None))

    def register(subject: Subject, proposed: ProposedSchema) =
      IO.pure(
        Right(RegisteredVersion(subject, SchemaId.unsafe(1), Some(SchemaVersion.unsafe(1))))
      )

    def globalCompatibility: IO[Either[KuiError, CompatibilityLevel]] =
      globalReads.update(_ + 1) *> IO.pure(Right(CompatibilityLevel.Full))

    def subjectCompatibility(subject: Subject) = IO.pure(Right(None))
    def setGlobalCompatibility(level: CompatibilityLevel) = IO.pure(Right(()))
    def setSubjectCompatibility(subject: Subject, level: CompatibilityLevel) = IO.pure(Right(()))

    def checkCompatibility(subject: Subject, version: VersionSelector, proposed: ProposedSchema) =
      IO.pure(Right(None))
  }

  /** The service, with no socket, over one cluster whose registry counts. */
  private def server(
      names: List[String] = many,
      broken: Set[String] = Set.empty
  ): Resource[IO, (Backend[IO], CountingRegistry)] =
    Resource.eval(
      for {
        logger <- FakeStructuredLogger[IO]
        meter <- MeterProvider.noop[IO].get("kui.schema")
        rejections <- PrincipalVerification.rejectionCounter[IO](meter)
        enriched <- Ref.of[IO, List[String]](Nil)
        globalReads <- Ref.of[IO, Int](0)
        interceptors <- SchemaApi.interceptors[IO](Telemetry.noop[IO], rejections, logger)
      } yield {
        val port = new CountingRegistry(names, broken, enriched, globalReads)

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

        val backend = TapirStubInterpreter(interceptors, BackendStub[IO](summon))
          .whenServerEndpointsRunLogic(routes)
          .backend()

        (backend, port)
      }
    )

  // -----------------------------------------------------------------------------------------------
  // Speaking to it
  // -----------------------------------------------------------------------------------------------

  /** Thirty-two bytes, the shortest key HS256 accepts. */
  private val key: SigningKey =
    SigningKey("test-1", Secret(Array.fill[Byte](32)(7)), Instant.parse("2020-01-01T00:00:00Z"))

  private val codec: PrincipalCodec[IO] =
    JwsPrincipalCodec
      .make[IO](NonEmptyList.of(key), "kui-gateway")
      .getOrElse(throw new IllegalStateException("the test signing key is too short for HS256"))

  private val subjectsPath: String = s"/internal/v1/clusters/${cluster.value}/schemas/subjects"

  /** A token for one request line. The digest covers the method and the path and not the query string
    * (ADR-020), which is why every case here can vary `?pageSize=` against one token.
    */
  private def token(path: String, validFor: FiniteDuration = 60.seconds): IO[SignedPrincipal] =
    IO.realTimeInstant.flatMap(now =>
      codec.sign(
        PrincipalClaims(
          subject = UserName.unsafe("alice"),
          roles = Set.empty,
          kind = PrincipalKind.Session,
          sessionRef = None,
          issuedAt = now,
          expiresAt = now.plusSeconds(validFor.toSeconds),
          audience = SchemaApi.Id,
          requestDigest = RequestDigest.ofRequestLine("GET", path.takeWhile(_ != '?'))
        )
      )
    )

  private def get(backend: Backend[IO], path: String): IO[Response[String]] =
    token(path).flatMap(signed =>
      basicRequest
        .get(Uri.unsafeParse(s"http://schema$path"))
        .header(KuiEndpoint.PrincipalHeader, signed.value)
        .response(asStringAlways)
        .send(backend)
    )

  private def body(response: Response[String]): Json =
    parse(response.body).fold(failure => fail(s"not JSON: ${failure.message} in ${response.body}"), identity)

  // -----------------------------------------------------------------------------------------------

  test("one row's enrichment failing is still a 200 with the bare row in it") {
    // The half of W2-09's acceptance that was never asserted anywhere a client can see. `result.isRight` at
    // the use-case layer says the fold did not fail; only the status code says the page arrived.
    val page = many.take(3)

    server(names = page, broken = Set(page(1))).use { (backend, _) =>
      get(backend, s"$subjectsPath?pageSize=3").map { response =>
        val rows = body(response).hcursor.downField("items")

        assertEquals(response.code.code, 200, response.body)
        assertEquals(rows.as[List[Json]].map(_.size), Right(3))
        // The row is in its place in the sort order, named, with its three facts absent — which is the wire
        // saying nobody found out, and is a different row from one that is missing.
        assertEquals(rows.downN(1).get[String]("subject"), Right(page(1)))
        assertEquals(rows.downN(1).get[Option[String]]("format"), Right(None))
        assertEquals(rows.downN(1).get[Option[Int]]("versionCount"), Right(None))
        assertEquals(rows.downN(0).get[Option[String]]("format"), Right(Some("AVRO")))
      }
    }
  }

  test("a count-only request answers the total and asks the registry nothing else") {
    // The drawer's schema badge. `pageSize=0` is the caller saying it draws no rows, and the answer costs
    // the subject list and nothing else — not the 250 rows, not the one row a `pageSize=1` probe used to
    // enrich, and not the registry-wide compatibility level that a page of any size pays for.
    server().use { (backend, registry) =>
      for {
        response <- get(backend, s"$subjectsPath?pageSize=${SchemaEndpoints.CountOnlyPageSize}")
        enriched <- registry.enriched.get
        globalReads <- registry.globalReads.get
      } yield {
        val json = body(response).hcursor

        assertEquals(response.code.code, 200, response.body)
        assertEquals(json.downField("page").get[Option[Long]]("totalItems"), Right(Some(250L)))
        assertEquals(json.downField("page").get[Int]("pageSize"), Right(0))
        assertEquals(json.downField("items").as[List[Json]].map(_.size), Right(0))
        assertEquals(enriched, Nil)
        assertEquals(globalReads, 0)
      }
    }
  }

  test("a page of one still enriches its row") {
    // The cheap path is a page size of zero and nothing else. A one-row page is a page, and a caller that
    // asked for a row must not be handed a name with three blank cells that mean "nobody found out".
    server().use { (backend, registry) =>
      for {
        response <- get(backend, s"$subjectsPath?pageSize=1")
        enriched <- registry.enriched.get
      } yield {
        assertEquals(response.code.code, 200, response.body)
        assertEquals(
          body(response).hcursor.downField("items").downN(0).get[Option[String]]("format"),
          Right(Some("AVRO"))
        )
        assertEquals(enriched, List(many.head))
      }
    }
  }

  test("a page size above this endpoint's maximum is clamped rather than refused") {
    // `PageSize.Max` is 500 and would be answered with 500 rows and 1500 registry requests. The ceiling this
    // endpoint enforces is its own, and the answer's `pageSize` is where a caller reads what they got.
    //
    // The numbers are literals, and that is the whole point of this case. Until this wave both sides of
    // the comparison were `SchemaEndpoints.MaxPageSize`, so the constant was asserted only against itself
    // and the value **250** shipped green — three quarters of the 500-row registry outage the bound exists
    // to prevent, because a row costs three registry GETs.
    server().use { (backend, registry) =>
      for {
        response <- get(backend, s"$subjectsPath?pageSize=500")
        enriched <- registry.enriched.get
      } yield {
        val json = body(response).hcursor

        assertEquals(response.code.code, 200, response.body)
        assertEquals(json.downField("page").get[Int]("pageSize"), Right(100))
        assertEquals(json.downField("items").as[List[Json]].map(_.size), Right(100))
        // The rows are the promise; the call count is what the ceiling is for.
        assertEquals(enriched.size, 100)
      }
    }
  }

  test("a page size of 250 is clamped to a hundred rows and three hundred registry requests") {
    // 250 is not an arbitrary number: it is the value that shipped green last wave, because
    // `SubjectListRoutesSuite` compared the answer's `pageSize` and its row count to the very constant
    // that produced them. Both numbers below are written out, and the arithmetic beside them is the
    // reason the bound is a hundred rather than the kernel's five hundred:
    //
    //   a row costs three registry GETs (`RegistryHttp.summary`), and the registry is a single-writer JVM
    //   in front of a Kafka topic. 100 rows is 300 requests in 13 rounds of eight
    //   (`SubjectListUseCase.MaxConcurrentRows`). 250 rows is 750, and 500 rows is 1500.
    //
    // So the two bounds are 100 and 500 and they must not be the same number.
    assert(
      SchemaEndpoints.MaxPageSize < kui.kernel.PageSize.Max.value,
      s"${SchemaEndpoints.MaxPageSize} is not below the kernel's ${kui.kernel.PageSize.Max.value}"
    )

    server().use { (backend, registry) =>
      for {
        response <- get(backend, s"$subjectsPath?pageSize=250")
        enriched <- registry.enriched.get
      } yield {
        val json = body(response).hcursor

        assertEquals(response.code.code, 200, response.body)
        assertEquals(json.downField("page").get[Int]("pageSize"), Right(100))
        assertEquals(json.downField("items").as[List[Json]].map(_.size), Right(100))
        assertEquals(enriched.size, 100)
        // Three registry GETs per enriched row, which is what the hundred is a bound on.
        assertEquals(enriched.size * 3, 300)
      }
    }
  }

  test("a negative page size is one row and not a count-only answer") {
    // `pageSize < 0` used to reach the count-only branch through a `<=`, which made `?pageSize=-1` answer
    // a total with no rows: a wire behaviour the published parameter description did not mention and no
    // case covered, so a caller with an off-by-one got silence instead of rows. Below the range is now
    // clamped up, which is the rule the page *number* already followed.
    server().use { (backend, registry) =>
      for {
        response <- get(backend, s"$subjectsPath?pageSize=-1")
        enriched <- registry.enriched.get
      } yield {
        val json = body(response).hcursor

        assertEquals(response.code.code, 200, response.body)
        assertEquals(json.downField("page").get[Int]("pageSize"), Right(1))
        assertEquals(json.downField("items").as[List[Json]].map(_.size), Right(1))
        assertEquals(enriched, List(many.head))
      }
    }
  }
}
