package kui.ksql.api

import java.time.Instant

import scala.concurrent.duration.{DurationInt, FiniteDuration}

import cats.data.NonEmptyList
import cats.effect.kernel.Resource
import cats.effect.{IO, Ref}
import fs2.Stream
import io.circe.Json
import io.circe.parser.parse
import org.typelevel.otel4s.metrics.MeterProvider
import sttp.capabilities.fs2.Fs2Streams
import sttp.client4.*
import sttp.client4.impl.cats.implicits.*
import sttp.client4.testing.StreamBackendStub
import sttp.model.Uri
import sttp.tapir.server.stub4.TapirStreamStubInterpreter

import kui.http.principal.{PrincipalVerification, RbacGuard}
import kui.kernel.error.KuiError
import kui.kernel.{ClusterId, RoleName, Secret, UserName}
import kui.ksql.application.*
import kui.ksql.domain.*
import kui.security.*
import kui.security.rbac.{ClusterFlags, RbacPolicy}
import kui.testkit.fakes.FakeStructuredLogger

/** This service's routes over Tapir's stub interpreter: the real interceptor chain, the real principal
  * verification and the real permission guard, with no socket.
  *
  * A bound port would add seconds per case and prove only that Netty works, which `libs/http` proves once for
  * every service. What cannot be proved anywhere else is the *order* — that a caller without `KSQL:EXECUTE`
  * is refused before the ksqlDB server is called — and the order only exists once the guard, the route and
  * the use case are assembled the way the composition root assembles them.
  *
  * `TapirStreamStubInterpreter` rather than the plain one, because this service's route list is typed on
  * `Fs2Streams`: the push query is in it, and a list that had to leave it out would be a list that is not
  * what the composition root serves.
  */
object KsqlTestServer {

  val cluster: ClusterId = ClusterId.unsafe("prod-eu")
  val readOnly: ClusterId = ClusterId.unsafe("prod-us")
  val bare: ClusterId = ClusterId.unsafe("no-ksql")

  val at: Instant = Instant.parse("2020-01-01T00:00:00Z")

  /** A role that may look and a role that may run. Two rather than one, because the rule this packet owns is
    * exactly the difference between them.
    */
  val viewer: RoleName = RoleName.unsafe("ksql-viewer")
  val operator: RoleName = RoleName.unsafe("ksql-operator")

  val orders: KsqlObject = KsqlObject.Stream("ORDERS", "orders", Some("JSON"))
  val users: KsqlObject = KsqlObject.Table("USERS", "users", Some("AVRO"), windowed = false)

  val listing: KsqlObjects = KsqlObjects.of(List(orders, users), Nil)

  /** A ksqlDB that answers what it was told to and records **every** call it was asked to make.
    *
    * That counter is the whole point of this fixture: a stub that answered a constant could not tell a caller
    * who was refused from one who was never allowed to reach it, and "refused before the server is called" is
    * a statement about which of those happened.
    */
  final class CountingServer(
      val executed: Ref[IO, List[String]],
      val reads: Ref[IO, Int],
      val opened: Ref[IO, List[String]]
  ) extends KsqlClient[IO] {

    def objects: IO[Either[KuiError, KsqlObjects]] = reads.update(_ + 1).as(Right(listing))

    def destructiveTarget(name: String, kind: KsqlObjectKind): IO[Either[KuiError, Option[KsqlObject]]] =
      IO.pure(Right(listing.items.filter(item => item.name == name && item.kind == kind) match {
        case item :: Nil => Some(item)
        case _ => None
      }))

    def execute(statement: KsqlStatement): IO[Either[KuiError, StatementOutcome]] =
      executed
        .update(_ :+ statement.canonical)
        .as(Right(StatementOutcome.Status("Stream created and running", None)))

    def rows(statement: KsqlStatement): Stream[IO, Either[KuiError, QueryFrame]] =
      Stream.exec(opened.update(_ :+ statement.canonical)) ++
        Stream.emits(
          List(
            Right(QueryFrame.Header(List("ID"))),
            Right(QueryFrame.Row(QueryRow(List(Some("17")))))
          )
        )
  }

  final class Source(clients: Map[ClusterId, KsqlClient[IO]]) extends ClusterKsqlSource[IO] {

    private val views = List(
      KsqlProfileView(cluster, "Production EU", readOnly = false, configured = true),
      KsqlProfileView(KsqlTestServer.readOnly, "Production US", readOnly = true, configured = true),
      KsqlProfileView(bare, "Staging", readOnly = false, configured = false)
    )

    def profileOf(id: ClusterId): IO[Either[KuiError, KsqlProfileView]] =
      IO.pure(
        views
          .find(_.cluster == id)
          .toRight(
            kui.kernel.error.ApplicationError
              .NotFound("cluster", id.value, kui.kernel.error.ErrorCode.ClusterNotFound): KuiError
          )
      )

    def all: IO[List[KsqlProfileView]] = IO.pure(views)

    def client(id: ClusterId): IO[Option[KsqlClient[IO]]] = IO.pure(clients.get(id))
  }

  /** Thirty-two bytes, which is the shortest key HS256 accepts. */
  private val key: SigningKey =
    SigningKey("test-1", Secret(Array.fill[Byte](32)(7)), Instant.parse("2020-01-01T00:00:00Z"))

  val codec: PrincipalCodec[IO] =
    JwsPrincipalCodec
      .make[IO](NonEmptyList.of(key), "kui-gateway")
      .getOrElse(throw new IllegalStateException("the test signing key is too short for HS256"))

  final case class Rig(backend: StreamBackend[IO, Fs2Streams[IO]], server: CountingServer)

  /** The service, with the deployment's policy and a ksqlDB behind every configured cluster. */
  def resource(rbac: RbacPolicy = RbacPolicy.Disabled): Resource[IO, Rig] =
    Resource.eval(
      for {
        logger <- FakeStructuredLogger[IO]
        meter <- MeterProvider.noop[IO].get("kui.ksql")
        rejections <- PrincipalVerification.rejectionCounter[IO](meter)
        interceptors <- KsqlApi.interceptors[IO](kui.observability.Telemetry.noop[IO], rejections, logger)
        executed <- Ref.of[IO, List[String]](Nil)
        reads <- Ref.of[IO, Int](0)
        opened <- Ref.of[IO, List[String]](Nil)
        server = new CountingServer(executed, reads, opened)
        sources = new Source(Map(cluster -> (server: KsqlClient[IO]), readOnly -> (server: KsqlClient[IO])))
        sink = KsqlStatementSink.noop[IO]
        tokens = KsqlPlanToken.make[IO](Secret(Array.fill[Byte](32)(3)))
        guard = MutationGuard.make[IO](sources, sink, logger)
        useCases = KsqlUseCases.make[IO](sources, tokens, guard)
        permissions = RbacGuard.fromPolicy[IO](rbac, id => ClusterFlags(id == readOnly), logger)
        routes = KsqlApi.routes[IO](
          useCases,
          Nil,
          KsqlCapabilities.make[IO](sources, logger),
          codec,
          rejections,
          logger,
          permissions,
          kui.observability.Telemetry.noop[IO]
        )
      } yield Rig(
        TapirStreamStubInterpreter(interceptors, StreamBackendStub[IO, Fs2Streams[IO]](summon))
          .whenServerEndpointsRunLogic(routes)
          .backend(),
        server
      )
    )

  def objectsPath(id: ClusterId): String = s"/internal/v1/clusters/${id.value}/ksql/objects"

  def statementsPath(id: ClusterId): String = s"/internal/v1/clusters/${id.value}/ksql/statements"

  def planPath(id: ClusterId): String = s"${statementsPath(id)}/plan"

  def streamPath(id: ClusterId): String = s"/internal/v1/clusters/${id.value}/ksql/stream"

  /** `http://ksql<path>`, parsed rather than interpolated: sttp's `uri` interpolator escapes an embedded
    * string as one segment, which would turn every path here into a single literal and every request into a
    * 404 that had nothing to do with the case.
    */
  def address(path: String): Uri = Uri.unsafeParse(s"http://ksql$path")

  /** A token for one request line. The digest covers the method, the *path* and — for a bodied request — the
    * body. A query string is outside it (ADR-020).
    */
  def token(
      method: String,
      path: String,
      roles: Set[RoleName] = Set.empty,
      body: Option[String] = None,
      validFor: FiniteDuration = 60.seconds
  ): IO[SignedPrincipal] =
    IO.realTimeInstant.flatMap(now =>
      codec.sign(
        PrincipalClaims(
          subject = UserName.unsafe("alice"),
          roles = roles,
          kind = PrincipalKind.Session,
          sessionRef = None,
          issuedAt = now,
          expiresAt = now.plusSeconds(validFor.toSeconds),
          audience = KsqlApi.Id,
          requestDigest = body match {
            case None => RequestDigest.ofRequestLine(method, path.takeWhile(_ != '?'))
            case Some(bytes) =>
              RequestDigests.of(method, path.takeWhile(_ != '?'), bytes.getBytes("UTF-8"))
          }
        )
      )
    )

  /** A stream response, drained to a string.
    *
    * `asStreamAlwaysUnsafe` rather than `asStringAlways`, because the body of a streaming endpoint is an
    * `fs2.Stream` and the stub backend hands it over as one; reading it as a string would ask the backend to
    * convert a capability it was given. Draining it is safe here because every stream this suite opens ends —
    * `Sse` promises exactly one terminal event — and a stream that did not would hang the case rather than
    * pass it, which is the right failure.
    */
  def stream(rig: Rig, path: String, roles: Set[RoleName] = Set.empty): IO[(Int, String)] =
    token("GET", path, roles).flatMap(signed =>
      basicRequest
        .get(address(path))
        .header(kui.contracts.KuiEndpoint.PrincipalHeader, signed.value)
        .response(asStreamAlwaysUnsafe(Fs2Streams[IO]))
        .send(rig.backend)
        .flatMap(response =>
          response.body.through(fs2.text.utf8.decode).compile.string.map(response.code.code -> _)
        )
    )

  def get(rig: Rig, path: String, roles: Set[RoleName] = Set.empty): IO[Response[String]] =
    token("GET", path, roles).flatMap(signed =>
      basicRequest
        .get(address(path))
        .header(kui.contracts.KuiEndpoint.PrincipalHeader, signed.value)
        .response(asStringAlways)
        .send(rig.backend)
    )

  /** A statement, as a request body. The body is hashed into the digest, which is what makes the statement
    * itself covered by the signature rather than left outside it.
    */
  def post(
      rig: Rig,
      path: String,
      statement: String,
      confirmation: Option[String] = None,
      roles: Set[RoleName] = Set.empty
  ): IO[Response[String]] = {
    val body = io.circe.Printer.noSpaces.print(
      summon[io.circe.Encoder[kui.ksql.contract.dto.StatementRequestDto]](
        kui.ksql.contract.dto.StatementRequestDto(statement, confirmation)
      )
    )

    token("POST", path, roles, Some(body)).flatMap(signed =>
      basicRequest
        .post(address(path))
        .header(kui.contracts.KuiEndpoint.PrincipalHeader, signed.value)
        .header(kui.contracts.HttpHeaders.Csrf, "test-csrf")
        .header("Content-Type", "application/json")
        .body(body)
        .response(asStringAlways)
        .send(rig.backend)
    )
  }

  def body(response: Response[String]): Json =
    parse(response.body).fold(
      failure => sys.error(s"not JSON: ${failure.message} in ${response.body}"),
      identity
    )
}
