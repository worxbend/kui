package kui.cluster.api

import java.time.Instant

import scala.concurrent.duration.{DurationInt, FiniteDuration}

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.kernel.Resource
import fs2.Stream
import io.circe.Json
import io.circe.parser.parse
import org.typelevel.otel4s.metrics.Counter
import org.typelevel.otel4s.oteljava.testkit.OtelJavaTestkit
import sttp.capabilities.fs2.Fs2Streams
import sttp.client4.StreamBackend
import sttp.client4.impl.cats.implicits.*
import sttp.client4.testing.StreamBackendStub
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.interceptor.Interceptor
import sttp.tapir.server.stub4.TapirStreamStubInterpreter

import kui.cluster.application.{
  CapabilityReportUseCase,
  ClusterService,
  MessageBrowserSettings,
  UiAppearance,
  UiSettingsStore,
  UiSettingsUseCase
}
import kui.cluster.domain.ClusterProfile
import kui.http.principal.{PrincipalVerification, RbacGuard}
import kui.kernel.{ClusterId, RoleName, Secret, ServiceId, UserName}
import kui.observability.Telemetry
import kui.security.*
import kui.security.rbac.{ClusterFlags, RbacPolicy}
import kui.testkit.fakes.FakeStructuredLogger

/** The cluster service, assembled the way `services/cluster/app` assembles it, with no socket.
  *
  * Everything under test here — the interceptor order, the principal check, the error envelope, the status
  * code — is decided by the routes and the interceptors, and Tapir's stub interpreter runs exactly those. A
  * bound port would add seconds to every case and would prove one extra thing (that Netty works) which
  * `libs/http` already proves once for all eleven services.
  *
  * The one thing the fixture does *not* fake is the principal codec. It signs real JWS tokens with a real
  * key, because a suite that verified tokens through a fake codec would pass just as happily against a
  * service that had forgotten to check the signature.
  */
final case class ClusterTestServer(
    backend: StreamBackend[IO, Fs2Streams[IO]],
    logger: FakeStructuredLogger[IO],
    principals: PrincipalCodec[IO],
    telemetry: OtelJavaTestkit[IO],
    routes: List[ServerEndpoint[Fs2Streams[IO], IO]] = Nil,
    interceptors: List[Interceptor[IO]] = Nil
)

object ClusterTestServer {

  /** Fixed so a token and an expiry can be reasoned about on paper. */
  val Now: Instant = Instant.parse("2026-09-03T10:11:12Z")

  val Cluster: ClusterId = ClusterId.unsafe("prod-eu")

  /** The endpoint the principal-verification cases drive.
    *
    * Any secured route would do; the cluster list is the cheapest, because it needs no cluster to exist and
    * answers from an empty registry without touching anything.
    */
  val ClustersPath: String = "/internal/v1/clusters"

  val ClustersUri: String = s"http://cluster$ClustersPath"

  /** Thirty-two bytes, which is the shortest key HS256 will accept. */
  private val KeyMaterial: Array[Byte] = Array.fill[Byte](32)(7)

  private val Key: SigningKey =
    SigningKey("test-1", Secret(KeyMaterial), Instant.parse("2020-01-01T00:00:00Z"))

  /** The issuer the codec stamps and checks. It is not a URL and does not have to be: it is a string the two
    * sides of one deployment agree on.
    */
  val Issuer: String = "kui-gateway"

  def codec: PrincipalCodec[IO] =
    JwsPrincipalCodec
      .make[IO](NonEmptyList.of(Key), Issuer)
      .getOrElse(throw new IllegalStateException("the test signing key is too short for HS256"))

  /** A token for `GET /internal/v1/clusters`, good for a minute from now unless the caller says otherwise.
    *
    * The lifetime is relative to the real clock and not to [[Now]] on purpose: expiry is checked against the
    * *service's* clock, and a token minted against a fixed instant would start failing the day the suite is
    * run after that instant has passed. `validFor` is how the expiry cases ask for a token that is already
    * past it.
    */
  def token(
      audience: ServiceId = ClusterService.Id,
      validFor: FiniteDuration = 60.seconds,
      digest: RequestDigest = RequestDigest.ofRequestLine("GET", ClustersPath),
      subject: String = "alice",
      roles: Set[RoleName] = Set.empty
  ): IO[SignedPrincipal] =
    IO.realTimeInstant.flatMap(now =>
      codec.sign(
        PrincipalClaims(
          subject = UserName.unsafe(subject),
          roles = roles,
          kind = PrincipalKind.Session,
          sessionRef = None,
          issuedAt = now,
          expiresAt = now.plusSeconds(validFor.toSeconds),
          audience = audience,
          requestDigest = digest
        )
      )
    )

  /** The whole service, ready to be spoken to.
    *
    * @param configured
    *   whether the deployment knows about a cluster. `false` is the "nothing configured yet" shape, which
    *   must still produce a document rather than an error.
    * @param available
    *   whether the configured cluster can be reached. `false` is the degraded case `/capabilities` exists
    *   for.
    */
  def resource(
      configured: Boolean = true,
      available: Boolean = true,
      profiles: List[ClusterProfile] = Nil,
      // What this service allows on its own account, independently of whatever the gateway decided. It
      // defaults to allowing everything so that a suite about clusters is about clusters; the suite that
      // is about the guard passes a real policy.
      rbac: RbacPolicy = RbacPolicy.Disabled,
      clusterFlags: Map[ClusterId, ClusterFlags] = Map.empty,
      published: Option[Stream[IO, kui.cluster.application.RegistrySnapshot]] = None
  ): Resource[IO, ClusterTestServer] =
    OtelJavaTestkit.inMemory[IO]().evalMap { testkit =>
      for {
        logger <- FakeStructuredLogger[IO]
        rejections <- rejectionCounter(testkit)
        interceptors <- ClusterApi.interceptors[IO](
          Telemetry.fromProviders(testkit.tracerProvider, testkit.meterProvider),
          rejections,
          logger
        )
      } yield {
        val registry = new ClusterFixtures.StubRegistry(profiles, published)
        val uiSettings = new UiSettingsUseCase[IO](
          registry,
          new UiSettingsStore[IO] {
            def get(cluster: ClusterId, principal: Principal) = IO.pure(Right(None))
            def put(cluster: ClusterId, principal: Principal, appearance: UiAppearance) =
              IO.pure(Right(appearance))
            def getMessageBrowser(cluster: ClusterId, principal: Principal) = IO.pure(Right(None))
            def putMessageBrowser(
                cluster: ClusterId,
                principal: Principal,
                settings: MessageBrowserSettings
            ) = IO.pure(Right(settings))
          }
        )
        val routes = ClusterApi.routes[IO](
          registry,
          new ClusterFixtures.StubTopology(Nil),
          new ClusterFixtures.StubBrokers(),
          new ClusterFixtures.StubWrites(),
          new ClusterFixtures.StubProbe(),
          uiSettings,
          capabilities(configured, available),
          Nil,
          codec,
          rejections,
          Telemetry.fromProviders(testkit.tracerProvider, testkit.meterProvider),
          logger,
          RbacGuard.fromPolicy[IO](
            rbac,
            cluster => clusterFlags.getOrElse(cluster, ClusterFlags.Writable),
            logger
          ),
          rbac
        )

        ClusterTestServer(
          TapirStreamStubInterpreter(interceptors, StreamBackendStub[IO, Fs2Streams[IO]](summon))
            .whenServerEndpointsRunLogic(routes)
            .backend(),
          logger,
          codec,
          testkit,
          routes,
          interceptors
        )
      }
    }

  /** A counter a suite can read back, without a whole server around it. */
  def rejectionCounter(testkit: OtelJavaTestkit[IO]): IO[Counter[IO, Long]] =
    testkit.meterProvider.get("kui.cluster").flatMap(PrincipalVerification.rejectionCounter[IO])

  private def capabilities(configured: Boolean, available: Boolean): CapabilityReportUseCase[IO] =
    new CapabilityReportUseCase[IO] {
      def report: IO[kui.cluster.application.CapabilityReport] =
        IO.pure(
          kui.cluster.application.CapabilityReport(
            Map(
              Cluster -> kui.cluster.application
                .ClusterCapabilityReport(configured, Set("CLUSTER_TOPOLOGY"), available)
                // The name an operator wrote. It is on the document because the browser's cluster
                // switcher is drawn from it and from nothing else.
                .copy(name = Some("Production EU"))
            )
          )
        )
    }

  /** An error body with the two fields that legitimately differ between two responses removed.
    *
    * `correlationId` and `timestamp` are different on every request by design. Everything else about a 401
    * must be identical whichever check failed, and this is what lets a suite assert that as an equality
    * rather than field by field.
    */
  def withoutVaryingFields(body: String): Json =
    parse(body)
      .fold(failure => Json.fromString(s"not JSON: ${failure.message} in $body"), identity)
      .mapObject(_.remove("correlationId").remove("timestamp"))
}
