package kui.cluster.api

import cats.effect.IO
import cats.effect.kernel.Resource
import fs2.Stream
import io.circe.parser.parse
import munit.CatsEffectSuite
import org.typelevel.otel4s.oteljava.testkit.OtelJavaTestkit
import sttp.capabilities.fs2.Fs2Streams
import sttp.client4.*
import sttp.client4.impl.cats.implicits.*
import sttp.client4.testing.StreamBackendStub
import sttp.model.Uri
import sttp.tapir.server.stub4.TapirStreamStubInterpreter

import kui.cluster.application.{ClusterRegistry, RegistrySnapshot, RegistryVersion}
import kui.cluster.contract.ProfileEndpoints
import kui.cluster.domain.{ClusterProfile, StoreHealth}
import kui.contracts.KuiEndpoint
import kui.http.principal.RbacGuard
import kui.observability.Telemetry
import kui.security.*
import kui.testkit.fakes.FakeStructuredLogger

/** That a service fetching a cluster's settings gets them, gets told when they have not changed, and never
  * gets a credential.
  */
final class ProfileRoutesSuite extends CatsEffectSuite {

  private val profile = ClusterFixtures.profile()
  private val path = "/internal/v1/clusters/prod-eu/profile"

  private def server(
      registry: ClusterRegistry[IO] = new ClusterFixtures.StubRegistry(List(profile))
  ): Resource[IO, ClusterTestServer] =
    OtelJavaTestkit.inMemory[IO]().evalMap { testkit =>
      val telemetry = Telemetry.fromProviders(testkit.tracerProvider, testkit.meterProvider)
      val codec = PrincipalCodec.inProcess[IO]

      for {
        logger <- FakeStructuredLogger[IO]
        rejections <- ClusterTestServer.rejectionCounter(testkit)
        interceptors <- ClusterApi.interceptors[IO](telemetry, rejections, logger)
      } yield ClusterTestServer(
        TapirStreamStubInterpreter(interceptors, StreamBackendStub[IO, Fs2Streams[IO]](summon))
          .whenServerEndpointsRunLogic(
            ProfileRoutes[IO](registry, codec, rejections, telemetry, logger, RbacGuard.allowAll[IO])
          )
          .backend(),
        logger,
        codec,
        testkit
      )
    }

  private def get(
      service: ClusterTestServer,
      at: String,
      ifNoneMatch: Option[String] = None
  ): IO[Response[String]] =
    IO.realTimeInstant.flatMap { now =>
      service.principals
        .sign(
          PrincipalClaims(
            subject = kui.kernel.UserName.unsafe("kui-topic"),
            roles = Set.empty,
            kind = PrincipalKind.Session,
            sessionRef = None,
            issuedAt = now,
            expiresAt = now.plusSeconds(60L),
            audience = kui.cluster.application.ClusterService.Id,
            requestDigest = RequestDigest.ofRequestLine("GET", at)
          )
        )
        .flatMap { token =>
          val base = basicRequest
            .get(Uri.unsafeParse(s"http://cluster$at"))
            .header(KuiEndpoint.PrincipalHeader, token.value)
            .response(asStringAlways)

          ifNoneMatch
            .fold(base)(tag => base.header(ProfileEndpoints.IfNoneMatchHeader, tag))
            .send(service.backend)
        }
    }

  test("theProfileIsAnswerableWithAnEtagThatIsItsStoreVersion") {
    server().use(get(_, path)).map { response =>
      val json = parse(response.body).getOrElse(fail(response.body)).hcursor

      assertEquals(response.code.code, 200, response.body)
      assertEquals(response.header("ETag"), Some("\"7\""))
      assertEquals(json.get[Long]("version"), Right(7L))
      assertEquals(json.get[String]("id"), Right("prod-eu"))
    }
  }

  test("aCallerHoldingTheCurrentVersionIsToldSoWithNoBody") {
    server().use(get(_, path, ifNoneMatch = Some("\"7\""))).map { response =>
      assertEquals(response.code.code, 304, response.body)
      assertEquals(response.header("ETag"), Some("\"7\""))
      assertEquals(response.body, "")
    }
  }

  test("aCallerHoldingAnOlderVersionGetsTheProfile") {
    server().use(get(_, path, ifNoneMatch = Some("\"6\""))).map { response =>
      assertEquals(response.code.code, 200, response.body)
    }
  }

  test("aWildcardAlwaysFetches") {
    // On a read, the only useful reading of `*` is "unconditional": answering 304 to it would leave a
    // caller that has nothing with nothing.
    server().use(get(_, path, ifNoneMatch = Some("*"))).map { response =>
      assertEquals(response.code.code, 200, response.body)
    }
  }

  test("theProfileCarriesTheSecurityMaterial") {
    // The inversion ADR-046 makes. Until M2 this asserted the opposite, and correctly: there was no
    // consumer that built a Kafka client from this document. There is one now, and a SCRAM password that
    // did not survive the route is a topic service that cannot connect.
    //
    // The assertion that used to live here has not been dropped — it moved to `SecretLeakSuite`, which
    // asserts over *every* declared endpoint that this is the only one a credential reaches.
    server().use(get(_, path)).map { response =>
      assertEquals(response.code.code, 200, response.body)
      assert(response.body.contains(ClusterFixtures.Canary), response.body)
      assert(response.body.contains("ssl.truststore.password"), response.body)
    }
  }

  test("anUnknownClusterIsFourOhFour") {
    server(registry = new ClusterFixtures.StubRegistry(Nil)).use(get(_, path)).map { response =>
      assertEquals(response.code.code, 404, response.body)
      assert(response.body.contains("KUI-CLUSTER-NOT-FOUND"), response.body)
    }
  }

  // -----------------------------------------------------------------------------------------------
  // The change stream
  // -----------------------------------------------------------------------------------------------

  /** A registry snapshot at the given generation. The generation is the registry's own counter and is not
    * what a consumer compares - each profile carries its own store version, which is what moves.
    */
  private def snapshotOf(profiles: List[ClusterProfile], generation: Int): RegistrySnapshot =
    RegistrySnapshot(
      profiles.map(p => p.id -> p).toMap,
      List.fill(generation)(()).foldLeft(RegistryVersion.Initial)((version, _) => version.next),
      StoreHealth.Online,
      ClusterFixtures.At
    )

  test("streamFiltersBothUpdatesAndRemovalsByPrincipalScope") {
    import kui.security.rbac.{ClusterFlags, RbacPolicy, Role}
    import kui.kernel.{ClusterId, RoleName, UserName}
    val hidden = ClusterFixtures.profile(id = ClusterId.unsafe("secret-cluster"))
    val role = RoleName.unsafe("reader")
    val policy = RbacPolicy(List(Role(role, Set(profile.id), Nil, Nil)), None)
    val caller = Principal(UserName.unsafe("alice"), Set(role), PrincipalKind.Session)
    val registry = new ClusterFixtures.StubRegistry(
      List(profile, hidden),
      published =
        Some(Stream.emits(List(snapshotOf(Nil, 1), snapshotOf(List(profile, hidden), 2), snapshotOf(Nil, 3))))
    )
    for {
      logger <- FakeStructuredLogger[IO]
      guard = RbacGuard.fromPolicy[IO](policy, _ => ClusterFlags.Writable, logger)
      visible <- ProfileRoutes.visibleChanges(registry, caller, guard).compile.toList
      anonymous <- ProfileRoutes.visibleChanges(registry, Principal.Anonymous, guard).compile.toList
    } yield {
      assertEquals(
        visible.map(_.data.hcursor.get[String]("id")),
        List(Right(profile.id.value), Right(profile.id.value))
      )
      assertEquals(
        visible.map(_.data.hcursor.get[String]("change")),
        List(Right("updated"), Right("removed"))
      )
      assertEquals(anonymous, Nil)
    }
  }

  test("anEditedProfileBecomesOneUpdatedEvent") {
    val edited = ClusterFixtures.profile(version = 8L)
    val events = ProfileRoutes.diff(
      snapshotOf(List(profile), 1),
      snapshotOf(List(edited), 2),
      ClusterFixtures.At
    )

    assertEquals(events.map(_.name), List("clusters"))
    assertEquals(events.head.data.hcursor.get[String]("change"), Right("updated"))
    assertEquals(events.head.data.hcursor.get[Long]("version"), Right(8L))
  }

  test("aProfileThatDidNotChangeProducesNoEvent") {
    // The registry republishes a whole snapshot on every reload, including the ones that resolved to the
    // same thing. Forwarding those would make every consumer rebuild its Kafka clients on a schedule.
    assertEquals(
      ProfileRoutes.diff(snapshotOf(List(profile), 1), snapshotOf(List(profile), 2), ClusterFixtures.At),
      Nil
    )
  }

  test("aRemovedClusterIsItsOwnEventRatherThanSilence") {
    // A cluster an operator deleted has to make every consumer drop its clients. "I have heard nothing
    // about it" is indistinguishable from a healthy quiet cluster.
    val events =
      ProfileRoutes.diff(snapshotOf(List(profile), 1), snapshotOf(Nil, 2), ClusterFixtures.At)

    assertEquals(events.size, 1)
    assertEquals(events.head.data.hcursor.get[String]("change"), Right("removed"))
    assertEquals(events.head.data.hcursor.get[String]("id"), Right("prod-eu"))
  }

  test("theFirstSnapshotProducesNoEvents") {
    // It is the state a subscriber has just fetched, not a change to it. Emitting it would make every
    // consumer re-fetch every profile the moment it connected.
    val registry = new ClusterFixtures.StubRegistry(List(profile))

    ProfileRoutes.changes[IO](registry).compile.toList.map(events => assertEquals(events, Nil))
  }

  test("aChangeAfterTheFirstSnapshotIsForwarded") {
    val edited = ClusterFixtures.profile(version = 9L)
    val registry = new ClusterFixtures.StubRegistry(
      List(profile),
      published = Some(Stream.emits(List(snapshotOf(List(profile), 1), snapshotOf(List(edited), 2))))
    )

    ProfileRoutes.changes[IO](registry).compile.toList.map { events =>
      assertEquals(events.map(_.name), List("clusters"))
      assertEquals(events.head.data.hcursor.get[Long]("version"), Right(9L))
    }
  }

  test("everyChangeCarriesAnIdAVersionAKindAndATime, and no profile") {
    val edited = ClusterFixtures.profile(version = 8L)
    val event =
      ProfileRoutes.diff(snapshotOf(List(profile), 1), snapshotOf(List(edited), 2), ClusterFixtures.At).head

    assertEquals(
      event.data.asObject.map(_.keys.toList.sorted),
      Some(List("at", "change", "id", "version"))
    )
    assert(!event.data.noSpaces.contains(ClusterFixtures.Canary), event.data.noSpaces)
  }

  test("theStreamPathIsTheOneTheEndpointDeclares") {
    assertEquals(ClusterStreamEndpoint.StreamPath, "/internal/v1/clusters/stream")
  }
}
