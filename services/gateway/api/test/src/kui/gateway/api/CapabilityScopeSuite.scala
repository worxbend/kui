package kui.gateway.api

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import sttp.capabilities.fs2.Fs2Streams
import sttp.client4.*

import kui.contracts.capability.{CapabilityKey, CapabilityState}
import kui.gateway.application.capability.{FakeCapabilityRegistry, Trigger}
import kui.kernel.{ClusterId, RoleName, ServiceId, UserName}
import kui.security.rbac.{RbacPolicy, Role}
import kui.security.{Principal, PrincipalKind}
import kui.testkit.fakes.FakeStructuredLogger

final class CapabilityScopeSuite extends munit.CatsEffectSuite {
  private val allowed = ClusterId.unsafe("allowed")
  private val hidden = ClusterId.unsafe("secret-cluster")
  private val role = RoleName.unsafe("reader")
  private val policy = RbacPolicy(List(Role(role, Set(allowed), Nil, Nil)), None)
  private val reader = Principal(UserName.unsafe("alice"), Set(role), PrincipalKind.Session)
  private val service = ServiceId.unsafe("cluster")

  private def fixture = for {
    registry <- Resource.eval(FakeCapabilityRegistry[IO])
    _ <- Resource.eval(
      List(allowed, hidden).traverse_(id =>
        registry.report(CapabilityKey(service, Some(id)), CapabilityState.NotConfigured, Some(id.value))
      )
    )
    logger <- Resource.eval(FakeStructuredLogger[IO])
    trigger = new Trigger[IO] { def probe(id: ServiceId): IO[Unit] = IO.unit }
    routes = CapabilityRoutes[IO](registry, trigger, GatewayTestServer.noTelemetry, logger, rbac = policy)
    server <- GatewayTestServer.resource(extraRoutes = routes, rbac = policy)
  } yield server

  List(reader, Principal.Anonymous).foreach { principal =>
    test(s"snapshot and SSE snapshot/deltas filter clusters for ${principal.name.value}") {
      fixture.use { server =>
        for {
          now <- IO.realTimeInstant
          session <- server.sessions.create(principal, now)
          cookie = s"kui_session=${session.id.value}"
          snapshot <- server.get("/api/v1/capabilities", Map("Cookie" -> cookie))
          response <- basicRequest
            .get(server.at("/api/v1/capabilities/stream"))
            .header("Cookie", cookie)
            .response(asStreamAlwaysUnsafe(Fs2Streams[IO]))
            .send(server.backend)
          stream <- response.body.through(fs2.text.utf8.decode).compile.string
        } yield {
          assertEquals(snapshot.code.code, 200)
          assert(!snapshot.body.contains(hidden.value), snapshot.body)
          assert(!stream.contains(hidden.value), stream)
          if principal == reader then {
            assert(snapshot.body.contains(allowed.value), snapshot.body)
            // One opening snapshot and one visible delta from the fake's history.
            assertEquals(stream.sliding("event: capabilities".length).count(_ == "event: capabilities"), 2)
          } else {
            assert(!snapshot.body.contains(allowed.value), snapshot.body)
            assert(!stream.contains(allowed.value), stream)
          }
        }
      }
    }
  }
}
