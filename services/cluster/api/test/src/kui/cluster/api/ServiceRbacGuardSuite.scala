package kui.cluster.api

import cats.effect.IO
import munit.CatsEffectSuite
import sttp.client4.*

import kui.contracts.KuiEndpoint
import kui.kernel.RoleName
import kui.security.RequestDigest
import kui.security.rbac.*

/** The cluster service refusing on its own account.
  *
  * ==Why this suite exists at all==
  *
  * The gateway already checks permissions, on the same declaration this service reads, so it is fair to ask
  * what a second check buys. The answer is the case every request below constructs: a call that never went
  * through the gateway. A KUI service listens on its own port and trusts a signed principal; anything on the
  * same network that can reach that port and hold a signing key can call it. If the only check lived at the
  * edge, the cluster service — which is the one that can hand back broker settings — would be open to
  * whatever else runs beside it.
  *
  * So every request here goes straight to this service's own routes, with a valid principal and no gateway
  * anywhere. That is the shape of the attack ADR-021's second check exists for, and the only shape in which
  * the second check can be observed at all.
  */
final class ServiceRbacGuardSuite extends CatsEffectSuite {

  import ClusterTestServer.{Cluster, resource, token}

  private val reader: RoleName = RoleName.unsafe("reader")

  private val configsPath: String = s"/internal/v1/clusters/${Cluster.value}/brokers/1/configs"

  /** A policy granting `reader` the named actions over one resource, on this cluster only. */
  private def policy(resource: Resource, actions: Action*): RbacPolicy =
    RbacPolicy(
      roles = List(
        Role(
          name = reader,
          clusters = Set(Cluster),
          subjects = Nil,
          permissions = List(RbacPolicy.permission(resource, None, actions.toSet))
        )
      ),
      defaultRole = None
    )

  private def get(
      server: ClusterTestServer,
      at: String,
      roles: Set[RoleName]
  ): IO[Response[String]] =
    token(digest = RequestDigest.ofRequestLine("GET", at), roles = roles).flatMap(principal =>
      basicRequest
        .get(uri"${s"http://cluster$at"}")
        .header(KuiEndpoint.PrincipalHeader, principal.value)
        .response(asStringAlways)
        .send(server.backend)
    )

  test("a caller without ClusterConfig.View is refused broker settings by the service itself") {
    // The role exists and is scoped to this cluster, so the cluster gate passes; what it lacks is the
    // one action the endpoint declares. Nothing about this request went near the gateway.
    resource(rbac = policy(Resource.Topic, Action.TopicView)).use { server =>
      get(server, configsPath, Set(reader)).map { response =>
        assertEquals(response.code.code, 403, response.body)
        assert(response.body.contains("KUI-FORBIDDEN"), response.body)
      }
    }
  }

  test("a caller holding ClusterConfig.View reaches the use case") {
    // 404 and not 403: the guard let the call through, and the fixture's registry simply has no broker 1.
    // That distinction is the assertion — a 403 here would mean the permission had not been granted.
    resource(rbac = policy(Resource.ClusterConfig, Action.ClusterConfigView)).use { server =>
      get(server, configsPath, Set(reader)).map { response =>
        assertEquals(response.code.code, 404, response.body)
      }
    }
  }

  test("a valid principal holding no role at all is refused, however it reached this port") {
    resource(rbac = policy(Resource.ClusterConfig, Action.ClusterConfigView)).use { server =>
      get(server, configsPath, Set.empty).map { response =>
        assertEquals(response.code.code, 403, response.body)
      }
    }
  }

  test("the cluster list is reachable by anyone the policy admits, and filters instead of refusing") {
    // ADR-021's rule: a 403 on a list would tell a caller that clusters they may not see exist.
    resource(rbac = policy(Resource.ClusterConfig, Action.ClusterConfigView)).use { server =>
      get(server, ClusterTestServer.ClustersPath, Set(reader)).map { response =>
        assertEquals(response.code.code, 200, response.body)
      }
    }
  }

  test("clusterRowsAreFilteredForRestrictedAndRolelessPrincipals") {
    val allowed = ClusterFixtures.profile(id = Cluster, name = "Allowed")
    val hidden = ClusterFixtures.profile(id = kui.kernel.ClusterId.unsafe("secret-cluster"), name = "Hidden")
    resource(
      profiles = List(allowed, hidden),
      rbac = policy(Resource.ClusterConfig, Action.ClusterConfigView)
    ).use { server =>
      for {
        restricted <- get(server, ClusterTestServer.ClustersPath, Set(reader))
        roleless <- get(server, ClusterTestServer.ClustersPath, Set.empty)
      } yield {
        assert(restricted.body.contains("Allowed"), restricted.body)
        assert(!restricted.body.contains("Hidden"), restricted.body)
        assert(!restricted.body.contains("secret-cluster"), restricted.body)
        assert(!roleless.body.contains("Allowed"), roleless.body)
        assert(!roleless.body.contains("Hidden"), roleless.body)
      }
    }
  }

  test("a deployment with no roles configured is unaffected") {
    resource().use { server =>
      get(server, configsPath, Set.empty).map { response =>
        assertEquals(response.code.code, 404, response.body)
      }
    }
  }
}
