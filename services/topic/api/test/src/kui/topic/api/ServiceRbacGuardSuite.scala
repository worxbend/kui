package kui.topic.api

import java.nio.charset.StandardCharsets
import java.time.Instant

import cats.effect.IO
import io.circe.syntax.*
import munit.CatsEffectSuite
import sttp.client4.*

import kui.cache.Snapshot
import kui.contracts.{HttpHeaders, KuiEndpoint}
import kui.kernel.{ClusterId, RoleName, TopicName, UserName}
import kui.security.rbac.*
import kui.security.{PrincipalClaims, PrincipalKind, RequestDigests}
import kui.topic.application.Fresh
import kui.topic.contract.dto.CreateTopicRequest
import kui.topic.domain.{CreatedTopic, TopicDetail, TopicSnapshot, TopicSummary}

/** The service refusing on its own account.
  *
  * ==Why this suite exists at all==
  *
  * The gateway already checks permissions, and it does so on the same declaration this service reads, so it
  * is fair to ask what a second check buys. The answer is the case this suite constructs: a request that
  * never went through the gateway. A KUI service listens on its own port and verifies a signed principal;
  * anything on the same network that can reach the port and hold a signing key can call it. If the only check
  * lived at the edge, the topic service would be open to whatever else runs beside it.
  *
  * So every request below goes straight to the service's own routes, with a valid principal and no gateway
  * anywhere, which is exactly the shape of the attack the second check exists for.
  */
final class ServiceRbacGuardSuite extends CatsEffectSuite {

  import TopicTestServer.{path, uri}

  private val cluster: ClusterId = TopicTestServer.Cluster
  private val reader: RoleName = RoleName.unsafe("reader")

  private def snapshot: Snapshot[TopicSnapshot] =
    TopicTestServer.online(
      TopicSnapshot.of(
        Vector(
          TopicSummary(
            name = TopicName.unsafe("payments.orders"),
            isInternal = false,
            partitionCount = 1,
            replicationFactor = Some(1),
            outOfSyncReplicas = 0,
            offlinePartitions = 0,
            messageCount = Some(0L),
            sizeBytes = Some(0L)
          )
        ),
        Instant.parse("2026-09-04T09:00:00Z")
      )
    )

  /** A policy granting `reader` the named actions over topics matching `pattern`, on this cluster only. */
  private def policy(pattern: String, actions: Action*): RbacPolicy =
    RbacPolicy(
      roles = List(
        Role(
          name = reader,
          clusters = Set(cluster),
          subjects = Nil,
          permissions = List(
            RbacPolicy.permission(
              Resource.Topic,
              Some(ResourcePattern.compile(pattern).getOrElse(fail(s"'$pattern' does not compile"))),
              actions.toSet
            )
          )
        )
      ),
      defaultRole = None
    )

  private def get(
      server: TopicTestServer,
      at: String,
      roles: Set[RoleName]
  ): IO[Response[Either[String, String]]] =
    TopicTestServer
      .token(at, roles = roles)
      .flatMap(token =>
        basicRequest
          .get(uri"${uri(at)}")
          .header(kui.contracts.KuiEndpoint.PrincipalHeader, token.value)
          .send(server.backend)
      )

  test("a topic the caller's pattern does not match is refused by the service itself") {
    TopicTestServer
      .resource(
        snapshot,
        detail = Right(
          Fresh.Live(TopicDetail.of(TopicName.unsafe("secrets"), isInternal = false, partitions = Nil))
        ),
        rbac = policy("payments\\..*", Action.TopicView)
      )
      .use { server =>
        get(server, path("/topics/secrets"), Set(reader)).map { response =>
          assertEquals(response.code.code, 403, response.body.fold(identity, identity))
          // The refusal says nothing about which permission was missing or which pattern was checked.
          val body = response.body.fold(identity, identity)
          assert(body.contains("KUI-FORBIDDEN"), body)
          assert(!body.contains("payments"), s"the refusal leaked the pattern: $body")
        }
      }
  }

  test("a topic the caller's pattern does match is served") {
    TopicTestServer
      .resource(
        snapshot,
        detail = Right(
          Fresh.Live(
            TopicDetail.of(TopicName.unsafe("payments.orders"), isInternal = false, partitions = Nil)
          )
        ),
        rbac = policy("payments\\..*", Action.TopicView)
      )
      .use { server =>
        get(server, path("/topics/payments.orders"), Set(reader)).map { response =>
          assertEquals(response.code.code, 200, response.body.fold(identity, identity))
        }
      }
  }

  test("a valid principal holding no role at all is refused, however it reached this port") {
    TopicTestServer
      .resource(
        snapshot,
        detail = Right(
          Fresh.Live(
            TopicDetail.of(TopicName.unsafe("payments.orders"), isInternal = false, partitions = Nil)
          )
        ),
        rbac = policy("payments\\..*", Action.TopicView)
      )
      .use { server =>
        get(server, path("/topics/payments.orders"), Set.empty).map { response =>
          assertEquals(response.code.code, 403, response.body.fold(identity, identity))
        }
      }
  }

  test("a list endpoint is reachable by anyone who may reach the cluster, and filters instead of refusing") {
    // ADR-021's rule, and the reason a list declares cluster scope rather than a per-topic permission:
    // a 403 here would tell a caller that topics they may not see exist.
    TopicTestServer
      .resource(snapshot, rbac = policy("payments\\..*", Action.TopicView))
      .use { server =>
        get(server, path("/topics"), Set(reader)).map { response =>
          assertEquals(response.code.code, 200, response.body.fold(identity, identity))
        }
      }
  }

  test("a deployment with no roles configured is unaffected") {
    TopicTestServer
      .resource(
        snapshot,
        detail =
          Right(Fresh.Live(TopicDetail.of(TopicName.unsafe("secrets"), isInternal = false, partitions = Nil)))
      )
      .use { server =>
        get(server, path("/topics/secrets"), Set.empty).map { response =>
          assertEquals(response.code.code, 200, response.body.fold(identity, identity))
        }
      }
  }

  /** A create, signed over its body the way the gateway signs it. */
  private def create(server: TopicTestServer, name: String, roles: Set[RoleName]): IO[(Int, String)] = {
    val at = path("/topics")
    val body = CreateTopicRequest(TopicName.unsafe(name), Some(1), Some(1), Map.empty).asJson.noSpaces

    IO.realTimeInstant
      .flatMap(now =>
        TopicTestServer.codec.sign(
          PrincipalClaims(
            subject = UserName.unsafe("alice"),
            roles = roles,
            kind = PrincipalKind.Session,
            sessionRef = None,
            issuedAt = now,
            expiresAt = now.plusSeconds(60),
            audience = TopicApi.Id,
            requestDigest = RequestDigests.of("POST", at, body.getBytes(StandardCharsets.UTF_8))
          )
        )
      )
      .flatMap(token =>
        basicRequest
          .post(uri"${uri(at)}")
          .header(KuiEndpoint.PrincipalHeader, token.value)
          .header(HttpHeaders.Csrf, "test-csrf")
          .contentType("application/json")
          .body(body)
          .response(asStringAlways)
          .send(server.backend)
      )
      .map(response => (response.code.code, response.body))
  }

  private def created(name: String): CreatedTopic =
    CreatedTopic(cluster, TopicName.unsafe(name), Some(1), Some(1))

  test("a create naming a topic outside the caller's pattern is refused, though they may create others") {
    // The name is in the body, so the gateway can only check that the caller may create *some* topic.
    // This is the check that has the name.
    TopicTestServer
      .resource(
        snapshot,
        created = Right(created("secrets")),
        rbac = policy("payments\\..*", Action.TopicCreate)
      )
      .use { server =>
        create(server, "secrets", Set(reader)).map { (status, body) =>
          assertEquals(status, 403, body)
          assert(body.contains("KUI-FORBIDDEN"), body)
        }
      }
  }

  test("a create naming a topic inside the caller's pattern is served") {
    TopicTestServer
      .resource(
        snapshot,
        created = Right(created("payments.refunds")),
        rbac = policy("payments\\..*", Action.TopicCreate)
      )
      .use { server =>
        create(server, "payments.refunds", Set(reader)).map { (status, body) =>
          assertEquals(status, 200, body)
        }
      }
  }
}
