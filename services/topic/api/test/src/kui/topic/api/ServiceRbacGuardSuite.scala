package kui.topic.api

import java.time.Instant

import cats.effect.IO
import io.circe.Json
import io.circe.parser.parse
import munit.CatsEffectSuite
import sttp.client4.*

import kui.cache.Snapshot
import kui.kernel.{ClusterId, RoleName, TopicName}
import kui.security.rbac.*
import kui.topic.application.Fresh
import kui.topic.domain.{TopicDetail, TopicSnapshot, TopicSummary}

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
    // a 403 here would tell a caller that topics they may not see exist. The hidden row has to be in the
    // fixture, or a 200 with the one permitted topic cannot tell filtering from returning everything.
    val both = TopicTestServer.online(
      TopicSnapshot.of(
        Vector(topic("payments.orders"), topic("secrets")),
        Instant.parse("2026-09-04T09:00:00Z"),
        incomplete = Map(TopicName.unsafe("secrets.unread") -> "describe failed")
      )
    )

    TopicTestServer.resource(both, rbac = policy("payments\\..*", Action.TopicView)).use { server =>
      for {
        listed <- get(server, path("/topics"), Set(reader))
        names <- get(server, path("/topics/names"), Set(reader))
        stats <- get(server, path("/topics/statistics"), Set(reader))
      } yield {
        val listBody = listed.body.fold(identity, identity)
        val listJson = parse(listBody).fold(failure => fail(failure.message), identity)
        assertEquals(listed.code.code, 200, listBody)
        assertEquals(field(listJson, "topics", "data", "page", "totalItems"), Some(Json.fromInt(1)))
        assertEquals(namesOf(listJson, "topics", "data", "items"), List("payments.orders"))
        assert(!listBody.contains("secrets"), listBody)
        assertEquals(field(listJson, "incompleteTopics"), Some(Json.fromInt(0)))

        val namesBody = names.body.fold(identity, identity)
        assertEquals(names.code.code, 200, namesBody)
        assert(!namesBody.contains("secrets"), namesBody)
        assert(namesBody.contains("payments.orders"), namesBody)

        val statsBody = stats.body.fold(identity, identity)
        val statsJson = parse(statsBody).fold(failure => fail(failure.message), identity)
        assertEquals(stats.code.code, 200, statsBody)
        assertEquals(field(statsJson, "statistics", "data", "topicCount"), Some(Json.fromInt(1)))
        assertEquals(field(statsJson, "statistics", "data", "incompleteTopics"), Some(Json.fromInt(0)))
      }
    }
  }

  private def topic(name: String): TopicSummary =
    TopicSummary(
      name = TopicName.unsafe(name),
      isInternal = false,
      partitionCount = 1,
      replicationFactor = Some(1),
      outOfSyncReplicas = 0,
      offlinePartitions = 0,
      messageCount = Some(0L),
      sizeBytes = Some(0L)
    )

  private def field(json: Json, path: String*): Option[Json] =
    path.foldLeft(Option(json))((current, key) => current.flatMap(_.hcursor.downField(key).focus))

  private def namesOf(json: Json, path: String*): List[String] =
    field(json, path*)
      .flatMap(_.as[List[Json]].toOption)
      .getOrElse(Nil)
      .flatMap(_.hcursor.get[String]("name").toOption)

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
}
