package kui.http.principal

import cats.effect.IO
import munit.CatsEffectSuite
import sttp.tapir.*

import kui.contracts.rbac.{EndpointAuthorization, ResourceRequirement}
import kui.kernel.{ClusterId, RoleName, UserName}
import kui.security.rbac.*
import kui.security.{Principal, PrincipalKind}
import kui.testkit.fakes.FakeStructuredLogger

/** The guard itself, constructed — which nothing in this repository did before.
  *
  * ==Why a suite here and not only in a service==
  *
  * `services/cluster/api`'s `ServiceRbacGuardSuite` drives real routes through a real server and is the
  * better test of the *seam*. What it cannot reach is the branch that fires when the question cannot be
  * formed at all: every endpoint a KUI service publishes carries a declaration, because each contract module
  * has a suite that fails the build if one does not. So `EndpointDecision.decide` never answers `Left` in
  * that suite, and `RbacGuard.fromPolicy`'s fail-closed branch was executed by nothing in the repository —
  * made to return `().asRight`, `./mill services.cluster.api.test` stayed at 752/752.
  *
  * An endpoint with no declaration is exactly what this file constructs, and it is not a contrived shape: it
  * is what a route added next year without one looks like from inside the guard. The rule the branch keeps is
  * the one `EndpointAuthorization.accessRequest`'s own scaladoc states — *"answering 'allowed' because the
  * question could not be formed is how authorization bypasses happen"* — and this is where it is asserted.
  */
final class RbacGuardSuite extends CatsEffectSuite {

  private val Cluster: ClusterId = ClusterId.unsafe("local")
  private val reader: RoleName = RoleName.unsafe("reader")

  private def principal(roles: RoleName*): Principal =
    Principal(UserName.unsafe("tester"), roles.toSet, PrincipalKind.Session)

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

  /** A cluster-scoped read, declared the way every KUI endpoint declares itself. */
  private val declared: AnyEndpoint =
    endpoint.get
      .in("internal" / "v1" / "clusters" / path[String]("clusterId") / "topics")
      .attribute(
        EndpointAuthorization.Key,
        EndpointAuthorization.one(
          "listTopics",
          ResourceRequirement.unnamed(Resource.Topic, Action.TopicView)
        )
      )

  /** The same path, with no declaration attached at all. This is the shape the fail-closed branch exists for.
    */
  private val undeclared: AnyEndpoint =
    endpoint.get.in("internal" / "v1" / "clusters" / path[String]("clusterId") / "topics")

  /** An endpoint that names its resource with a path parameter its own path does not carry — the second way
    * `accessRequest` can fail to form a question, and the one a rename produces.
    */
  private val misdeclared: AnyEndpoint =
    endpoint.get
      .in("internal" / "v1" / "clusters" / path[String]("clusterId") / "topics" / path[String]("topic"))
      .attribute(
        EndpointAuthorization.Key,
        EndpointAuthorization.one(
          "getTopic",
          ResourceRequirement.named(Resource.Topic, "topicName", Action.TopicView)
        )
      )

  private def authorize(
      target: AnyEndpoint,
      requestPath: String,
      roles: Set[RoleName] = Set(reader),
      rbac: RbacPolicy = policy(Resource.Topic, Action.TopicView)
  ): IO[Either[kui.kernel.error.KuiError, Unit]] =
    FakeStructuredLogger[IO].flatMap { logger =>
      RbacGuard
        .fromPolicy[IO](rbac, _ => ClusterFlags.Writable, logger)
        .authorize(principal(roles.toSeq*), target, requestPath)
    }

  test("an endpoint that declares no permission is refused rather than allowed") {
    // The caller here holds the permission the *declared* endpoint needs, so a refusal cannot be a
    // permission failure: it is the guard refusing a question it could not form.
    authorize(undeclared, s"/internal/v1/clusters/${Cluster.value}/topics").map { decision =>
      assertEquals(decision.left.map(_.code.wire), Left("KUI-FORBIDDEN"))
    }
  }

  test("an endpoint naming a path parameter its path does not carry is refused") {
    authorize(misdeclared, s"/internal/v1/clusters/${Cluster.value}/topics/orders").map { decision =>
      assertEquals(decision.left.map(_.code.wire), Left("KUI-FORBIDDEN"))
    }
  }

  test("an undecidable endpoint is logged at error, because it is a bug and not a caller's mistake") {
    // `EndpointDecision`'s own scaladoc draws the distinction: a denial is a fact about the caller and
    // belongs in the audit trail; this is a fact about the code and should wake somebody. WARN and
    // ERROR are how the two are told apart by whatever reads the log.
    FakeStructuredLogger[IO].flatMap { logger =>
      val guard = RbacGuard.fromPolicy[IO](
        policy(Resource.Topic, Action.TopicView),
        _ => ClusterFlags.Writable,
        logger
      )

      guard
        .authorize(principal(reader), undeclared, s"/internal/v1/clusters/${Cluster.value}/topics")
        .flatMap(_ => logger.entries)
        .map { entries =>
          assertEquals(entries.map(_.level), List("error"))
          assert(
            entries.head.message.contains("could not be authorized"),
            entries.head.message
          )
        }
    }
  }

  test("a declared endpoint the caller holds the action for is allowed") {
    // The positive half, so that the two refusals above cannot be satisfied by a guard that refuses
    // everything — which is the shape a fail-closed assertion is easiest to fake with.
    authorize(declared, s"/internal/v1/clusters/${Cluster.value}/topics").map { decision =>
      assertEquals(decision, Right(()))
    }
  }

  test("a declared endpoint the caller holds no action for is refused by the ordinary denial branch") {
    authorize(
      declared,
      s"/internal/v1/clusters/${Cluster.value}/topics",
      rbac = policy(Resource.ConsumerGroup, Action.ConsumerGroupView)
    ).map { decision =>
      assertEquals(decision.left.map(_.code.wire), Left("KUI-FORBIDDEN"))
    }
  }

  test("a denial is logged at warn, and an undecidable endpoint at error") {
    FakeStructuredLogger[IO].flatMap { logger =>
      val guard = RbacGuard.fromPolicy[IO](
        policy(Resource.ConsumerGroup, Action.ConsumerGroupView),
        _ => ClusterFlags.Writable,
        logger
      )

      guard
        .authorize(principal(reader), declared, s"/internal/v1/clusters/${Cluster.value}/topics")
        .flatMap(_ => logger.entries)
        .map(entries => assertEquals(entries.map(_.level), List("warn")))
    }
  }

  /** A cluster-scoped **write**, which is what a read-only cluster is allowed to refuse. */
  private val altering: AnyEndpoint =
    endpoint.post
      .in("internal" / "v1" / "clusters" / path[String]("clusterId") / "topics")
      .attribute(
        EndpointAuthorization.Key,
        EndpointAuthorization.one(
          "createTopic",
          ResourceRequirement.unnamed(Resource.Topic, Action.TopicCreate)
        )
      )

  test("a write to a read-only cluster is refused by the service's own guard") {
    // ADR-021's second enforcement point, and the half of it nothing reached. Every existing case in
    // this file — and every route-driven case in the five service suites — hands the guard
    // `_ => ClusterFlags.Writable`, so `flagsFor`'s answer was never anything else: replacing the
    // lookup with a constant `Writable` left `libs.http.test` at 150, `services.cluster.api.test` at
    // 109 and the four other service `api` suites at 125, all green, with a read-only cluster's writes
    // allowed by the only check that runs when a caller reaches a service port directly.
    //
    // A service knows read-only from its own configuration precisely so the refusal does not depend on
    // the gateway having been asked first.
    val rbac = policy(Resource.Topic, Action.TopicCreate, Action.TopicView)

    FakeStructuredLogger[IO].flatMap { logger =>
      val guard = RbacGuard.fromPolicy[IO](rbac, _ => ClusterFlags(readOnly = true), logger)

      guard
        .authorize(principal(reader), altering, s"/internal/v1/clusters/${Cluster.value}/topics")
        .map(decision => assertEquals(decision.left.map(_.code.wire), Left("KUI-FORBIDDEN")))
    }
  }

  test("the same write to a writable cluster is allowed, so the refusal is the flag and not the role") {
    // The other half, and it is what stops the case above from passing against a guard that refuses
    // every write: one caller, one policy, one endpoint, one bit different.
    val rbac = policy(Resource.Topic, Action.TopicCreate, Action.TopicView)

    FakeStructuredLogger[IO].flatMap { logger =>
      RbacGuard
        .fromPolicy[IO](rbac, _ => ClusterFlags.Writable, logger)
        .authorize(principal(reader), altering, s"/internal/v1/clusters/${Cluster.value}/topics")
        .map(decision => assertEquals(decision, Right(())))
    }
  }

  test("a read of a read-only cluster is still allowed, because read-only refuses writes and not reads") {
    FakeStructuredLogger[IO].flatMap { logger =>
      RbacGuard
        .fromPolicy[IO](policy(Resource.Topic, Action.TopicView), _ => ClusterFlags(readOnly = true), logger)
        .authorize(principal(reader), declared, s"/internal/v1/clusters/${Cluster.value}/topics")
        .map(decision => assertEquals(decision, Right(())))
    }
  }

  test("connector visibility keeps the granted name, and a parent connect grant shows every connector") {
    // A pattern on `Resource.Connector` must not fall through to every connector, and a `CONNECT VIEW` on
    // the parent must still show them: `ResourceAccess.connector` is what attaches that fallback.
    val connect = "payments"

    def granting(resource: Resource, pattern: String, action: Action): RbacPolicy =
      RbacPolicy(
        roles = List(
          Role(
            name = reader,
            clusters = Set(Cluster),
            subjects = Nil,
            permissions = List(
              RbacPolicy.permission(
                resource,
                Some(ResourcePattern.compile(pattern).fold(msg => fail(msg), identity)),
                Set(action)
              )
            )
          )
        ),
        defaultRole = None
      )

    val access = (name: String) => ResourceAccess.connector(connect, name, Action.ConnectorView)

    FakeStructuredLogger[IO].flatMap { logger =>
      val named = RbacGuard.fromPolicy[IO](
        granting(Resource.Connector, s"$connect/a", Action.ConnectorView),
        _ => ClusterFlags.Writable,
        logger
      )
      val parent = RbacGuard.fromPolicy[IO](
        granting(Resource.Connect, connect, Action.ConnectView),
        _ => ClusterFlags.Writable,
        logger
      )

      for {
        onlyA <- named.visibility(principal(reader), Cluster, access)
        both <- parent.visibility(principal(reader), Cluster, access)
      } yield {
        assertEquals(onlyA("a"), true)
        assertEquals(onlyA("b"), false)
        assertEquals(both("a"), true)
        assertEquals(both("b"), true)
      }
    }
  }

  test("the cluster the flags are looked up for is the one named in the request path") {
    // Stated separately from the refusal above because the two failures are different: a guard that
    // ignores `flagsFor` and a guard that asks it about the wrong cluster both allow the write, and a
    // deployment with one read-only cluster beside three writable ones is where the second one hides.
    // `flagsFor` is a pure function, so what it was asked is recorded beside it rather than in `IO`.
    val asked = new java.util.concurrent.atomic.AtomicReference[List[String]](Nil)

    FakeStructuredLogger[IO].flatMap { logger =>
      val guard = RbacGuard.fromPolicy[IO](
        policy(Resource.Topic, Action.TopicCreate, Action.TopicView),
        id => {
          asked.updateAndGet(seen => seen :+ id.value)
          ClusterFlags(readOnly = true)
        },
        logger
      )

      guard
        .authorize(principal(reader), altering, "/internal/v1/clusters/frozen/topics")
        .map(_ => assertEquals(asked.get, List("frozen")))
    }
  }
}
