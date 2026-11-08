package kui.contracts.rbac

import munit.FunSuite
import sttp.tapir.*

import kui.contracts.{ErrorEnvelope, KuiEndpoint}
import kui.kernel.{ClusterId, RoleName, UserName}
import kui.security.rbac.*
import kui.security.{Principal, PrincipalKind}

/** The shared half of authorization: an endpoint's declaration, and the decision taken from it.
  *
  * Everything here is pure, which is the point — the gateway, every service and the browser reach the same
  * verdicts because they call these two functions, so the cases that matter are worth pinning down once,
  * here, rather than three times over HTTP.
  */
final class EndpointDecisionSuite extends FunSuite {

  private val cluster = ClusterId.unsafe("local")

  private val readTopic: Endpoint[?, ?, ErrorEnvelope, ?, Any] =
    KuiEndpoint.internal.get
      .in("internal" / "v1" / "clusters" / path[String]("clusterId") / "topics" / path[String]("topicName"))
      .name("topic.get")
      .attribute(
        EndpointAuthorization.Key,
        EndpointAuthorization.one(
          "topic.get",
          ResourceRequirement.named(Resource.Topic, "topicName", Action.TopicView)
        )
      )

  private val createTopic: Endpoint[?, ?, ErrorEnvelope, ?, Any] =
    KuiEndpoint.internal.post
      .in("internal" / "v1" / "clusters" / path[String]("clusterId") / "topics")
      .name("topic.create")
      .attribute(
        EndpointAuthorization.Key,
        EndpointAuthorization.one(
          "topic.create",
          ResourceRequirement.inBody(Resource.Topic, "name", Action.TopicCreate)
        )
      )

  private val undeclared: Endpoint[?, ?, ErrorEnvelope, ?, Any] =
    KuiEndpoint.internal.get.in("internal" / "v1" / "clusters").name("cluster.list")

  private def principal(roles: String*): Principal =
    Principal(UserName.unsafe("ada"), roles.map(RoleName.unsafe).toSet, PrincipalKind.Session)

  private def policyGranting(pattern: String, actions: Action*): RbacPolicy =
    RbacPolicy(
      roles = List(
        Role(
          name = RoleName.unsafe("reader"),
          clusters = Set(cluster),
          subjects = Nil,
          permissions = List(
            RbacPolicy.permission(
              actions.head.resource,
              Some(ResourcePattern.compile(pattern).getOrElse(fail(s"'$pattern' does not compile"))),
              actions.toSet
            )
          )
        )
      ),
      defaultRole = None
    )

  // -- reading the resource's name out of a request -----------------------------------------------

  test("a path parameter is read from the end of the request, so the gateway's prefix does not matter") {
    // The endpoint says /internal/v1/...; the gateway serves the same call under /api/v1/...
    val fromGateway = List("api", "v1", "clusters", "local", "topics", "payments.orders")
    val fromService = List("internal", "v1", "clusters", "local", "topics", "payments.orders")

    assertEquals(
      EndpointAuthorization.pathValue(readTopic, "topicName", fromGateway),
      Some("payments.orders")
    )
    assertEquals(
      EndpointAuthorization.pathValue(readTopic, "topicName", fromService),
      Some("payments.orders")
    )
  }

  test("a base path in front of the public prefix does not shift the parameter either") {
    val behindABasePath = List("kui", "api", "v1", "clusters", "local", "topics", "payments.orders")

    assertEquals(
      EndpointAuthorization.pathValue(readTopic, "topicName", behindABasePath),
      Some("payments.orders")
    )
  }

  test("a request shorter than the endpoint's own path yields no name, and never a wrong one") {
    assertEquals(EndpointAuthorization.pathValue(readTopic, "topicName", List("topics")), None)
  }

  test("a short request that would still index into something yields no name rather than the wrong one") {
    // The alignment is from the end, so a request one segment short slides every parameter left by one.
    // `readTopic` declares six slots and this request carries five; aligning them anyway would read the
    // topic name out of the slot the *cluster* id is in and authorize a call about `local` against a
    // pattern for a topic. `None` is the only safe answer, and the caller treats it as a refusal.
    val short = List("v1", "clusters", "local", "topics", "orders")

    assertEquals(EndpointAuthorization.pathValue(readTopic, "topicName", short), None)
    assertEquals(EndpointAuthorization.pathValue(readTopic, "clusterId", short), None)
  }

  // -- the decision -------------------------------------------------------------------------------

  test("an endpoint with no declaration cannot be authorized, and says so rather than allowing") {
    val outcome = EndpointDecision.decide(
      RbacPolicy.Disabled,
      Principal.Anonymous,
      ClusterFlags.Writable,
      undeclared,
      Some(cluster),
      List("api", "v1", "clusters")
    )

    assert(outcome.isLeft, s"an undeclared endpoint must not be decidable, got $outcome")
  }

  test("the name the decision is taken over is the one in the path, not any other topic") {
    val policy = policyGranting("payments\\..*", Action.TopicView)

    def verdict(topic: String): Decision =
      EndpointDecision
        .decide(
          policy,
          principal("reader"),
          ClusterFlags.Writable,
          readTopic,
          Some(cluster),
          List("api", "v1", "clusters", "local", "topics", topic)
        )
        .getOrElse(fail("the request is decidable"))

    assertEquals(verdict("payments.orders"), Decision.Allowed)
    assertEquals(
      verdict("secrets"),
      Decision.Denied(DenialReason.MissingActions(Resource.Topic, Some("secrets"), Set(Action.TopicView)))
    )
  }

  test("a principal with no role for the cluster is refused before the resource is considered") {
    val policy = policyGranting("payments\\..*", Action.TopicView)

    val outcome = EndpointDecision.decide(
      policy,
      principal("someone-else"),
      ClusterFlags.Writable,
      readTopic,
      Some(cluster),
      List("api", "v1", "clusters", "local", "topics", "payments.orders")
    )

    assertEquals(outcome, Right(Decision.Denied(DenialReason.NoClusterAccess(cluster))))
  }

  // -- the resource whose name is only in the body ------------------------------------------------

  test("a body-named create is allowed when the caller may create some topic") {
    val policy = policyGranting("payments\\..*", Action.TopicCreate)

    assertEquals(
      EndpointDecision.decide(
        policy,
        principal("reader"),
        ClusterFlags.Writable,
        createTopic,
        Some(cluster),
        List("api", "v1", "clusters", "local", "topics")
      ),
      Right(Decision.Allowed)
    )
  }

  test("a body-named create is refused when the caller may create no topic at all") {
    val policy = policyGranting("payments\\..*", Action.TopicView)

    val outcome = EndpointDecision.decide(
      policy,
      principal("reader"),
      ClusterFlags.Writable,
      createTopic,
      Some(cluster),
      List("api", "v1", "clusters", "local", "topics")
    )

    assertEquals(
      outcome,
      Right(Decision.Denied(DenialReason.MissingActions(Resource.Topic, None, Set(Action.TopicCreate))))
    )
  }

  test("with the body's names in hand, a body-named create is decided over the exact name") {
    // The service's half of the check. The coarse gate above lets `payments\..*` reach the endpoint;
    // here, with the decoded body, `orders` is refused and `payments.new` is not.
    val policy = policyGranting("payments\\..*", Action.TopicCreate)

    def verdict(name: String): Either[String, Decision] =
      EndpointDecision.decide(
        policy,
        principal("reader"),
        ClusterFlags.Writable,
        createTopic,
        Some(cluster),
        List("internal", "v1", "clusters", "local", "topics"),
        Some(Map("name" -> List(name)))
      )

    assertEquals(verdict("payments.new"), Right(Decision.Allowed))
    assertEquals(
      verdict("orders"),
      Right(
        Decision.Denied(DenialReason.MissingActions(Resource.Topic, Some("orders"), Set(Action.TopicCreate)))
      )
    )
  }

  test("every name in a body field must be permitted, so one name outside the pattern refuses the call") {
    val policy = policyGranting("payments\\..*", Action.TopicCreate)

    assertEquals(
      EndpointDecision.decide(
        policy,
        principal("reader"),
        ClusterFlags.Writable,
        createTopic,
        Some(cluster),
        List("internal", "v1", "clusters", "local", "topics"),
        Some(Map("name" -> List("payments.a", "secrets", "payments.b")))
      ),
      Right(
        Decision.Denied(DenialReason.MissingActions(Resource.Topic, Some("secrets"), Set(Action.TopicCreate)))
      )
    )
  }

  test("a body field whose names the service did not supply cannot be decided, even with RBAC off") {
    // A route that forgot to hand over its body's names would otherwise be the coarse check again, which
    // is the bypass this parameter exists to close. It is a fact about the code, so it is a `Left`.
    val outcome = EndpointDecision.decide(
      RbacPolicy.Disabled,
      Principal.Anonymous,
      ClusterFlags.Writable,
      createTopic,
      Some(cluster),
      List("internal", "v1", "clusters", "local", "topics"),
      Some(Map.empty)
    )

    assert(outcome.isLeft, s"an unsupplied body field must not be decidable, got $outcome")
  }

  test("a read-only cluster refuses a body-named create even with every permission and no RBAC") {
    val outcome = EndpointDecision.decide(
      RbacPolicy.Disabled,
      Principal.Anonymous,
      ClusterFlags(readOnly = true),
      createTopic,
      Some(cluster),
      List("api", "v1", "clusters", "local", "topics")
    )

    assertEquals(
      outcome,
      Right(Decision.Denied(DenialReason.ReadOnlyCluster(cluster, Set(Action.TopicCreate))))
    )
  }

  test("a read-only cluster still serves a read") {
    assertEquals(
      EndpointDecision.decide(
        RbacPolicy.Disabled,
        Principal.Anonymous,
        ClusterFlags(readOnly = true),
        readTopic,
        Some(cluster),
        List("api", "v1", "clusters", "local", "topics", "payments.orders")
      ),
      Right(Decision.Allowed)
    )
  }

  test("a list endpoint declares cluster scope, which is a declaration and not an absence") {
    val list =
      undeclared.attribute(EndpointAuthorization.Key, EndpointAuthorization.clusterScoped("topic.list"))

    assertEquals(
      EndpointDecision.decide(
        policyGranting("payments\\..*", Action.TopicView),
        principal("reader"),
        ClusterFlags.Writable,
        list,
        Some(cluster),
        List("api", "v1", "clusters")
      ),
      Right(Decision.Allowed)
    )
  }
}
