package kui.security.rbac

import java.util.Locale

import munit.ScalaCheckSuite
import org.scalacheck.Prop.forAll
import org.scalacheck.{Gen, Prop}

import kui.kernel.{ClusterId, RoleName, UserName}
import kui.security.{Principal, PrincipalKind}

/** The six laws ADR-021 names, plus the read-only rule, as properties.
  *
  * These are laws rather than examples because the interesting failures of an authorization evaluator are not
  * the cases somebody thought to write down. A worked example proves that deleting `orders` works; a property
  * proves that no combination of roles, patterns and actions the generators can produce makes a grant
  * disappear.
  */
final class RbacLawsSuite extends ScalaCheckSuite {

  import RbacGenerators.*

  private val Cluster: ClusterId = ClusterId.unsafe("production")
  private val Writable: ClusterFlags = ClusterFlags.Writable
  private val ReadOnly: ClusterFlags = ClusterFlags(readOnly = true)

  private def user(roles: RoleName*): Principal =
    Principal(UserName.unsafe("tester"), roles.toSet, PrincipalKind.Session)

  test("exported default-role grants are fallback-only, matching server decisions") {
    val default =
      DefaultRole(List(RbacPolicy.permission(Resource.Topic, Some(orders), Set(Action.TopicView))))
    val policy = RbacPolicy(List(editors), Some(default))
    val principal = user(RoleName.unsafe("editors"))
    val exported = Rbac.grants(policy, principal)
    List(Some(Cluster), Some(ClusterId.unsafe("other")), None).foreach { cluster =>
      val scoped = exported.filter(grant => cluster.forall(grant.clusters.includes))
      val primary = scoped.filterNot(_.defaultRole)
      val effective = if primary.nonEmpty then primary else scoped.filter(_.defaultRole)
      List("orders", "payments").foreach { name =>
        val client = effective.exists(grant =>
          grant.permission.covers(Resource.Topic, Some(name)) &&
            grant.permission.actions.contains(Action.TopicView)
        )
        val server = Rbac
          .decide(
            policy,
            principal,
            Writable,
            AccessRequest(
              cluster,
              List(ResourceAccess.named(Resource.Topic, name, Action.TopicView)),
              OperationName("view")
            )
          )
          .isAllowed
        assertEquals(client, server, s"cluster=$cluster name=$name")
      }
    }
  }

  test("RBAC vocabulary parsing does not depend on the host locale") {
    val previous = Locale.getDefault
    try {
      Locale.setDefault(Locale.forLanguageTag("tr-TR"))
      assertEquals(Resource.fromWire("audit"), Some(Resource.Audit))
      assertEquals(Action.fromWire(Resource.Topic, "view"), Some(Action.TopicView))
      assertEquals(Provider.fromWire("oauth_github"), Some(Provider.OauthGithub))
      assertEquals(SubjectKind.fromWire("ORGANIZATION"), Some(SubjectKind.Organization))
    } finally Locale.setDefault(previous)
  }

  // -- Law 1: the implies closure is idempotent -----------------------------------------------------

  property("theImpliesClosureIsIdempotent") {
    forAll(Gen.listOf(anyAction)) { actions =>
      val once = Action.closure(actions.toSet)
      Action.closure(once) == once
    }
  }

  property("theImpliesClosureIsExtensive") {
    // Closing a set never takes an action away. Stated separately because an idempotent function that
    // returned the empty set would satisfy the law above.
    forAll(Gen.listOf(anyAction))(actions => actions.toSet.subsetOf(Action.closure(actions.toSet)))
  }

  property("everyWriteImpliesTheMatchingRead") {
    // The rule behind the whole dependency table: nobody is granted a change to a thing they may not see.
    // A delete without a view is a screen that shows an empty list with a delete button on it.
    forAll(anyAction) { action =>
      val view = Action.values.find(other => other.resource == action.resource && other.wire == "VIEW")

      view match {
        case Some(v) if action.isAlter && action.resource != Resource.Schema =>
          Prop(Action.closure(Set(action)).contains(v))
            .label(s"${action.resource.wire}.${action.wire} does not imply VIEW")
        case _ => Prop.passed
      }
    }
  }

  // -- Law 2: ALL is the full action set ------------------------------------------------------------

  property("allExpandsToEveryActionOfTheResource") {
    forAll(resource) { target =>
      val granted = RbacPolicy.allPermission(target, Some(ResourcePattern.Everything))
      granted.actions == target.allActions && granted.actions.forall(_.resource == target)
    }
  }

  property("allIsAlreadyClosed") {
    // Every action of a resource is present, so following `implies` cannot add one. This is what makes
    // "ALL" safe to write in a configuration file without a second expansion step.
    forAll(resource)(target => Action.closure(target.allActions) == target.allActions)
  }

  // -- Law 3: decide is monotone in permissions -----------------------------------------------------

  property("addingAPermissionNeverWithdrawsOne") {
    // Stated on policies with no default role, and that restriction is the interesting part. The default
    // role is a *floor* that applies only while the principal's own roles grant nothing on this cluster,
    // so giving somebody their first real permission legitimately replaces the floor — and the floor can
    // be the larger of the two. That is Kafbat's behaviour and ADR-021 keeps it; it means monotonicity
    // holds for the roles and not for the fallback between them.
    val setup = for {
      roles <- Gen.nonEmptyListOf(role)
      policy = RbacPolicy(roles, None)
      principal <- principalIn(policy)
      request <- request
      extra <- permission
    } yield (policy, principal, request, extra)

    forAll(setup) { case (policy, principal, request, extra) =>
      val richer = RbacPolicy(
        policy.roles.map(role =>
          if principal.roles.contains(role.name) then role.copy(permissions = extra :: role.permissions)
          else role
        ),
        None
      )

      val before = Rbac.decide(policy, principal, Writable, request)
      val after = Rbac.decide(richer, principal, Writable, request)

      Prop(!before.isAllowed || after.isAllowed)
        .label(s"was $before, became $after after granting $extra")
    }
  }

  // -- Law 4: the default role applies only when no role matches ------------------------------------

  test("theDefaultRoleIsUsedWhenThePrincipalHoldsNothingOnThisCluster") {
    val default =
      DefaultRole(List(RbacPolicy.permission(Resource.Topic, Some(orders), Set(Action.TopicView))))
    val policy = RbacPolicy(List(readers), Some(default))

    assertEquals(
      Rbac.decide(policy, user(), Writable, viewing("orders")),
      Decision.Allowed
    )
  }

  test("theDefaultRoleIsNotUsedWhenARoleAlreadyGrantsSomethingHere") {
    // `editors` grants `payments` on production and nothing on `orders`. The default role would allow
    // `orders`, and it must not be consulted, because this principal's own roles answered the question.
    val default =
      DefaultRole(List(RbacPolicy.permission(Resource.Topic, Some(orders), Set(Action.TopicView))))
    val policy = RbacPolicy(List(editors), Some(default))

    assertEquals(
      Rbac.decide(policy, user(RoleName.unsafe("editors")), Writable, viewing("orders")),
      Decision.Denied(
        DenialReason.MissingActions(Resource.Topic, Some("orders"), Set(Action.TopicView))
      )
    )
  }

  test("theDefaultRoleAlsoOpensTheClusterGate") {
    // Without this, a default role would be unreachable on every cluster: the cluster gate runs first and
    // the default role names no clusters at all.
    val default = DefaultRole(List(RbacPolicy.permission(Resource.Audit, None, Set(Action.AuditView))))
    val policy = RbacPolicy(List(readers), Some(default))

    assertEquals(
      Rbac.decide(
        policy,
        user(),
        Writable,
        AccessRequest(Cluster, "readAudit", ResourceAccess.unnamed(Resource.Audit, Action.AuditView))
      ),
      Decision.Allowed
    )
  }

  // -- Law 5: the cluster gate precedes the resource gate -------------------------------------------

  test("aClusterNoRoleNamesIsRefusedForTheCluster") {
    // `readers` names `staging`. Asking about production must say "no role grants access to this cluster"
    // rather than "you lack TOPIC.VIEW" — an operator who is told the second goes and asks for a topic
    // permission they already have.
    val policy = RbacPolicy(List(readers), None)

    assertEquals(
      Rbac.decide(policy, user(RoleName.unsafe("readers")), Writable, viewing("orders")),
      Decision.Denied(DenialReason.NoClusterAccess(Cluster))
    )
  }

  property("aRefusedClusterIsAlwaysRefusedForTheClusterAndNotForAResource") {
    val setup = for {
      roles <- Gen.nonEmptyListOf(role)
      policy = RbacPolicy(roles, None)
      principal <- principalIn(policy)
      accesses <- Gen.nonEmptyListOf(access)
    } yield (policy, principal, accesses)

    forAll(setup) { case (policy, principal, accesses) =>
      val unknown = ClusterId.unsafe("nowhere")
      val decision = Rbac.decide(
        policy,
        principal,
        Writable,
        AccessRequest(Some(unknown), accesses, OperationName("law"))
      )

      decision == Decision.Denied(DenialReason.NoClusterAccess(unknown))
    }
  }

  // -- Law 6: a connector falls back to its parent connect cluster ----------------------------------

  test("aPermissionOnTheConnectClusterCoversItsConnectors") {
    val policy = RbacPolicy(
      List(
        Role(
          RoleName.unsafe("connect-admins"),
          Set(Cluster),
          List(Subject(Provider.Oauth, SubjectKind.User, "tester", isRegex = false)),
          List(
            RbacPolicy.permission(
              Resource.Connect,
              Some(compile("payments")),
              Set(Action.ConnectEdit)
            )
          )
        )
      ),
      None
    )

    assertEquals(
      Rbac.decide(
        policy,
        user(RoleName.unsafe("connect-admins")),
        Writable,
        AccessRequest(
          Cluster,
          "editConnector",
          ResourceAccess.connector("payments", "sink", Action.ConnectorEdit)
        )
      ),
      Decision.Allowed
    )
  }

  test("theConnectorFallbackDoesNotCrossToAnotherConnectCluster") {
    val policy = RbacPolicy(
      List(
        Role(
          RoleName.unsafe("connect-admins"),
          Set(Cluster),
          List(Subject(Provider.Oauth, SubjectKind.User, "tester", isRegex = false)),
          List(RbacPolicy.permission(Resource.Connect, Some(compile("payments")), Set(Action.ConnectEdit)))
        )
      ),
      None
    )

    val decision = Rbac.decide(
      policy,
      user(RoleName.unsafe("connect-admins")),
      Writable,
      AccessRequest(
        Cluster,
        "editConnector",
        ResourceAccess.connector("orders", "sink", Action.ConnectorEdit)
      )
    )

    assert(!decision.isAllowed, s"a permission on 'payments' allowed a connector on 'orders': $decision")
  }

  // -- Read-only clusters (ADR-047) -----------------------------------------------------------------

  property("aReadOnlyClusterRefusesEveryAlteringRequest") {
    forAll(Gen.nonEmptyListOf(access)) { accesses =>
      val altering = accesses.flatMap(_.actions).exists(_.isAlter)
      val decision = Rbac.decide(
        RbacPolicy.Disabled,
        user(),
        ReadOnly,
        AccessRequest(Some(Cluster), accesses, OperationName("law"))
      )

      if altering then !decision.isAllowed else decision.isAllowed
    }
  }

  test("aReadOnlyClusterStillAllowsAnAnalysisRun") {
    // The Kafbat exception, and the reason `isAlter` is a field on the action rather than a list of
    // exempt URLs somewhere else: running an analysis reads records and writes nothing.
    assertEquals(
      Rbac.decide(
        RbacPolicy.Disabled,
        user(),
        ReadOnly,
        AccessRequest(
          Cluster,
          "analyseTopic",
          ResourceAccess.named(Resource.Topic, "orders", Action.TopicAnalysisRun)
        )
      ),
      Decision.Allowed
    )
  }

  test("readOnlyAppliesEvenWhenRbacIsSwitchedOff") {
    // It is a fact about the cluster, not an authorization rule. A deployment with no roles configured is
    // exactly the one most likely to be relying on the flag.
    val decision = Rbac.decide(
      RbacPolicy.Disabled,
      user(),
      ReadOnly,
      AccessRequest(
        Cluster,
        "deleteTopic",
        ResourceAccess.named(Resource.Topic, "orders", Action.TopicDelete)
      )
    )

    assertEquals(
      decision,
      Decision.Denied(DenialReason.ReadOnlyCluster(Cluster, Set(Action.TopicDelete)))
    )
  }

  // -- The resources with no read (W1-07) -----------------------------------------------------------

  test("runningAKsqlStatementImpliesBeingAbleToListKsqlObjects") {
    // The same rule as TopicCreate -> TopicView, applied where it was missing: a principal allowed to run a
    // statement has to be allowed to know the objects it names exist.
    assert(Action.closure(Set(Action.KsqlExecute)).contains(Action.KsqlView))
  }

  test("aReadOnlyClusterStillListsKsqlObjects") {
    // Ksql's only action used to be EXECUTE, which alters, so the read-only gate refused the resource
    // outright and the ksqlDB screen was empty on exactly the clusters most likely to be read-only.
    assertEquals(
      Rbac.decide(
        RbacPolicy.Disabled,
        user(),
        ReadOnly,
        AccessRequest(Cluster, "listKsqlObjects", ResourceAccess.unnamed(Resource.Ksql, Action.KsqlView))
      ),
      Decision.Allowed
    )
  }

  test("aReadOnlyClusterStillRefusesAKsqlStatement") {
    // The other half of the pair, and the reason the fix is a second action rather than making EXECUTE a
    // read: a statement can create a stream and the topic under it.
    assertEquals(
      Rbac.decide(
        RbacPolicy.Disabled,
        user(),
        ReadOnly,
        AccessRequest(Cluster, "runKsqlStatement", ResourceAccess.unnamed(Resource.Ksql, Action.KsqlExecute))
      ),
      Decision.Denied(DenialReason.ReadOnlyCluster(Cluster, Set(Action.KsqlExecute)))
    )
  }

  test("acknowledgingAnAlertImpliesSeeingTheFeed") {
    assert(Action.closure(Set(Action.AlertsAcknowledge)).contains(Action.AlertsView))
  }

  test("aReadOnlyClusterStillShowsItsMetricsAndItsAlerts") {
    // Neither reads anything from the cluster that a read-only flag is about. A dashboard that went blank
    // because the cluster was marked read-only would be read the wrong way round by every operator.
    assertEquals(
      Rbac.decide(
        RbacPolicy.Disabled,
        user(),
        ReadOnly,
        AccessRequest(
          Cluster,
          "readDashboard",
          ResourceAccess.unnamed(Resource.Metrics, Action.MetricsView),
          ResourceAccess.unnamed(Resource.Alerts, Action.AlertsView)
        )
      ),
      Decision.Allowed
    )
  }

  test("theTwoNewResourcesAreUnnamed") {
    // `isNamed` is what the configuration loader asks before it demands a `value`, so getting this wrong
    // makes `resource: METRICS` a permission that grants nothing while looking like it grants everything.
    assert(!Resource.Metrics.isNamed)
    assert(!Resource.Alerts.isNamed)
  }

  test("aRoleGrantingTheAlertsFeedDoesNotGrantAcknowledgement") {
    // Reading and silencing are two grants. `ALL` is how an operator asks for both.
    val policy = RbacPolicy(
      List(
        Role(
          RoleName.unsafe("on-call"),
          Set(Cluster),
          List(Subject(Provider.Oauth, SubjectKind.User, "tester", isRegex = false)),
          List(RbacPolicy.permission(Resource.Alerts, None, Set(Action.AlertsView)))
        )
      ),
      None
    )

    assertEquals(
      Rbac.decide(
        policy,
        user(RoleName.unsafe("on-call")),
        Writable,
        AccessRequest(
          Cluster,
          "acknowledgeAlert",
          ResourceAccess.unnamed(Resource.Alerts, Action.AlertsAcknowledge)
        )
      ),
      Decision.Denied(DenialReason.MissingActions(Resource.Alerts, None, Set(Action.AlertsAcknowledge)))
    )
  }

  // -- RBAC switched off ----------------------------------------------------------------------------

  property("everyNonAlteringRequestIsAllowedWhenRbacIsOff") {
    forAll(request) { req =>
      Rbac.decide(RbacPolicy.Disabled, user(), Writable, req) == Decision.Allowed
    }
  }

  test("aDisabledPolicyGrantsEverythingToTheBrowserToo") {
    // `/auth/me` must not answer with an empty permission list here, or the interface hides every write
    // control in the deployment that asked for no authorization at all — which is the quickstart.
    val granted = Rbac.grants(RbacPolicy.Disabled, user())

    assertEquals(granted.map(_.permission.resource).toSet, Resource.values.toSet)
    assert(granted.forall(_.clusters == ClusterScope.Every))
    assert(granted.forall(_.clusters.includes(Cluster)))
    assert(granted.forall(entry => entry.permission.actions == entry.permission.resource.allActions))
  }

  // -- Visibility filtering -------------------------------------------------------------------------

  test("aListIsFilteredRatherThanRefused") {
    val policy = RbacPolicy(
      List(
        Role(
          RoleName.unsafe("readers"),
          Set(Cluster),
          List(Subject(Provider.Oauth, SubjectKind.User, "tester", isRegex = false)),
          List(RbacPolicy.permission(Resource.Topic, Some(compile("orders.*")), Set(Action.TopicView)))
        )
      ),
      None
    )

    assertEquals(
      Rbac.visible(
        policy,
        user(RoleName.unsafe("readers")),
        Writable,
        Some(Cluster),
        Resource.Topic,
        Action.TopicView
      )(List("orders", "orders-dlq", "payments"))(identity),
      List("orders", "orders-dlq")
    )
  }

  property("visibilityNeverInventsAnItem") {
    val setup = for {
      policy <- enabledPolicy
      principal <- principalIn(policy)
    } yield (policy, principal)

    forAll(setup) { case (policy, principal) =>
      val items = resourceNames
      val shown = Rbac.visible(policy, principal, Writable, Some(Cluster), Resource.Topic, Action.TopicView)(
        items
      )(identity)

      shown.forall(items.contains) && shown.distinct == shown
    }
  }

  // -- Role resolution ------------------------------------------------------------------------------

  test("aSubjectMatchesItsOwnProviderOnly") {
    val github = Subject(Provider.OauthGithub, SubjectKind.Organization, "acme", isRegex = false)
    val policy = RbacPolicy(
      List(Role(RoleName.unsafe("acme"), Set(Cluster), List(github), List.empty)),
      None
    )

    val fromGithub =
      Map(Provider.OauthGithub -> IdentityAttributes(Map(SubjectKind.Organization -> Set("acme"))))
    val fromLdap = Map(Provider.Ldap -> IdentityAttributes(Map(SubjectKind.Organization -> Set("acme"))))

    assertEquals(Rbac.resolveRoles(policy, fromGithub), Set(RoleName.unsafe("acme")))
    assertEquals(Rbac.resolveRoles(policy, fromLdap), Set.empty[RoleName])
  }

  test("aNonRegexSubjectIsMatchedCaseInsensitively") {
    val subject = Subject(Provider.Ldap, SubjectKind.User, "Alice", isRegex = false)
    assert(subject.matches("alice"))
    assert(subject.matches("ALICE"))
    assert(!subject.matches("alice2"))
  }

  test("aRegexSubjectMustMatchInFull") {
    val subject = Subject(Provider.Ldap, SubjectKind.Group, "team-.*", isRegex = true)
    assert(subject.matches("team-platform"))
    assert(!subject.matches("other-team-platform"))
  }

  test("aRegexSubjectThatDoesNotCompileMatchesNothing") {
    // It must not throw at login. A configuration mistake that made every sign-in fail with a stack trace
    // would be a worse outcome than the role simply not applying, and the pattern linter is where an
    // operator is told about it.
    val subject = Subject(Provider.Ldap, SubjectKind.Group, "team-[", isRegex = true)
    assert(!subject.matches("team-platform"))
  }

  // -- Fixtures -------------------------------------------------------------------------------------

  private def compile(raw: String): ResourcePattern =
    ResourcePattern.compile(raw).getOrElse(ResourcePattern.Everything)

  private val orders: ResourcePattern = compile("orders")

  /** Names `staging` only, so that any question about `production` fails the cluster gate. */
  private val readers: Role = Role(
    RoleName.unsafe("readers"),
    Set(ClusterId.unsafe("staging")),
    List(Subject(Provider.Oauth, SubjectKind.User, "tester", isRegex = false)),
    List(RbacPolicy.permission(Resource.Topic, Some(orders), Set(Action.TopicView)))
  )

  /** Names `production` and grants something there, so the default role is not consulted. */
  private val editors: Role = Role(
    RoleName.unsafe("editors"),
    Set(Cluster),
    List(Subject(Provider.Oauth, SubjectKind.User, "tester", isRegex = false)),
    List(RbacPolicy.permission(Resource.Topic, Some(compile("payments")), Set(Action.TopicEdit)))
  )

  private def viewing(topic: String): AccessRequest =
    AccessRequest(Cluster, "viewTopic", ResourceAccess.named(Resource.Topic, topic, Action.TopicView))
}
