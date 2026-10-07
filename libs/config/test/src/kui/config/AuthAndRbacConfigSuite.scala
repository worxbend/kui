package kui.config

import java.nio.file.Path

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import kui.kernel.{ClusterId, RoleName}
import kui.security.rbac.{Action, Provider, Resource, SubjectKind}
import kui.testkit.KuiSuite

/** That `kui.auth` and `kui.rbac` are read, and that every way of writing them wrongly is a startup error
  * naming the key.
  *
  * The reason this suite is long is the reason the sections exist: a role is a security control, and a
  * security control that silently grants nothing is worse than one that refuses to start. Every `assert`
  * below about a *problem* is one such silence made loud.
  */
final class AuthAndRbacConfigSuite extends KuiSuite {

  test("explicit trusted proxy literals are accepted as auth configuration") {
    assertEquals(
      loaded("kui:\n  auth:\n    trustedProxies: [127.0.0.1, '::1']\n").auth.trustedProxies,
      Set("127.0.0.1", "0:0:0:0:0:0:0:1")
    )
    assertEquals(loaded("kui: {}\n").auth.trustedProxies, Set.empty[String])
  }

  test("proxy trust never accepts hostnames, wildcard networks or malformed IPs") {
    List("localhost", "0.0.0.0/0", "*", "127.1", "999.1.1.1").foreach { value =>
      assert(
        problems(s"kui:\n  auth:\n    trustedProxies: ['$value']\n")
          .exists(_.key == "kui.auth.trustedProxies")
      )
    }
  }

  private def load(
      yaml: String,
      env: Map[String, String] = Map.empty
  ): Either[ConfigErrors, KuiConfig] =
    KuiConfigSource
      .loadFrom[IO](Nil, List[Path](ConfigFixtures.yaml(yaml)), env, UrlPolicy.Dev)
      .unsafeRunSync()

  private def loaded(yaml: String, env: Map[String, String] = Map.empty): KuiConfig =
    load(yaml, env).fold(errors => fail(errors.render), identity)

  private def problems(yaml: String, env: Map[String, String] = Map.empty): List[ConfigProblem] =
    load(yaml, env) match {
      case Left(errors) => errors.problems.toList
      case Right(_) => fail("expected the load to fail, but it succeeded")
    }

  // -----------------------------------------------------------------------------------------------
  // The default, which is the one behaviour that must never change
  // -----------------------------------------------------------------------------------------------

  test("a file that says nothing about authentication gets none, and a policy that allows everything") {
    val config = loaded("kui:\n  server:\n    port: 8080\n")

    assertEquals(config.auth, AuthConfig.Default)
    assertEquals(config.auth.authType, AuthType.Disabled)
    assertEquals(config.rbac.enabled, false)
  }

  test("type: disabled is still spellable, and still means the same thing") {
    assertEquals(loaded("kui:\n  auth:\n    type: disabled\n").auth.authType, AuthType.Disabled)
  }

  // -----------------------------------------------------------------------------------------------
  // kui.auth
  // -----------------------------------------------------------------------------------------------

  test("form users are read, with their groups, and the hash is followed through env:") {
    val config = loaded(
      """kui:
        |  auth:
        |    type: form
        |    users:
        |      - name: ada
        |        passwordHash: "env:KUI_ADA_HASH"
        |        groups: [admins, oncall]
        |      - name: bob
        |        passwordHash: "pbkdf2-sha256$210000$c2FsdA$aGFzaA"
        |        mustChangePassword: true
        |""".stripMargin,
      env = Map("KUI_ADA_HASH" -> "pbkdf2-sha256$210000$c2FsdA$b3RoZXI")
    )

    assertEquals(config.auth.authType, AuthType.Form)
    assertEquals(config.auth.users.map(_.name), List("ada", "bob"))
    assertEquals(config.auth.users.head.groups, Set("admins", "oncall"))
    assertEquals(config.auth.users.head.passwordHash.value, "pbkdf2-sha256$210000$c2FsdA$b3RoZXI")
    assertEquals(config.auth.users.head.mustChangePassword, false)
    assertEquals(config.auth.users(1).mustChangePassword, true)
  }

  test("a password hash never appears in a rendered configuration problem") {
    // The hash resolves, and an unrelated key is wrong. The rendered error must still not carry it.
    val rendered = load(
      """kui:
        |  auth:
        |    type: form
        |    users:
        |      - name: ada
        |        passwordHash: "pbkdf2-sha256$210000$c2FsdA$c3VwZXJzZWNyZXQ"
        |  server:
        |    port: not-a-port
        |""".stripMargin
    ) match {
      case Left(errors) => errors.render
      case Right(_) => fail("expected the load to fail")
    }

    assert(!rendered.contains("c3VwZXJzZWNyZXQ"), rendered)
  }

  test("type: form with no users is refused, rather than starting a login nobody can pass") {
    val found = problems("kui:\n  auth:\n    type: form\n")

    assertEquals(found.map(_.key), List("kui.auth.users"))
    assert(found.head.problem.contains("is required when kui.auth.type is 'form'"), found.head.problem)
  }

  test("type: ldap says it is not implemented rather than that it is not a word") {
    val found = problems("kui:\n  auth:\n    type: ldap\n")

    assertEquals(found.map(_.key), List("kui.auth.type"))
    assert(found.head.problem.contains("not implemented yet"), found.head.problem)
  }

  test("an unknown auth type lists the ones that exist") {
    val found = problems("kui:\n  auth:\n    type: saml\n")

    assertEquals(found.map(_.key), List("kui.auth.type"))
    assert(found.head.problem.contains("disabled, form, oidc"), found.head.problem)
  }

  test("the oidc block is read, openid is added when it was forgotten, and the secret is followed") {
    val config = loaded(
      """kui:
        |  auth:
        |    type: oidc
        |    oidc:
        |      issuer: https://accounts.example.com
        |      clientId: kui
        |      clientSecret: "env:KUI_OIDC_SECRET"
        |      redirectUri: http://localhost:8080/api/v1/auth/oidc/callback
        |      scopes: [profile, email]
        |      usernameClaim: email
        |      groupsClaim: groups
        |      label: Example
        |""".stripMargin,
      env = Map("KUI_OIDC_SECRET" -> "s3cret")
    )

    val oidc = config.auth.oidc.getOrElse(fail("expected the oidc block to be read"))
    assertEquals(config.auth.authType, AuthType.Oidc)
    assertEquals(oidc.scopes, List("openid", "profile", "email"))
    assertEquals(oidc.usernameClaim, "email")
    assertEquals(oidc.groupsClaim, Some("groups"))
    assertEquals(oidc.clientSecret.value, "s3cret")
  }

  test("half an oidc block is a half-finished edit, not a block to ignore") {
    val found = problems(
      """kui:
        |  auth:
        |    oidc:
        |      issuer: https://accounts.example.com
        |""".stripMargin
    )

    assertEquals(
      found.map(_.key).sorted,
      List("kui.auth.oidc.clientId", "kui.auth.oidc.clientSecret", "kui.auth.oidc.redirectUri")
    )
  }

  test("type: oidc with no provider configured is refused") {
    assertEquals(problems("kui:\n  auth:\n    type: oidc\n").map(_.key), List("kui.auth.oidc"))
  }

  // -----------------------------------------------------------------------------------------------
  // kui.rbac
  // -----------------------------------------------------------------------------------------------

  private val ValidRoles: String =
    """kui:
      |  rbac:
      |    roles:
      |      - name: developers
      |        clusters: [local]
      |        subjects:
      |          - provider: FORM
      |            kind: group
      |            value: devs
      |        permissions:
      |          - resource: TOPIC
      |            value: "orders.*"
      |            actions: [MESSAGES_DELETE]
      |          - resource: AUDIT
      |            actions: [ALL]
      |    defaultRole:
      |      permissions:
      |        - resource: TOPIC
      |          value: ".*"
      |          actions: [VIEW]
      |""".stripMargin

  test("a role is read into the evaluator's own policy, with its actions already expanded") {
    val policy = loaded(ValidRoles).rbac

    assertEquals(policy.enabled, true)
    assertEquals(policy.roles.map(_.name), List(RoleName.unsafe("developers")))
    assertEquals(policy.roles.head.clusters, Set(ClusterId.unsafe("local")))
    assertEquals(
      policy.roles.head.subjects.head,
      kui.security.rbac.Subject(Provider.Form, SubjectKind.Group, "devs", isRegex = false)
    )

    val topic = policy.roles.head.permissions
      .find(_.resource == Resource.Topic)
      .getOrElse(fail("expected a topic permission"))

    // `MESSAGES_DELETE` implies being able to see the topic at all. The closure is applied at load
    // time so that the browser is handed an expanded list and never has to re-derive it.
    assert(topic.actions.contains(Action.TopicMessagesDelete), topic.actions.toString)
    assert(topic.actions.contains(Action.TopicView), topic.actions.toString)

    assertEquals(policy.defaultRole.map(_.permissions.size), Some(1))
  }

  test("ALL expands to every action the resource has") {
    val policy = loaded(ValidRoles).rbac
    val audit = policy.roles.head.permissions
      .find(_.resource == Resource.Audit)
      .getOrElse(fail("expected an audit permission"))

    assertEquals(audit.actions, Resource.Audit.allActions)
    assertEquals(audit.value, None)
  }

  test("an action that does not exist on that resource names the ones that do") {
    val found = problems(
      """kui:
        |  rbac:
        |    roles:
        |      - name: developers
        |        clusters: [local]
        |        subjects:
        |          - provider: FORM
        |            kind: group
        |            value: devs
        |        permissions:
        |          - resource: TOPIC
        |            value: ".*"
        |            actions: [DEMOLISH]
        |""".stripMargin
    )

    assertEquals(found.map(_.key), List("kui.rbac.roles.0.permissions.0.actions"))
    assert(found.head.problem.contains("'DEMOLISH' is not an action on TOPIC"), found.head.problem)
  }

  test("a named resource with no value is refused, because it would silently grant nothing") {
    val found = problems(
      """kui:
        |  rbac:
        |    roles:
        |      - name: developers
        |        clusters: [local]
        |        subjects:
        |          - provider: FORM
        |            kind: group
        |            value: devs
        |        permissions:
        |          - resource: TOPIC
        |            actions: [VIEW]
        |""".stripMargin
    )

    assertEquals(found.map(_.key), List("kui.rbac.roles.0.permissions.0.value"))
    assert(found.head.problem.contains("write '.*'"), found.head.problem)
  }

  test("a pattern that will not compile is reported at start-up and not at the first request") {
    val found = problems(
      """kui:
        |  rbac:
        |    roles:
        |      - name: developers
        |        clusters: [local]
        |        subjects:
        |          - provider: FORM
        |            kind: group
        |            value: devs
        |        permissions:
        |          - resource: TOPIC
        |            value: "orders(["
        |            actions: [VIEW]
        |""".stripMargin
    )

    assertEquals(found.map(_.key), List("kui.rbac.roles.0.permissions.0.value"))
  }

  test("a role with no subjects and no permissions is refused on both counts at once") {
    val found = problems(
      """kui:
        |  rbac:
        |    roles:
        |      - name: developers
        |        clusters: [local]
        |""".stripMargin
    )

    assertEquals(
      found.map(_.key).sorted,
      List("kui.rbac.roles.0.permissions", "kui.rbac.roles.0.subjects")
    )
  }

  test("two roles with one name is a role that would silently disappear") {
    val found = problems(
      """kui:
        |  rbac:
        |    roles:
        |      - name: developers
        |        clusters: [local]
        |        subjects:
        |          - provider: FORM
        |            kind: user
        |            value: ada
        |        permissions:
        |          - resource: KSQL
        |            actions: [ALL]
        |      - name: developers
        |        clusters: [local]
        |        subjects:
        |          - provider: FORM
        |            kind: user
        |            value: bob
        |        permissions:
        |          - resource: KSQL
        |            actions: [ALL]
        |""".stripMargin
    )

    assertEquals(found.map(_.key), List("kui.rbac.roles.0.name"))
    assert(found.head.problem.contains("names more than one role"), found.head.problem)
  }

  test("an unknown provider or subject kind is named, with the legal values") {
    val found = problems(
      """kui:
        |  rbac:
        |    roles:
        |      - name: developers
        |        clusters: [local]
        |        subjects:
        |          - provider: CARRIER_PIGEON
        |            kind: flock
        |            value: devs
        |        permissions:
        |          - resource: KSQL
        |            actions: [ALL]
        |""".stripMargin
    )

    assertEquals(
      found.map(_.key).sorted,
      List("kui.rbac.roles.0.subjects.0.kind", "kui.rbac.roles.0.subjects.0.provider")
    )
  }

  test("a mistyped key under kui.rbac is still an unknown key") {
    val found = problems(
      """kui:
        |  rbac:
        |    roles:
        |      - name: developers
        |        clusters: [local]
        |        permisions:
        |          - resource: KSQL
        |            actions: [ALL]
        |""".stripMargin
    )

    // The unknown-key check reports the *leaves* it found, so the mistyped parent appears as the
    // prefix of every key under it. Either way the operator is told the word they wrote, which is
    // the promise; and the role itself is separately refused for having no permissions at all.
    assert(
      found.exists(_.key.startsWith("kui.rbac.roles.0.permisions")),
      found.map(_.key).toString
    )
    assert(found.exists(_.key == "kui.rbac.roles.0.permissions"), found.map(_.key).toString)
  }

  // -----------------------------------------------------------------------------------------------
  // A role on a cluster that is not here
  // -----------------------------------------------------------------------------------------------

  /** One configured cluster and one role, with the role's cluster list left to the caller.
    *
    * PLAINTEXT and one bootstrap address, because nothing here opens a socket: the cluster exists so that
    * `kui.clusters[]` is non-empty and has an id worth naming or mis-naming.
    */
  private def oneClusterAndARoleOn(named: String, extra: String = ""): String =
    s"""kui:
       |  clusters:
       |    - name: "Local"
       |      id: "local"
       |      bootstrapServers:
       |        - "kafka:9092"
       |$extra
       |  rbac:
       |    roles:
       |      - name: developers
       |        clusters: [$named]
       |        subjects:
       |          - provider: FORM
       |            kind: group
       |            value: devs
       |        permissions:
       |          - resource: TOPIC
       |            value: ".*"
       |            actions: [VIEW]
       |""".stripMargin

  test("a role on a cluster this file does not configure is refused, and the message names both") {
    // THE DEMONSTRATION THIS PREVENTS IS THE ONE IN `kui-quickstart-auth.yaml`. Both of its roles named a
    // cluster; if either id were wrong the file still loaded, both accounts still signed in, and every
    // screen was empty -- because `RbacPolicy.held` matches a role's cluster ids against the registry's and
    // a role that matches nothing grants nothing. That file's own comment claimed this suite caught it,
    // which it did not until this case existed.
    val found = problems(oneClusterAndARoleOn("no-such-cluster"))

    assertEquals(found.map(_.key), List("kui.rbac.roles.0.clusters"))
    assert(
      found.head.problem.contains("'no-such-cluster'"),
      s"the message must name the id that is wrong: ${found.head.problem}"
    )
    assert(
      found.head.problem.contains("local"),
      s"and the ids that are right, or the operator cannot see the typo: ${found.head.problem}"
    )
  }

  test("a role on a cluster this file does configure loads, and keeps the id it named") {
    // The other half, and the reason the case above cannot be satisfied by refusing every role: the shipped
    // `kui-quickstart-auth.yaml` is exactly this shape and has to keep loading.
    val policy = loaded(oneClusterAndARoleOn("local")).rbac

    assertEquals(policy.roles.head.clusters, Set(ClusterId.unsafe("local")))
  }

  test("a file that configures no cluster at all says nothing about a role's clusters") {
    // `kui.clusters: []` is not an empty set of clusters, it is a deployment that registered none *here* --
    // the M0 default and the Compose gateway's own `kui.yaml`. There is no set to compare against, so the
    // rule stays quiet rather than refusing a file it cannot judge. Every other case in this section relies
    // on it: they all write `clusters: [local]` on a role with no `kui.clusters` anywhere.
    val policy = loaded(ValidRoles).rbac

    assertEquals(policy.roles.head.clusters, Set(ClusterId.unsafe("local")))
  }

  test("a deployment with a metadata store may name a cluster this file has never heard of") {
    // ADR-036 as amended by ADR-042: the store's records overlay `kui.clusters[]` at run time, so an
    // operator who registered `prod-3` through the UI and wrote a role for it is correct, and this file
    // cannot see the cluster to agree. Refusing there would refuse a working deployment at start-up.
    //
    // A directory store rather than a Kafka one, because `checkStoreRules` demands an encryption key
    // alongside `kui.store.kafka.bootstrapServers` and that is a different rule being exercised.
    val stored = loaded(oneClusterAndARoleOn("prod-3", extra = "  store:\n    dir: \"/var/lib/kui\"")).rbac

    assertEquals(stored.roles.head.clusters, Set(ClusterId.unsafe("prod-3")))
  }

  test("a deployment with a KAFKA metadata store may too, which is the arm the case above cannot reach") {
    // THE SAME EXEMPTION THROUGH THE OTHER HALF OF ITS CONDITION, AND UNTIL THIS CASE ONLY ONE HALF WAS
    // LOAD-BEARING. `rolesNameConfiguredClusters` writes `draft.store.kafka.isDefined ||
    // draft.store.dir.isDefined`; deleting the `kafka` arm left `./mill libs.config.test` at 395/395,
    // because the case above configures a directory store and nothing anywhere configured a Kafka one
    // beside a role. The two stores are not interchangeable in this repository -- ADR-042 §7 makes the
    // Kafka store the production shape and the directory one the single-node convenience -- so the arm
    // that was ungated is the arm every real ADR-036 deployment takes, and under the mutation such a
    // deployment is refused at boot for naming a cluster its own store registered.
    //
    // The encryption key is here because `checkStoreRules` refuses a Kafka store without one; it is the
    // fixture's cost of reaching this arm at all, and it is why the case above took the cheaper one.
    val kafkaStore =
      """|  store:
         |    kafka:
         |      bootstrapServers:
         |        - "kafka-store:9092"
         |    encryptionKey: "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="
         |""".stripMargin.stripSuffix("\n")

    val stored = loaded(oneClusterAndARoleOn("prod-3", extra = kafkaStore)).rbac

    assertEquals(stored.roles.head.clusters, Set(ClusterId.unsafe("prod-3")))
    // And the store really is the Kafka one, so a fixture that silently stopped configuring it -- a
    // renamed key, a policy that dropped the address -- cannot keep this case green by taking the
    // `configured.isEmpty` exit or the directory arm instead.
    assertEquals(loaded(oneClusterAndARoleOn("prod-3", extra = kafkaStore)).store.kafkaEnabled, true)
  }

  /** One configured cluster and two roles, each naming a cluster of the caller's choosing.
    *
    * Every other fixture in this section writes exactly one role, which is what left the fold below it unable
    * to fail: with one role there is no second offender to lose and `$index` is never anything but `0`.
    */
  private def oneClusterAndTwoRolesOn(first: String, second: String): String =
    s"""kui:
       |  clusters:
       |    - name: "Local"
       |      id: "local"
       |      bootstrapServers:
       |        - "kafka:9092"
       |  rbac:
       |    roles:
       |      - name: developers
       |        clusters: [$first]
       |        subjects:
       |          - provider: FORM
       |            kind: group
       |            value: devs
       |        permissions:
       |          - resource: TOPIC
       |            value: ".*"
       |            actions: [VIEW]
       |      - name: analysts
       |        clusters: [$second]
       |        subjects:
       |          - provider: FORM
       |            kind: group
       |            value: analysts
       |        permissions:
       |          - resource: TOPIC
       |            value: ".*"
       |            actions: [VIEW]
       |""".stripMargin

  test("two roles on clusters this file does not configure are BOTH reported, with their own indices") {
    // AN OPERATOR FIXES WHAT THEY WERE TOLD ABOUT AND RESTARTS. If only the first offending role is
    // reported, the second startup fails on the second role, the third on a third, and a file with four bad
    // roles costs four restarts of a process that takes twenty seconds to come up -- which is the failure
    // mode every other rule in this loader was written against, since `ConfigProblem` is accumulated in a
    // `NonEmptyList` precisely so that one boot names everything that is wrong.
    //
    // Measured before this case existed: `draft.rbac.roles.zipWithIndex.flatMap` -> `... .take(1).flatMap`
    // left `./mill libs.config.test` at 395/395 SUCCESS. Every case in this section used a one-role
    // fixture, so nothing could tell a fold that reports all offenders from one that reports the first.
    //
    // The indices are asserted and not just the count, because `$index` is the only thing in the message
    // that tells the operator WHICH role to edit, and with one role it is `0` whether it is read off the
    // fold or written as a literal.
    val found = problems(oneClusterAndTwoRolesOn("no-such-cluster", "also-missing"))

    assertEquals(found.map(_.key), List("kui.rbac.roles.0.clusters", "kui.rbac.roles.1.clusters"))
    assertEquals(
      found.map(_.problem.contains("'no-such-cluster'")),
      List(true, false),
      clue = s"the first problem is the first role's: ${found.map(_.problem)}"
    )
    assertEquals(
      found.map(_.problem.contains("'also-missing'")),
      List(false, true),
      clue = s"and the second is the second role's: ${found.map(_.problem)}"
    )
  }

  test("a good role before a bad one does not shift the bad one's index") {
    // The companion of the case above and the reason it cannot be satisfied by numbering the PROBLEMS
    // rather than the roles. `kui.rbac.roles.1.clusters` has to be the path an operator can follow into
    // their own file, so the index is the offending role's position among all the roles and not its
    // position among the complaints -- a distinction that is invisible while every role in the fixture is
    // wrong.
    val found = problems(oneClusterAndTwoRolesOn("local", "no-such-cluster"))

    assertEquals(found.map(_.key), List("kui.rbac.roles.1.clusters"))
  }
}
