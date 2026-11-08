package kui.config

import java.nio.file.Path

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import kui.kernel.cluster.PropertyValue
import kui.testkit.KuiSuite

/** That `kui.clusters[]` is read the way the operator documentation says it is, and that a file with several
  * mistakes in it produces several messages rather than one per restart.
  *
  * This is the cluster half of M1's "configuration fails at startup with all errors accumulated in one
  * message" exit criterion. The other half — an unknown key and an invalid URL — is `ValidationSuite`'s, and
  * `accumulatesEveryClusterProblem` asserts the two halves add up rather than shadowing each other.
  */
final class ClusterConfigSuite extends KuiSuite {

  private def load(
      files: List[Path],
      env: Map[String, String] = Map.empty
  ): Either[ConfigErrors, KuiConfig] =
    KuiConfigSource.loadFrom[IO](Nil, files, env, UrlPolicy.Dev).unsafeRunSync()

  private def problems(result: Either[ConfigErrors, KuiConfig]): List[ConfigProblem] =
    result match {
      case Left(errors) => errors.problems.toList.sortBy(_.key)
      case Right(_) => fail("expected the load to fail, but it succeeded")
    }

  private def loaded(result: Either[ConfigErrors, KuiConfig]): KuiConfig =
    result.fold(errors => fail(errors.render), identity)

  test("accumulatesEveryClusterProblem") {
    val found = problems(load(List(ConfigFixtures.fixture("clusters-multiple-errors.yaml"))))

    assertEquals(
      found.map(_.key),
      List(
        "kui.clusters.0.bootstrapServers",
        "kui.clusters.0.security.mechanism",
        "kui.clusters.1.security.password"
      )
    )
    assert(found.head.problem.contains("host:port"), found.head.problem)
    assert(found(1).problem.contains("SCRAM-SHA-512"), found(1).problem)
    assert(found(2).problem.contains("KUI_PROD_PASSWORD"), found(2).problem)
  }

  test("noProblemEchoesASecret") {
    val found = problems(
      load(
        List(ConfigFixtures.fixture("clusters-multiple-errors.yaml")),
        env = Map("KUI_CLUSTERS_1_SECURITY_PASSWORD" -> "hunter2")
      )
    )

    assert(found.forall(problem => !problem.render.contains("hunter2")), found.map(_.render).mkString("\n"))
  }

  test("unknownKeyUnderAClusterIsRejected") {
    val file = ConfigFixtures.yaml(
      """kui:
        |  clusters:
        |    - name: One
        |      bootstrapServer: broker:9092
        |""".stripMargin
    )

    val found = problems(load(List(file)))

    assert(
      found.exists(problem => problem.key == "kui.clusters.0.bootstrapServer"),
      found.map(_.key).mkString(", ")
    )
  }

  test("propertiesAreNotSubjectToUnknownKeyChecking") {
    val file = ConfigFixtures.yaml(
      """kui:
        |  clusters:
        |    - name: One
        |      bootstrapServers: broker:9092
        |      properties:
        |        a.property.kui.has.never.heard.of: 7
        |""".stripMargin
    )

    val config = loaded(load(List(file)))

    assertEquals(
      config.clusters.head.properties.get("a.property.kui.has.never.heard.of"),
      Some(PropertyValue.Plain("7"))
    )
  }

  test("propertiesFromTheEnvironmentAreRejectedWithTheReason") {
    val file = ConfigFixtures.yaml(
      """kui:
        |  clusters:
        |    - name: One
        |      bootstrapServers: broker:9092
        |""".stripMargin
    )

    val found = problems(
      load(List(file), env = Map("KUI_CLUSTERS_0_PROPERTIES_SSL_CIPHER_SUITES" -> "TLS_AES_256_GCM_SHA384"))
    )

    assertEquals(found.map(_.key), List("kui.clusters.<n>.properties"))
    assert(found.head.problem.contains("KUI_CLUSTERS_0_PROPERTIES_SSL_CIPHER_SUITES"), found.head.problem)
    assert(found.head.problem.contains("literal"), found.head.problem)
    assert(!found.head.problem.contains("still uses env:NAME"), found.head.problem)
  }

  test("aPropertyValueIsLiteralAndDoesNotResolveEnv") {
    val file = ConfigFixtures.yaml(
      """kui:
        |  clusters:
        |    - name: One
        |      bootstrapServers: broker:9092
        |      properties:
        |        ssl.truststore.password: env:TS_PW
        |""".stripMargin
    )

    val cluster = loaded(load(List(file), env = Map("TS_PW" -> "x"))).clusters.head

    assertEquals(
      cluster.properties.get("ssl.truststore.password").map(_.unsafeValue),
      Some("env:TS_PW")
    )
  }

  test("emptyClusterListLoads") {
    val file = ConfigFixtures.yaml("kui:\n  clusters: []\n")

    assertEquals(loaded(load(List(file))).clusters, Nil)
  }

  test("noClustersAtAllLoads") {
    assertEquals(loaded(load(Nil)).clusters, Nil)
  }

  test("sparseIndexIsRejected") {
    val found = problems(
      load(
        Nil,
        env = Map(
          "KUI_CLUSTERS_0_NAME" -> "Zero",
          "KUI_CLUSTERS_0_BOOTSTRAPSERVERS" -> "a:9092",
          "KUI_CLUSTERS_2_NAME" -> "Two",
          "KUI_CLUSTERS_2_BOOTSTRAPSERVERS" -> "b:9092"
        )
      )
    )

    assertEquals(found.map(_.key), List("kui.clusters.2"))
    assert(found.head.problem.contains("2 follows 0"), found.head.problem)
  }

  test("duplicateSlugIsRejectedNamingBothClusters") {
    val found = problems(load(List(ConfigFixtures.fixture("clusters-duplicate-slug.yaml"))))

    assertEquals(found.map(_.key), List("kui.clusters.0.id"))
    assert(found.head.problem.contains("'Production EU'"), found.head.problem)
    assert(found.head.problem.contains("'production-eu'"), found.head.problem)
  }

  test("anExplicitIdSurvivesARename") {
    val file = ConfigFixtures.yaml(
      """kui:
        |  clusters:
        |    - name: Production EU (renamed)
        |      id: prod-eu
        |      bootstrapServers: broker:9092
        |""".stripMargin
    )

    assertEquals(loaded(load(List(file))).clusters.map(_.id.value), List("prod-eu"))
  }

  test("theWholeMechanismTableLoadsFromOneFile") {
    val config = loaded(
      load(
        List(ConfigFixtures.fixture("clusters-all-mechanisms.yaml")),
        env = Map("KUI_PROD_PASSWORD" -> "s3cret", "KUI_OAUTH_SECRET" -> "oauth-s3cret")
      )
    )

    assertEquals(config.clusters.size, 10)
    assertEquals(
      config.clusters.map(_.id.value).take(3),
      List("local-development", "production-eu", "staging-plain")
    )
    assertEquals(config.clusters(1).readOnly, true)
    assertEquals(config.clusters.head.readOnly, false)
  }

  test("propertiesSurviveVerbatimAndAreRedactedByKeyPattern") {
    val config = loaded(
      load(
        List(ConfigFixtures.fixture("clusters-all-mechanisms.yaml")),
        env = Map("KUI_PROD_PASSWORD" -> "s3cret", "KUI_OAUTH_SECRET" -> "oauth-s3cret")
      )
    )

    val mtls = config.clusters.find(_.name == "Mutual TLS").getOrElse(fail("the mTLS cluster is missing"))

    assertEquals(
      mtls.properties.get("ssl.cipher.suites"),
      Some(PropertyValue.Plain("TLS_AES_256_GCM_SHA384"))
    )
    assertEquals(
      mtls.properties.get("ssl.truststore.password").map(_.unsafeValue),
      Some("overridden-secret")
    )
    assertEquals(mtls.properties.redactedValues("ssl.truststore.password"), "***")
    assert(!mtls.toString.contains("overridden-secret"), mtls.toString)
  }

  test("aRawKeystoreKeyIsRedactedFromClusterToString") {
    val pem = "-----BEGIN PRIVATE KEY-----hunter2"
    val file = ConfigFixtures.yaml(
      s"""kui:
         |  clusters:
         |    - name: One
         |      bootstrapServers: broker:9092
         |      properties:
         |        ssl.keystore.key: "$pem"
         |""".stripMargin
    )

    val cluster = loaded(load(List(file))).clusters.head

    assertEquals(cluster.properties.get("ssl.keystore.key").map(_.unsafeValue), Some(pem))
    assert(!cluster.toString.contains("BEGIN PRIVATE KEY"), cluster.toString)
    assert(!cluster.toString.contains("hunter2"), cluster.toString)
    assert(!cluster.connection.toString.contains("BEGIN PRIVATE KEY"), cluster.connection.toString)
  }

  test("aClusterNameLongerThanTheLimitIsRejected") {
    val file = ConfigFixtures.yaml(
      s"""kui:
         |  clusters:
         |    - name: ${"n" * (ClusterConfig.MaxNameLength + 1)}
         |      bootstrapServers: broker:9092
         |""".stripMargin
    )

    assertEquals(problems(load(List(file))).map(_.key), List("kui.clusters.0.name"))
  }
}
