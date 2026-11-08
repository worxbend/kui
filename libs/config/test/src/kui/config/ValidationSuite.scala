package kui.config

import java.nio.file.Path

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import kui.testkit.KuiSuite

/** That a bad configuration fails at startup, says everything that is wrong, and never prints a secret while
  * doing it.
  *
  * The alternative — the one every operator has lived through — is a process that starts, ignores the key
  * that was mistyped, and reveals the problem at three in the morning as a null pointer.
  */
final class ValidationSuite extends KuiSuite {

  private def load(
      files: List[Path],
      env: Map[String, String] = Map.empty,
      policy: UrlPolicy = UrlPolicy.Dev
  ): Either[ConfigErrors, KuiConfig] =
    KuiConfigSource.loadFrom[IO](Nil, files, env, policy).unsafeRunSync()

  private def problems(result: Either[ConfigErrors, KuiConfig]): List[ConfigProblem] =
    result match {
      case Left(errors) => errors.problems.toList
      case Right(_) => fail("expected the load to fail, but it succeeded")
    }

  test("the complete valid fixture loads, and every key in it reaches the model") {
    val loaded =
      load(List(ConfigFixtures.fixture("valid.yaml"))).fold(errors => fail(errors.render), identity)

    assertEquals(loaded.server.port.value, 9090)
    assertEquals(loaded.server.basePath, "/kui")
    assertEquals(loaded.gateway.readinessInterval.toMillis, 5000L)
    assertEquals(loaded.gateway.services.keySet.map(_.value), Set("cluster", "topic"))
    assertEquals(loaded.gateway.services.head._2.maxConcurrent.value, 8)
    assertEquals(loaded.gateway.cors, CorsConfig(enabled = true, List("https://console.example.com")))
    assertEquals(loaded.gateway.principalKeys.map(_.kid), List("k1"))
    assertEquals(loaded.telemetry.logFormat, LogFormat.Text)
    assertEquals(loaded.telemetry.prometheusPort.map(_.value), Some(9464))
    assertEquals(loaded.telemetry.hashUserIds, false)
  }

  test("reportsEveryProblemNotJustTheFirst") {
    val found =
      problems(load(List(ConfigFixtures.fixture("multiple-errors.yaml")), policy = UrlPolicy.Strict))

    assertEquals(
      found.map(_.key).sorted,
      List("kui.gateway.services.cluster.url", "kui.server.port", "kui.telemetry.logFormat")
    )
  }

  test("rejectsUnknownKeys") {
    val found = problems(load(List(ConfigFixtures.fixture("unknown-key.yaml"))))

    assertEquals(found.map(_.key), List("kui.server.prot"))
    assert(found.head.problem.contains("not a KUI configuration key"), found.head.problem)
    // A typo-suggestion feature is explicitly out of scope: naming the key is the whole promise.
    assert(!found.head.problem.contains("did you mean"), found.head.problem)
  }

  test("devInsecureCookiesCanBeSetInAFileAndNotOnlyInTheEnvironment") {
    // It was decoded but missing from the list of recognised keys, so `--kui.server....` and
    // `KUI_SERVER_DEVINSECURECOOKIES` worked while the same setting in a YAML file was refused as a
    // typo. That asymmetry is invisible until someone writes it in the file that documents it --
    // which is exactly what the Compose development environment does (INFRA-002).
    val file = ConfigFixtures.yaml(
      """kui:
        |  server:
        |    devInsecureCookies: true
        |""".stripMargin
    )

    val loaded = load(List(file)).fold(errors => fail(errors.render), identity)

    assertEquals(loaded.gateway.devInsecureCookies, true)
  }

  test("anEmptyListIsAcceptedWhereItsElementsAreKnown") {
    // The walk that finds unknown keys stops at an empty container, so `origins: []` arrives as
    // the path `kui.gateway.cors.origins` rather than as any element under it. Declaring only
    // `origins.*` therefore rejected an empty allow-list -- which is a legitimate setting, and the
    // one the shipped Compose file uses, so the gateway refused to start with its own defaults.
    val file = ConfigFixtures.yaml(
      """kui:
        |  gateway:
        |    cors:
        |      enabled: false
        |      origins: []
        |""".stripMargin
    )

    val loaded = load(List(file)).fold(errors => fail(errors.render), identity)

    assertEquals(loaded.gateway.cors, CorsConfig(enabled = false, Nil))
  }

  /** Empty sections, one per shape the model has: a map of services, a list of signing keys, and a whole
    * block whose only key the operator commented out. None of them supplies a value, so none of them can be a
    * wrong value -- and the shipped `deployment/compose/kui.yaml` tells the operator in a comment that
    * `services` is empty in the all-in-one deployment, so this is not a hypothetical shape.
    */
  private val emptySections = Map(
    "an empty map of services" -> """kui:
      |  gateway:
      |    services: {}
      |""".stripMargin,
    "an empty list of signing keys" -> """kui:
      |  gateway:
      |    principalKeys: []
      |""".stripMargin,
    "an empty telemetry block" -> """kui:
      |  telemetry: {}
      |""".stripMargin,
    "an empty server block" -> """kui:
      |  server: {}
      |""".stripMargin,
    "an empty cors block" -> """kui:
      |  gateway:
      |    cors: {}
      |""".stripMargin
  )

  emptySections.foreach { case (description, contents) =>
    test(s"$description is accepted rather than reported as an unknown key") {
      val loaded =
        load(List(ConfigFixtures.yaml(contents))).fold(errors => fail(errors.render), identity)

      assertEquals(loaded.gateway.services, Map.empty)
      assertEquals(loaded.gateway.principalKeys, Nil)
    }
  }

  test("a section that holds a value rather than keys is still an unknown key") {
    // The relaxation above is for containers that supply nothing. A scalar where a section belongs
    // is a real mistake and must still be named.
    val found = problems(load(List(ConfigFixtures.yaml("kui:\n  telemetry: 7\n"))))

    assertEquals(found.map(_.key), List("kui.telemetry"))
  }

  test("a principal key set with nothing active yet is refused") {
    val material = "0123456789abcdef0123456789abcdef"
    val file = ConfigFixtures.yaml(
      s"""kui:
         |  gateway:
         |    principalKeys:
         |      - kid: main
         |        key: "$material"
         |        notBefore: "2062-01-01T00:00:00Z"
         |""".stripMargin
    )

    val found = problems(load(List(file)))
    val rendered = found.map(_.render).mkString("\n")

    assertEquals(found.map(_.key), List("kui.gateway.principalKeys"))
    assert(found.head.problem.contains("no signing key is active yet"), found.head.problem)
    assert(!rendered.contains(material), rendered)
  }

  test("a future signing key beside an active one still loads") {
    val file = ConfigFixtures.yaml(
      """kui:
        |  gateway:
        |    principalKeys:
        |      - kid: current
        |        key: "0123456789abcdef0123456789abcdef"
        |      - kid: next
        |        key: "abcdef0123456789abcdef0123456789"
        |        notBefore: "2062-01-01T00:00:00Z"
        |""".stripMargin
    )

    val loaded = load(List(file)).fold(errors => fail(errors.render), identity)
    assertEquals(loaded.gateway.principalKeys.map(_.kid), List("current", "next"))
  }

  test("a signing key written as a named child is reported, not silently dropped") {
    // An indentation slip turns the list `- kid: k1` into the map `k1:`. Those children used to be
    // filtered out for not being numbers, leaving an empty key list from a file with a key plainly
    // written in it: the cluster service refuses to start saying no signing keys are configured,
    // while the operator is looking straight at one.
    val file = ConfigFixtures.yaml(
      """kui:
        |  gateway:
        |    principalKeys:
        |      first:
        |        kid: "k1"
        |        key: "s3cret"
        |""".stripMargin
    )

    val found = problems(load(List(file)))

    assertEquals(found.map(_.key), List("kui.gateway.principalKeys.first"))
    assert(found.head.problem.contains("is not a list entry"), found.head.problem)
    assert(!found.head.render.contains("s3cret"), found.head.render)
  }

  test("a YAML syntax error never echoes the line it choked on") {
    // The parser's own message ends with the offending source line. That happens before anything is
    // decoded, so nothing is a `Secret` yet and the redaction that protects every other error path
    // cannot apply -- an unclosed quote on the signing-key line would print the key to stderr and
    // into `docker logs`. Only the parser's first line, which names the position, is reported.
    val file = ConfigFixtures.yaml(
      """kui:
        |  gateway:
        |    principalKeys:
        |      - kid: "k1"
        |        key: "TOPSECRETVALUE
        |""".stripMargin
    )

    val rendered = problems(load(List(file))).map(_.render).mkString("\n")

    assert(!rendered.contains("TOPSECRETVALUE"), rendered)
    assert(rendered.contains("is not valid YAML"), rendered)
  }

  test("the placeholder sections for M1 and M6 are accepted but read no keys") {
    val file = ConfigFixtures.yaml(
      """kui:
        |  clusters:
        |    - name: prod
        |      bootstrapServers: "kafka:9092"
        |  rbac:
        |    roles: []
        |""".stripMargin
    )

    assert(load(List(file)).isRight, load(List(file)).left.map(_.render).merge.toString)
  }

  test("secretValuesAreRedactedInProblemMessages") {
    val file = ConfigFixtures.yaml(
      """kui:
        |  gateway:
        |    principalKeys:
        |      - kid: "k1"
        |        key: ""
        |""".stripMargin
    )

    val found = problems(load(List(file)))
    val rendered = found.map(_.render).mkString("\n")

    assertEquals(found.map(_.key), List("kui.gateway.principalKeys.0.key"))
    assert(rendered.contains("***"), rendered)
  }

  test("a file: secret reference that cannot be read never prints the file's contents") {
    val secretFile = ConfigFixtures.yaml("hunter2-DO-NOT-LEAK")
    val file = ConfigFixtures.yaml(
      s"""kui:
         |  gateway:
         |    principalKeys:
         |      - kid: "k1"
         |        key: "file:${secretFile.toString}/not-a-directory"
         |""".stripMargin
    )

    val rendered = problems(load(List(file))).map(_.render).mkString("\n")

    assert(!rendered.contains("hunter2-DO-NOT-LEAK"), rendered)
    assert(rendered.contains("could not be read"), rendered)
  }

  test("a file: secret reference that can be read produces the secret") {
    val secretFile = ConfigFixtures.yaml("hunter2-DO-NOT-LEAK")
    val file = ConfigFixtures.yaml(
      s"""kui:
         |  gateway:
         |    principalKeys:
         |      - kid: "k1"
         |        key: "file:${secretFile.toString}"
         |""".stripMargin
    )

    val loaded = load(List(file)).fold(errors => fail(errors.render), identity)
    assertEquals(loaded.gateway.principalKeys.head.key.value, "hunter2-DO-NOT-LEAK")
    assertEquals(loaded.gateway.principalKeys.head.key.toString, "Secret(***)")
  }

  test("an env: secret reference reads the environment") {
    val file = ConfigFixtures.yaml(
      """kui:
        |  gateway:
        |    principalKeys:
        |      - kid: "k1"
        |        key: "env:KUI_SIGNING_KEY"
        |""".stripMargin
    )

    val loaded = load(List(file), env = Map("KUI_SIGNING_KEY" -> "from-the-environment"))
      .fold(errors => fail(errors.render), identity)

    assertEquals(loaded.gateway.principalKeys.head.key.value, "from-the-environment")
  }

  test("an authentication type KUI does not have is still refused, and lists the ones it does") {
    // This used to refuse everything but `disabled` and point at M6. The identity service is that
    // milestone: `form` and `oidc` are now real, and the check is that an unrecognised word is still
    // a startup error rather than a mode that silently becomes no authentication at all.
    val file = ConfigFixtures.yaml("kui:\n  auth:\n    type: oauth2\n")
    val found = problems(load(List(file)))

    assertEquals(found.map(_.key), List("kui.auth.type"))
    assert(found.head.problem.contains("disabled, form, oidc"), found.head.problem)
  }

  test("a cors origin list containing '*' is refused at load time") {
    val file = ConfigFixtures.yaml(
      """kui:
        |  gateway:
        |    cors:
        |      enabled: true
        |      origins: ["*"]
        |""".stripMargin
    )

    val found = problems(load(List(file)))
    assertEquals(found.map(_.key), List("kui.gateway.cors.origins"))
  }

  test("a refused URL does not echo user-info or a query token") {
    val credential = "https://u:hunter2@registry.example.com/"
    val queried = "https://10.1.2.3/path?access_token=hunter2"
    val file = ConfigFixtures.yaml(
      s"""kui:
         |  gateway:
         |    services:
         |      cluster:
         |        url: "$credential"
         |  telemetry:
         |    otlpEndpoint: "$queried"
         |  clusterProfiles:
         |    url: "$credential"
         |  clusters:
         |    - name: One
         |      bootstrapServers: broker:9092
         |      schemaRegistry:
         |        url: "$credential"
         |        auth:
         |          type: oauth
         |          tokenEndpoint: "$credential"
         |          clientId: kui
         |          clientSecret: not-the-canary
         |      connect:
         |        - url: "$credential"
         |      ksql:
         |        url: "$credential"
         |""".stripMargin
    )

    val found = problems(load(List(file), policy = UrlPolicy.Strict))
    val rendered = found.map(_.render).mkString("\n")

    assert(found.nonEmpty, "the credential-bearing URLs were accepted")
    assert(!rendered.contains("hunter2"), rendered)
    assert(rendered.contains("registry.example.com"), rendered)
  }

  test("an upstream service without a url is a named, required-key problem") {
    val file = ConfigFixtures.yaml(
      "kui:\n  gateway:\n    services:\n      cluster:\n        timeout: \"3s\"\n"
    )

    val found = problems(load(List(file)))
    assertEquals(found.map(_.key), List("kui.gateway.services.cluster.url"))
    assert(found.head.problem.contains("is required"), found.head.problem)
  }

  test("every problem names its source, so the operator knows which layer to edit") {
    val file = ConfigFixtures.yaml("kui:\n  server:\n    port: 0\n")

    val fromFile = problems(load(List(file))).head
    assert(fromFile.source.isInstanceOf[ConfigSourceName.File], fromFile.render)

    val fromEnv = problems(load(Nil, env = Map("KUI_SERVER_PORT" -> "0"))).head
    assertEquals(fromEnv.source, ConfigSourceName.Env)
  }

  test("a YAML file that does not parse fails the load with the file named") {
    val file = ConfigFixtures.yaml("kui:\n  server:\n  port: [unclosed\n")
    val found = problems(load(List(file)))

    assert(found.exists(_.source.isInstanceOf[ConfigSourceName.File]), found.map(_.render).mkString)
  }

  test("the rendered report is one line per problem, in key order") {
    val rendered = load(List(ConfigFixtures.fixture("multiple-errors.yaml")), policy = UrlPolicy.Strict).swap
      .map(_.render)
      .getOrElse(fail("expected a failure"))

    val lines = rendered.linesIterator.toList
    assertEquals(lines.size, 3)
    assertEquals(lines.map(_.takeWhile(_ != ':')), lines.map(_.takeWhile(_ != ':')).sorted)
  }
}
