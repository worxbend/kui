package kui.config

import java.util.Locale

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.scalacheck.{Gen, Prop}

import kui.kernel.cluster.{ClusterSecurity, SaslMechanism, SaslProtocol}
import kui.testkit.KuiSuite

/** The mechanism table of `docs/operations/configuration.md`, asserted one row at a time.
  *
  * Every row an operator can write down has to decode into the ADT case the renderer expects, and every row
  * with a field missing has to say which field — once, naming that field, and not naming the other seven. An
  * operator filling in a Kerberos cluster should learn about all three missing keys in one restart.
  */
final class ClusterSecuritySuite extends KuiSuite {

  private def load(
      yaml: String,
      env: Map[String, String] = Map.empty,
      policy: UrlPolicy = UrlPolicy.Dev
  ): Either[ConfigErrors, KuiConfig] =
    KuiConfigSource
      .loadFrom[IO](Nil, List(ConfigFixtures.yaml(yaml)), env, policy)
      .unsafeRunSync()

  /** One cluster whose `security` block is the indented text supplied. */
  private def cluster(security: String): String =
    s"""kui:
       |  clusters:
       |    - name: One
       |      bootstrapServers: broker:9092
       |${if security.isEmpty then "" else s"      security:\n$security"}
       |""".stripMargin

  private def securityOf(yaml: String, env: Map[String, String] = Map.empty): ClusterSecurity =
    load(yaml, env).fold(errors => fail(errors.render), _.clusters.head.security)

  private def problemKeys(yaml: String, policy: UrlPolicy = UrlPolicy.Dev): List[String] =
    load(yaml, policy = policy) match {
      case Left(errors) => errors.problems.toList.map(_.key).sorted
      case Right(_) => fail("expected the load to fail, but it succeeded")
    }

  private def saslOf(security: ClusterSecurity): SaslMechanism =
    security match {
      case ClusterSecurity.Sasl(_, mechanism, _) => mechanism
      case other => fail(s"expected a SASL security model, got $other")
    }

  // -------------------------------------------------------------------------------------------
  // Every mechanism decodes
  // -------------------------------------------------------------------------------------------

  test("aClusterWithNoSecurityKeyIsPlaintext") {
    assertEquals(securityOf(cluster("")), ClusterSecurity.Plaintext)
  }

  test("plainDecodes") {
    val security = securityOf(
      cluster("""        protocol: SASL_PLAINTEXT
                |        mechanism: PLAIN
                |        username: kui
                |        password: pw""".stripMargin)
    )

    assertEquals(saslOf(security), SaslMechanism.Plain("kui", kui.kernel.Secret("pw")))
    security match {
      case ClusterSecurity.Sasl(protocol, _, tls) =>
        assertEquals(protocol, SaslProtocol.SaslPlaintext)
        assertEquals(tls, None)
      case other => fail(s"expected SASL, got $other")
    }
  }

  test("securityTokensDecodeIndependentlyOfHostLocale") {
    val prefix = "kui.clusters.0.security"
    val values = Map(
      s"$prefix.protocol" -> "sasl_plaintext",
      s"$prefix.mechanism" -> "plain",
      s"$prefix.username" -> "kui",
      s"$prefix.password" -> "pw"
    )
    val previous = Locale.getDefault
    val decoded =
      try {
        Locale.setDefault(Locale.forLanguageTag("tr-TR"))
        ClusterSecurityConfig.decode(
          prefix,
          key => values.get(key).map(value => ConfigSourceName.Default -> value)
        )
      } finally Locale.setDefault(previous)

    val security = decoded.toEither.map(
      _.assemble(Map(s"$prefix.password" -> kui.kernel.Secret("pw")))
    )
    assertEquals(
      security,
      Right(
        ClusterSecurity.Sasl(
          SaslProtocol.SaslPlaintext,
          SaslMechanism.Plain("kui", kui.kernel.Secret("pw")),
          None
        )
      )
    )
  }

  test("scramSha256Decodes") {
    val security = securityOf(
      cluster("""        protocol: SASL_PLAINTEXT
                |        mechanism: SCRAM-SHA-256
                |        username: kui
                |        password: pw""".stripMargin)
    )
    assertEquals(saslOf(security), SaslMechanism.ScramSha256("kui", kui.kernel.Secret("pw")))
  }

  test("scramSha512Decodes") {
    val security = securityOf(
      cluster("""        protocol: SASL_SSL
                |        mechanism: SCRAM-SHA-512
                |        username: kui
                |        password: pw""".stripMargin)
    )
    assertEquals(saslOf(security), SaslMechanism.ScramSha512("kui", kui.kernel.Secret("pw")))
  }

  test("gssapiDecodes") {
    val security = securityOf(
      cluster("""        protocol: SASL_PLAINTEXT
                |        mechanism: GSSAPI
                |        serviceName: kafka
                |        principal: kui@EXAMPLE.COM
                |        keytab: /etc/kui/kui.keytab""".stripMargin)
    )
    assertEquals(
      saslOf(security),
      SaslMechanism.Gssapi("kafka", "kui@EXAMPLE.COM", Some("/etc/kui/kui.keytab"), false, true)
    )
  }

  test("anOAuthTokenEndpointOnAMetadataAddressIsRefused") {
    // Kafka's OAuth client does not apply UrlPolicy when it dials the token endpoint, so the check
    // has to happen here, before that URL is allow-listed. Strict is what production loads with.
    val yaml = cluster("""        protocol: SASL_SSL
                          |        mechanism: OAUTHBEARER
                          |        tokenEndpoint: http://169.254.169.254/latest/api/token
                          |        clientId: kui
                          |        clientSecret: secret""".stripMargin)

    assertEquals(
      problemKeys(yaml, UrlPolicy.Strict),
      List("kui.clusters.0.security.tokenEndpoint")
    )
    assertEquals(
      saslOf(securityOf(yaml, env = Map.empty)),
      SaslMechanism.OAuthBearer(
        "http://169.254.169.254/latest/api/token",
        "kui",
        kui.kernel.Secret("secret"),
        None
      )
    )
  }

  test("anOverrideThatReplacesTheTokenEndpointIsRefusedWhenThePolicyRefusesIt") {
    // `properties` is applied after the typed endpoint, and that is the URL Kafka will request, so
    // the policy has to see the override too. Otherwise the allow-list would name an address the
    // typed field was not allowed to.
    val yaml =
      """kui:
        |  clusters:
        |    - name: One
        |      bootstrapServers: broker:9092
        |      security:
        |        protocol: SASL_SSL
        |        mechanism: OAUTHBEARER
        |        tokenEndpoint: https://login.example.com/token
        |        clientId: kui
        |        clientSecret: secret
        |      properties:
        |        sasl.oauthbearer.token.endpoint.url: http://169.254.169.254/latest
        |""".stripMargin

    assertEquals(
      problemKeys(yaml, UrlPolicy.Strict),
      List("kui.clusters.0.properties.sasl.oauthbearer.token.endpoint.url")
    )
  }

  test("oauthBearerDecodes") {
    val security = securityOf(
      cluster("""        protocol: SASL_SSL
                |        mechanism: OAUTHBEARER
                |        tokenEndpoint: https://login.example.com/token
                |        clientId: kui
                |        clientSecret: env:OAUTH
                |        scope: kafka.read""".stripMargin),
      env = Map("OAUTH" -> "oauth-secret")
    )
    assertEquals(
      saslOf(security),
      SaslMechanism.OAuthBearer(
        "https://login.example.com/token",
        "kui",
        kui.kernel.Secret("oauth-secret"),
        Some("kafka.read")
      )
    )
  }

  test("awsMskIamDecodes") {
    val security = securityOf(
      cluster("""        protocol: SASL_SSL
                |        mechanism: AWS_MSK_IAM
                |        profile: kui-readonly""".stripMargin)
    )
    assertEquals(saslOf(security), SaslMechanism.AwsMskIam(Some("kui-readonly"), None, None))
  }

  test("azureEntraDecodes") {
    val security = securityOf(
      cluster("""        protocol: SASL_SSL
                |        mechanism: AZURE_ENTRA
                |        namespace: kui-namespace""".stripMargin)
    )
    assertEquals(saslOf(security), SaslMechanism.AzureEntra("kui-namespace", None))
  }

  test("gcpDecodes") {
    val security = securityOf(
      cluster("""        protocol: SASL_SSL
                |        mechanism: GCP""".stripMargin)
    )
    assertEquals(saslOf(security), SaslMechanism.GcpManagedKafka)
  }

  test("sslWithoutSaslDecodes") {
    val security = securityOf(
      cluster("""        protocol: SSL
                |        ssl:
                |          verifyHostname: false""".stripMargin)
    )
    security match {
      case ClusterSecurity.Ssl(tls) => assertEquals(tls.verifyHostname, false)
      case other => fail(s"expected SSL, got $other")
    }
  }

  // -------------------------------------------------------------------------------------------
  // Every mechanism says which field is missing
  // -------------------------------------------------------------------------------------------

  test("aMissingPasswordNamesThatKeyAndNoOther") {
    assertEquals(
      problemKeys(
        cluster("""        protocol: SASL_PLAINTEXT
                  |        mechanism: PLAIN
                  |        username: kui""".stripMargin)
      ),
      List("kui.clusters.0.security.password")
    )
  }

  test("everyMissingKerberosKeyIsNamedAtOnce") {
    assertEquals(
      problemKeys(
        cluster("""        protocol: SASL_PLAINTEXT
                  |        mechanism: GSSAPI""".stripMargin)
      ),
      List("kui.clusters.0.security.principal", "kui.clusters.0.security.serviceName")
    )
  }

  test("everyMissingOAuthKeyIsNamedAtOnce") {
    assertEquals(
      problemKeys(
        cluster("""        protocol: SASL_SSL
                  |        mechanism: OAUTHBEARER""".stripMargin)
      ),
      List(
        "kui.clusters.0.security.clientId",
        "kui.clusters.0.security.clientSecret",
        "kui.clusters.0.security.tokenEndpoint"
      )
    )
  }

  test("aMissingAzureNamespaceIsNamed") {
    assertEquals(
      problemKeys(
        cluster("""        protocol: SASL_SSL
                  |        mechanism: AZURE_ENTRA""".stripMargin)
      ),
      List("kui.clusters.0.security.namespace")
    )
  }

  // -------------------------------------------------------------------------------------------
  // The two cross-field rules
  // -------------------------------------------------------------------------------------------

  test("aSaslProtocolWithNoMechanismNamesMechanism") {
    val yaml = cluster("        protocol: SASL_SSL")
    assertEquals(problemKeys(yaml), List("kui.clusters.0.security.mechanism"))
  }

  test("aMechanismUnderANonSaslProtocolIsRefusedRatherThanIgnored") {
    val yaml = cluster("""        protocol: PLAINTEXT
                         |        mechanism: SCRAM-SHA-512
                         |        username: kui
                         |        password: pw""".stripMargin)

    assertEquals(problemKeys(yaml), List("kui.clusters.0.security.mechanism"))
    load(yaml) match {
      case Left(errors) =>
        assert(errors.render.contains("only meaningful for a SASL protocol"), errors.render)
      case Right(_) => fail("expected the load to fail")
    }
  }

  test("anUnknownMechanismNamesTheEightLegalValues") {
    val yaml = cluster("""        protocol: SASL_SSL
                         |        mechanism: scram512""".stripMargin)

    load(yaml) match {
      case Left(errors) =>
        val message = errors.render
        assert(message.contains("SCRAM-SHA-512"), message)
        assert(message.contains("AWS_MSK_IAM"), message)
        assert(message.contains("'SCRAM512'"), message)
      case Right(_) => fail("expected the load to fail")
    }
  }

  // -------------------------------------------------------------------------------------------
  // The loader does not mangle a password on the way in
  // -------------------------------------------------------------------------------------------

  /** Anything an operator might realistically paste into a password field, including the four characters that
    * break a naively assembled JAAS string (Kouncil's `String.format` injection,
    * `research/scala/security-research.md` §3). Rendering it is KAFKA-002's property test; this one asserts
    * the *loader* hands over exactly the bytes it was given, which is where the same bug would be invisible.
    */
  private val awkwardPassword: Gen[String] =
    Gen
      .nonEmptyListOf(
        Gen.oneOf(
          Gen.alphaNumChar,
          Gen.oneOf('"', '\\', ' ', '=', ';', '\'', '$', '{', '}', '\n', '\t', 'é')
        )
      )
      .map(_.mkString)

  property("aPasswordRoundTripsThroughTheEnvironmentUnchanged") {
    Prop.forAll(awkwardPassword) { password =>
      val security = securityOf(
        cluster("""        protocol: SASL_PLAINTEXT
                  |        mechanism: PLAIN
                  |        username: kui
                  |        password: env:KUI_PW""".stripMargin),
        env = Map("KUI_PW" -> password)
      )

      saslOf(security) match {
        case SaslMechanism.Plain(_, secret) => secret.value == password
        case other => fail(s"expected PLAIN, got $other")
      }
    }
  }

  // -------------------------------------------------------------------------------------------
  // The TLS passwords are secrets too
  // -------------------------------------------------------------------------------------------

  /** The failure these three cases pin, in full, because it cost a day to find.
    *
    * `deployment/examples/production.yaml` — the file a new operator copies first — writes the truststore
    * password as `password: "env:KUI_ANALYTICS_TRUSTSTORE_PASSWORD"`. The decoder used to wrap whatever it
    * found there in `Secret` verbatim, without parsing the `env:` prefix, so the password Kafka was given was
    * the fourteen-character string "env:KUI_ANALY…" rather than the value of that variable.
    *
    * Nothing said so. The configuration loaded, KUI started, and `Admin.create` then threw while opening the
    * PKCS12 store — which the cluster service reports as `KUI-UPSTREAM-UNAVAILABLE`, the same code a broker
    * that is simply switched off produces. The secured cluster sat on the dashboard saying "unavailable" for
    * ever, and the one place the real cause could have been read was the exception class name that nothing
    * logged.
    */
  private def tlsOf(security: ClusterSecurity): kui.kernel.cluster.TlsConfig =
    security match {
      case ClusterSecurity.Sasl(_, _, Some(tls)) => tls
      case ClusterSecurity.Ssl(tls) => tls
      case other => fail(s"expected a security model carrying TLS, got $other")
    }

  private def securedCluster(extra: String): String =
    cluster(
      s"""        protocol: SASL_SSL
         |        mechanism: SCRAM-SHA-512
         |        username: kui
         |        password: env:KUI_PW
         |        ssl:
         |$extra""".stripMargin
    )

  test("aTruststorePasswordIsReadFromTheEnvironmentLikeEveryOtherSecret") {
    val security = securityOf(
      securedCluster("""          truststore:
                       |            location: /etc/kui/certs/kafka-ca.p12
                       |            password: env:KUI_TRUSTSTORE_PW
                       |            type: PKCS12""".stripMargin),
      env = Map("KUI_PW" -> "scram", "KUI_TRUSTSTORE_PW" -> "the-real-store-password")
    )

    assertEquals(
      tlsOf(security).truststore.flatMap(_.password).map(_.value),
      Some("the-real-store-password")
    )
  }

  test("aKeystorePasswordComesFromTheEnvironmentAndAKeyPasswordFromAMountedFile") {
    // `file:` is how Kubernetes and Docker Compose deliver a secret, so it is checked with a real file
    // rather than asserted to be "the same code path" as `env:`.
    val keyFile = java.nio.file.Files.createTempFile("kui-key-password", ".txt")
    val _ = java.nio.file.Files.writeString(keyFile, "the-real-key-password\n")

    try {
      val security = securityOf(
        securedCluster(s"""          keystore:
                          |            location: /etc/kui/certs/client.p12
                          |            password: env:KUI_KEYSTORE_PW
                          |            type: PKCS12
                          |          keyPassword: file:$keyFile""".stripMargin),
        env = Map("KUI_PW" -> "scram", "KUI_KEYSTORE_PW" -> "the-real-keystore-password")
      )

      assertEquals(
        tlsOf(security).keystore.flatMap(_.password).map(_.value),
        Some("the-real-keystore-password")
      )
      assertEquals(
        tlsOf(security).keystore.flatMap(_.keyPassword).map(_.value),
        Some("the-real-key-password")
      )
    } finally {
      val _ = java.nio.file.Files.deleteIfExists(keyFile)
    }
  }

  test("aTlsPasswordThatNamesAnUnsetVariableIsAStartupFailureAndNotAWrongPassword") {
    val problems = load(
      securedCluster("""          truststore:
                       |            location: /etc/kui/certs/kafka-ca.p12
                       |            password: env:KUI_TRUSTSTORE_PW
                       |            type: PKCS12""".stripMargin),
      env = Map("KUI_PW" -> "scram")
    ) match {
      case Left(errors) => errors.problems.toList
      case Right(_) => fail("expected the load to fail because KUI_TRUSTSTORE_PW is not set")
    }

    assertEquals(problems.map(_.key), List("kui.clusters.0.security.ssl.truststore.password"))
    assert(
      problems.head.problem.contains("KUI_TRUSTSTORE_PW"),
      clue = s"the problem should name the variable: ${problems.head.problem}"
    )
  }
}
