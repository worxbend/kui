package kui.kafka.auth

import java.nio.charset.StandardCharsets
import java.util.Base64

import org.scalacheck.Prop.forAll

import kui.kernel.cluster.*
import kui.kernel.{ClusterId, Secret}
import kui.testkit.{ClusterGenerators, KuiSuite, RedactionAssertions}

/** What the renderer promises about the map it builds.
  *
  * Three of these are the properties a reviewer would otherwise have to take on trust: the override layer
  * really does win over the keys KUI computes itself, hostname verification is rendered in both directions
  * rather than left to a client default, and no secret survives into either of the two renderings that are
  * safe to print.
  */
final class ClientPropertyRendererSuite extends KuiSuite {

  private val password: Secret[String] = Secret("kUiS3cr3t-password")

  private def connection(
      security: ClusterSecurity,
      overrides: ClientProperties = ClientProperties.empty
  ): ClusterConnection = ClusterConnection(
    id = ClusterId.unsafe("prod"),
    bootstrapServers = BootstrapServers.unsafe("broker:9093"),
    security = security,
    overrides = overrides,
    admin = AdminTuning.default
  )

  private def render(
      security: ClusterSecurity,
      overrides: ClientProperties = ClientProperties.empty,
      purpose: ClientPurpose = ClientPurpose.Admin,
      clientId: String = "kui-admin-prod-1",
      materialized: Map[ClientPropertyRenderer.StoreRole, String] = Map.empty
  ): Either[List[String], ClientProperties] =
    ClientPropertyRenderer
      .render(connection(security, overrides), purpose, clientId, materialized)
      .left
      .map(_.toList.map(_.fieldName))

  private def values(
      security: ClusterSecurity,
      overrides: ClientProperties = ClientProperties.empty
  ): Map[String, String] =
    render(security, overrides).fold(errors => fail(errors.mkString(", ")), _.unsafeValues)

  private val sasl: ClusterSecurity =
    ClusterSecurity.Sasl(SaslProtocol.SaslSsl, SaslMechanism.ScramSha512("u", password), None)

  test("securityProtocolIsDerivedNotConfigured") {
    val table = List(
      ClusterSecurity.Plaintext -> "PLAINTEXT",
      ClusterSecurity.Ssl(TlsConfig.default) -> "SSL",
      ClusterSecurity
        .Sasl(SaslProtocol.SaslPlaintext, SaslMechanism.Plain("u", password), None) ->
        "SASL_PLAINTEXT",
      ClusterSecurity.Sasl(SaslProtocol.SaslSsl, SaslMechanism.Plain("u", password), None) ->
        "SASL_SSL"
    )

    table.foreach { (security, expected) =>
      assertEquals(values(security).get("security.protocol"), Some(expected))
    }
  }

  test("clientIdIsSetForEveryPurpose") {
    ClientPurpose.values.foreach { purpose =>
      val explicit = render(sasl, purpose = purpose, clientId = "explicit-id")
        .fold(errors => fail(errors.mkString(", ")), _.unsafeValues)

      assertEquals(explicit.get("client.id"), Some("explicit-id"))

      val derived = render(sasl, purpose = purpose, clientId = "  ")
        .fold(errors => fail(errors.mkString(", ")), _.unsafeValues)

      assertEquals(derived.get("client.id"), Some(s"${purpose.prefix}-prod"))
    }
  }

  test("hostnameVerificationOnRendersHttps") {
    val on = ClusterSecurity.Ssl(TlsConfig.default.copy(verifyHostname = true))

    assertEquals(values(on).get("ssl.endpoint.identification.algorithm"), Some("https"))
  }

  test("hostnameVerificationOffRendersTheEmptyAlgorithm") {
    val off = ClusterSecurity.Ssl(TlsConfig.default.copy(verifyHostname = false))

    assertEquals(values(off).get("ssl.endpoint.identification.algorithm"), Some(""))
  }

  test("inlineNonPemStoreWithoutAMaterializedPathIsAnError") {
    val inline = ClusterSecurity.Ssl(
      TlsConfig.default.copy(
        truststore = Some(
          TrustStoreRef(StoreSource.Inline(Secret("AAAA")), None, StoreType.Pkcs12)
        )
      )
    )

    assertEquals(render(inline), Left(List("ssl.truststore.location")))

    val withPath = render(
      inline,
      materialized = Map(ClientPropertyRenderer.StoreRole.TrustStore -> "/tmp/kui/truststore.p12")
    ).fold(errors => fail(errors.mkString(", ")), _.unsafeValues)

    assertEquals(withPath.get("ssl.truststore.location"), Some("/tmp/kui/truststore.p12"))
    assertEquals(withPath.get("ssl.truststore.type"), Some("PKCS12"))
  }

  test("bothStoresReportTheirMissingPathsTogether") {
    val inline = ClusterSecurity.Ssl(
      TlsConfig.default.copy(
        truststore = Some(TrustStoreRef(StoreSource.Inline(Secret("AAAA")), None, StoreType.Jks)),
        keystore = Some(KeyStoreRef(StoreSource.Inline(Secret("AAAA")), None, None, StoreType.Jks))
      )
    )

    assertEquals(render(inline), Left(List("ssl.truststore.location", "ssl.keystore.location")))
  }

  test("pemStoreRendersInlineAndNeedsNoPath") {
    val certificate = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----"
    val key = "-----BEGIN PRIVATE KEY-----\nMIIE\n-----END PRIVATE KEY-----"

    def inlineStore(text: String): StoreSource.Inline =
      StoreSource.Inline(
        Secret(Base64.getEncoder.encodeToString(text.getBytes(StandardCharsets.UTF_8)))
      )

    val pem = ClusterSecurity.Ssl(
      TlsConfig.default.copy(
        truststore = Some(TrustStoreRef(inlineStore(certificate), None, StoreType.Pem)),
        keystore = Some(KeyStoreRef(inlineStore(s"$key\n$certificate"), None, None, StoreType.Pem))
      )
    )

    val rendered = values(pem)

    assertEquals(rendered.get("ssl.truststore.type"), Some("PEM"))
    assertEquals(rendered.get("ssl.truststore.certificates"), Some(certificate))
    assertEquals(rendered.get("ssl.keystore.key"), Some(key))
    assertEquals(rendered.get("ssl.keystore.certificate.chain"), Some(certificate))
    assert(!rendered.contains("ssl.truststore.location"))
    assert(!rendered.contains("ssl.keystore.location"))
  }

  test("anInlinePemKeystoreWithNoPrivateKeyIsAnError") {
    val certificateOnly = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----"

    val pem = ClusterSecurity.Ssl(
      TlsConfig.default.copy(
        keystore = Some(
          KeyStoreRef(
            StoreSource.Inline(
              Secret(Base64.getEncoder.encodeToString(certificateOnly.getBytes(StandardCharsets.UTF_8)))
            ),
            None,
            None,
            StoreType.Pem
          )
        )
      )
    )

    assertEquals(render(pem), Left(List("ssl.keystore.key")))
  }

  test("everyRenderedSensitiveKeyIsMarkedSensitive") {
    val everything = ClusterSecurity.Sasl(
      SaslProtocol.SaslSsl,
      SaslMechanism.ScramSha512("u", password),
      Some(
        TlsConfig(
          truststore = Some(
            TrustStoreRef(
              StoreSource.FromPath("/t.p12"),
              Some(Secret("kUiS3cr3t-truststore")),
              StoreType.Pkcs12
            )
          ),
          keystore = Some(
            KeyStoreRef(
              StoreSource.FromPath("/k.p12"),
              Some(Secret("kUiS3cr3t-keystore")),
              Some(Secret("kUiS3cr3t-key")),
              StoreType.Pkcs12
            )
          ),
          verifyHostname = true,
          enabledProtocols = None,
          cipherSuites = None
        )
      )
    )

    val properties =
      render(everything).fold(errors => fail(errors.mkString(", ")), identity)

    val markedSensitive = properties.keys.filter { key =>
      properties.get(key).exists {
        case PropertyValue.Sensitive(_) => true
        case PropertyValue.Plain(_) => false
      }
    }

    assertEquals(
      markedSensitive,
      Set("sasl.jaas.config", "ssl.truststore.password", "ssl.keystore.password", "ssl.key.password")
    )

    // Everything marked sensitive is on the documented list, in both directions.
    assert(markedSensitive.subsetOf(ClientPropertyRenderer.sensitiveKeys))
  }

  test("everyDocumentedSensitiveKeyIsClassifiedSensitiveFromRaw") {
    val raw = ClientPropertyRenderer.sensitiveKeys.map(_ -> "kUiS3cr3t-raw").toMap
    val properties = ClientProperties.fromRaw(raw)

    ClientPropertyRenderer.sensitiveKeys.foreach { key =>
      properties.get(key) match {
        case Some(PropertyValue.Sensitive(_)) => ()
        case other => fail(s"$key classified as $other")
      }
    }
  }

  test("overrideLayerWinsOverTheKeysKuiComputesItself") {
    val overrides = ClientProperties.fromRaw(
      Map(
        "security.protocol" -> "SASL_PLAINTEXT",
        "sasl.jaas.config" -> "org.example.Custom required;",
        "bootstrap.servers" -> "elsewhere:9092",
        "client.dns.lookup" -> "use_all_dns_ips"
      )
    )

    val rendered = values(sasl, overrides)

    assertEquals(rendered.get("security.protocol"), Some("SASL_PLAINTEXT"))
    assertEquals(rendered.get("sasl.jaas.config"), Some("org.example.Custom required;"))
    assertEquals(rendered.get("bootstrap.servers"), Some("elsewhere:9092"))
    assertEquals(rendered.get("client.dns.lookup"), Some("use_all_dns_ips"))
  }

  property("overrideLayerWinsOverEveryRenderedKey") {
    forAll(ClusterGenerators.genClientProperties) { overrides =>
      val properties = render(sasl, overrides).fold(errors => fail(errors.mkString(", ")), identity)

      overrides.keys.foreach { key =>
        assertEquals(
          properties.get(key).map(_.unsafeValue),
          overrides.get(key).map(_.unsafeValue),
          s"the override layer lost on $key"
        )
      }
    }
  }

  property("noSecretAppearsInRenderOrRedactedValues") {
    forAll(ClusterGenerators.genClusterSecurity) { security =>
      val properties = ClientPropertyRenderer
        .render(
          connection(security),
          ClientPurpose.Admin,
          "kui-admin-prod",
          Map(
            ClientPropertyRenderer.StoreRole.TrustStore -> "/tmp/t",
            ClientPropertyRenderer.StoreRole.KeyStore -> "/tmp/k"
          )
        )
        .toOption

      properties.foreach { rendered =>
        val printed = rendered.render + rendered.redactedValues.mkString(",")

        ClusterGenerators
          .secretsOfSecurity(security)
          .foreach(secret => RedactionAssertions.assertNoLeak(printed, secret))
      }
    }
  }
}
