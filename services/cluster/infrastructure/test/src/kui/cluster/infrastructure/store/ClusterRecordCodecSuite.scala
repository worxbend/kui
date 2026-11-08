package kui.cluster.infrastructure.store

import io.circe.Json

import kui.cluster.domain.{ClusterProfile, ProfileOrigin, ProfileVersion}
import kui.config.store.SecretJson
import kui.kernel.cluster.*
import kui.kernel.{ClusterId, Secret}
import kui.testkit.KuiSuite

/** What a `cluster/<id>` record holds, and — the part that matters — where its secrets are.
  *
  * The record's format is an operator's configuration on disk. A field silently renamed by a refactoring
  * would stop being read, and every cluster in the store would come back missing a setting; a secret written
  * anywhere other than the store's marker would reach the topic in the clear.
  */
final class ClusterRecordCodecSuite extends KuiSuite {

  /** A profile whose every secret is a distinctive token, so a leak can be found by searching. */
  private val loud: ClusterProfile = ClusterProfile
    .from(
      id = ClusterId.unsafe("prod-eu"),
      displayName = "Production EU",
      bootstrap = BootstrapServers.unsafe("broker-1:9093,broker-2:9093"),
      security = ClusterSecurity.Sasl(
        SaslProtocol.SaslSsl,
        SaslMechanism.ScramSha512("kui", Secret("kUiS3cr3t-sasl-password")),
        Some(
          TlsConfig(
            truststore = Some(
              TrustStoreRef(
                StoreSource.Inline(Secret("kUiS3cr3t-truststore-bytes")),
                Some(Secret("kUiS3cr3t-truststore-password")),
                StoreType.Pkcs12
              )
            ),
            keystore = Some(
              KeyStoreRef(
                StoreSource.FromPath("/etc/kui/keystore.p12"),
                Some(Secret("kUiS3cr3t-keystore-password")),
                Some(Secret("kUiS3cr3t-key-password")),
                StoreType.Pkcs12
              )
            ),
            verifyHostname = true,
            enabledProtocols = Some(List("TLSv1.3")),
            cipherSuites = None
          )
        )
      ),
      properties = ClientProperties.fromMap(
        Map(
          "metadata.max.age.ms" -> PropertyValue.Plain("120000"),
          "custom.token" -> PropertyValue.Sensitive(Secret("kUiS3cr3t-override"))
        )
      ),
      admin = AdminTuning.default,
      readOnly = true,
      colour = Some("amber"),
      version = ProfileVersion.unsafe(4L),
      origin = ProfileOrigin.Stored
    )
    .fold(error => throw new IllegalStateException(error.message), identity)

  test("aProfileRoundTripsThroughItsPayload") {
    val payload = ClusterRecordCodec.encode(loud)

    ClusterRecordCodec.decode(loud.id, loud.version, loud.origin, payload) match {
      case Left(why) => fail(s"the payload did not decode: $why")
      case Right(decoded) => assertEquals(decoded, loud)
    }
  }

  test("everySecretIsUnderTheStoresMarkerAndNowhereElse") {
    // The obligation this module owes the crypto layer, stated as an assertion. The layer replaces every
    // `$secret` marker with a ciphertext node before the record reaches Kafka; a secret that reached the
    // JSON through some other path would simply be written in the clear, and nothing would notice.
    val payload = ClusterRecordCodec.encode(loud)
    val markers = SecretJson.plaintextPaths(payload)

    assertEquals(markers.size, 6, s"expected six secrets, found $markers")

    // Every distinctive token appears exactly as often as it appears under a marker: no second copy of a
    // password anywhere in the tree.
    val text = payload.noSpaces
    val tokens = List(
      "kUiS3cr3t-sasl-password",
      "kUiS3cr3t-truststore-bytes",
      "kUiS3cr3t-truststore-password",
      "kUiS3cr3t-keystore-password",
      "kUiS3cr3t-key-password",
      "kUiS3cr3t-override"
    )

    tokens.foreach { token =>
      assertEquals(
        text.sliding(token.length).count(_ == token),
        1,
        s"'$token' appears more than once in the payload"
      )
      assert(
        text.contains(s"""{"${SecretJson.PlaintextField}":"$token"}"""),
        s"'$token' is not written as a secret marker"
      )
    }
  }

  test("thePayloadMatchesTheCommittedSample") {
    // The committed file is what makes this a wire format rather than an accident: changing the shape means
    // changing a file a reviewer reads, not adjusting a test's expectation in passing. Run the suite with
    // KUI_UPDATE_GOLDEN=1 to rewrite it after an intended change.
    //
    // The sample holds the plaintext markers, not ciphertext: this is the payload the crypto layer is
    // handed, and pinning an encrypted form would pin a fresh random IV that changes on every write.
    val committed = scala.util.Using
      .resource(Option(getClass.getResourceAsStream("/golden/cluster-record.json")).getOrElse {
        fail("golden/cluster-record.json is missing from the test resources")
      })(stream => scala.io.Source.fromInputStream(stream, "UTF-8").mkString)
      .stripLineEnd

    assertNoDiff(io.circe.Printer.spaces2SortKeys.print(ClusterRecordCodec.encode(loud)), committed)
  }

  test("aPlaintextClusterHasNoSecretsAtAll") {
    val plain = ClusterProfile
      .from(
        ClusterId.unsafe("local"),
        "Local",
        BootstrapServers.unsafe("localhost:9092"),
        ClusterSecurity.Plaintext,
        ClientProperties.empty,
        AdminTuning.default,
        readOnly = false,
        colour = None,
        version = ProfileVersion.Static,
        origin = ProfileOrigin.Static
      )
      .fold(error => throw new IllegalStateException(error.message), identity)

    assertEquals(SecretJson.plaintextPaths(ClusterRecordCodec.encode(plain)), Nil)
  }

  test("theIdVersionAndOriginAreNotInThePayload") {
    // They are facts about the record, not about the profile: the key carries the id and the store owns the
    // version. Writing either into the payload would give KUI two sources of truth for one number.
    val text = ClusterRecordCodec.encode(loud).noSpaces

    assert(!text.contains("prod-eu"), s"the payload repeats the key: $text")
    assert(!text.contains("\"version\""), s"the payload repeats the record version: $text")
    assert(!text.contains("Stored"), s"the payload repeats the origin: $text")
  }

  test("everySaslMechanismRoundTrips") {
    // The discriminator is KUI's own token and not the Kafka `sasl.mechanism` value, because Azure Entra,
    // GCP Managed Kafka and a generic OAuth cluster all send `OAUTHBEARER`. A record written as Azure Entra
    // that came back as generic OAuth would name a different callback handler and fail to authenticate.
    val mechanisms = List(
      SaslMechanism.Plain("u", Secret("p")),
      SaslMechanism.ScramSha256("u", Secret("p")),
      SaslMechanism.ScramSha512("u", Secret("p")),
      SaslMechanism
        .Gssapi("kafka", "kui@REALM", Some("/etc/kui.keytab"), useTicketCache = false, storeKey = true),
      SaslMechanism.OAuthBearer("https://issuer/token", "kui", Secret("p"), Some("kafka")),
      SaslMechanism.AwsMskIam(Some("default"), None, Some("eu-west-1")),
      SaslMechanism.AzureEntra("ns", None),
      SaslMechanism.GcpManagedKafka
    )

    mechanisms.foreach { mechanism =>
      val profile = withSecurity(ClusterSecurity.Sasl(SaslProtocol.SaslSsl, mechanism, None))
      val payload = ClusterRecordCodec.encode(profile)

      ClusterRecordCodec.decode(profile.id, profile.version, profile.origin, payload) match {
        case Left(why) => fail(s"$mechanism did not round-trip: $why")
        case Right(decoded) => assertEquals(decoded.security, profile.security)
      }
    }
  }

  test("anUnknownMechanismIsANamedFailureAndNeverAGuess") {
    val payload = ClusterRecordCodec
      .encode(withSecurity(ClusterSecurity.Plaintext))
      .deepMerge(Json.obj("security" -> Json.obj("type" -> Json.fromString("kerberos-v9"))))

    ClusterRecordCodec.decode(
      ClusterId.unsafe("local"),
      ProfileVersion.Static,
      ProfileOrigin.Stored,
      payload
    ) match {
      case Right(profile) => fail(s"an unknown security mode must not decode: $profile")
      case Left(why) => assert(why.contains("kerberos-v9"), s"the failure names what it found: $why")
    }
  }

  test("aMissingFieldIsAFailureThatNamesTheField") {
    val payload = ClusterRecordCodec.encode(loud).mapObject(_.remove("bootstrapServers"))

    ClusterRecordCodec.decode(loud.id, loud.version, loud.origin, payload) match {
      case Right(profile) => fail(s"a record with no bootstrap servers must not decode: $profile")
      case Left(why) => assert(why.contains("bootstrapServers"), s"the failure names the field: $why")
    }
  }

  test("aRecordThatBreaksADomainRuleIsRefusedWithTheOperatorsOwnMessage") {
    // Hand-edited into an illegal state: an override of a property KUI renders itself. Refusing it here, with
    // the message the operator would have seen at startup, is much better than the alternative — failing
    // later inside a refresh loop on a background fiber.
    val payload = ClusterRecordCodec
      .encode(loud)
      .deepMerge(
        Json.obj(
          "properties" -> Json.obj(
            "sasl.jaas.config" -> Json.obj("value" -> Json.fromString("anything"))
          )
        )
      )

    ClusterRecordCodec.decode(loud.id, loud.version, loud.origin, payload) match {
      case Right(profile) => fail(s"a reserved property override must not decode: $profile")
      case Left(why) => assert(why.contains("not configured correctly"), why)
    }
  }

  test("hostnameVerificationHasNoDefaultOnTheWayIn") {
    // A decoder that supplied `true` for a missing field would let a record written by a future, buggier
    // writer come back as safe when it was not.
    val payload = ClusterRecordCodec
      .encode(withSecurity(ClusterSecurity.Ssl(TlsConfig.default)))
      .mapObject(obj =>
        obj.add(
          "security",
          Json.obj(
            "type" -> Json.fromString("ssl"),
            "tls" -> Json.obj("truststore" -> Json.Null, "keystore" -> Json.Null)
          )
        )
      )

    assert(
      ClusterRecordCodec.decode(loud.id, loud.version, loud.origin, payload).isLeft,
      "a TLS block with no verifyHostname must be refused"
    )
  }

  test("aRawKeystoreKeyIsWrittenAsASecretMarker") {
    val pem = "-----BEGIN PRIVATE KEY-----\nkUiS3cr3t-keystore-key\n-----END PRIVATE KEY-----"
    val profile = ClusterProfile
      .from(
        ClusterId.unsafe("local"),
        "Local",
        BootstrapServers.unsafe("localhost:9092"),
        ClusterSecurity.Plaintext,
        ClientProperties.fromRaw(Map("ssl.keystore.key" -> pem)),
        AdminTuning.default,
        readOnly = false,
        colour = None,
        version = ProfileVersion.Static,
        origin = ProfileOrigin.Static
      )
      .fold(error => throw new IllegalStateException(error.message), identity)

    val payload = ClusterRecordCodec.encode(profile)
    val paths = SecretJson.plaintextPaths(payload)

    assert(
      paths.exists(_.contains("ssl.keystore.key")),
      s"ssl.keystore.key was not stored under a $$secret marker: $paths ${payload.noSpaces}"
    )
  }

  private def withSecurity(security: ClusterSecurity): ClusterProfile =
    ClusterProfile
      .from(
        ClusterId.unsafe("local"),
        "Local",
        BootstrapServers.unsafe("localhost:9092"),
        security,
        ClientProperties.empty,
        AdminTuning.default,
        readOnly = false,
        colour = None,
        version = ProfileVersion.Static,
        origin = ProfileOrigin.Static
      )
      .fold(error => throw new IllegalStateException(error.message), identity)
}
