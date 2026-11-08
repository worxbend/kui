package kui.cluster.contract

import java.time.Instant

import io.circe.parser.parse
import io.circe.syntax.*
import munit.ScalaCheckSuite
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll
import sttp.tapir.{EndpointIO, EndpointInput}

import kui.cluster.contract.dto.*
import kui.contracts.KuiEndpoint
import kui.kernel.cluster.{
  AdminTuning,
  BootstrapServers,
  ClientProperties,
  ClusterSecurity,
  KeyStoreRef,
  SaslMechanism,
  SaslProtocol,
  StoreSource,
  StoreType,
  TlsConfig,
  TrustStoreRef
}
import kui.kernel.{ClusterId, Secret}

/** That the profile a service fetches carries exactly what a Kafka client is built from, and that its ETag
  * follows that document rather than the store version alone.
  *
  * M1's version of this suite asserted the opposite — that the document carried no credential — because M1
  * had no consumer that built a client from it. ADR-046 is the decision that changed that, and the
  * corresponding assertion moved rather than disappeared: `kui.cluster.api.SecretLeakSuite` now asserts, over
  * every declared endpoint, that this is the *only* one a credential can reach.
  */
final class ProfileEndpointsSuite extends ScalaCheckSuite {

  private val at = Instant.parse("2026-09-03T10:11:12Z")

  /** A SCRAM-over-TLS cluster with one plain property override and one sensitive one: the shape that
    * exercises every kind of credential this document can carry.
    */
  private val profile = ClusterProfileDto(
    id = ClusterId.unsafe("prod-eu"),
    name = "Production EU",
    version = 7L,
    readOnly = false,
    bootstrapServers = BootstrapServers.unsafe("broker-1.example.com:9093,broker-2.example.com:9093"),
    security = ClusterSecurity.Sasl(
      SaslProtocol.SaslSsl,
      SaslMechanism.ScramSha512("kui-service", Secret("hunter2")),
      Some(
        TlsConfig(
          truststore = Some(
            TrustStoreRef(
              StoreSource.FromPath("/etc/kui/truststore.p12"),
              Some(Secret("truststore-pass")),
              StoreType.Pkcs12
            )
          ),
          keystore = None,
          verifyHostname = true,
          enabledProtocols = None,
          cipherSuites = None
        )
      )
    ),
    properties = ClientProperties.fromRaw(
      Map(
        "ssl.endpoint.identification.algorithm" -> "https",
        "ssl.truststore.password" -> "truststore-pass"
      )
    ),
    admin = AdminTuning.default,
    updatedAt = at
  )

  private val change = ClusterChangeDto(ClusterId.unsafe("prod-eu"), 8L, ClusterChangeDto.Updated, at)

  private def leaves(input: EndpointInput[?]): List[EndpointInput[?]] =
    input match {
      case EndpointInput.Pair(left, right, _, _) => leaves(left) ++ leaves(right)
      case EndpointIO.Pair(left, right, _, _) => leaves(left) ++ leaves(right)
      case EndpointInput.MappedPair(wrapped, _) => leaves(wrapped)
      case EndpointIO.MappedPair(wrapped, _) => leaves(wrapped)
      case leaf => List(leaf)
    }

  test("theProfileGoldenDocumentDecodes") {
    assertNoDiff(
      profile.asJson.spaces2,
      parse(GoldenDocuments.clusterProfile).fold(failure => fail(failure.message), _.spaces2)
    )
    assertEquals(parse(GoldenDocuments.clusterProfile).flatMap(_.as[ClusterProfileDto]), Right(profile))
    assertEquals(profile.asJson.as[ClusterProfileDto], Right(profile))
  }

  test("theChangeGoldenDocumentDecodes") {
    assertNoDiff(
      change.asJson.spaces2,
      parse(GoldenDocuments.clusterChange).fold(failure => fail(failure.message), _.spaces2)
    )
    assertEquals(parse(GoldenDocuments.clusterChange).flatMap(_.as[ClusterChangeDto]), Right(change))
    assertEquals(change.asJson.as[ClusterChangeDto], Right(change))
  }

  test("theProfileCarriesTheSecurityMaterialItsConsumerNeeds") {
    // The inverse of the assertion this suite carried in M1, and the inversion is the point of ADR-046:
    // until M2 there was no consumer that built a Kafka client from this document, so it shipped with
    // every credential removed. There is one now, and a profile without the password is a profile that
    // cannot open a connection.
    val encoded = profile.asJson.noSpaces

    assert(encoded.contains("hunter2"), encoded)
    assert(encoded.contains("truststore-pass"), encoded)
    assert(encoded.contains("kui-service"), encoded)
  }

  test("everySecurityMechanismRoundTripsWithItsSecretsIntact") {
    // A consumer that decoded a mechanism into the wrong case, or lost a password on the way, would fail
    // to authenticate against a production cluster and nowhere else. One case per mechanism, because
    // that is the granularity at which this can be silently wrong.
    val mechanisms = List(
      SaslMechanism.Plain("user", Secret("p1")),
      SaslMechanism.ScramSha256("user", Secret("p2")),
      SaslMechanism.ScramSha512("user", Secret("p3")),
      SaslMechanism.Gssapi("kafka", "kui@EXAMPLE", Some("/etc/kui.keytab"), true, false),
      SaslMechanism.OAuthBearer("https://issuer/token", "kui", Secret("p4"), Some("kafka")),
      SaslMechanism.AwsMskIam(Some("default"), Some("arn:aws:iam::1:role/kui"), Some("eu-west-1")),
      SaslMechanism.AzureEntra("kui.servicebus.windows.net", None),
      SaslMechanism.GcpManagedKafka
    )

    mechanisms.foreach { mechanism =>
      val security = ClusterSecurity.Sasl(SaslProtocol.SaslSsl, mechanism, None)
      val candidate = profile.copy(security = security)
      assertEquals(candidate.asJson.as[ClusterProfileDto], Right(candidate), mechanism.toString)
    }
  }

  test("everySecurityModeRoundTrips") {
    List(
      ClusterSecurity.Plaintext,
      ClusterSecurity.Ssl(TlsConfig.default),
      ClusterSecurity.Ssl(
        TlsConfig(
          truststore = Some(TrustStoreRef(StoreSource.Inline(Secret("YmFzZTY0")), None, StoreType.Jks)),
          keystore = Some(
            KeyStoreRef(
              StoreSource.Inline(Secret("a2V5")),
              Some(Secret("store")),
              Some(Secret("key")),
              StoreType.Pem
            )
          ),
          verifyHostname = false,
          enabledProtocols = Some(List("TLSv1.3")),
          cipherSuites = Some(List("TLS_AES_256_GCM_SHA384"))
        )
      )
    ).foreach { security =>
      val candidate = profile.copy(security = security)
      assertEquals(candidate.asJson.as[ClusterProfileDto], Right(candidate), security.toString)
    }
  }

  test("aPropertyOverrideKeepsItsSensitivityAcrossTheWire") {
    // Losing the flag would mean the consuming service redacts nothing, which is how `sasl.jaas.config`
    // reaches a log line two services away from the one that was careful about it.
    val decoded = profile.asJson.as[ClusterProfileDto].fold(failure => fail(failure.message), identity)

    assertEquals(decoded.properties.redactedValues("ssl.truststore.password"), "***")
    assertEquals(decoded.properties.redactedValues("ssl.endpoint.identification.algorithm"), "https")
    assertEquals(decoded.properties.unsafeValues("ssl.truststore.password"), "truststore-pass")
  }

  test("theConnectionRebuiltFromTheProfileIsTheOneKafkaNeeds") {
    val connection = ClusterProfileDto.connectionOf(profile)

    assertEquals(connection.id, profile.id)
    assertEquals(connection.bootstrapServers, profile.bootstrapServers)
    assertEquals(connection.security.securityProtocol, "SASL_SSL")
    assertEquals(connection.security.saslMechanism.map(_.wireName), Some("SCRAM-SHA-512"))
    // And it still refuses to print itself.
    assert(!connection.toString.contains("hunter2"), connection.toString)
  }

  test("theProfilePathIsUnderInternalV1AndCarriesTheSignedPrincipal") {
    val segments = leaves(ProfileEndpoints.profile.input).collect {
      case EndpointInput.FixedPath(segment, _, _) => segment
    }
    val headers = leaves(ProfileEndpoints.profile.securityInput).collect {
      case EndpointIO.Header(name, _, _) => name
    }

    assertEquals(segments, List("internal", "v1", "clusters", "profile"))
    assertEquals(headers, List(KuiEndpoint.PrincipalHeader))
    assertEquals(
      ProfileEndpoints.profile.showPathTemplate().takeWhile(_ != '?'),
      "/internal/v1/clusters/{clusterId}/profile"
    )
  }

  test("anUnknownChangeKindDecodes") {
    // A future "renamed" must reach a consumer as a word it can log and ignore, not as a decode failure
    // that costs it the frame - and with it, the version bump that would have made it re-fetch.
    val decoded = parse("""{"id":"prod-eu","version":9,"change":"renamed","at":"2026-09-03T10:11:12.000Z"}""")
      .flatMap(_.as[ClusterChangeDto])

    assertEquals(decoded.map(_.change), Right("renamed"))
  }

  property("theEtagFollowsTheProfileAndNotTheClockOrTheVersionText") {
    // These assertions used to require the tag to *be* the version. That is the defect: every static
    // profile is version 0, so the tag never moved when the resolved settings did.
    forAll(Gen.choose(0L, Long.MaxValue - 1L)) { version =>
      val versioned = profile.copy(version = version)
      val tag = ClusterProfileDto.etagOf(versioned)

      assertEquals(ProfileResult.current(versioned).entityTag, tag)
      assertEquals(ProfileResult.notModified(tag).entityTag, tag)
      assertEquals(ClusterProfileDto.etagOf(versioned.copy(updatedAt = at.plusSeconds(30))), tag)
      assert(tag != s""""$version"""")
      assert(tag != ClusterProfileDto.etagOf(versioned.copy(version = version + 1L)))
    }
  }

  property("a caller holding the current etag is told so, whatever a proxy did to the quotes") {
    forAll(Gen.choose(0L, 1000000L)) { version =>
      val versioned = profile.copy(version = version)
      val tag = ClusterProfileDto.etagOf(versioned)
      val moved = ClusterProfileDto.etagOf(versioned.copy(version = version + 1L))

      assert(ProfileResult.isCurrent(Some(tag), tag))
      assert(ProfileResult.isCurrent(Some(tag.replace("\"", "")), tag))
      assert(ProfileResult.isCurrent(Some(s"W/$tag"), tag))
      assert(!ProfileResult.isCurrent(Some(tag), moved))
      // A version-shaped tag is a previous incarnation's validator, not this document's.
      assert(!ProfileResult.isCurrent(Some(version.toString), tag))
      // A wildcard is a request for the profile regardless: on a read, "unconditional" is the only
      // reading of `*` that is useful, and answering 304 to it would leave a caller with nothing.
      assert(!ProfileResult.isCurrent(Some(ProfileEndpoints.AnyEtag), tag))
      assert(!ProfileResult.isCurrent(None, tag))
    }
  }

  test("the profile endpoint is not in the list the gateway turns into public routes") {
    // ADR-043: a service calls another service's contract directly, one hop. Publishing the profile at
    // /api/v1 as well would add a browser-reachable surface with no browser consumer.
    assert(!ClusterEndpoints.all.exists(_.info.name.contains("cluster.profile")))
    assertEquals(ProfileEndpoints.all.flatMap(_.info.name), List("cluster.profile"))
  }
}
