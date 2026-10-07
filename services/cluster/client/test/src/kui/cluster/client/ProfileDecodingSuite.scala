package kui.cluster.client

import java.time.Instant

import scala.io.Source
import scala.util.Using

import io.circe.parser.parse
import io.circe.syntax.*
import munit.FunSuite

import kui.cluster.contract.dto.{ClusterChangeDto, ClusterProfileDto}
import kui.http.sse.SseWire
import kui.kernel.cluster.*
import kui.kernel.{ClusterId, Secret}

/** The seam, with no network in it.
  *
  * ==Why recorded bytes and not a hand-built value==
  *
  * M1's second integration defect was a browser decoding a document nobody sends: two components, each
  * correct, each unit-tested against a value its own test had constructed, and never run against each other's
  * bytes. This module is the same shape of risk one hop further out — the cluster service encodes a profile,
  * this client decodes it, and every test on either side could pass while the two disagreed.
  *
  * So the fixtures here are *recordings*. `cluster-profile-200.json` is byte-for-byte the golden document
  * `services/cluster/contract` commits for its own encoder, and `theRecordedResponseIsWhatTheServiceWrites`
  * re-encodes through that encoder and compares — so a change on the producing side fails here, in the
  * consumer, which is where the failure would otherwise have been silent.
  */
final class ProfileDecodingSuite extends FunSuite {

  private def recorded(name: String): String =
    Using
      .resource(Option(getClass.getResourceAsStream(s"/recorded/$name")).getOrElse {
        fail(s"recorded/$name is missing from the test resources")
      })(stream => Source.fromInputStream(stream, "UTF-8").mkString)

  private val expected = ClusterProfileDto(
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
    updatedAt = Instant.parse("2026-09-03T10:11:12Z")
  )

  test("theRecordedResponseDecodes") {
    // The assertion M1's second integration defect would have failed.
    assertEquals(
      parse(recorded("cluster-profile-200.json")).flatMap(_.as[ClusterProfileDto]),
      Right(expected)
    )
  }

  test("theRecordedResponseIsWhatTheServiceWrites") {
    // The other half: the recording is not merely decodable, it is what the producing side's encoder
    // actually produces. Without this, a recording could drift into a shape only this suite believes in.
    val normalised = parse(recorded("cluster-profile-200.json"))
      .fold(failure => fail(s"the recording is not JSON: ${failure.message}"), _.spaces2)

    assertNoDiff(expected.asJson.spaces2, normalised)
  }

  test("theRecordedProfileRebuildsAUsableConnection") {
    val profile = parse(recorded("cluster-profile-200.json"))
      .flatMap(_.as[ClusterProfileDto])
      .fold(failure => fail(failure.toString), identity)
    val connection = ClusterProfileDto.connectionOf(profile)

    assertEquals(connection.id, ClusterId.unsafe("prod-eu"))
    assertEquals(connection.security.securityProtocol, "SASL_SSL")
    assertEquals(connection.security.saslMechanism.map(_.wireName), Some("SCRAM-SHA-512"))
    assertEquals(connection.overrides.unsafeValues("ssl.truststore.password"), "truststore-pass")
    // The credentials arrived, and the value still refuses to print itself.
    assert(!connection.toString.contains("truststore-pass"), connection.toString)
  }

  test("theRecordedStreamParses") {
    val events = fs2.Stream
      .emits(recorded("clusters-stream.sse").getBytes("UTF-8").toList)
      .through(SseWire.parse[fs2.Pure])
      .toList

    assertEquals(events.map(_.name), List("clusters", "clusters", "heartbeat", "clusters"))
    assertEquals(ProfileSubscription.EventName, kui.cluster.contract.ProfileEndpoints.EventName)
    assertEquals(events.flatMap(_.id), List("1", "2", "3"))
  }

  test("theRecordedStreamBecomesTheRightInstructions") {
    val instructions = fs2.Stream
      .emits(recorded("clusters-stream.sse").getBytes("UTF-8").toList)
      .through(SseWire.parse[fs2.Pure])
      .toList
      .map(ProfileSubscription.instructionFor)

    assertEquals(
      instructions.map {
        case ProfileSubscription.Instruction.Refetch(change) => s"refetch:${change.id.value}"
        case ProfileSubscription.Instruction.Forget(change) => s"forget:${change.id.value}"
        case ProfileSubscription.Instruction.Ignored(_) => "ignored"
      },
      List("refetch:prod-eu", "forget:staging", "ignored", "ignored")
    )
  }

  test("aChangeKindThisVersionDoesNotKnowIsIgnoredAndNotFatal") {
    // A producer is allowed to add an event name or a change kind, and a consumer that failed the
    // connection on meeting one would go blind to every other change as well (ADR-035).
    val renamed = ClusterChangeDto(ClusterId.unsafe("prod-eu"), 9L, "renamed", Instant.EPOCH)
    val event = kui.http.sse.SseEvent(ProfileSubscription.EventName, renamed.asJson)

    ProfileSubscription.instructionFor(event) match {
      case ProfileSubscription.Instruction.Ignored(reason) => assert(reason.contains("renamed"), reason)
      case other => fail(s"expected the change to be ignored, got $other")
    }
  }

  test("everySecurityMechanismRoundTrips") {
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
      val candidate =
        expected.copy(security = ClusterSecurity.Sasl(SaslProtocol.SaslSsl, mechanism, None))
      val decoded = candidate.asJson.as[ClusterProfileDto]

      assertEquals(decoded, Right(candidate), mechanism.toString)
      // And the secrets survived. A round trip that lost a password would still compare equal if the
      // comparison were on the redacted rendering, so it is asserted through the rebuilt connection.
      assertEquals(
        decoded.map(dto => ClusterProfileDto.connectionOf(dto).security.saslMechanism),
        Right(Some(mechanism))
      )
    }
  }
}
