package kui.config

import java.nio.file.Path

import scala.concurrent.duration.DurationInt

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.scalacheck.{Gen, Prop}

import kui.kernel.cluster.{ClusterSecurity, SaslMechanism, SaslProtocol}
import kui.testkit.KuiSuite

/** That an operator pointing KUI at a store cluster is told everything that is wrong with their configuration
  * at once, and that nothing in that message is a key or a password.
  *
  * The presence of `kui.store.kafka.bootstrapServers` is the whole on/off switch. There is no `enabled` flag,
  * because two settings that must agree eventually disagree, and the disagreement here would read as "why is
  * nothing being saved" rather than as a startup error.
  */
final class StoreConfigSuite extends KuiSuite {

  private val keyMaterial = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="

  private def load(
      files: List[Path],
      env: Map[String, String] = Map.empty
  ): Either[ConfigErrors, KuiConfig] =
    KuiConfigSource.loadFrom[IO](Nil, files, env, UrlPolicy.Dev).unsafeRunSync()

  private def problems(result: Either[ConfigErrors, KuiConfig]): List[ConfigProblem] =
    result match {
      case Left(errors) => errors.problems.toList
      case Right(_) => fail("expected the load to fail, but it succeeded")
    }

  private def loaded(result: Either[ConfigErrors, KuiConfig]): KuiConfig =
    result.fold(errors => fail(errors.render), identity)

  private val validEnv = Map(
    "KUI_STORE_ENCRYPTION_KEY" -> keyMaterial,
    "KUI_STORE_PASSWORD" -> "store-password-canary"
  )

  test("storeValidYamlLoads") {
    val store = loaded(load(List(ConfigFixtures.fixture("store-valid.yaml")), validEnv)).store
    assertEquals(store.topicPrefix, "__kui_")
    assertEquals(store.replicationFactor, 3.toShort)
    assertEquals(store.minInSyncReplicas.value, 2)
    assertEquals(store.maxFileBytes, 4194304L)
    assertEquals(store.replayTimeout, 45.seconds)
    assertEquals(store.writeTimeout, 15.seconds)
    assertEquals(store.kafkaEnabled, true)
    assertEquals(store.configTopic, "__kui_config")

    val kafka = store.kafka.getOrElse(fail("the Kafka store should have been configured"))
    assertEquals(kafka.bootstrapServers.toString, "kafka-1:9092,kafka-2:9092")
    assertEquals(kafka.properties("client.dns.lookup"), "use_all_dns_ips")
    kafka.security match {
      case ClusterSecurity.Sasl(SaslProtocol.SaslSsl, SaslMechanism.ScramSha512(username, password), _) =>
        assertEquals(username, "kui")
        assertEquals(password.value, "store-password-canary")
      case other => fail(s"expected SASL_SSL/SCRAM-SHA-512, got $other")
    }

    val encryption = store.encryption.getOrElse(fail("the encryption key should have been configured"))
    assertEquals(encryption.activeKeyId, "k1")
    assertEquals(encryption.keys.keySet, Set("k1"))
    assertEquals(encryption.keys("k1").value, keyMaterial)
  }

  test("absentStoreSectionYieldsDefaultsAndNoKafka") {
    // The regression guard for "with kui.store.kafka.* unset, everything else in M1 still passes": an
    // M0-era file has no store section at all and must keep loading unchanged.
    val store = loaded(load(List(ConfigFixtures.fixture("valid.yaml")))).store
    assertEquals(store, StoreConfig.Default)
    assertEquals(store.kafkaEnabled, false)
    assertEquals(store.encryption, None)
  }

  test("envBeatsFileForEveryStoreKey") {
    val env = validEnv ++ Map(
      "KUI_STORE_TOPICPREFIX" -> "kui2_",
      "KUI_STORE_REPLICATIONFACTOR" -> "1",
      "KUI_STORE_MININSYNCREPLICAS" -> "1",
      "KUI_STORE_MAXFILEBYTES" -> "8388608",
      "KUI_STORE_REPLAYTIMEOUT" -> "90s",
      "KUI_STORE_WRITETIMEOUT" -> "20s",
      "KUI_STORE_KAFKA_BOOTSTRAPSERVERS" -> "elsewhere:9092"
    )
    val store = loaded(load(List(ConfigFixtures.fixture("store-valid.yaml")), env)).store
    assertEquals(store.topicPrefix, "kui2_")
    assertEquals(store.replicationFactor, 1.toShort)
    assertEquals(store.minInSyncReplicas.value, 1)
    assertEquals(store.maxFileBytes, 8388608L)
    assertEquals(store.replayTimeout, 90.seconds)
    assertEquals(store.writeTimeout, 20.seconds)
    assertEquals(store.kafka.map(_.bootstrapServers.toString), Some("elsewhere:9092"))
  }

  test("accumulatesEveryCrossFieldError") {
    val found = problems(load(List(ConfigFixtures.fixture("store-invalid.yaml")))).map(_.key).sorted
    assertEquals(
      found,
      List("kui.store.encryptionKey", "kui.store.minInSyncReplicas", "kui.store.replayTimeout")
    )
  }

  test("kafkaWithoutEncryptionKeyIsRejected") {
    val yaml = ConfigFixtures.yaml(
      """|kui:
         |  store:
         |    kafka:
         |      bootstrapServers: ["kafka-1:9092"]
         |""".stripMargin
    )
    val message = problems(load(List(yaml))).find(_.key == "kui.store.encryptionKey").map(_.problem)
    assert(message.exists(_.contains("openssl rand -base64 32")), message.toString)
  }

  test("encryptionKeyAndEncryptionKeysTogetherAreRejected") {
    val yaml = ConfigFixtures.yaml(
      s"""|kui:
          |  store:
          |    encryptionKey: "$keyMaterial"
          |    encryptionKeys: "k1:$keyMaterial"
          |    encryptionKeyId: "k1"
          |""".stripMargin
    )
    val found = problems(load(List(yaml)))
    val message = found.find(_.key == "kui.store.encryptionKeys").map(_.problem)
    assert(message.exists(_.contains("kui.store.encryptionKey")), message.toString)
    assert(!found.map(_.problem).mkString.contains(keyMaterial), "a problem message echoed the key material")
  }

  test("encryptionKeyIdMustBeAmongTheKeys") {
    val yaml = ConfigFixtures.yaml(
      s"""|kui:
          |  store:
          |    encryptionKeys: "k1:$keyMaterial,k2:$keyMaterial"
          |    encryptionKeyId: "k9"
          |""".stripMargin
    )
    val message = problems(load(List(yaml))).find(_.key == "kui.store.encryptionKeyId").map(_.problem)
    assert(message.exists(_.contains("k1, k2")), message.toString)
    assert(message.exists(!_.contains(keyMaterial)), "the message echoed the key material")
  }

  test("duplicateEncryptionKeyIdsAreRejectedWithoutEchoingMaterial") {
    val other = "AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHw=="
    val yaml = ConfigFixtures.yaml(
      s"""|kui:
          |  store:
          |    encryptionKeys: "k1:$keyMaterial,k1:$other"
          |    encryptionKeyId: "k1"
          |""".stripMargin
    )
    val found = problems(load(List(yaml)))
    val message = found.find(_.key == "kui.store.encryptionKeys").map(_.problem)
    assert(message.exists(_.contains("k1")), message.toString)
    val rendered = found.map(_.render).mkString("\n")
    assert(!rendered.contains(keyMaterial), rendered)
    assert(!rendered.contains(other), rendered)
  }

  test("encryptionKeysWithoutAnIdIsRejected") {
    val yaml = ConfigFixtures.yaml(
      s"""|kui:
          |  store:
          |    encryptionKeys: "k1:$keyMaterial,k2:$keyMaterial"
          |""".stripMargin
    )
    val message = problems(load(List(yaml))).find(_.key == "kui.store.encryptionKeyId").map(_.problem)
    assert(message.exists(_.contains("k1, k2")), message.toString)
  }

  test("minIsrAboveReplicationFactorIsRejected") {
    val yaml = ConfigFixtures.yaml(
      """|kui:
         |  store:
         |    replicationFactor: 1
         |    minInSyncReplicas: 2
         |""".stripMargin
    )
    val message = problems(load(List(yaml))).find(_.key == "kui.store.minInSyncReplicas").map(_.problem)
    assert(message.exists(_.contains("(1)")), message.toString)
    assert(message.exists(_.contains("2")), message.toString)
  }

  test("singleKeySugarGetsTheIdK1") {
    val yaml = ConfigFixtures.yaml(
      s"""|kui:
          |  store:
          |    encryptionKey: "$keyMaterial"
          |""".stripMargin
    )
    val encryption = loaded(load(List(yaml))).store.encryption.getOrElse(fail("no encryption configured"))
    assertEquals(encryption.activeKeyId, StoreConfig.DefaultKeyId)
    assertEquals(encryption.keys.keySet, Set("k1"))
  }

  test("unknownStoreKeyIsRejected") {
    val yaml = ConfigFixtures.yaml(
      """|kui:
         |  store:
         |    topicPrefx: "__kui_"
         |""".stripMargin
    )
    assertEquals(problems(load(List(yaml))).map(_.key), List("kui.store.topicPrefx"))
  }

  test("replicationFactorOneLoads") {
    // Single-broker development is a supported mode (ADR-042 §7), so 1 is legal.
    val yaml = ConfigFixtures.yaml(
      """|kui:
         |  store:
         |    replicationFactor: 1
         |    minInSyncReplicas: 1
         |""".stripMargin
    )
    assertEquals(loaded(load(List(yaml))).store.replicationFactor, 1.toShort)
  }

  test("aDirectoryThatDoesNotExistIsNotAConfigurationError") {
    // A Kubernetes volume that mounts a moment after the process starts is a real thing, and the file
    // adapter treats a missing root as an empty store anyway.
    val yaml = ConfigFixtures.yaml(
      """|kui:
         |  store:
         |    dir: "/not/mounted/yet"
         |""".stripMargin
    )
    assertEquals(loaded(load(List(yaml))).store.dir, Some(Path.of("/not/mounted/yet")))
  }

  private val secretPropertyNames: Gen[String] =
    Gen.oneOf("ssl.keystore.password", "sasl.jaas.config", "some.api.token", "MY_SECRET", "x.credential.y")

  property("everyRenderingRedactsSecrets") {
    Prop.forAllNoShrink(secretPropertyNames, Gen.alphaNumStr.map(text => s"$text-canary-value")) {
      (name, value) =>
        val properties = Map(name -> value, "client.id" -> "kui")
        val redacted = ClientPropertyOverrides.redact(properties)
        val kafka = StoreKafkaConfig(
          kui.kernel.cluster.BootstrapServers.unsafe("kafka-1:9092"),
          ClusterSecurity.Plaintext,
          properties
        )
        !redacted.values.toList.contains(value) &&
        redacted("client.id") == "kui" &&
        !kafka.toString.contains(value) &&
        !ClientPropertyOverrides.render(properties).contains(value)
    }
  }

  test("theEncryptionConfigNeverPrintsMaterial") {
    val encryption = StoreEncryptionConfig(Map("k1" -> kui.kernel.Secret(keyMaterial)), "k1")
    assertEquals(encryption.toString, "StoreEncryptionConfig(keys=[k1], active=k1)")
    assert(!StoreConfig.Default.copy(encryption = Some(encryption)).toString.contains(keyMaterial))
  }
}
