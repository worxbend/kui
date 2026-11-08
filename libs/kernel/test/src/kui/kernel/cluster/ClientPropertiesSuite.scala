package kui.kernel.cluster

import org.scalacheck.Prop.forAll

import kui.kernel.Secret
import kui.testkit.{ClusterGenerators, KuiSuite, RedactionAssertions}

/** The override layer's precedence, and the classification that decides what may be printed. */
final class ClientPropertiesSuite extends KuiSuite {

  test("overrideLayerWins") {
    val base = ClientProperties.fromRaw(Map("a" -> "1", "b" -> "2"))
    val override0 = ClientProperties.fromRaw(Map("b" -> "override", "c" -> "3"))

    assertEquals(
      (base ++ override0).redactedValues,
      Map("a" -> "1", "b" -> "override", "c" -> "3")
    )
  }

  property("overrideLayerWinsForEveryGeneratedPair") {
    forAll(ClusterGenerators.genClientProperties, ClusterGenerators.genClientProperties) { (left, right) =>
      val merged = left ++ right

      right.keys.foreach(key => assertEquals(merged.get(key).map(_.redacted), right.get(key).map(_.redacted)))
      (left.keys -- right.keys).foreach(key =>
        assertEquals(merged.get(key).map(_.redacted), left.get(key).map(_.redacted))
      )
    }
  }

  test("sensitiveKeysAreClassifiedFromRaw") {
    val sensitive = List(
      "sasl.jaas.config",
      "ssl.key.password",
      "ssl.keystore.password",
      "ssl.truststore.password",
      "ssl.keystore.key",
      "ssl.truststore.certificates",
      "ssl.keystore.certificate.chain",
      "ssl.key.passphrase",
      "some.vendor.passwd",
      "basic.auth.user.info",
      "schema.registry.basic.auth.user.info",
      "some.vendor.secret",
      "vendor.credential.file",
      "sasl.oauthbearer.token.value"
    )

    val notSensitive = List(
      "ssl.truststore.location",
      "ssl.keystore.location",
      "ssl.keystore.type",
      "bootstrap.servers",
      "client.dns.lookup",
      "metadata.max.age.ms",
      "security.protocol"
    )

    sensitive.foreach(key => assert(ClientProperties.isSensitiveKey(key), key))
    notSensitive.foreach(key => assert(!ClientProperties.isSensitiveKey(key), key))
  }

  test("theClassificationRulesAreReadableAndNonEmpty") {
    assert(ClientProperties.sensitiveKeyRules.nonEmpty)
    assert(ClientProperties.sensitiveKeyRules.contains("sasl.jaas.config"))
  }

  test("aRawKeystoreKeyIsSensitiveAndDoesNotRender") {
    val pem = "-----BEGIN PRIVATE KEY-----\nhunter2\n-----END PRIVATE KEY-----"
    val properties = ClientProperties.fromRaw(Map("ssl.keystore.key" -> pem))

    assert(ClientProperties.isSensitiveKey("ssl.keystore.key"))
    assertEquals(properties.get("ssl.keystore.key").map(_.unsafeValue), Some(pem))
    assertEquals(properties.redactedValues("ssl.keystore.key"), "***")
    assert(!properties.render.contains("BEGIN PRIVATE KEY"), properties.render)
    assert(!properties.render.contains("hunter2"), properties.render)
  }

  test("aSensitiveValueIsAvailableThroughUnsafeValuesAndNowhereElse") {
    val properties = ClientProperties.fromRaw(Map("sasl.jaas.config" -> "PlainLoginModule required;"))

    assertEquals(properties.redactedValues, Map("sasl.jaas.config" -> "***"))
    assertEquals(
      properties.unsafeValues,
      Map("sasl.jaas.config" -> "PlainLoginModule required;")
    )
  }

  property("redactedValuesNeverContainASensitiveValue") {
    forAll(ClusterGenerators.genClientProperties) { properties =>
      val secrets = properties.keys.toList.flatMap { key =>
        properties.get(key).collect { case PropertyValue.Sensitive(value) => value.value }
      }

      val rendered = properties.redactedValues.mkString(",") + properties.render

      secrets.foreach(secret => RedactionAssertions.assertNoLeak(rendered, secret))
    }
  }

  test("renderIsSortedAndStable") {
    val properties = ClientProperties(
      "z.key" -> PropertyValue.Plain("last"),
      "a.key" -> PropertyValue.Plain("first"),
      "m.password" -> PropertyValue.Sensitive(Secret("nope"))
    )

    assertEquals(properties.render, "a.key=first, m.password=***, z.key=last")
    assertEquals(properties.render, properties.render)
  }

  test("emptyIsEmpty") {
    assert(ClientProperties.empty.isEmpty)
    assertEquals(ClientProperties.empty.render, "")
  }
}
