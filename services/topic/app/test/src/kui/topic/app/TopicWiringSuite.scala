package kui.topic.app

import java.nio.charset.StandardCharsets

import cats.effect.IO

import kui.config.{
  GatewayConfig,
  KuiConfig,
  ProfileClientConfig,
  SafeUrl,
  TopicsConfig,
  UpstreamServiceConfig
}
import kui.kernel.{Secret, ServiceId}
import kui.testkit.KuiIOSuite
import kui.testkit.fakes.FakeStructuredLogger

/** The plan-signing key, and the line an operator needs when it was not configured.
  *
  * `services/topic/app` declared a test module in `build.mill` and shipped no test source, so both halves of
  * `signingKey` were ungated. The fallback's consequence is invisible until a second replica exists — a plan
  * minted by one process is refused by the other, and the operator sees a confirmation that will not confirm
  * — which is exactly why the file says it is "logged loudly rather than being silent", and why that sentence
  * needs a case rather than a comment.
  */
final class TopicWiringSuite extends KuiIOSuite {

  test("the configured cursor key is the key plan tokens are signed with, byte for byte") {
    // Byte for byte and in UTF-8: two replicas given the same `kui.streaming.cursorKey` have to derive
    // the same bytes from it, or a plan minted on one is refused by the other.
    val configured = Secret("a-shared-secret-é")

    FakeStructuredLogger[IO].flatMap(fake =>
      TopicWiring.signingKey[IO](Some(configured), fake).map { key =>
        assertEquals(
          key.value.toSeq,
          configured.value.getBytes(StandardCharsets.UTF_8).toSeq
        )
      }
    )
  }

  test("a configured key is announced, so a log says which of the two modes this process is in") {
    FakeStructuredLogger[IO].flatMap(fake =>
      TopicWiring.signingKey[IO](Some(Secret("shared")), fake) *> fake.entries.map { entries =>
        assertEquals(entries.map(_.level), List("info"))
        assert(entries.head.message.contains("configured kui.streaming.cursorKey"), entries.head.message)
      }
    )
  }

  test("an unconfigured deployment gets a fresh key and is told what that costs") {
    // The three things the fallback promises: a key of its own, a different one each time, and a line
    // that names the consequence. Deleting the line leaves a deployment where confirmations fail at the
    // last step behind a load balancer and nothing ever said why.
    for {
      fake <- FakeStructuredLogger[IO]
      first <- TopicWiring.signingKey[IO](None, fake)
      second <- TopicWiring.signingKey[IO](None, fake)
      entries <- fake.entries
    } yield {
      assertEquals(first.value.length, 32)
      assert(
        !java.util.Arrays.equals(first.value, second.value),
        "two processes would sign plan tokens with the same generated key"
      )
      assertEquals(entries.map(_.level), List("info", "info"))
      val line = entries.head.message
      assert(line.contains("no kui.streaming.cursorKey is configured"), line)
      assert(line.contains("second replica"), clue = s"the line does not name the consequence: $line")
    }
  }

  test("a profile url outranks the gateway cluster url") {
    val profiles = SafeUrl.unsafe("http://kui-cluster:8081")
    val gateway = SafeUrl.unsafe("http://gateway-cluster:8081")
    val config = KuiConfig.Default.copy(
      topics = TopicsConfig.Default.copy(clusterProfiles = Some(profileClient(profiles))),
      gateway = gatewayCluster(gateway)
    )
    IO(assertEquals(TopicWiring.profileTarget(config).map(_.value), Some(profiles.value)))
  }

  test("the gateway cluster url is the profile target when no profile url is set") {
    val gateway = SafeUrl.unsafe("http://gateway-cluster:8081")
    val config = KuiConfig.Default.copy(gateway = gatewayCluster(gateway))
    IO(assertEquals(TopicWiring.profileTarget(config).map(_.value), Some(gateway.value)))
  }

  test("neither url means the topic process keeps the static cluster list") {
    IO(assertEquals(TopicWiring.profileTarget(KuiConfig.Default), None))
  }

  test("the service and its instrumentation are named the same thing everywhere") {
    // A dashboard filtering on the meter's scope and a log search filtering on `service.name` have to
    // agree; nothing else in this repository compares them.
    assertEquals(TopicWiring.Instrumentation, "kui.topic")
    assertEquals(kui.topic.api.TopicApi.ServiceName, "kui-topic")
    assertEquals(kui.topic.api.TopicApi.Id.value, "topic")
  }

  private def profileClient(url: SafeUrl): ProfileClientConfig =
    ProfileClientConfig(
      url,
      ProfileClientConfig.DefaultPollInterval,
      ProfileClientConfig.DefaultRequestTimeout,
      ProfileClientConfig.DefaultReconnectBackoff,
      ProfileClientConfig.DefaultMaxReconnectBackoff,
      ProfileClientConfig.DefaultStartupTimeout
    )

  private def gatewayCluster(url: SafeUrl): GatewayConfig =
    GatewayConfig.Default.copy(services =
      Map(
        ServiceId.unsafe("cluster") -> UpstreamServiceConfig(
          url,
          UpstreamServiceConfig.DefaultTimeout,
          UpstreamServiceConfig.DefaultMaxConcurrent
        )
      )
    )
}
