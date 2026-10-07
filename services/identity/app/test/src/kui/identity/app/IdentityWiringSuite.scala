package kui.identity.app

import java.time.Instant

import cats.data.NonEmptyList
import cats.effect.IO

import kui.config.{AuthConfig, AuthType, OidcConfig, StoreConfig, StoreKafkaConfig}
import kui.identity.domain.AuthMode
import kui.kernel.Secret
import kui.kernel.cluster.{BootstrapServers, ClusterSecurity}
import kui.observability.Telemetry
import kui.security.rbac.RbacPolicy
import kui.security.{JwsPrincipalCodec, SigningKey}
import kui.testkit.KuiIOSuite
import kui.testkit.fakes.FakeStructuredLogger

/** The three lines that decide whether a deployment asks anybody to sign in.
  *
  * `services/identity/app` declared a test module in `build.mill` and shipped no test source, so every rule
  * in this composition root was ungated by construction — including the one that turns `kui.auth.type: form`
  * into "a username and password". A mapping that sent `Form` to `AuthMode.Disabled` would make a deployment
  * that configured accounts serve every request anonymously, and nothing in this repository would have said
  * so.
  */
final class IdentityWiringSuite extends KuiIOSuite {

  private val kafkaWithoutEncryption = StoreConfig.Default.copy(kafka =
    Some(
      StoreKafkaConfig(BootstrapServers.unsafe("localhost:1"), ClusterSecurity.Plaintext, Map.empty)
    )
  )

  private val signingKey = SigningKey("test", Secret(Array.fill[Byte](32)(7)), Instant.EPOCH)
  private val codec = JwsPrincipalCodec.make[IO](NonEmptyList.one(signingKey), "kui-gateway").toOption.get

  List(AuthType.Disabled, AuthType.Oidc).foreach { mode =>
    test(s"complete $mode wiring does not acquire the Kafka password store") {
      val auth = authConfig(mode, Option.when(mode == AuthType.Oidc)(provider("Corporate SSO")))
      FakeStructuredLogger[IO].flatMap { logger =>
        IdentityWiring
          .make[IO](
            auth,
            RbacPolicy.Disabled,
            kafkaWithoutEncryption,
            Telemetry.noop[IO],
            codec,
            logger
          )
          .use { server =>
            server.capabilities.map { _ =>
              assert(server.routes.nonEmpty)
              assert(server.interceptors.nonEmpty)
              assert(server.readiness.nonEmpty)
            }
          }
      }
    }
  }

  test("complete form wiring still acquires the Kafka password store and requires encryption") {
    FakeStructuredLogger[IO].flatMap { logger =>
      IdentityWiring
        .make[IO](
          authConfig(AuthType.Form),
          RbacPolicy.Disabled,
          kafkaWithoutEncryption,
          Telemetry.noop[IO],
          codec,
          logger
        )
        .use(_ => IO.unit)
        .attempt
        .map { result =>
          assert(result.left.exists(_.getMessage.contains("encryption")), result.toString)
        }
    }
  }

  test("configured Kafka credentials store is selected and refuses missing encryption") {
    val kafka = kui.config.StoreKafkaConfig(
      kui.kernel.cluster.BootstrapServers.unsafe("localhost:1"),
      kui.kernel.cluster.ClusterSecurity.Plaintext,
      Map.empty
    )
    FakeStructuredLogger[IO].flatMap { logger =>
      given org.typelevel.log4cats.LoggerFactory[IO] = kui.http.ProcessLoggerFactory.of(logger)
      IdentityWiring
        .configStoreOf[IO](kui.config.StoreConfig.Default.copy(kafka = Some(kafka)), logger)
        .use(_ => IO.unit)
        .attempt
        .map { result =>
          assert(result.left.exists(_.getMessage.contains("encryption")), result.toString)
        }
    }
  }

  private def authConfig(authType: AuthType, oidc: Option[OidcConfig] = None): AuthConfig =
    AuthConfig(authType = authType, users = Nil, oidc = oidc)

  private def provider(label: String): OidcConfig =
    OidcConfig(
      issuer = "https://issuer.example",
      clientId = "kui",
      clientSecret = Secret("s3cr3t"),
      redirectUri = "https://kui.example/callback",
      scopes = List("openid"),
      usernameClaim = "preferred_username",
      groupsClaim = None,
      label = label
    )

  test("every authentication mode the loader can produce reaches the domain as itself") {
    // Over `AuthType.values` rather than over three literals, so a fourth mode added to the loader
    // fails here until somebody decides what the identity service should do with it — which is the
    // same reason `configOf`'s own match has no default case.
    AuthType.values.toList.foreach { configured =>
      val mode = IdentityWiring.configOf(authConfig(configured), RbacPolicy.Disabled).mode
      assertEquals(
        mode.wire,
        configured.wire,
        clue = s"kui.auth.type: ${configured.wire} reached the domain as ${mode.wire}"
      )
    }
    // Stated once by name as well, because the wire strings agreeing is only meaningful if at least
    // one of the pairs is written out rather than derived.
    assertEquals(
      IdentityWiring.configOf(authConfig(AuthType.Form), RbacPolicy.Disabled).mode,
      AuthMode.Form
    )
  }

  test("the provider's label is carried, and a deployment without a provider offers none") {
    val configured =
      IdentityWiring.configOf(authConfig(AuthType.Oidc, Some(provider("Corporate SSO"))), RbacPolicy.Disabled)

    assertEquals(configured.provider.map(_.label), Some("Corporate SSO"))
    assertEquals(
      IdentityWiring.configOf(authConfig(AuthType.Disabled), RbacPolicy.Disabled).provider,
      None
    )
  }

  test("a deployment that disables authentication says so at startup, and says it is deliberate") {
    // The method's own reason: "why does KUI not ask me to log in" is otherwise answered by reading a
    // YAML file on a machine somebody else deployed. The line could be deleted, or could name the
    // wrong mode, with this module's whole test suite green — because this module had no test suite.
    val config = IdentityWiring.configOf(authConfig(AuthType.Disabled), RbacPolicy.Disabled)

    FakeStructuredLogger[IO].flatMap(fake =>
      IdentityWiring.announce[IO](config, fake) *> fake.entries.map { entries =>
        assertEquals(entries.map(_.level), List("info"))
        val entry = entries.head
        assert(entry.message.contains("every request is anonymous"), entry.message)
        assertEquals(entry.context.get("auth.type"), Some("disabled"))
        assertEquals(entry.context.get("rbac.enabled"), Some("false"))
        assertEquals(entry.context.get("rbac.roles"), Some("0"))
      }
    )
  }

  test("a deployment with sign-in configured announces the mode it will use") {
    val config =
      IdentityWiring.configOf(authConfig(AuthType.Form), RbacPolicy.Disabled)

    FakeStructuredLogger[IO].flatMap(fake =>
      IdentityWiring.announce[IO](config, fake) *> fake.entries.map { entries =>
        val entry = entries.head
        assert(entry.message.contains("username and password"), entry.message)
        assertEquals(entry.context.get("auth.type"), Some("form"))
      }
    )
  }
}
