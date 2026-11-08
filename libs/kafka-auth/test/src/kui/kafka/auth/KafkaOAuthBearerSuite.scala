package kui.kafka.auth

import scala.jdk.CollectionConverters.*

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler

import kui.kernel.cluster.*
import kui.kernel.{ClusterId, Secret}
import kui.testkit.KuiSuite

/** That what KUI renders for OAUTHBEARER is a configuration kafka-clients itself accepts.
  *
  * `properties-oauthbearer.properties` records what KUI renders, and it cannot say whether the pinned
  * kafka-clients can use it: it pinned a callback handler class that kafka-clients 4.x no longer ships. This
  * suite hands the rendered map to the code a Kafka client runs on it. The callback handler is loaded by the
  * name KUI rendered, `JaasContext` parses the JAAS line, and `configure` runs the validation a client runs
  * when it is constructed: the token endpoint URL, the client id and the client secret.
  *
  * It runs on this module's runtime classpath, which is what every Kafka-facing KUI process inherits. That is
  * how it also proves jose4j is on that classpath: kafka-clients' pom leaves jose4j out, and without it
  * `configure` fails with `NoClassDefFoundError: org/jose4j/keys/resolvers/VerificationKeyResolver`.
  *
  * `configure` makes no network call. The first token request is made from `handle`, at login, so nothing
  * here contacts `idp.example.com`.
  */
final class KafkaOAuthBearerSuite extends KuiSuite {

  /** kafka-clients 4.x refuses every OAuth token endpoint that this JVM-wide allow-list does not name, and
    * its default is empty. The suite sets it the way an operator's `-D` flag would, and restores the previous
    * value afterwards.
    */
  private val AllowedUrls = "org.apache.kafka.sasl.oauthbearer.allowed.urls"

  private val tokenEndpoint = "https://idp.example.com/oauth2/token"

  private def rendered(mechanism: SaslMechanism): Map[String, String] =
    ClientPropertyRenderer
      .render(
        ClusterConnection(
          id = ClusterId.unsafe("oauth"),
          bootstrapServers = BootstrapServers.unsafe("broker-1:9093"),
          security = ClusterSecurity.Sasl(SaslProtocol.SaslSsl, mechanism, None),
          overrides = ClientProperties.empty,
          admin = AdminTuning.default
        ),
        ClientPurpose.Admin,
        "kui-admin-oauth"
      )
      .fold(errors => fail(errors.toList.map(_.message).mkString("; ")), _.unsafeValues)

  private def withAllowedUrl[A](url: String)(body: => A): A = {
    val previous = Option(System.getProperty(AllowedUrls))
    val _ = System.setProperty(AllowedUrls, url)
    try body
    finally {
      val _ = previous.fold(System.clearProperty(AllowedUrls))(System.setProperty(AllowedUrls, _))
    }
  }

  test("theRenderedOAuthBearerConfigurationIsAcceptedByKafkaClients") {
    val properties =
      rendered(SaslMechanism.OAuthBearer(tokenEndpoint, "kui", Secret("kui-secret"), Some("kafka")))
    val config = new AdminClientConfig(properties.asJava).values
    val entries = JaasContext.loadClientContext(config).configurationEntries

    // Loaded by name, the way Kafka loads `sasl.login.callback.handler.class`, so a name that no
    // longer exists in the pinned kafka-clients fails here rather than on a client's login thread.
    val handler = Class
      .forName(properties("sasl.login.callback.handler.class"))
      .asSubclass(classOf[AuthenticateCallbackHandler])
      .getDeclaredConstructor()
      .newInstance()

    try withAllowedUrl(tokenEndpoint)(handler.configure(config, properties("sasl.mechanism"), entries))
    finally handler.close()
  }

  test("buildingAClientAllowListsTheTokenEndpointKafkaWouldOtherwiseRefuse") {
    // kafka-clients 4.x rejects every token endpoint absent from
    // org.apache.kafka.sasl.oauthbearer.allowed.urls, and the default is empty. The property is not
    // something an operator sets: ConnectionProperties, which every client factory goes through,
    // has to add the endpoint it just rendered. This test clears the property first, so a pass
    // means KUI added it. configure makes no network call.
    val previous = Option(System.getProperty(AllowedUrls))
    val _ = System.clearProperty(AllowedUrls)
    try {
      val properties = ConnectionProperties
        .resource[IO](
          ClusterConnection(
            id = ClusterId.unsafe("oauth"),
            bootstrapServers = BootstrapServers.unsafe("broker-1:9093"),
            security = ClusterSecurity.Sasl(
              SaslProtocol.SaslSsl,
              SaslMechanism.OAuthBearer(tokenEndpoint, "kui", Secret("kui-secret"), Some("kafka")),
              None
            ),
            overrides = ClientProperties.empty,
            admin = AdminTuning.default
          ),
          ClientPurpose.Admin,
          "kui-admin-oauth"
        )
        .use {
          case Left(error) => IO.raiseError(new RuntimeException(error.message))
          case Right(rendered) => IO.pure(rendered.unsafeValues)
        }
        .unsafeRunSync()

      val config = new AdminClientConfig(properties.asJava).values
      val entries = JaasContext.loadClientContext(config).configurationEntries
      val handler = Class
        .forName(properties("sasl.login.callback.handler.class"))
        .asSubclass(classOf[AuthenticateCallbackHandler])
        .getDeclaredConstructor()
        .newInstance()

      try handler.configure(config, properties("sasl.mechanism"), entries)
      finally handler.close()
    } finally {
      val _ = previous.fold(System.clearProperty(AllowedUrls))(System.setProperty(AllowedUrls, _))
    }
  }
}
