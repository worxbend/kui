package kui.kafka.auth

import cats.effect.IO
import cats.syntax.all.*

import kui.kernel.cluster.SaslMechanism
import kui.kernel.error.ErrorCode
import kui.kernel.{ClusterId, Secret}
import kui.testkit.KuiIOSuite

/** That a deployment missing an optional login library is told so, at startup, in a sentence it can act on.
  */
final class CloudHandlersSuite extends KuiIOSuite {

  private val id: ClusterId = ClusterId.unsafe("prod")
  private val secret: Secret[String] = Secret("p")

  /** A class loader that resolves nothing.
    *
    * The suite drives the check with this rather than with the real classpath, because what the test
    * classpath happens to contain is not something a test should assert against: `kafka-clients` is on it,
    * the three cloud SDKs are not, and both of those facts could change without the behaviour under test
    * changing at all.
    */
  private val resolvesNothing: ClassLoader = new ClassLoader(null) {
    override def loadClass(name: String, resolve: Boolean): Class[?] =
      throw new ClassNotFoundException(name)
  }

  private val everyMechanism: List[SaslMechanism] = List(
    SaslMechanism.Plain("u", secret),
    SaslMechanism.ScramSha256("u", secret),
    SaslMechanism.ScramSha512("u", secret),
    SaslMechanism.Gssapi("kafka", "kui@EXAMPLE.COM", None, useTicketCache = true, storeKey = false),
    SaslMechanism.OAuthBearer("https://idp/token", "client", secret, None),
    SaslMechanism.AwsMskIam(None, None, None),
    SaslMechanism.AzureEntra("ns", None),
    SaslMechanism.GcpManagedKafka
  )

  test("requiredClassesTable") {
    // Written as a `match` with no default case, so a mechanism added to the ADT cannot ship
    // without somebody deciding what classes it needs at run time.
    def expected(mechanism: SaslMechanism): Int = mechanism match {
      case SaslMechanism.Plain(_, _) => 1
      case SaslMechanism.ScramSha256(_, _) => 1
      case SaslMechanism.ScramSha512(_, _) => 1
      case SaslMechanism.Gssapi(_, _, _, _, _) => 1
      case SaslMechanism.OAuthBearer(_, _, _, _) => 2
      case SaslMechanism.AwsMskIam(_, _, _) => 2
      case SaslMechanism.AzureEntra(_, _) => 2
      case SaslMechanism.GcpManagedKafka => 2
    }

    everyMechanism.foreach { mechanism =>
      assertEquals(CloudHandlers.requiredClasses(mechanism).size, expected(mechanism))
      assert(CloudHandlers.requiredClasses(mechanism).forall(_.contains(".")))
    }
  }

  test("plainAndScramNeedNothingBeyondKafkaClients") {
    List(
      SaslMechanism.Plain("u", secret),
      SaslMechanism.ScramSha256("u", secret),
      SaslMechanism.ScramSha512("u", secret),
      SaslMechanism.Gssapi("kafka", "p", None, useTicketCache = true, storeKey = false),
      SaslMechanism.OAuthBearer("https://idp/token", "c", secret, None),
      SaslMechanism.AzureEntra("ns", None)
    ).foreach(mechanism => assertEquals(CloudHandlers.requiredCoordinate(mechanism), None))
  }

  test("theTwoMechanismsThatNeedALibraryNameTheCoordinate") {
    assertEquals(
      CloudHandlers.requiredCoordinate(SaslMechanism.AwsMskIam(None, None, None)),
      Some("software.amazon.msk:aws-msk-iam-auth:2.3.7")
    )
    assert(
      CloudHandlers
        .requiredCoordinate(SaslMechanism.GcpManagedKafka)
        .exists(_.contains("managed-kafka-auth-login-handler"))
    )
  }

  test("missingClassProducesAnActionableError") {
    val mechanism = SaslMechanism.AwsMskIam(None, None, None)

    CloudHandlers.checkWith[IO](id, mechanism, resolvesNothing).map { result =>
      val message = result.left.map(_.message).left.getOrElse("")

      assert(message.contains("AWS_MSK_IAM"), message)
      assert(message.contains("prod"), message)
      assert(message.contains("software.amazon.msk.auth.iam.IAMLoginModule"), message)
      assert(message.contains("software.amazon.msk:aws-msk-iam-auth:2.3.7"), message)
      assert(message.contains("docs/operations/configuration.md"), message)
      assertEquals(result.left.map(_.code), Left(ErrorCode.Unsupported))
    }
  }

  test("aMechanismWhoseClassesResolveIsAccepted") {
    // PLAIN's login module ships with kafka-clients, which is on this module's test classpath.
    CloudHandlers
      .check[IO](id, SaslMechanism.Plain("u", secret))
      .map(result => assertEquals(result, Right(())))
  }

  test("everyMechanismThatNeedsNoLibraryIsAcceptedOnKafkaClientsAlone") {
    // `requiredCoordinate == None` claims that every class the mechanism names ships with the JDK or
    // with kafka-clients. This module's test classpath carries the kafka-clients release production
    // ships (`Versions.kafkaClients`), so the claim can be checked against the real jar, which is
    // what catches a class that a kafka-clients upgrade moved or removed. The `resolvesNothing`
    // loader above cannot: it fails every name, right or wrong.
    everyMechanism
      .filter(CloudHandlers.requiredCoordinate(_).isEmpty)
      .traverse(CloudHandlers.check[IO](id, _))
      .map(results => assertEquals(results.collect { case Left(error) => error.message }, Nil))
  }

  test("gcpUsesKafkasLoginModuleAndOnlyGooglesCallbackHandlerIsMissing") {
    // managed-kafka-auth-login-handler 1.0.6 ships GcpLoginCallbackHandler and no login module.
    // Google's documented configuration names Kafka's OAuthBearerLoginModule in sasl.jaas.config.
    // On this classpath, which has kafka-clients and not Google's jar, that handler is the only
    // name still unresolved, so the coordinate the error names is enough to make the check pass.
    // Requiring GcpLoginModule fails here too: that class is not in the jar the message names.
    assertEquals(
      CloudHandlers.requiredClasses(SaslMechanism.GcpManagedKafka),
      List(LoginModules.OAuthBearer, LoginModules.GcpManagedKafkaCallbackHandler)
    )
    assertEquals(
      CloudHandlers
        .requiredClasses(SaslMechanism.GcpManagedKafka)
        .filterNot(CloudHandlers.resolvesClass(_, getClass.getClassLoader)),
      List(LoginModules.GcpManagedKafkaCallbackHandler)
    )
  }

  test("checkDoesNotInitializeTheClass") {
    // `Class.forName(name, false, loader)` must not run a static initializer. It matters because
    // initialising an AWS or Google login module runs code that can reach for an instance metadata
    // service, and a startup probe for "is this class here" must never make a network call.
    val exploding = "kui.kafka.auth.ExplodingOnInit$"

    // Caught as `Throwable`, not through `Try`: a failing class initializer is reported as an
    // `ExceptionInInitializerError`, which is an `Error`, which `scala.util.Try` deliberately does
    // not catch — it would escape and take a runtime worker thread with it.
    def initializes: Boolean =
      try {
        val _ = Class.forName(exploding, true, getClass.getClassLoader)
        true
      } catch { case _: Throwable => false }

    assert(
      CloudHandlers.resolvesClass(exploding, getClass.getClassLoader),
      "the probe could not find a class that is on the classpath"
    )
    assert(!initializes, "the fixture class no longer fails on initialization, so it proves nothing")
  }
}

/** A class whose static initializer fails, so that "the probe did not initialize it" is testable. */
object ExplodingOnInit {
  val boom: String = sys.error("this initializer must never run")
}
