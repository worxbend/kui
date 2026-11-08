package kui.kafka.auth

import cats.effect.Sync
import cats.syntax.all.*

import kui.kernel.ClusterId
import kui.kernel.cluster.SaslMechanism
import kui.kernel.error.{ApplicationError, KuiError}

/** Are the classes this mechanism needs actually on the classpath?
  *
  * ADR-022 keeps the cloud SDKs off KUI's default classpath because they are large and almost no deployment
  * needs any of them. The cost of that decision is one failure mode — a mechanism configured without its
  * library — and this object is where that cost is paid: at startup, once, with a message naming the
  * coordinate to add.
  *
  * The alternative, which is what happens without this check, is a `ClassNotFoundException` thrown from
  * inside a Kafka login callback on a network thread, several seconds after startup, in a stack trace that
  * names neither the cluster nor the mechanism.
  */
object CloudHandlers {

  /** The class names a mechanism needs at run time: its login module and, where it has one, its callback
    * handler.
    *
    * The `match` has no default case, so a mechanism added to the ADT cannot reach production without
    * somebody deciding what it needs.
    */
  def requiredClasses(mechanism: SaslMechanism): List[String] = mechanism match {
    case SaslMechanism.Plain(_, _) => List(LoginModules.Plain)
    case SaslMechanism.ScramSha256(_, _) => List(LoginModules.Scram)
    case SaslMechanism.ScramSha512(_, _) => List(LoginModules.Scram)
    case SaslMechanism.Gssapi(_, _, _, _, _) => List(LoginModules.Gssapi)
    case SaslMechanism.OAuthBearer(_, _, _, _) =>
      List(LoginModules.OAuthBearer, LoginModules.OAuthBearerCallbackHandler)
    case SaslMechanism.AwsMskIam(_, _, _) =>
      List(LoginModules.AwsMskIam, LoginModules.AwsMskIamCallbackHandler)
    case SaslMechanism.AzureEntra(_, _) =>
      List(LoginModules.OAuthBearer, LoginModules.OAuthBearerCallbackHandler)
    case SaslMechanism.GcpManagedKafka =>
      // Google's `managed-kafka-auth-login-handler` ships `GcpLoginCallbackHandler` and no login
      // module. Its documented JAAS line is Kafka's own `OAuthBearerLoginModule`.
      List(LoginModules.OAuthBearer, LoginModules.GcpManagedKafkaCallbackHandler)
  }

  /** The Maven coordinate an operator has to add to make `mechanism` work, if any.
    *
    * `None` means every class the mechanism needs ships with something KUI already has: the JDK for Kerberos,
    * `kafka-clients` for PLAIN, SCRAM and the generic OAUTHBEARER handler. Azure Entra is deliberately in
    * that group — the generic handler is enough for a client-credentials flow, and `com.azure:azure-identity`
    * is only needed for managed-identity flows an operator configures through the `properties` override
    * layer.
    */
  def requiredCoordinate(mechanism: SaslMechanism): Option[String] = mechanism match {
    case SaslMechanism.AwsMskIam(_, _, _) => Some("software.amazon.msk:aws-msk-iam-auth:2.3.7")
    case SaslMechanism.GcpManagedKafka =>
      Some(
        "com.google.cloud.hosted.kafka:managed-kafka-auth-login-handler:1.0.6 and " +
          "com.google.oauth-client:google-oauth-client:1.39.0"
      )
    case SaslMechanism.Plain(_, _) | SaslMechanism.ScramSha256(_, _) | SaslMechanism.ScramSha512(_, _) |
        SaslMechanism.Gssapi(_, _, _, _, _) | SaslMechanism.OAuthBearer(_, _, _, _) |
        SaslMechanism.AzureEntra(_, _) =>
      None
  }

  /** `Right(())` when every class the mechanism needs resolves through the given class loader. */
  def checkWith[F[_]: Sync](
      id: ClusterId,
      mechanism: SaslMechanism,
      loader: ClassLoader
  ): F[Either[KuiError, Unit]] =
    Sync[F]
      .delay(requiredClasses(mechanism).filterNot(resolvesClass(_, loader)))
      .map {
        case Nil => Right(())
        case missing => Left(unsupported(id, mechanism, missing))
      }

  /** The same check against the class loader this process actually uses. */
  def check[F[_]: Sync](id: ClusterId, mechanism: SaslMechanism): F[Either[KuiError, Unit]] =
    Sync[F].delay(contextClassLoader).flatMap(checkWith(id, mechanism, _))

  private def contextClassLoader: ClassLoader =
    Option(Thread.currentThread.getContextClassLoader)
      .orElse(Option(getClass.getClassLoader))
      .getOrElse(ClassLoader.getSystemClassLoader)

  /** `initialize = false` is load-bearing, not a micro-optimisation: initialising an AWS or Google login
    * module runs a static initializer that can reach for an instance metadata service, and a startup check
    * must not make a network call to decide whether a class exists.
    */
  private[auth] def resolvesClass(name: String, loader: ClassLoader): Boolean =
    scala.util.Try(Class.forName(name, false, loader)).isSuccess

  private def unsupported(
      id: ClusterId,
      mechanism: SaslMechanism,
      missing: List[String]
  ): KuiError = {
    val classes = missing.mkString(", ")
    val remedy = requiredCoordinate(mechanism) match {
      case Some(coordinate) =>
        s" (add $coordinate to the deployment image; see docs/operations/configuration.md)"
      case None =>
        " (the class should ship with the JDK or with kafka-clients, so this is a broken or " +
          "shaded deployment image; see docs/operations/configuration.md)"
    }

    ApplicationError.Unsupported(
      s"SASL mechanism ${mechanism.wireName} for cluster '${id.value}', whose $classes " +
        s"is not on the classpath$remedy"
    )
  }
}
