package kui.kafka.auth

/** The fully qualified login-module and callback-handler class names, in one object.
  *
  * They are strings rather than `classOf[...]` deliberately. Naming the class would put `kafka-clients` and
  * the optional cloud SDKs on this module's compile classpath, which is exactly what layering rule A10 and
  * ADR-022's "cloud handlers are optional runtime modules" exist to prevent: KUI must start, and must render
  * a configuration, on a classpath that has none of them. KAFKA-003 checks at runtime whether a name resolves
  * and reports an error naming the missing coordinate when it does not.
  */
object LoginModules {

  val Plain: String = "org.apache.kafka.common.security.plain.PlainLoginModule"
  val Scram: String = "org.apache.kafka.common.security.scram.ScramLoginModule"
  val Gssapi: String = "com.sun.security.auth.module.Krb5LoginModule"
  val OAuthBearer: String =
    "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule"
  val AwsMskIam: String = "software.amazon.msk.auth.iam.IAMLoginModule"
  val GcpManagedKafka: String = "com.google.cloud.hosted.kafka.auth.GcpLoginModule"

  val AwsMskIamCallbackHandler: String = "software.amazon.msk.auth.iam.IAMClientCallbackHandler"

  /** kafka-clients' own client-credentials login handler. kafka-clients 3.x shipped it under both this name
    * and a deprecated `...oauthbearer.secured.` alias; 4.x ships only this one, so the alias resolves
    * nowhere.
    */
  val OAuthBearerCallbackHandler: String =
    "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler"

  /** The callback handler shipped by `managed-kafka-auth-login-handler`, which obtains a Google
    * application-default credential and presents it as an OAuth bearer token.
    */
  val GcpManagedKafkaCallbackHandler: String =
    "com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler"
}
