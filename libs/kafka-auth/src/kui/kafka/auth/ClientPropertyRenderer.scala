package kui.kafka.auth

import java.nio.charset.StandardCharsets
import java.util.Base64

import scala.util.Try

import cats.data.NonEmptyList

import kui.kafka.auth.Jaas.JaasValue
import kui.kernel.cluster.*
import kui.kernel.{Secret, ValidationError}

/** The single place in KUI where a Kafka client property map is assembled.
  *
  * Everything else — the admin client factory, the store's consumer, the store's producer, and in M8 the
  * configuration wizard's validator — calls this and nothing else. That is what makes the quoting rule, the
  * redaction rule and the override precedence one implementation each instead of one per call site.
  */
object ClientPropertyRenderer {

  /** Which of the two stores a materialized path belongs to. */
  enum StoreRole {
    case TrustStore
    case KeyStore
  }

  object StoreRole {
    given CanEqual[StoreRole, StoreRole] = CanEqual.derived
  }

  /** The keys this renderer marks `Sensitive`, as a set a reviewer can read in one place.
    *
    * `everyRenderedSensitiveKeyIsMarkedSensitive` asserts set equality against what a render actually
    * produced, so a key added on either side without the other fails the suite.
    */
  val sensitiveKeys: Set[String] = Set(
    "sasl.jaas.config",
    "ssl.truststore.password",
    "ssl.keystore.password",
    "ssl.key.password",
    "ssl.keystore.key",
    "ssl.truststore.certificates",
    "ssl.keystore.certificate.chain"
  )

  /** Turns a typed connection into the property map a Kafka client is constructed from.
    *
    * The order of assembly is binding, because the last writer of a key wins:
    *
    *   1. `bootstrap.servers` and `client.id`
    *   2. `security.protocol`, derived from the security setting rather than configured beside it
    *   3. the TLS block, whenever the connection uses TLS at all
    *   4. the SASL block: `sasl.mechanism`, `sasl.jaas.config` and the mechanism-specific keys
    *   5. the per-purpose non-security defaults
    *   6. `connection.overrides`, applied last and winning on every key (ADR-022)
    *
    * An inline keystore that is not PEM renders a `location` pointing at the path `materialized` supplies.
    * When `materialized` has no entry for such a store the render fails, rather than emitting a `location`
    * property with an empty value that a Kafka client would report as a missing file with no indication of
    * which cluster asked for it. KAFKA-003 owns filling that map.
    */
  def render(
      connection: ClusterConnection,
      purpose: ClientPurpose,
      clientId: String,
      materialized: Map[StoreRole, String] = Map.empty
  ): Either[NonEmptyList[ValidationError], ClientProperties] = {
    val identity = ClientProperties.fromMap(
      Map(
        "bootstrap.servers" -> PropertyValue.Plain(connection.bootstrapServers.value),
        "client.id" -> PropertyValue.Plain(effectiveClientId(connection, purpose, clientId)),
        "security.protocol" -> PropertyValue.Plain(connection.security.securityProtocol)
      )
    )

    val tls: Either[NonEmptyList[ValidationError], ClientProperties] =
      connection.security.tlsConfig.fold(Right(ClientProperties.empty))(renderTls(_, materialized))

    val sasl: Either[NonEmptyList[ValidationError], ClientProperties] =
      connection.security.saslMechanism.fold(Right(ClientProperties.empty))(renderSasl)

    (tls, sasl) match {
      case (Right(tlsProperties), Right(saslProperties)) =>
        Right(
          identity ++ tlsProperties ++ saslProperties ++ defaultsFor(purpose) ++
            connection.overrides
        )
      case (Left(left), Left(right)) => Left(left ++ right.toList)
      case (Left(left), _) => Left(left)
      case (_, Left(right)) => Left(right)
    }
  }

  /** A blank `clientId` is filled in from the purpose and the cluster, so that a caller that has nothing
    * better to say still produces an attributable id rather than letting Kafka generate an anonymous one.
    */
  private def effectiveClientId(
      connection: ClusterConnection,
      purpose: ClientPurpose,
      clientId: String
  ): String =
    if clientId.isBlank then s"${purpose.prefix}-${connection.id.value}" else clientId

  /** M1 adds no non-security defaults. The hook exists because `ClientPurpose` is threaded through for
    * exactly this reason and a later task (`isolation.level` for the store's consumer, `enable.idempotence`
    * for its producer) needs somewhere to put them that is not a call site.
    */
  private def defaultsFor(purpose: ClientPurpose): ClientProperties = purpose match {
    case ClientPurpose.Admin => ClientProperties.empty
    case ClientPurpose.Consumer => ClientProperties.empty
    case ClientPurpose.Producer => ClientProperties.empty
  }

  // ---------------------------------------------------------------- TLS

  private def renderTls(
      tls: TlsConfig,
      materialized: Map[StoreRole, String]
  ): Either[NonEmptyList[ValidationError], ClientProperties] = {
    val truststore = tls.truststore.fold(Right(ClientProperties.empty))(
      renderTrustStore(_, materialized.get(StoreRole.TrustStore))
    )

    val keystore = tls.keystore.fold(Right(ClientProperties.empty))(
      renderKeyStore(_, materialized.get(StoreRole.KeyStore))
    )

    val fixed = ClientProperties.fromMap(
      Map(
        // Rendered explicitly in both directions. Relying on the client's default for the "on"
        // case is how a hostname check silently disappears in a client upgrade, and an empty
        // value is what turns it off.
        "ssl.endpoint.identification.algorithm" ->
          PropertyValue.Plain(if tls.verifyHostname then "https" else "")
      ) ++
        tls.enabledProtocols.map(list =>
          "ssl.enabled.protocols" -> PropertyValue.Plain(list.mkString(","))
        ) ++
        tls.cipherSuites.map(list => "ssl.cipher.suites" -> PropertyValue.Plain(list.mkString(",")))
    )

    (truststore, keystore) match {
      case (Right(t), Right(k)) => Right(fixed ++ t ++ k)
      case (Left(left), Left(right)) => Left(left ++ right.toList)
      case (Left(left), _) => Left(left)
      case (_, Left(right)) => Left(right)
    }
  }

  private def renderTrustStore(
      ref: TrustStoreRef,
      path: Option[String]
  ): Either[NonEmptyList[ValidationError], ClientProperties] = {
    val password =
      ref.password.map(p => "ssl.truststore.password" -> PropertyValue.Sensitive(p)).toMap

    (ref.storeType, ref.source) match {
      case (StoreType.Pem, StoreSource.Inline(base64)) =>
        decode(base64, "ssl.truststore.certificates").map { pem =>
          ClientProperties.fromMap(
            Map(
              "ssl.truststore.type" -> PropertyValue.Plain("PEM"),
              "ssl.truststore.certificates" -> PropertyValue.Sensitive(Secret(pem))
            ) ++ password
          )
        }

      case (storeType, StoreSource.FromPath(location)) =>
        Right(
          ClientProperties.fromMap(
            Map(
              "ssl.truststore.type" -> PropertyValue.Plain(storeType.wireName),
              "ssl.truststore.location" -> PropertyValue.Plain(location)
            ) ++ password
          )
        )

      case (storeType, StoreSource.Inline(_)) =>
        path.toRight(missingPath("ssl.truststore.location")).map { location =>
          ClientProperties.fromMap(
            Map(
              "ssl.truststore.type" -> PropertyValue.Plain(storeType.wireName),
              "ssl.truststore.location" -> PropertyValue.Plain(location)
            ) ++ password
          )
        }
    }
  }

  private def renderKeyStore(
      ref: KeyStoreRef,
      path: Option[String]
  ): Either[NonEmptyList[ValidationError], ClientProperties] = {
    val passwords =
      ref.password.map(p => "ssl.keystore.password" -> PropertyValue.Sensitive(p)).toMap ++
        ref.keyPassword.map(p => "ssl.key.password" -> PropertyValue.Sensitive(p)).toMap

    (ref.storeType, ref.source) match {
      case (StoreType.Pem, StoreSource.Inline(base64)) =>
        for {
          pem <- decode(base64, "ssl.keystore.key")
          split <- splitPem(pem)
        } yield ClientProperties.fromMap(
          Map(
            "ssl.keystore.type" -> PropertyValue.Plain("PEM"),
            "ssl.keystore.key" -> PropertyValue.Sensitive(Secret(split._1)),
            "ssl.keystore.certificate.chain" -> PropertyValue.Sensitive(Secret(split._2))
          ) ++ passwords
        )

      case (storeType, StoreSource.FromPath(location)) =>
        Right(
          ClientProperties.fromMap(
            Map(
              "ssl.keystore.type" -> PropertyValue.Plain(storeType.wireName),
              "ssl.keystore.location" -> PropertyValue.Plain(location)
            ) ++ passwords
          )
        )

      case (storeType, StoreSource.Inline(_)) =>
        path.toRight(missingPath("ssl.keystore.location")).map { location =>
          ClientProperties.fromMap(
            Map(
              "ssl.keystore.type" -> PropertyValue.Plain(storeType.wireName),
              "ssl.keystore.location" -> PropertyValue.Plain(location)
            ) ++ passwords
          )
        }
    }
  }

  private def missingPath(field: String): NonEmptyList[ValidationError] =
    NonEmptyList.one(
      ValidationError.Invariant(
        field,
        "an inline JKS or PKCS12 store has to be written to a file before a Kafka client can read " +
          "it, and no materialized path was supplied for it"
      )
    )

  private def decode(
      base64: Secret[String],
      field: String
  ): Either[NonEmptyList[ValidationError], String] =
    Try(Base64.getDecoder.decode(base64.value.replaceAll("\\s", ""))).toEither.left
      .map(_ =>
        NonEmptyList.one(
          ValidationError.Format(field, "valid base64", "<the configured value>")
        )
      )
      .map(bytes => new String(bytes, StandardCharsets.UTF_8))

  /** Splits a PEM bundle into its private-key blocks and its certificate blocks, which is what Kafka's two
    * inline keystore properties want: `ssl.keystore.key` takes the key, and `ssl.keystore.certificate.chain`
    * takes the certificates. An operator supplies one file containing both, because that is what every tool
    * emits.
    */
  private def splitPem(pem: String): Either[NonEmptyList[ValidationError], (String, String)] = {
    val blocks = "(?s)-----BEGIN [^-]+-----.*?-----END [^-]+-----".r.findAllIn(pem).toList
    val (keys, certificates) = blocks.partition(_.contains("PRIVATE KEY"))

    if keys.isEmpty || certificates.isEmpty then
      Left(
        NonEmptyList.one(
          ValidationError.Invariant(
            "ssl.keystore.key",
            "an inline PEM keystore must contain both a PRIVATE KEY block and at least one " +
              s"CERTIFICATE block; found ${keys.size} key block(s) and ${certificates.size} " +
              "certificate block(s)"
          )
        )
      )
    else Right((keys.mkString("\n"), certificates.mkString("\n")))
  }

  // ---------------------------------------------------------------- SASL

  private def renderSasl(
      mechanism: SaslMechanism
  ): Either[NonEmptyList[ValidationError], ClientProperties] = {
    val extra: Map[String, PropertyValue] = mechanism match {
      case SaslMechanism.Gssapi(serviceName, _, _, _, _) =>
        Map("sasl.kerberos.service.name" -> PropertyValue.Plain(serviceName))

      case SaslMechanism.OAuthBearer(tokenEndpoint, _, _, _) =>
        Map(
          "sasl.login.callback.handler.class" ->
            PropertyValue.Plain(LoginModules.OAuthBearerCallbackHandler),
          "sasl.oauthbearer.token.endpoint.url" -> PropertyValue.Plain(tokenEndpoint)
        )

      case SaslMechanism.AwsMskIam(_, _, _) =>
        Map(
          "sasl.client.callback.handler.class" ->
            PropertyValue.Plain(LoginModules.AwsMskIamCallbackHandler)
        )

      case SaslMechanism.AzureEntra(namespace, tokenEndpoint) =>
        Map(
          "sasl.login.callback.handler.class" ->
            PropertyValue.Plain(LoginModules.OAuthBearerCallbackHandler),
          "sasl.oauthbearer.token.endpoint.url" ->
            PropertyValue.Plain(tokenEndpoint.getOrElse(azureDefaultEndpoint(namespace)))
        )

      case SaslMechanism.GcpManagedKafka =>
        Map(
          "sasl.login.callback.handler.class" ->
            PropertyValue.Plain(LoginModules.GcpManagedKafkaCallbackHandler)
        )

      case SaslMechanism.Plain(_, _) | SaslMechanism.ScramSha256(_, _) | SaslMechanism.ScramSha512(_, _) =>
        Map.empty
    }

    jaasFor(mechanism).left
      .map(NonEmptyList.one)
      .map { jaas =>
        ClientProperties.fromMap(
          Map(
            "sasl.mechanism" -> PropertyValue.Plain(mechanism.wireName),
            "sasl.jaas.config" -> PropertyValue.Sensitive(jaas)
          ) ++ extra
        )
      }
  }

  /** Azure Event Hubs' Kafka endpoint issues its tokens from the tenant's own v2 endpoint; the namespace is
    * the only part an operator always knows, so it is what the default is built from.
    */
  private def azureDefaultEndpoint(namespace: String): String =
    s"https://login.microsoftonline.com/$namespace/oauth2/v2.0/token"

  private def jaasFor(mechanism: SaslMechanism): Either[ValidationError, Secret[String]] =
    mechanism match {
      case SaslMechanism.Plain(username, password) =>
        Jaas.module(
          LoginModules.Plain,
          "required",
          List("username" -> JaasValue.Plain(username), "password" -> JaasValue.Hidden(password))
        )

      case SaslMechanism.ScramSha256(username, password) =>
        Jaas.module(
          LoginModules.Scram,
          "required",
          List("username" -> JaasValue.Plain(username), "password" -> JaasValue.Hidden(password))
        )

      case SaslMechanism.ScramSha512(username, password) =>
        Jaas.module(
          LoginModules.Scram,
          "required",
          List("username" -> JaasValue.Plain(username), "password" -> JaasValue.Hidden(password))
        )

      case SaslMechanism.Gssapi(_, principal, keyTab, useTicketCache, storeKey) =>
        Jaas.module(
          LoginModules.Gssapi,
          "required",
          List(
            "useKeyTab" -> JaasValue.Plain(keyTab.isDefined.toString),
            "storeKey" -> JaasValue.Plain(storeKey.toString),
            "useTicketCache" -> JaasValue.Plain(useTicketCache.toString),
            "refreshKrb5Config" -> JaasValue.Plain("true"),
            "principal" -> JaasValue.Plain(principal)
          ) ++ keyTab.map(path => "keyTab" -> JaasValue.Plain(path)).toList
        )

      case SaslMechanism.OAuthBearer(_, clientId, clientSecret, scope) =>
        Jaas.module(
          LoginModules.OAuthBearer,
          "required",
          List(
            "clientId" -> JaasValue.Plain(clientId),
            "clientSecret" -> JaasValue.Hidden(clientSecret)
          ) ++ scope.map(s => "scope" -> JaasValue.Plain(s)).toList
        )

      case SaslMechanism.AwsMskIam(profile, roleArn, stsRegion) =>
        Jaas.module(
          LoginModules.AwsMskIam,
          "required",
          profile.map(p => "awsProfileName" -> JaasValue.Plain(p)).toList ++
            roleArn.map(r => "awsRoleArn" -> JaasValue.Plain(r)).toList ++
            stsRegion.map(r => "awsStsRegion" -> JaasValue.Plain(r)).toList
        )

      case SaslMechanism.AzureEntra(_, _) =>
        Jaas.module(LoginModules.OAuthBearer, "required", Nil)

      // Google's callback handler obtains the token from application-default credentials, so the
      // login module carries no options. It is Kafka's own `OAuthBearerLoginModule`: the published
      // handler jar ships `GcpLoginCallbackHandler` and no login module.
      case SaslMechanism.GcpManagedKafka =>
        Jaas.module(LoginModules.OAuthBearer, "required", Nil)
    }
}
