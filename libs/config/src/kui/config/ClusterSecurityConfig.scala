package kui.config

import java.util.Locale

import cats.data.{NonEmptyList, ValidatedNel}
import cats.effect.kernel.Sync
import cats.syntax.all.*

import kui.kernel.Secret
import kui.kernel.cluster.*

/** Decodes ADR-022's typed security model out of a configuration layer, once, for everybody who needs it.
  *
  * There are two places a cluster connection is configured — `kui.store.kafka.security` for the metadata
  * store (STORE-004) and `kui.clusters.<n>.security` for a managed cluster (CFGOP-001) — and they must accept
  * exactly the same spellings. Two decoders for one ADT is precisely what ADR-013's "one hand-written loader
  * per field" discipline exists to make visible, so this is the only one.
  *
  * It does not know about the loader's internals. It reads through a `Lookup`, which is any function that
  * answers "what was configured at this dotted key, and where did it come from", so a caller can drive it
  * from the layered precedence chain, from a test map, or from anything else.
  *
  * Secrets are decoded in two phases, matching the rest of the loader. The first phase is pure: it validates
  * the shape and produces a [[ClusterSecurityDraft]] holding unresolved `SecretRef`s. The second phase,
  * [[resolve]], follows the `env:` and `file:` references, which needs an effect. Keeping them apart is what
  * lets every syntactic problem be found and reported in one pass, before anything touches the filesystem.
  */
object ClusterSecurityConfig {

  /** What was configured at one dotted key, and which layer supplied it. */
  type Lookup = String => Option[(ConfigSourceName, String)]

  private type Problems[A] = ValidatedNel[ConfigProblem, A]

  /** The security model with its secrets still unresolved.
    *
    * `assemble` is a function rather than a second copy of the ADT with `SecretRef` in place of
    * `Secret[String]`. A mirrored ADT would be nine more cases to keep in step with `ClusterSecurity`, and
    * the first thing anyone would forget when adding a mechanism.
    */
  final case class ClusterSecurityDraft(
      secretRefs: Map[String, SecretRef],
      assemble: Map[String, Secret[String]] => ClusterSecurity
  )

  /** A TLS block with its three passwords still unresolved.
    *
    * Same shape and same reason as [[ClusterSecurityDraft]]: the pure phase collects `SecretRef`s keyed by
    * the dotted key they were written at, and [[resolve]] turns that map into secrets before `assemble` runs.
    */
  final case class TlsDraft(
      secretRefs: Map[String, SecretRef],
      assemble: Map[String, Secret[String]] => TlsConfig
  )

  /** Looks up the secret that was resolved for one dotted key.
    *
    * `Option` rather than a lookup that throws. Every key an `assemble` function asks for was put into the
    * same draft's `secretRefs` and [[resolve]] resolves exactly those, so the `None` case is unreachable
    * today; making it a value means a future draft that forgets to register a reference produces a connection
    * without that password — which Kafka reports — rather than an exception out of a pure decoder.
    */
  private def resolved(
      secrets: Map[String, Secret[String]]
  )(ref: (String, SecretRef)): Option[Secret[String]] = secrets.get(ref._1)

  /** Everything the decoder reads, so a caller can register the keys with an unknown-key check.
    *
    * Returned rather than written down twice: a key that is decoded but not registered becomes an "unknown
    * configuration key" error on a perfectly valid file, which is the bug this list prevents.
    */
  def keysUnder(prefix: String): List[String] =
    List(
      "protocol",
      "mechanism",
      "username",
      "password",
      "serviceName",
      "principal",
      "keytab",
      "useTicketCache",
      "storeKey",
      "tokenEndpoint",
      "clientId",
      "clientSecret",
      "scope",
      "profile",
      "roleArn",
      "stsRegion",
      "namespace",
      "ssl.verifyHostname",
      "ssl.enabledProtocols",
      "ssl.cipherSuites",
      "ssl.keyPassword",
      "ssl.truststore.location",
      "ssl.truststore.inline",
      "ssl.truststore.password",
      "ssl.truststore.type",
      "ssl.keystore.location",
      "ssl.keystore.inline",
      "ssl.keystore.password",
      "ssl.keystore.type"
    ).map(leaf => s"$prefix.$leaf")

  /** Reads `<prefix>.protocol` and everything the chosen protocol and mechanism require.
    *
    * A missing `protocol` is `PLAINTEXT`, which is the only sensible default for a development broker and the
    * one an operator who set nothing meant. Everything else that is missing but required is a problem,
    * reported with its own key, so the operator sees every field they still have to fill in rather than one
    * per restart.
    */
  def decode(prefix: String, lookup: Lookup): Problems[ClusterSecurityDraft] =
    decode(prefix, lookup, UrlPolicy.Strict)

  /** `policy` is applied to every token endpoint this mechanism will hand to Kafka.
    *
    * Kafka's OAuth HTTP client does not apply [[UrlPolicy]] when it dials, so a metadata address written here
    * would be requested once the client allow-lists it. The check is the same one every other outbound URL
    * goes through. The two-argument [[decode]] uses [[UrlPolicy.Strict]].
    */
  def decode(prefix: String, lookup: Lookup, policy: UrlPolicy): Problems[ClusterSecurityDraft] = {
    val reader = Reader(prefix, lookup, policy)
    reader.enumerated("protocol", Protocols, "PLAINTEXT").andThen {
      case "PLAINTEXT" =>
        (rejectMechanism(reader, "PLAINTEXT"), draft(Map.empty, _ => ClusterSecurity.Plaintext).validNel)
          .mapN((_, plaintext) => plaintext)
      case "SSL" =>
        (rejectMechanism(reader, "SSL"), reader.tls)
          .mapN((_, tls) => draft(tls.secretRefs, secrets => ClusterSecurity.Ssl(tls.assemble(secrets))))
      case "SASL_PLAINTEXT" => sasl(reader, SaslProtocol.SaslPlaintext, withTls = false)
      case "SASL_SSL" => sasl(reader, SaslProtocol.SaslSsl, withTls = true)
      case other =>
        reader.problem("protocol", s"'$other' is not one of ${Protocols.mkString(", ")}").invalidNel
    }
  }

  /** A `mechanism` under a protocol that has no SASL is refused, not ignored.
    *
    * Silently dropping it is how an operator ends up with an unauthenticated connection they believe is
    * authenticated: they wrote `mechanism: SCRAM-SHA-512`, KUI read `protocol: PLAINTEXT` (the default when
    * `protocol` is misspelled or absent), and the credentials were never sent.
    */
  private def rejectMechanism(reader: Reader, protocol: String): Problems[Unit] =
    reader.raw("mechanism").map(_.trim).filter(_.nonEmpty) match {
      case None => ().validNel
      case Some(mechanism) =>
        reader
          .problem(
            "mechanism",
            s"'$mechanism' is only meaningful for a SASL protocol, and protocol is $protocol; " +
              "set protocol to SASL_PLAINTEXT or SASL_SSL, or remove the mechanism"
          )
          .invalidNel
    }

  /** Follows the `env:` and `file:` references the draft collected.
    *
    * Every failure is reported against the dotted key that held the reference, because "environment variable
    * KUI_STORE_PASSWORD is not set" is only half an answer without knowing which cluster wanted it.
    */
  def resolve[F[_]: Sync](
      draft: ClusterSecurityDraft,
      env: Map[String, String]
  ): F[Problems[ClusterSecurity]] =
    draft.secretRefs.toList
      .sortBy(_._1)
      .traverse { (key, ref) =>
        SecretRef.resolve[F](ref, env).map {
          case Right(secret) => (key -> secret).validNel
          case Left(problem) => ConfigProblem(key, problem, ConfigSourceName.Default).invalidNel
        }
      }
      .map(_.sequence.map(pairs => draft.assemble(pairs.toMap)))

  // -----------------------------------------------------------------------------------------------
  // The mechanisms
  // -----------------------------------------------------------------------------------------------

  private val Protocols: List[String] = List("PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL")

  private val Mechanisms: List[String] =
    List(
      "PLAIN",
      "SCRAM-SHA-256",
      "SCRAM-SHA-512",
      "GSSAPI",
      "OAUTHBEARER",
      "AWS_MSK_IAM",
      "AZURE_ENTRA",
      "GCP"
    )

  private def draft(
      refs: Map[String, SecretRef],
      assemble: Map[String, Secret[String]] => ClusterSecurity
  ): ClusterSecurityDraft = ClusterSecurityDraft(refs, assemble)

  private def sasl(
      reader: Reader,
      protocol: SaslProtocol,
      withTls: Boolean
  ): Problems[ClusterSecurityDraft] = {
    val tls: Problems[Option[TlsDraft]] =
      if withTls then reader.tls.map(Some(_)) else None.validNel
    (reader.required("mechanism", Mechanisms), tls).tupled.andThen { (mechanism, tlsDraft) =>
      mechanismDraft(reader, mechanism).map { (refs, build) =>
        draft(
          refs ++ tlsDraft.fold(Map.empty[String, SecretRef])(_.secretRefs),
          secrets => ClusterSecurity.Sasl(protocol, build(secrets), tlsDraft.map(_.assemble(secrets)))
        )
      }
    }
  }

  private type MechanismDraft = (Map[String, SecretRef], Map[String, Secret[String]] => SaslMechanism)

  private def mechanismDraft(reader: Reader, mechanism: String): Problems[MechanismDraft] =
    mechanism match {
      case "PLAIN" => userAndPassword(reader, SaslMechanism.Plain.apply)
      case "SCRAM-SHA-256" => userAndPassword(reader, SaslMechanism.ScramSha256.apply)
      case "SCRAM-SHA-512" => userAndPassword(reader, SaslMechanism.ScramSha512.apply)

      case "GSSAPI" =>
        (
          reader.text("serviceName"),
          reader.text("principal"),
          reader.optionalText("keytab"),
          reader.boolean("useTicketCache", default = false),
          reader.boolean("storeKey", default = true)
        ).mapN { (serviceName, principal, keytab, useTicketCache, storeKey) =>
          (
            Map.empty[String, SecretRef],
            (_: Map[String, Secret[String]]) =>
              SaslMechanism.Gssapi(serviceName, principal, keytab, useTicketCache, storeKey)
          )
        }

      case "OAUTHBEARER" =>
        (
          reader.httpUrl("tokenEndpoint"),
          reader.text("clientId"),
          reader.secret("clientSecret"),
          reader.optionalText("scope")
        ).mapN { (endpoint, clientId, clientSecret, scope) =>
          (
            Map(clientSecret._1 -> clientSecret._2),
            (secrets: Map[String, Secret[String]]) =>
              SaslMechanism.OAuthBearer(endpoint, clientId, secrets(clientSecret._1), scope)
          )
        }

      case "AWS_MSK_IAM" =>
        (reader.optionalText("profile"), reader.optionalText("roleArn"), reader.optionalText("stsRegion"))
          .mapN { (profile, roleArn, region) =>
            (
              Map.empty[String, SecretRef],
              (_: Map[String, Secret[String]]) => SaslMechanism.AwsMskIam(profile, roleArn, region)
            )
          }

      case "AZURE_ENTRA" =>
        (reader.text("namespace"), reader.optionalHttpUrl("tokenEndpoint")).mapN { (namespace, endpoint) =>
          (
            Map.empty[String, SecretRef],
            (_: Map[String, Secret[String]]) => SaslMechanism.AzureEntra(namespace, endpoint)
          )
        }

      case "GCP" =>
        (
          Map.empty[String, SecretRef],
          (_: Map[String, Secret[String]]) => SaslMechanism.GcpManagedKafka
        ).validNel

      case other =>
        reader.problem("mechanism", s"'$other' is not one of ${Mechanisms.mkString(", ")}").invalidNel
    }

  private def userAndPassword(
      reader: Reader,
      build: (String, Secret[String]) => SaslMechanism
  ): Problems[MechanismDraft] =
    (reader.text("username"), reader.secret("password")).mapN { (username, password) =>
      (
        Map(password._1 -> password._2),
        (secrets: Map[String, Secret[String]]) => build(username, secrets(password._1))
      )
    }

  // -----------------------------------------------------------------------------------------------
  // Reading one prefix
  // -----------------------------------------------------------------------------------------------

  /** Reads leaves under one prefix and builds problems that name the full dotted key. */
  final private case class Reader(prefix: String, lookup: Lookup, policy: UrlPolicy) {

    def key(leaf: String): String = s"$prefix.$leaf"

    def raw(leaf: String): Option[String] = lookup(key(leaf)).map(_._2)

    def source(leaf: String): ConfigSourceName =
      lookup(key(leaf)).map(_._1).getOrElse(ConfigSourceName.Default)

    def problem(leaf: String, message: String): ConfigProblem =
      ConfigProblem(key(leaf), message, source(leaf))

    def text(leaf: String): Problems[String] =
      raw(leaf).map(_.trim).filter(_.nonEmpty) match {
        case Some(value) => value.validNel
        case None => problem(leaf, "is required for the configured mechanism and was not set").invalidNel
      }

    def optionalText(leaf: String): Problems[Option[String]] =
      raw(leaf).map(_.trim).filter(_.nonEmpty).validNel

    /** A required http(s) URL, checked with the deployment's [[UrlPolicy]]. */
    def httpUrl(leaf: String): Problems[String] =
      text(leaf).andThen(checkUrl(leaf, _))

    /** The same check, for a URL that may be absent. */
    def optionalHttpUrl(leaf: String): Problems[Option[String]] =
      optionalText(leaf).andThen {
        case None => none[String].validNel
        case Some(raw) => checkUrl(leaf, raw).map(Some(_))
      }

    private def checkUrl(leaf: String, raw: String): Problems[String] =
      SafeUrl.from(raw, policy) match {
        case Right(_) => raw.validNel
        case Left(error) => problem(leaf, error.message).invalidNel
      }

    /** A secret leaf: the dotted key it lives at, and the reference found there, still unresolved. */
    def secret(leaf: String): Problems[(String, SecretRef)] =
      raw(leaf) match {
        case Some(value) if value.nonEmpty => (key(leaf) -> SecretRef.parse(value)).validNel
        case _ =>
          problem(
            leaf,
            "is required for the configured mechanism and was not set; it takes a literal value, env:NAME or file:/path"
          ).invalidNel
      }

    /** The same, for a leaf that may simply be absent. */
    def optionalSecret(leaf: String): Option[(String, SecretRef)] =
      raw(leaf).filter(_.nonEmpty).map(value => key(leaf) -> SecretRef.parse(value))

    def boolean(leaf: String, default: Boolean): Problems[Boolean] =
      raw(leaf).map(_.trim.toLowerCase(Locale.ROOT)) match {
        case None => default.validNel
        case Some("true" | "yes" | "on") => true.validNel
        case Some("false" | "no" | "off") => false.validNel
        case Some(other) => problem(leaf, s"'$other' is not a boolean").invalidNel
      }

    def enumerated(leaf: String, allowed: List[String], default: String): Problems[String] =
      raw(leaf).map(_.trim.toUpperCase(Locale.ROOT)) match {
        case None => default.validNel
        case Some(value) if allowed.contains(value) => value.validNel
        case Some(other) => problem(leaf, s"'$other' is not one of ${allowed.mkString(", ")}").invalidNel
      }

    def required(leaf: String, allowed: List[String]): Problems[String] =
      raw(leaf).map(_.trim.toUpperCase(Locale.ROOT)) match {
        case None =>
          problem(
            leaf,
            s"is required for a SASL protocol; expected one of ${allowed.mkString(", ")}"
          ).invalidNel
        case Some(value) => value.validNel
      }

    def list(leaf: String): Problems[Option[List[String]]] =
      optionalText(leaf).map(_.map(_.split(',').toList.map(_.trim).filter(_.nonEmpty)))

    /** The TLS block. Every field is optional: a cluster with a certificate from a public CA needs none of
      * them, and that is by far the most common case.
      *
      * It answers with a [[TlsDraft]] rather than a `TlsConfig` for the same reason the SASL mechanisms do:
      * the three passwords in here — the truststore's, the keystore's and the private key's — are secrets,
      * and a secret in KUI may be written as a literal, as `env:NAME` or as `file:/path`. Resolving those
      * needs an effect, so this phase collects the references and the second phase follows them.
      */
    def tls: Problems[TlsDraft] =
      (
        store("truststore"),
        store("keystore"),
        boolean("ssl.verifyHostname", default = true),
        list("ssl.enabledProtocols"),
        list("ssl.cipherSuites"),
        optionalSecret("ssl.keyPassword").validNel
      ).mapN { (trust, keys, verify, protocols, ciphers, keyPass) =>
        val refs =
          (trust.flatMap(_._2).toList ++ keys.flatMap(_._2).toList ++ keyPass.toList).toMap

        TlsDraft(
          refs,
          secrets =>
            TlsConfig(
              truststore =
                trust.map((src, pwd, kind) => TrustStoreRef(src, pwd.flatMap(resolved(secrets)), kind)),
              keystore = keys.map((src, pwd, kind) =>
                KeyStoreRef(src, pwd.flatMap(resolved(secrets)), keyPass.flatMap(resolved(secrets)), kind)
              ),
              verifyHostname = verify,
              enabledProtocols = protocols,
              cipherSuites = ciphers
            )
        )
      }

    private def store(
        which: String
    ): Problems[Option[(StoreSource, Option[(String, SecretRef)], StoreType)]] = {
      val location = raw(s"ssl.$which.location").map(_.trim).filter(_.nonEmpty)
      val inline = raw(s"ssl.$which.inline").map(_.trim).filter(_.nonEmpty)
      val password = optionalSecret(s"ssl.$which.password")
      (location, inline) match {
        case (Some(_), Some(_)) =>
          problem(
            s"ssl.$which.location",
            s"cannot be set together with ssl.$which.inline; a store comes from a path or from inline base64, not both"
          ).invalidNel
        case (None, None) => None.validNel
        case (Some(path), None) =>
          storeType(which).map(kind => Some((StoreSource.FromPath(path), password, kind)))
        case (None, Some(base64)) =>
          storeType(which).map(kind => Some((StoreSource.Inline(Secret(base64)), password, kind)))
      }
    }

    private def storeType(which: String): Problems[StoreType] =
      raw(s"ssl.$which.type").map(_.trim.toUpperCase(Locale.ROOT)) match {
        case None => StoreType.Pkcs12.validNel
        case Some("PKCS12") => StoreType.Pkcs12.validNel
        case Some("JKS") => StoreType.Jks.validNel
        case Some("PEM") => StoreType.Pem.validNel
        case Some(other) => problem(s"ssl.$which.type", s"'$other' is not PKCS12, JKS or PEM").invalidNel
      }
  }

  /** Wraps a single problem, for callers outside this object that need the same shape. */
  private[config] def one(problem: ConfigProblem): NonEmptyList[ConfigProblem] = NonEmptyList.one(problem)
}
