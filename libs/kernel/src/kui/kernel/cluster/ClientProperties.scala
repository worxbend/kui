package kui.kernel.cluster

import kui.kernel.Secret

/** One value of a Kafka client property, carrying whether it may be printed. */
enum PropertyValue {
  case Plain(value: String)
  case Sensitive(value: Secret[String])

  /** The string a Kafka client needs.
    *
    * Named `unsafe` on purpose: every call site is a place where a secret leaves the type that protects it,
    * and "who calls `unsafeValue`" should be a question with a short, greppable answer.
    */
  def unsafeValue: String = this match {
    case Plain(value) => value
    case Sensitive(value) => value.value
  }

  /** The value, or `***` when it is sensitive. Safe for a log line, an error message or a DTO. */
  def redacted: String = this match {
    case Plain(value) => value
    case Sensitive(_) => PropertyValue.RedactedMarker
  }
}

object PropertyValue {

  /** What a sensitive value renders as everywhere. */
  val RedactedMarker: String = "***"

  given CanEqual[PropertyValue, PropertyValue] = CanEqual.derived
}

/** A Kafka client property map that knows which of its own keys are secret.
  *
  * The problem it solves is that redaction is normally the caller's job, and the caller is a log statement
  * written six months later by someone who has never read this file. Here the map itself knows, so
  * `redactedValues` is always right and `unsafeValues` is always visible in review.
  *
  * It is also the type of the ADR-022 **override layer**: `kui.clusters[].properties` is parsed into a
  * `ClientProperties` whose sensitive keys were classified by `isSensitiveKey`, and the renderer applies it
  * last with `++`.
  */
opaque type ClientProperties = Map[String, PropertyValue]

object ClientProperties {

  val empty: ClientProperties = Map.empty

  def apply(entries: (String, PropertyValue)*): ClientProperties = entries.toMap

  def fromMap(entries: Map[String, PropertyValue]): ClientProperties = entries

  /** Builds from raw configuration text, classifying each key with `isSensitiveKey`. */
  def fromRaw(entries: Map[String, String]): ClientProperties =
    entries.map { (key, value) =>
      key -> (if isSensitiveKey(key) then PropertyValue.Sensitive(Secret(value))
              else PropertyValue.Plain(value))
    }

  /** The rules that decide whether a property key names a secret, written so that a test can print them and a
    * reviewer can read them.
    *
    * The syntax is deliberately tiny: a bare key matches exactly, `*x` matches a suffix, `x*` a prefix, and
    * `*x*` a substring. `isSensitiveKey` interprets exactly this list, so a rule added here changes behaviour
    * and a rule that is not here does not exist.
    */
  val sensitiveKeyRules: List[String] = List(
    "sasl.jaas.config",
    "*.password",
    "*.key",
    "*secret*",
    "*credential*",
    "*token*",
    "*passphrase*",
    "*passwd*",
    "*auth.user.info*",
    "*certificate*"
  )

  /** `true` for a key that names a secret.
    *
    * That is `sasl.jaas.config`, anything ending in `.password` or `.key` (where `ssl.key.password`,
    * `ssl.keystore.password` and the inline PEM `ssl.keystore.key` come from), and anything containing
    * `secret`, `credential`, `token`, `passphrase`, `passwd`, `auth.user.info` or `certificate` (the last
    * covers `ssl.truststore.certificates` and `ssl.keystore.certificate.chain`).
    *
    * Deliberately *not* true for `ssl.truststore.location`, `ssl.keystore.location` or `ssl.keystore.type`: a
    * path or a store type is what an operator needs to see when the file is missing, and a substring match on
    * `key` would hide both.
    */
  def isSensitiveKey(key: String): Boolean = {
    val lowered = key.toLowerCase(java.util.Locale.ROOT)
    sensitiveKeyRules.exists(matchesRule(_, lowered))
  }

  private def matchesRule(rule: String, key: String): Boolean = {
    val startsWildcard = rule.startsWith("*")
    val endsWildcard = rule.endsWith("*")

    if startsWildcard && endsWildcard && rule.length > 2 then key.contains(rule.substring(1, rule.length - 1))
    else if startsWildcard then key.endsWith(rule.drop(1))
    else if endsWildcard then key.startsWith(rule.dropRight(1))
    else key == rule
  }

  extension (p: ClientProperties) {

    /** Right-biased union: the argument wins on a duplicate key.
      *
      * This is what makes "the override layer is applied last and wins" one line of code rather than a
      * convention every renderer has to remember.
      */
    def ++(that: ClientProperties): ClientProperties = {
      val left: Map[String, PropertyValue] = p
      val right: Map[String, PropertyValue] = that
      left ++ right
    }

    def get(key: String): Option[PropertyValue] = p.get(key)
    def keys: Set[String] = p.keySet

    /** The whole map, values and sensitivity flags intact.
      *
      * The inverse of [[ClientProperties.fromMap]], and the only way to get one back out without losing the
      * classification. Named `entries` rather than `toMap` so that it does not read as a cheap conversion: a
      * caller holding this map holds every sensitive value in it, and `PropertyValue` is what keeps the
      * redaction attached to each one.
      */
    def entries: Map[String, PropertyValue] = p
    def isEmpty: Boolean = p.isEmpty
    def nonEmpty: Boolean = p.nonEmpty
    def size: Int = p.size

    /** For a Kafka client constructor and nothing else. */
    def unsafeValues: Map[String, String] = p.map((key, value) => key -> value.unsafeValue)

    /** For logs, error messages and DTOs. */
    def redactedValues: Map[String, String] = p.map((key, value) => key -> value.redacted)

    /** `k=v, k2=***`, sorted by key. Stable, so two renderings of the same map compare equal, and safe, so it
      * is what `toString` shows.
      */
    def render: String =
      p.toList.sortBy(_._1).map((key, value) => s"$key=${value.redacted}").mkString(", ")
  }

  given CanEqual[ClientProperties, ClientProperties] = CanEqual.derived
}
