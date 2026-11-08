package kui.config

import kui.kernel.cluster.ClientProperties

/** The raw Kafka client properties an operator may set alongside KUI's typed model, and the rule that keeps
  * them out of log lines.
  *
  * ADR-022's escape hatch: whatever KUI renders from the typed security ADT, these properties are applied
  * last and win. That is what lets a mechanism KUI has not modelled yet be used without waiting for a
  * release. The cost is that KUI has no idea what is in the map, which is why redaction follows
  * [[ClientProperties.isSensitiveKey]] — the same rules `fromRaw` uses when the map is parsed. A second
  * pattern here is how `ssl.keystore.key` was logged as a secret and still stored in the clear.
  *
  * Shared by the `kui.store.kafka.properties` slice (STORE-004) and the `kui.clusters[].properties` slice
  * (CFGOP-001), so that the two cannot drift into redacting different things.
  */
object ClientPropertyOverrides {

  /** Same decision as [[ClientProperties.isSensitiveKey]]. The rules live in one list, so a key classified
    * secret at parse time is the key redacted at log time.
    */
  def isSecretKey(name: String): Boolean = ClientProperties.isSensitiveKey(name)

  /** The map as it may be printed: sensitive values replaced, keys kept.
    *
    * Keys are kept because an operator debugging a connection needs to see *which* properties are set, and a
    * property name is not a secret. It is the value that is.
    */
  def redact(properties: Map[String, String]): Map[String, String] =
    properties.map((name, value) => name -> (if isSecretKey(name) then "***" else value))

  /** One line, in key order, safe to log. */
  def render(properties: Map[String, String]): String =
    redact(properties).toList.sortBy(_._1).map((name, value) => s"$name=$value").mkString(", ")
}
