package kui.kafka.auth

import kui.kernel.cluster.ClientProperties

/** Kafka's OAuth URL allow-list, which kafka-clients 4.x requires and leaves empty.
  *
  * `ConfigurationUtils.validateUrl` accepts `sasl.oauthbearer.token.endpoint.url` (and the JWKS endpoint,
  * when one is set) only when the exact string is one of the comma-separated entries in the system property
  * [[Property]]. The default is `""`, so a client KUI itself configured is refused with `ConfigException`
  * ("The URL cannot be accessed due to restrictions") before it can request a token. There is no client
  * config for the list. KUI adds the endpoint it rendered, at the moment a client is about to be built.
  *
  * The property is process-global and read again on every `configure`. Entries are unioned under a lock and
  * never replaced, so one cluster's endpoint does not drop another's, and an operator `-D` list is kept.
  * Kafka compares the trimmed config value with `Set.contains`, not a prefix or a wildcard.
  *
  * Kafka's own HTTP client does not apply KUI's `UrlPolicy` connected-IP check: once a URL is listed here,
  * the client dials whatever address the name resolves to, including an address that became link-local after
  * configuration was accepted. The name is checked with that policy when configuration is loaded. Listing it
  * here does not repeat the check, and it cannot, because the client is not KUI's HTTP stack.
  */
object OAuthBearerAllowList {

  val Property: String = "org.apache.kafka.sasl.oauthbearer.allowed.urls"

  /** The client properties `validateUrl` checks against [[Property]]. */
  val UrlKeys: Set[String] = Set(
    "sasl.oauthbearer.token.endpoint.url",
    "sasl.oauthbearer.jwks.endpoint.url"
  )

  /** Adds every OAuth endpoint in `properties` to [[Property]]. A no-op when none are present. */
  def permit(properties: ClientProperties): Unit = {
    val urls = UrlKeys.flatMap(key => properties.get(key).map(_.unsafeValue.trim)).filter(_.nonEmpty)
    if urls.nonEmpty then add(urls)
  }

  private val lock = new Object

  private def add(urls: Set[String]): Unit = lock.synchronized {
    val current = Option(System.getProperty(Property)).toList
      .flatMap(_.split(','))
      .map(_.trim)
      .filter(_.nonEmpty)
    val merged = (current ++ urls).distinct
    val _ = System.setProperty(Property, merged.mkString(","))
  }
}
