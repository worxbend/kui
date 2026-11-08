package kui.config

import scala.concurrent.duration.{DurationInt, FiniteDuration}

import kui.kernel.PageSize
import kui.kernel.search.SearchMode

/** Where the topic service finds the cluster service, and how patiently it waits for it.
  *
  * This is the `kui.clusterProfiles.*` section. When a URL is set, it is the topic service's source of
  * clusters: a second list that drifted from the cluster service shows up as "that cluster exists on the
  * dashboard but its topics 404" (DEVPLAN §10 D1, risk R-7; ADR-046). This type still holds no cluster list
  * of its own. A process with neither this URL nor `kui.gateway.services.cluster.url` falls back to
  * `KuiConfig.clusters` and says so in the log.
  *
  * The values here are the wire configuration. Turning them into the shared profile client's own
  * `kui.cluster.client.ClusterProfilesConfig` happens in the topic service's composition root, which is the
  * only place that can see both types: `libs/config` sits below every service and cannot depend on one.
  *
  * @param url
  *   the cluster service's base URL, e.g. `http://kui-cluster:8081`. The `/internal/v1` path is appended by
  *   the client from the contract's own constants, so it is not spelled here and cannot be spelled wrong
  * @param pollInterval
  *   how often the cluster list is re-read even when the change stream looks healthy. It is a fallback, not
  *   the primary mechanism: a dropped socket that nobody told either end about looks exactly like a quiet
  *   cluster, and this bounds how long that lie lasts
  * @param requestTimeout
  *   how long one profile fetch may take. Five seconds, because the far side answers from memory and a longer
  *   bound would only make a wedged connection take longer to notice
  * @param reconnectBackoff
  *   the first delay after the change stream drops
  * @param maxReconnectBackoff
  *   the cap on that delay. Thirty seconds, so a cluster service that is down for an hour is retried 120
  *   times rather than twice: a client that had backed off to ten minutes would take ten minutes to notice a
  *   recovery that happened one second after its last attempt
  * @param startupTimeout
  *   how long the first fetch may take before the service gives up waiting and starts degraded. It starts
  *   rather than exits, because a topic service that refuses to boot while the cluster service restarts turns
  *   one restart into two outages
  */
final case class ProfileClientConfig(
    url: SafeUrl,
    pollInterval: FiniteDuration,
    requestTimeout: FiniteDuration,
    reconnectBackoff: FiniteDuration,
    maxReconnectBackoff: FiniteDuration,
    startupTimeout: FiniteDuration
)

object ProfileClientConfig {

  // The four timings below are `ClusterProfilesConfig`'s own defaults, spelled again here because this is
  // the wire configuration and that is the runtime type. `ProfileClientConfigSuite`'s
  // `theDefaultsAreTheSharedClientsOwn` cannot live in this module — `libs/config` sits below every service
  // and cannot see `services/cluster/client` — so the composition root that maps one to the other owns that
  // assertion. Until it exists, the two are held together by this comment and by the documented table.
  val DefaultPollInterval: FiniteDuration = 60.seconds
  val DefaultRequestTimeout: FiniteDuration = 5.seconds
  val DefaultReconnectBackoff: FiniteDuration = 1.second
  val DefaultMaxReconnectBackoff: FiniteDuration = 30.seconds
  val DefaultStartupTimeout: FiniteDuration = 10.seconds

  val MinPollInterval: FiniteDuration = 5.seconds
  val MaxPollInterval: FiniteDuration = 1.hour
  val MinRequestTimeout: FiniteDuration = 1.second
  val MaxRequestTimeout: FiniteDuration = 60.seconds
  val MinBackoff: FiniteDuration = 100.milliseconds
  val MaxBackoff: FiniteDuration = 10.minutes
  val MinStartupTimeout: FiniteDuration = 1.second
  val MaxStartupTimeout: FiniteDuration = 60.seconds

  given CanEqual[ProfileClientConfig, ProfileClientConfig] = CanEqual.derived
}

/** The topic service's own dials: the `kui.topics.*` slice.
  *
  * Every default here is a number with a reason, and the reasons are in `docs/operations/configuration.md`
  * rather than only in this file, because the person who needs to change one is reading the operator
  * documentation and not the source.
  *
  * What is deliberately *not* here:
  *
  *   - **no cluster list of its own.** Resolved clusters come from [[ProfileClientConfig]] when a URL is set.
  *     The static `kui.clusters[]` list lives on [[KuiConfig]] and is only the topic process's fallback.
  *   - **no store settings.** The topic service is not a metadata-store client; it never opens the store.
  *   - **no admin chunk sizes.** They live in `AdminTuning`, per cluster, and arrive in the profile — the
  *     right chunk size depends on the broker being scraped, not on which KUI process is scraping it
  *     (`research/kafka/admin-capabilities.md` DC-D4).
  *
  * @param refreshInterval
  *   how often a cluster's topic snapshot is rebuilt. Sixty seconds, twice the cluster service's thirty,
  *   because a topic scrape is an order of magnitude more expensive than a cluster one and its data changes
  *   an order of magnitude less often
  * @param scrapeTimeout
  *   a whole scrape's budget. Past it the scrape is cancelled and the previous snapshot kept: a scrape that
  *   outlives its interval would overlap the next one and double the load on a cluster that is already
  *   struggling. Validated to be shorter than `refreshInterval`, so that combination cannot be configured
  * @param internalPrefix
  *   a topic is internal if Kafka says so **or** its name starts with this. Both conditions, never one:
  *   `__kui_config` is an ordinary topic to Kafka and noise to an operator (DEVPLAN §10 D3)
  * @param defaultSearchMode
  *   what `mode` means when a request omits it (ADR-038)
  * @param defaultPageSize
  *   what `pageSize` means when a request omits it
  * @param maxPageSize
  *   the largest page a request may ask for, itself capped at ADR-026's 500 because these lists are built in
  *   memory and a request for a million rows is not a big page, it is an outage
  * @param clusterProfiles
  *   the cluster-service endpoint, when one is configured. `None` is legal in this type because [[KuiConfig]]
  *   is loaded by every KUI process and the gateway, the cluster service and the store have no profile client
  *   at all. The topic process prefers this URL, then `kui.gateway.services.cluster.url`, and serves the
  *   static `kui.clusters[]` list when neither is set
  */
final case class TopicsConfig(
    refreshInterval: FiniteDuration,
    scrapeTimeout: FiniteDuration,
    internalPrefix: String,
    defaultSearchMode: SearchMode,
    defaultPageSize: PageSize,
    maxPageSize: PageSize,
    clusterProfiles: Option[ProfileClientConfig]
) {

  /** Whether a name is internal by the prefix rule alone.
    *
    * Given a name so that no caller re-derives it and gets it subtly different. The other half of the union —
    * Kafka's own `isInternal` flag — belongs to the adapter that has a Kafka answer to consult; this half is
    * pure configuration, and the two are combined exactly once, in the topic domain.
    */
  def isInternalByPrefix(topicName: String): Boolean = topicName.startsWith(internalPrefix)
}

object TopicsConfig {

  val DefaultRefreshInterval: FiniteDuration = 60.seconds
  val DefaultScrapeTimeout: FiniteDuration = 45.seconds
  val DefaultInternalPrefix: String = "__"

  val MinRefreshInterval: FiniteDuration = 5.seconds
  val MaxRefreshInterval: FiniteDuration = 1.hour
  val MinScrapeTimeout: FiniteDuration = 1.second
  val MaxScrapeTimeout: FiniteDuration = 1.hour

  val MinInternalPrefixLength: Int = 1
  val MaxInternalPrefixLength: Int = 16

  /** What a process gets when nothing under `kui.topics` is configured. Every field here is also the default
    * used per key, so configuring one key never changes another.
    */
  val Default: TopicsConfig = TopicsConfig(
    refreshInterval = DefaultRefreshInterval,
    scrapeTimeout = DefaultScrapeTimeout,
    internalPrefix = DefaultInternalPrefix,
    defaultSearchMode = SearchMode.Default,
    defaultPageSize = PageSize.Default,
    maxPageSize = PageSize.Max,
    clusterProfiles = None
  )

  given CanEqual[TopicsConfig, TopicsConfig] = CanEqual.derived
}
