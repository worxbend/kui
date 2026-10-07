package kui.observability

/** Every metric KUI emits, as a constant.
  *
  * A metric name is an interface. Someone builds a dashboard on `kui.upstream.duration`, and a rename — or a
  * second module inventing `kui.upstream.latency` for the same thing — breaks it silently: the panel does not
  * fail, it just goes empty, and nobody notices until an incident. Names living here as constants means a
  * rename is a compile error at every call site, and `MetricNamesSuite` asserts this list against the
  * operator catalogue and the approved feature specifications, so documentation and code cannot drift.
  *
  * The milestone in a comment is when the metric starts being *emitted*. Every name is declared now, because
  * the alternative is discovering during M3 that the obvious name was already taken by something else.
  */
object MetricNames {

  /** How long a request took, by route and status. The one metric that answers "is KUI slow". */
  val HttpServerDuration: String = "kui.http.server.duration" // {service, route, status}

  /** How long a call to another system took, and how it ended. */
  val UpstreamDuration: String = "kui.upstream.duration" // {service, upstream, outcome}
  val UpstreamAttemptDuration: String = "kui.upstream.attempt.duration" // {service, upstream, outcome}

  /** Whether an upstream's circuit breaker is closed, open or half-open. */
  val UpstreamCircuitState: String = "kui.upstream.circuit.state" // {upstream}

  val KafkaAdminDuration: String = "kui.kafka.admin.duration" // {cluster, operation, outcome} (M1)
  val KafkaConsumeRecords: String = "kui.kafka.consume.records" // {cluster, topic}              (M3)
  val KafkaConsumeBytes: String = "kui.kafka.consume.bytes" // {cluster, topic}                  (M3)

  val CacheHits: String = "kui.cache.hits" // {cache}
  val CacheMisses: String = "kui.cache.misses" // {cache}

  val PrometheusQueryDuration: String = "kui.prometheus.query.duration"
  val PrometheusQueryRequests: String = "kui.prometheus.query.requests"
  val PrometheusResponseBytes: String = "kui.prometheus.response.bytes"
  val PrometheusResponseSeries: String = "kui.prometheus.response.series"
  val PrometheusResponseSamples: String = "kui.prometheus.response.samples"
  val PrometheusQueryInFlight: String = "kui.prometheus.query.inflight"
  val PrometheusQueryCacheAccess: String = "kui.prometheus.query.cache.access"
  val PrometheusQueryCoalesced: String = "kui.prometheus.query.coalesced"
  val PrometheusQueryLimitRejected: String = "kui.prometheus.query.limit.rejected"
  val PrometheusQueryDiagnostics: String = "kui.prometheus.query.diagnostics"

  /** What the UI is allowed to show, per service and cluster (ADR-039). */
  val CapabilityState: String = "kui.capability.state" // {service, cluster, state}

  /** How often each section of an aggregated response is served in each state.
    *
    * Labelled `{aggregation, section, status}`. It is the number that answers "how often do operators see a
    * degraded topic page, and which part of it is degrading" — which no per-endpoint metric can, because an
    * aggregation that answers 200 with four missing sections looks perfectly healthy to a status-code
    * histogram.
    */
  val AggregationSection: String = "kui.gateway.aggregation.section" // {aggregation, section, status} (M2)

  val StreamEvents: String = "kui.stream.events" // {service, stream, event}

  /** Open streams. A gauge that never returns to zero is a leak, which is exactly what it is for. */
  val StreamActive: String = "kui.stream.active" // {service, stream}

  val CursorRejected: String = "kui.cursor.rejected" // {reason}                                 (M3)
  val PrincipalRejected: String = "kui.principal.rejected" // {reason}
  val ConfigVersion: String = "kui.config.version" // {section}                                  (M1)

  /** Cluster-profile fetches by a Kafka-facing service, by how they ended (ADR-046). */
  val ClusterProfileFetch: String = "kui.cluster.profile.fetch" // {outcome}                      (M2)

  /** Whether that service's change subscription is open. Together with the counter above, these answer the
    * question an operator asks when a cluster edit does not take effect: is this service being *told* about
    * changes, or is it polling because the stream is broken? Those look identical in a latency graph.
    */
  val ClusterProfileSubscribed: String = "kui.cluster.profile.subscribed" //                       (M2)

  /** How often a record's intended serde could not read it, so the fallback rendered it instead.
    *
    * The metric that tells an operator their default serde is wrong for a topic. Without it, that
    * misconfiguration is visible only as a screen full of mojibake that nobody reports.
    */
  val SerdeDeserializeFailures: String = "kui.serde.deserialize.failures" // {serde, target, topic}  (M3)

  /** Payloads decoded by an auto-detected serde: what the topics nobody configured actually contain. */
  val SerdeAutodetected: String = "kui.serde.autodetected" // {serde}                                (M3)

  /** Produce payloads that could not become bytes, split by whose problem it is. */
  val SerdeSerializeFailures: String = "kui.serde.serialize.failures" // {serde, topic, reason}       (M3)

  /** Serde registries built for a cluster. A number that climbs on a stable configuration means profile churn
    * is rebuilding them, which throws away every cached schema each time.
    */
  val SerdeRegistryBuilt: String = "kui.serde.registry.built" // {cluster, reason}                    (M3)

  /** Calls to a Schema Registry, by how they ended. */
  val SerdeRegistryRequests: String = "kui.serde.registry.requests" // {cluster, outcome}             (M3)

  /** Whether a cluster's Schema Registry is answering: the number the capability fold reads (ADR-039). */
  val SerdeRegistryUp: String = "kui.serde.registry.up" // {cluster}                                  (M3)

  /** Smart-filter compilations, by outcome (ADR-017). */
  val FilterCompile: String = "kui.filter.compile" // {outcome}                                       (M3)

  /** How long one record's filter evaluation took. Its p99 is what tells an operator that a user's filter,
    * and not Kafka, is the reason browsing is slow.
    */
  val FilterEvaluateDuration: String = "kui.filter.evaluate.duration" //                              (M3)

  /** Records a filter could not decide on, split into runtime errors and timeouts. The same number the
    * `consumed` stream event reports as `filterErrors`.
    */
  val FilterErrors: String = "kui.filter.errors" // {kind}                                            (M3)

  /** Browses on which a masking rule applied (ADR-023, DM-001).
    *
    * Deliberately not a count of fields masked: that number is a function of the payload, and it would leak
    * the shape of protected data into metrics that are routinely less protected than the data itself.
    */
  val MaskingApplied: String = "kui.masking.applied" // {cluster, topic, target}                      (M3)

  /** Every name, in operator-catalogue order.
    *
    * Every name declared above is in here. There is no "declared but unpinned" tier: a constant that is not
    * in this list is invisible to `MetricNamesSuite`, to the operator table that suite compares against, and
    * to anybody counting what this build emits.
    */
  val all: List[String] = List(
    HttpServerDuration,
    UpstreamDuration,
    UpstreamAttemptDuration,
    UpstreamCircuitState,
    KafkaAdminDuration,
    KafkaConsumeRecords,
    KafkaConsumeBytes,
    CacheHits,
    CacheMisses,
    PrometheusQueryDuration,
    PrometheusQueryRequests,
    PrometheusResponseBytes,
    PrometheusResponseSeries,
    PrometheusResponseSamples,
    PrometheusQueryInFlight,
    PrometheusQueryCacheAccess,
    PrometheusQueryCoalesced,
    PrometheusQueryLimitRejected,
    PrometheusQueryDiagnostics,
    CapabilityState,
    StreamEvents,
    StreamActive,
    CursorRejected,
    PrincipalRejected,
    ConfigVersion,
    // W10-04 found this declared, emitted at `TopicOverviewUseCase.scala:230` and missing from this list,
    // so the process emitted twenty-seven series while every gate that reads `all` pinned twenty-six. A
    // name that is absent here is a name a rename cannot break, which is the one thing this file exists
    // to prevent. Placed where `docs/operations/observability.md` prints it, so the two read in step.
    AggregationSection,
    ClusterProfileFetch,
    ClusterProfileSubscribed,
    SerdeDeserializeFailures,
    SerdeAutodetected,
    SerdeSerializeFailures,
    SerdeRegistryBuilt,
    SerdeRegistryRequests,
    SerdeRegistryUp,
    FilterCompile,
    FilterEvaluateDuration,
    FilterErrors,
    MaskingApplied
  )

  /** The attribute keys, for the same reason the metric names are here: a label that drifts from `upstream`
    * to `upstreamName` in one module splits a dashboard's series in two without failing anything.
    */
  object Attr {
    val Service: String = "service"
    val Route: String = "route"
    val Status: String = "status"
    val Upstream: String = "upstream"
    val Outcome: String = "outcome"
    val Cluster: String = "cluster"
    val Operation: String = "operation"
    val Cache: String = "cache"
    val Source: String = "source"
    val Query: String = "query"
    val Limit: String = "limit"
    val State: String = "state"
    val Stream: String = "stream"
    val Event: String = "event"
    val Reason: String = "reason"
    val Section: String = "section"
    val Topic: String = "topic"
    val Serde: String = "serde"
    val Target: String = "target"
    val Kind: String = "kind"

    val all: List[String] = List(
      Service,
      Route,
      Status,
      Upstream,
      Outcome,
      Cluster,
      Operation,
      Cache,
      Source,
      Query,
      Limit,
      State,
      Stream,
      Event,
      Reason,
      Section,
      Topic,
      Serde,
      Target,
      Kind
    )
  }
}

/** How a call to another system ended, as the value of the `outcome` label.
  *
  * An enum rather than free strings because `outcome` is what an operator groups by when an upstream
  * misbehaves, and "timeout" appearing as `timeout`, `Timeout` and `timed_out` in three modules makes that
  * grouping useless.
  */
enum UpstreamOutcome {
  case Success, ClientError, ServerError, Timeout, CircuitOpen, Unreachable, Canceled

  def wire: String = this match {
    case Success => "success"
    case ClientError => "client_error"
    case ServerError => "server_error"
    case Timeout => "timeout"
    case CircuitOpen => "circuit_open"
    case Unreachable => "unreachable"
    case Canceled => "canceled"
  }
}

object UpstreamOutcome {

  /** The outcome for a response that came back, by status. */
  def ofStatus(status: Int): UpstreamOutcome =
    if status >= 500 then ServerError
    else if status >= 400 then ClientError
    else Success

  def fromWire(raw: String): Option[UpstreamOutcome] = values.find(_.wire == raw)

  given CanEqual[UpstreamOutcome, UpstreamOutcome] = CanEqual.derived
}
