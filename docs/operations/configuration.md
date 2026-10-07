# Configuring metrics sources

KUI looks up a metrics source by cluster ID. For a source to be used, the member below
`kui.metrics.sources` must exactly match an `id` in `kui.clusters`; a cluster may also have no source, which is
a supported `not_configured` state. The loader currently validates the member as a legal cluster ID but does
not reject a legal ID absent from `kui.clusters`; such an unmatched entry measures nothing.

## Loading and secrets

Configuration precedence is command line, environment, configuration files, then built-in defaults. A later
file overrides an earlier file; a file supplied with `--config` is added last. Nested source keys follow the
same rule. For example, `--kui.metrics.sources.production.auth.token=env:PROM_TOKEN` overrides
`KUI_METRICS_SOURCES_PRODUCTION_AUTH_TOKEN`, which overrides YAML.

Secret fields accept `env:NAME`, `file:/absolute/path`, or a literal. Prefer environment or mounted-file
references outside local development. References are resolved at startup; missing, unreadable, or empty
values fail configuration without printing secret contents. The examples below intentionally contain only
references.

## Source kinds

| `kind` | Address | Current behavior |
| --- | --- | --- |
| `prometheus` (default) | A Prometheus text exposition endpoint, normally ending in `/metrics` | Supported and scraped on `kui.metrics.scrapeInterval`. This is what the quickstart uses. |
| `prometheus-api` | A Prometheus server base URL, optionally with a reverse-proxy prefix | A source-owned bounded instant/range client is wired internally and kept out of the exposition scraper. No server-owned Kafka metrics catalog maps it to a public metrics route yet, so screens report the source as unreadable. |
| `jmx` | A JMX address | Declared but not implemented. Run a JMX exporter in HTTP-server mode and use `kind: prometheus`. |

### Text exposition

The [quickstart configuration](../../deployment/quickstart/kui-quickstart.yaml) is the executable example:

```yaml
kui:
  metrics:
    scrapeInterval: 30s
    retention: 24h
    maxSamplesPerSeries: 5000
    sources:
      quickstart:
        kind: prometheus
        url: http://kafka-metrics:5556/metrics
        callTimeout: 10s
```

`callTimeout` must be shorter than `scrapeInterval`. Authentication, custom HTTP TLS, and all query/cache
keys are rejected for exposition sources rather than ignored. Protect a plain-HTTP exporter with network
policy; KUI does not add authentication to this source kind.

### Prometheus query API

This example is accepted, fully validated, and allocates the internal source-owned query provider. Per the
current product boundary above, no public route consumes it yet, so it does not feed KUI screens:

```yaml
kui:
  metrics:
    sources:
      production:
        kind: prometheus-api
        url: https://prometheus.example.net/tenant-a/prometheus
        callTimeout: 10s
        queryTimeout: 8s
        maxConcurrentQueries: 4
        maxSeriesPerQuery: 200
        maxPointsPerSeries: 600
        maxResponseBytes: 4194304
        maxCacheBytes: 67108864
        cacheTtl: 15s
        staleTtl: 2m
        auth:
          type: bearer
          token: env:KUI_PROMETHEUS_TOKEN
```

The URL is the server base, not `/api/v1/query` or `/api/v1/query_range`. Query strings, fragments, and URL
user-info are rejected; a reverse-proxy path is preserved. Requests use bounded form-encoded `POST`, disable
redirects, send the configured Prometheus timeout and series limit, and are not retried.

## Exact bounds

Global exposition/window settings are:

| Setting | Default | Accepted range or rule |
| --- | ---: | --- |
| `scrapeInterval` | 30s | 5s–1h |
| `retention` | 24h | 1m–30d and not shorter than `scrapeInterval` |
| `maxSamplesPerSeries` | 5,000 | 60–100,000 |
| source `callTimeout` | 10s | 1s–60s; for exposition, shorter than `scrapeInterval` |

The following source keys are valid only for `prometheus-api`:

| Setting | Default | Accepted range or rule |
| --- | ---: | --- |
| `queryTimeout` | 8s | 1s–55s and strictly shorter than `callTimeout` |
| `maxConcurrentQueries` | 4 | 1–32 |
| `maxSeriesPerQuery` | 200 | 1–1,000 |
| `maxPointsPerSeries` | 600 | 60–2,000 |
| `maxResponseBytes` | 4 MiB | 64 KiB–32 MiB |
| `maxCacheBytes` | 64 MiB | 4 MiB–512 MiB |
| `cacheTtl` | 15s | 1s–5m |
| `staleTtl` | 2m | from `cacheTtl` through 30m |

Bounds are inclusive except the two explicitly strict timeout relationships.

Independent fixed protocol bounds also apply: a compiled expression is at most 64 KiB UTF-8; a series has at
most 64 labels and 16 KiB of UTF-8 label names and values; warnings and info entries are each counted only up
to 32; total decoded samples are at most `maxSeriesPerQuery × maxPointsPerSeries`. Range timestamps must be
ordered, inside the requested window, aligned to the step, and cannot exceed
`floor((to - from) / step) + 1` points per series. Native histograms and scalar or string query results are
rejected rather than coerced.

## Authentication

Authentication is supported only by `prometheus-api`; omitting `auth` is anonymous.

| `auth.type` | Required keys |
| --- | --- |
| `none` | none |
| `basic` | `username`, secret `password` |
| `bearer` | secret `token` |
| `oauth` | HTTPS `tokenEndpoint`, `clientId`, secret `clientSecret`; `scope` is optional |

Exactly one mechanism may be configured. Surplus keys from another mechanism are startup errors.
Authenticated Prometheus API sources and OAuth token endpoints require HTTPS.

Basic and OAuth use the same secret-reference contract:

```yaml
# Basic
auth:
  type: basic
  username: kui
  password: file:/run/secrets/prometheus_password

# Or OAuth client credentials
auth:
  type: oauth
  tokenEndpoint: https://issuer.example.net/oauth2/token
  clientId: kui-metrics
  clientSecret: env:KUI_PROMETHEUS_CLIENT_SECRET
  scope: metrics.read
```

OAuth token acquisition has a fixed 5s request timeout and 64 KiB response-body ceiling. `expires_in` must
be a positive whole number no greater than 365 days; refresh begins 30s early or halfway through shorter
lifetimes. Concurrent callers share one token refresh. The source `callTimeout` still bounds the complete
query operation, including waiting for authentication.

## TLS and mutual TLS

Without a `tls` block, HTTPS uses JVM trust roots, mandatory hostname verification, and no client
certificate. There is no setting that disables certificate or hostname verification. Custom material is
valid only for an HTTPS `prometheus-api` source:

```yaml
kui:
  metrics:
    sources:
      production:
        kind: prometheus-api
        url: https://prometheus.example.net/prometheus
        tls:
          truststore:
            type: PKCS12
            location: /etc/kui/prometheus-truststore.p12
            password: file:/run/secrets/prometheus_truststore_password
          keystore:
            type: PKCS12
            location: /etc/kui/prometheus-client.p12
            password: file:/run/secrets/prometheus_keystore_password
            keyPassword: file:/run/secrets/prometheus_key_password
```

Trust stores and client key stores accept `JKS` or `PKCS12`. Each uses exactly one of `location` or secret
`inline` Base64 material. A custom trust store replaces JVM trust roots. A client `keystore` requires both
its store password and key password. PEM is not supported. Store decoding and SSL-context construction are
startup validation, so missing or malformed local TLS material prevents the listener from starting.

The OAuth token transport currently uses JVM trust independently of the source's custom trust/mTLS stores.

## Query cache and stale data

The query client keeps separate instant and range caches under one per-source budget: at most 1,024 entries
and `maxCacheBytes` in total. The key contains bounded source/query identities, an expression digest, and the
aligned instant or range—not raw PromQL or resource names. Identical in-flight requests share one physical
request; successful empty results are cacheable, while failures are not.

A result is fresh through `cacheTtl`. After that it may be returned with explicit stale freshness, but only
until `staleTtl` and only when refresh fails because of transport/unreachable, timeout, circuit-open, HTTP
429, or HTTP 5xx. Authentication, HTTP 400/404/422, configuration, protocol/decoder, unsupported-result,
and response-size failures never serve stale data.

These semantics are implemented by the internally wired provider. They become product-visible only when a
server-owned Kafka metrics catalog consumes that provider and maps its answers to public metric sections.

## Lifecycle and operations

- Invalid keys, bounds, secret references, URLs, authentication combinations, and local TLS material fail
  startup with accumulated, redacted diagnostics.
- An unreachable text exporter does not fail startup or readiness. The metrics service has no upstream
  readiness checks because its routes can still return an honest unavailable/not-configured section.
- Each supported exposition source owns one background scrape fiber, one-call bulkhead, zero retries, and a
  bounded retention window. Resource shutdown cancels an in-flight scrape and releases its HTTP resources.
- A `prometheus-api` source owns its query transport, credentials, caches, and telemetry for the source
  lifetime, but starts no exposition collector and currently has no public caller. Shutdown cancels shared
  loads, clears both caches, and releases credential and HTTP resources. A `jmx` source starts neither kind
  of collector. Startup logs and capabilities explain both boundaries instead of presenting empty data.

Use `GET /health/live` for restart decisions, `GET /health/ready` for routing, and `GET /capabilities` to see
per-cluster feature state. See [Observing KUI](observability.md#health-endpoints-what-to-probe-and-what-a-503-means)
for probe semantics and the [metrics catalog](observability.md#the-metrics) for cache, query, upstream, and
circuit telemetry.

## Security notes

- Production URL policy rejects loopback names and private, loopback, link-local, cloud-metadata, and other
  non-public addresses. Host names are not resolved during configuration validation, but the HTTP transport
  checks resolved addresses when connecting. Network egress policy remains part of the SSRF boundary.
  `KUI_ALLOW_PRIVATE_UPSTREAMS=true` is explicitly enabled only in the shipped local-demo backends; it broadly
  permits non-public destinations. Do not copy this relaxation into production without a security review
  and egress controls; see [local-demo upstream policy](deployment.md#local-demo-upstream-policy).
- Prometheus API redirects are disabled so credentials cannot be forwarded to a different destination.
  Response bodies, warning text, PromQL, URLs, and credentials are excluded from browser-facing errors,
  telemetry labels, and per-failure logs.
- The text-exposition integration has no application-layer auth or custom TLS configuration. Isolate it at
  the network layer, or terminate security at a trusted proxy/exporter endpoint.
- OAuth uses JVM system trust; the source's custom trust store and client certificate do not apply to the
  token endpoint in this version.
- KUI bounds response bytes and decoded structure, cache memory and entries, concurrency, and time. Those
  controls limit resource exhaustion; they do not make an untrusted Prometheus deployment safe to expose
  directly to users.

The [observability guide](observability.md#labels-never-carry-user-data) documents the bounded labels and
redaction boundary in detail.
