# KUI architecture

Status: written 2026-09-03 as the early-architecture design and revised since; corrected against
the 0.1.0 tree on 2026-10-24. Where this document and the tree disagree, the tree wins. Statements
marked *implemented*, *what exists today* or *delivered* describe shipped code; anything marked
**planned** or **not built** describes intent. The `ADR-NNN` identifiers cited below are historical
decision records that are **not in this repository**: there is no `docs/adr/` directory and no
`DECISIONS.md`. The current decision record is [`DESIGN.md`](DESIGN.md) §10. The research reports
(`research/…`), `TECH_DEBT.md`, and the `docs/domain/…`, `docs/testing.md`, `docs/frontend/README.md`
and `docs/operations/metadata-store.md` pages cited below are likewise not tracked; their citations record provenance, not readable
evidence.

This document answers one question: *how are the pieces of KUI shaped, and how do they fit
together so that KUI's product properties hold?* Anything about "why this library"
is in an ADR. Anything about "what does this screen do" is in the feature matrix.

Sections:

1. Topology and deployment shapes
2. Service catalog and tiers
3. Bounded contexts and module layout
4. Shared libraries and their public APIs
5. Inter-service contracts, headers and principal propagation
6. Capability registry and the degraded-response envelope
7. Streaming (SSE) envelope
8. Paging: offset pages and signed cursors
9. State, caching and refresh schedulers per service
10. Configuration ownership and distribution without restart
11. All-in-one composition
12. Frontend shell and microfrontends
13. Observability standard
14. Security boundaries
15. Errors
16. Repository layout

---

## 1. Topology and deployment shapes

```
 Browser: SolidJS / TypeScript shell + lazily loaded feature chunks (ADR-048, ADR-012)
   │  served by its own nginx image, which proxies /api to the gateway (same origin)
   │  HTTPS, session cookie or bearer token, SSE for streams
   ▼
 kui-gateway  (BFF: edge auth, RBAC pre-check, routing from contracts, screen aggregations,
               SSE fan-in, capability registry, OpenAPI merge; in development it also serves
               the built interface off its classpath, in a release it does not)
   │  HTTP over Tapir contracts, signed principal header, W3C trace context
   ├── kui-cluster-service     Core        cluster registry, topology, broker config, log dirs,
   │                                       cluster capability probing, cluster config wizard
   ├── kui-topic-service       Degradable  topics, configs, partitions, RF, analysis, producers
   ├── kui-message-service     Degradable  browse/stream/page, produce, resend, purge, filters,
   │                                       serdes, masking, event tracking
   ├── kui-consumer-service    Degradable  groups, lag, offset reset/delete, group delete
   ├── kui-security-service    PLANNED — not built (no services/security, no image)
   ├── kui-schema-service      Optional    subjects, versions, compatibility (Confluent-compatible)
   ├── kui-connect-service     Optional    Connect clusters, connectors, tasks, plugins, offsets
   ├── kui-ksql-service        Optional    statements, push/pull queries, tables, streams
   ├── kui-metrics-service     Degradable  JMX/Prometheus scrape, inferred metrics, graphs,
   │                                       /metrics exposition
   ├── kui-alerts-service      Degradable  rules over facts KUI already reads, the event store,
   │                                       the feed, the open count and the acknowledgement
   └── kui-identity-service    Core*       login (form/OIDC/LDAP), sessions, roles, audit sink
                                           (* only when auth is enabled)
   ▼
 Kafka clusters · Schema Registries · Connect · ksqlDB · Prometheus / JMX
```

**Delivered catalog (0.1.0).** `build.mill`'s `deployment.docker` declares images for the gateway,
the nine services `deployment/compose/docker-compose.yml` runs (cluster, topic, message, consumer,
schema, connect, ksql, metrics, alerts) and the all-in-one. `kui-security-service` is planned and
not built; ACL and client-quota management are outside the current product scope (README).
`kui-identity-service` has a `services/identity/app` main but **no image target and no container**
in the distributed stack, which runs with `kui.auth.type: disabled`. Authentication is delivered
through the all-in-one composition (the quickstart's `--with-auth` mode). An authenticated
distributed deployment is an open question (`DESIGN.md` §2 Q-c), not a delivered shape.

Two shapes are built from the same Mill modules (ADR-005):

| Shape | What runs | Used for |
| --- | --- | --- |
| Distributed | one container per service + the gateway; discovery by static `kui.gateway.services.<name>.url` (any platform DNS name works; no Helm chart is shipped) | production |
| All-in-one | `apps/allinone`: one JVM, one listening port, every service's application layer wired into the gateway's composition root; the gateway's client for each service is an in-process Tapir interpreter | local development, small installs, E2E tests |

No service mesh, no registry service, no shared database. Services own their
state; most state is a rebuildable snapshot of the target cluster (§9).

The distributed shape is not only described here, it is **verified in CI over HTTP**. The
`compose` job in `.github/workflows/ci.yml` ("distributed stack + fault isolation") builds the
backend images and runs `deployment/compose/smoke.sh` against
`deployment/compose/docker-compose.yml`. The script checks that every routed service answers
through the gateway, then stops the `kui-cluster` container. It asserts that
`/api/v1/capabilities` reports `cluster` unavailable while every other service stays available,
and that the gateway and the interface (`/ui/`) keep answering. It then starts the container again
and asserts that the capability recovers without intervention. That script is the check that
keeps the table above a statement of fact rather than of intent. The all-in-one shape cannot make
it, because "stopping a service" there would mean calling a method that pretends to be down.

The former browser-level Scala suite (`kui.e2e.ClusterServiceDownSuite`) no longer exists. The
`browser` CI job runs the Playwright suite in `frontend/e2e/` against the **all-in-one** quickstart
(`deployment/quickstart/quickstart.sh`), not against the distributed stack. In the distributed
shape, the rendering half of the claim is therefore **not verified by CI**: that the entry dims but
stays clickable, that the fallback panel names the reason, and that the page heals without a
reload. `deployment/compose/docker-compose.e2e.yml` (a 3-second readiness override) is used by the
manual `distributed-e2e` mode of `deployment/manual-qa.sh`.

## 2. Service catalog and tiers

Validated against the original service catalog with two amendments, both recorded in ADR-004:

1. **`kui-security-service` stays a separate service** — *if it is built*. It is **planned, not
   built** (see the delivered catalog in §1); the reasoning below is the design intent. It has its own capability gate
   (authorizer present, `ALTER` on the cluster), its own failure signature
   (`SecurityDisabledException`, slow `describeAcls` on clusters with thousands of bindings)
   and functionality (CSV sync, ACL presets) that is not cluster topology. Merging it into
   the Core `kui-cluster-service` would put a slow, optional feature inside the one service
   the UI cannot live without. Evidence: `research/kafka/admin-capabilities.md` §5 and DC-D11.
2. **`kui-config-service` is dissolved.** "Configuration management" is not a bounded context;
   it is three ownerships. Cluster configuration (registry, wizard validate/apply,
   test-connection, cluster CRUD, related file uploads) belongs to the Cluster Registry
   context, which already owns the runtime cluster registry (`research/kafbat/architecture.md`
   D2, D8). Auth and RBAC configuration belongs to Application Identity. Gateway configuration
   belongs to the gateway. The `/api/v1/config` facade is a gateway aggregation over the two
   owners. Details in §10 and ADR-036.

Tier = what the UI does when the service is down.

| Service | Bounded context (`docs/domain/context-map.md`) | Owns | Tier |
| --- | --- | --- | --- |
| `kui-gateway` | Edge (application, not a domain context) | sessions at the edge, RBAC pre-check, routing, aggregations, capability registry, SSE fan-in, static assets, OpenAPI merge, audit forwarding | **Core** |
| `kui-cluster-service` | Cluster Registry and Topology | cluster registry + resolved connection profiles, `describeCluster`, KRaft quorum, brokers, broker configs, log dirs, cluster stats, cluster capability probing, cluster config wizard and store | **Core** per cluster: shell and other clusters keep working |
| `kui-topic-service` | Topic Management | topic snapshot (descriptions, configs, offsets), list/search/sort/page, CRUD, clone, recreate, partitions, replication factor, topic analysis, active producers | Degradable |
| `kui-message-service` | Message Exploration | browse stream (seek/polling modes, cursors), per-partition page mode, tailing, event tracking, produce, resend, purge, smart filters, serde registry and resolution, masking | Degradable |
| `kui-consumer-service` | Consumer Group Management | group snapshot, list/page, details, lag, offset reset/delete, group delete | Degradable |
| `kui-security-service` (**planned, not built**) | Kafka Security Objects | ACL list/create/delete/presets/CSV, client quotas | Degradable (intended) |
| `kui-schema-service` | Schema Registry Management | subjects, versions, compatibility, checks, registry auth | Optional plugin (NotConfigured when a cluster has no registry) |
| `kui-connect-service` | Kafka Connect Management | connect clusters, connectors, tasks, plugins, validation, actions, offsets | Optional plugin |
| `kui-ksql-service` | ksqlDB | statement execution, query streaming, tables/streams | Optional plugin |
| `kui-metrics-service` | Kafka Observability | JMX/Prometheus scrapers, inferred metrics, graph descriptions, PromQL templates, `/metrics` exposition | Degradable |
| `kui-alerts-service` | Alerting | the rules KUI runs over facts it already reads, the event store and its retention, the feed with its open count and per-principal read marker, and the acknowledgement | Degradable |
| `kui-identity-service` | Application Identity and Access | authentication adapters, session store, role model and hot reload, permission query, audit sink | **Core when `kui.auth.type != disabled`**; the gateway runs anonymous when auth is disabled. Delivered in-process in the all-in-one; no distributed image (§1) |

Two Degradable/Optional services with different failure domains are never merged.

## 3. Bounded contexts and module layout

Each service is one Mill module tree, hexagonal, with the dependency rule
enforced by `moduleDeps` (a module cannot import what it does not depend on) and machine-checked
by `./mill checkArchitecture`, whose rule table is ADR-041 §2. Two consequences of that ADR are
worth stating here: a domain-owning service's `application` never depends on `libs/contracts-core`
or `libs/http` (it owns its types; `api` maps them), and the **gateway is outside that rule** —
it owns no `domain`, the wire is its subject matter, and its real constraints are enforced
instead as "only a service's `contract`" (A4) and "no Kafka client" (A8).

```
services/<name>/
  domain/          entities, value objects, domain errors, ports (traits). Depends on kui-kernel
                   and cats-core only. No Tapir, Circe, fs2-kafka, kafka-clients, otel4s, MacWire.
  application/     use cases, orchestration, DTO<->domain mapping (Chimney, ADR-033),
                   snapshot/refresh logic. Depends on domain, cats-effect, fs2, log4cats, otel4s-core.
  infrastructure/  adapters implementing the domain ports: kui-kafka, sttp clients, caches,
                   file stores. Java SDKs live only here. Exceptions become KuiError here.
  contract/        Tapir endpoints + DTOs + Circe codecs. JVM only since ADR-048; the browser's
                   types are generated from the OpenAPI these produce.
                   Depends on tapir-core, circe, kui-contracts-core. Never on domain.
  api/             server logic: contract -> application, KuiError -> HTTP envelope,
                   principal verification, OpenAPI document.
  app/             MacWire composition root, Ciris config slice, Netty server, telemetry bootstrap.
```

```
app -> api -> application -> domain <- infrastructure
        \-> contract                      \-> kui-kafka / kui-serde / external SDKs
```

Per-service specifics (what each `domain` module models; details in `docs/domain/<context>.md`):

| Service | Key aggregates / value objects | Ports in `domain` | Adapters in `infrastructure` |
| --- | --- | --- | --- |
| cluster | `ClusterProfile` (config + resolved endpoints + security), `ClusterDescription`, `Broker`, `LogDir`, `ClusterFeature` set | `ClusterAdmin[F]`, `ClusterConfigStore[F]`, `ConnectivityProbe[F]`, `ClockPort[F]` | kui-kafka admin adapter, Kafka `ConfigStore` adapter (file adapter for dev), probe clients |
| topic | `Topic` (NonEmptyList[Partition], ISR ⊆ replicas), `TopicConfig`, `TopicAnalysis` | `TopicAdmin[F]`, `TopicWriter[F]`, `ClusterProfiles[F]`, `ClockPort[F]` | kui-kafka, cluster-service contract client, datasketches |
| message | `BrowseRequest`, `PageRequest`, `PageWindow`, `DecodedRecord`, `PurgePlan`, `TrackQuery`; `SeekMode` and `OffsetRange` are `libs/kernel`'s and are used here rather than declared here | `ClusterProfileSource[F]`, `SerdeSource[F]`, `FilterSource[F]`, `CompiledFilter[F]`, `RecordDeleter[F]` — and `RecordMasking[F]` in **`application`**, with `ConfiguredRecordMasking` its adapter | fs2-kafka consumer/producer, kui-serde, kui-filter (CEL), the configured masking adapter |
| consumer | `ConsumerGroup`, `Member`, `PartitionLag` (`Option[Lag]` + anomaly flags), `ResetSpec` | `GroupAdminPort[F]` | kui-kafka |
| security | `AclBinding`, `AclFilter`, `ClientQuotaEntity`, `AclPreset` | **not built**: there is no `services/security`, and this row is the intent | not built |
| schema | `Subject`, `SchemaVersion`, `CompatibilityLevel` | `SchemaRegistryPort[F]` | own sttp client (ADR-014) |
| connect | `Connector` (state + tasks + `reason`), `ConnectorTask`, `ConnectorState`, `ConnectorKind`, `ConnectorFacts` (readable connectors + the ones KUI could not describe), `ConnectorOperation` | `ConnectWorkerPort[F]` | sttp client with 409 retry (ADR-037), the 2.3 `?expand=` reader and its pre-2.3 fallback |
| ksql | `KsqlObject` (stream, table, running query, topic), `KsqlObjects`, `KsqlObjectKind`, `KsqlStatement` with `StatementShape` (push query, pull query, statement) and `StatementProblem`, `StatementOutcome` | `KsqlServerPort[F]` | sttp client over the ksqlDB REST API. The push query's port is `KsqlQueryStream` in `application`, not here: an unbounded answer is an `fs2.Stream` and rule A1 keeps fs2 out of a `domain` |
| metrics | `MetricSnapshot`, `GraphDescription`, `PromQuery` | `MetricsSourcePort[F]` | Prometheus scrape and exposition, per-cluster source resolution, the bounded sample buffer |
| alerts | `AlertEvent` (opened-at, severity, category, resolution), `ClusterFacts`, `AlertLimits` | `ClusterFactsPort[F]` | kui-kafka admin facts, in-memory event store, logging acknowledgement sink, configured profile source |
| identity | `Principal`, `Session`, `Role`, `Subject`, `Permission` (from kui-security-core), `AuditRecord` | `UserDirectory[F]`, `PasswordHasher[F]` | UnboundID LDAP, nimbus OIDC, PBKDF2 users, in-memory/Kafka session and audit sinks |

**A service's outbound ports are not all in `domain`, and the column above names only the ones that
are.** A port whose subject is a *store* or another *process* rather than the domain is declared in
`application`, where the use case that owns it lives: `services/alerts` is the worked example, with
`AlertStore[F]` and `AcknowledgementSink[F]` in `services/alerts/application/` and only
`ClusterFactsPort[F]` in `services/alerts/domain/`. Rule A1 is what forces the split — a `domain`
module may depend on `libs/kernel` and cats-core and nothing else — so a port that needs to speak of
anything wider cannot live there.

**These services own a `domain`** — `cluster`, `topic`, `message`, `consumer`, `schema`, `connect`,
`ksql`, `metrics`, `alerts` and `identity` — and `security` is the one row above describing a service
that does not exist. That roster is read off disk and compared to this sentence by
`ArchitectureDocumentSuite`, because the previous version of this paragraph published a count that
had been wrong for two waves and nothing could tell.

**The *Key aggregates* column is not read by anything, and the measurement of what that costs is
worth more than the correction it prompted.** `MaskingPolicy` stood in the `message` row and
`grep -rn MaskingPolicy --include='*.scala'` finds it **zero** times in this repository; the real
type wave 9 shipped is `RecordMasking[F]`, a port in `application`, and the row says so now. But
`MaskingPolicy` is not the only one. Filtering that column to the tokens that look like Scala
identifiers and asking, for each, whether
`grep -rE '(class|trait|object|enum|type) +<Name>' services/<service>` finds a declaration answers
**no for nine of them**: `Topic` and `TopicAnalysis` (`topic`), `PollingMode` (`message`, now
removed), `Member` (`consumer`), `MetricSnapshot`, `GraphDescription` and `PromQuery` (`metrics`),
`AuditRecord` (`identity`), and `MaskingPolicy` itself — beside four more in the `security` row,
which is the row that says out loud that the service is not built. Several others resolve somewhere
in the tree but not under the service the row is about: `SeekMode` and `OffsetRange` are
`libs/kernel`'s, `Principal`, `Role` and `Permission` are `libs/security-core`'s, `Session` is the
gateway's, and `Subject` is two different types — `libs/kernel`'s opaque subject id and
`libs/security-core`'s RBAC subject — in the two rows that name it.

So the cheap direction the wave plan proposed — *every name in this column resolves to a `.scala`
declaration somewhere under that service* — is **false of nine names today, not of one**, and
landing it as a gate would be a rewrite of five rows rather than a check over a correct table. It is
filed with the seam instead, in `TECH_DEBT.md`: the check belongs in `ArchitectureDocumentSuite`,
which already walks `services/<n>/domain/src` and is in `services/gateway/api/test/`, a tree this
packet does not own. The measurement above is the input that closure needs; what it does not have is
a decision about whether this column names *types* or names *concepts*, and that decision is the
whole of the work.

The column was re-read against the tree in wave 7. Eight of the nine rows that had a `domain` then
named a port (`connect`'s still
said *not built*, and it is corrected above), and **six of those eight** named at least one
identifier that is not a `trait` declared in that service's own `domain`: `topic`, `message`,
`consumer`, `metrics`, `alerts` and `identity`. `cluster` and `schema` were correct.
Three of the six named a port that is real and lives in `application` (`alerts`, `consumer`,
`identity`); the rest named identifiers the tree declares nowhere — `TopicAnalysisPort[F]`,
`MessageBrowsePort[F]`, `BrokerMetricsScraper[F]`, `MetricsStore[F]`,
`TopicSnapshotSource[F]`, `GroupSnapshotSource[F]`, `IdentityProviderPort[F]` and
`RolePolicySource[F]`, which is **eight** and was published as nine — or named one that
belongs to a library or to another service (`SerdeRegistry[F]` in `libs/serde`, `AuditSink[F]` in
`libs/security-core`, `GroupAdmin[F]` in `libs/kafka`, `MessageFilterPort[F]` in `libs/filter` at
`CelFilterEngine.scala`, `SessionStore[F]` in the gateway's own
`application`). `MessageFilterPort[F]` was in the first bucket for a wave and is real: the message
service's use case holds it, and it is declared by the filter library rather than by that service,
which is the same shape `SerdeRegistry[F]` has. Nothing read this column, so it drifted for six
milestones.

`ArchitectureDocumentSuite` (in `services/gateway/api/test/`) reads this table off disk and fails
when an identifier in the ports column is not a `trait` declared under that service's own
`domain/src`, when a `trait` declared under `domain/src` is named by no row, and when a built
service has no row at all. A row for a service that does not exist
yet says so in words and is checked no further; that exemption is the one hole in this gate and is
deliberate, because the row is a plan rather than a description until the service lands.

The second of those three arrived in wave 8 and it is the interesting one. The first version read
the row and asked the tree, so it could only see an identifier that is *wrong*; deleting
`ClusterFactsPort[F]` from the alerts row left it green, and an emptied cell read as a service with
no outbound ports rather than as a claim nobody had checked. Omission is the failure mode this
table is most exposed to, because a row that says too little looks exactly like a row about a
service that does little.

The gateway has `contract` (its own `/api/v1` endpoint definitions, so
the frontend derives typed clients from them), `application` (aggregations, capability
registry, session cache), `api` and `app`. It depends on every service's `contract` module and
on nothing else from a service. It has no `domain` and no `infrastructure`: it holds no
business rules (ADR-004) and its only outbound adapters are contract clients.

That dependency is what routing a service *is*, and it is the one thing about a new service the
gateway cannot do for itself. `ServiceContracts.byService` names an endpoint list; the list is a
value in the owning service's `contract` module; a module the gateway does not depend on is a
value it cannot name. `services.gateway.api` therefore depends on `alerts.contract.jvm`, and the
alerts entry in `ServiceContracts` exposes its feed and acknowledgement through the same derived
proxy path as the other services.

The alerts **stream** has a second, separate requirement, and it is the same one the message
service's browse stream has. A stream cannot be a derived proxy route — `ContractRouting` decodes
and re-encodes, which is the wrong thing to do to a stream — so it needs a hand-written relay in
the gateway, of which `MessageStreamRoutes` is the worked example. That relay names the upstream
endpoint value directly, so the endpoint has to live in the service's `contract` module, where
rule A4 lets the gateway see it: `MessageEndpoints.browseStream` is in
`services/message/contract/src-jvm/` for exactly this reason, a JVM-only source inside a
cross-compiled contract. `AlertsStreamEndpoint` likewise lives in
`services/alerts/contract/src-jvm/`, and `AlertsStreamRoutes` rewrites its prefix, applies the
gateway's RBAC check before opening the upstream, and relays its event stream.

The ksqlDB **push query** is the third of these and the same shape exactly. `KsqlEndpoints.all` —
the object listing, the statement plan and the statement apply — is derived like any other service's
contract; the push query never finishes, so it is declared in
`services/ksql/contract/src-jvm/` as `KsqlStreamEndpoint` and relayed by `KsqlStreamRoutes`. Its
permission is re-decided at the relay rather than inherited: ADR-020 leaves the query string outside
the signed request digest and the statement travels in the query string, so nothing the gateway
signed can have covered it.

Every one of the three relays appends ADR-035's terminal `error` event when the upstream body ends
without one (`StreamProxy.withTerminalEvent`). That is the promise a browser depends on to tell a
finished stream from a broken one, and it is the rule each relay's own suite drives end to end —
asserting that the *relay* uses the helper, not only that the helper works.

The layering rules above are checked by `./mill checkArchitecture` on every build, not by
review (ADR-041). The task reads each module's declared `moduleDeps` and `mvnDeps` and fails on
a forbidden edge, naming the rule, both modules and the reason the rule exists:

| Rule | What it forbids |
| --- | --- |
| A1 | a service's `domain` depending on anything but `libs/kernel` and cats-core |
| A2 | a service's `contract` depending on any `domain` or `application` module |
| A3 | the `application` of a service **that owns a `domain`** depending on `libs/http`, `libs/contracts-core`, Tapir, Circe or an `infrastructure` module |
| A4 | the gateway reaching into any module of another service other than its `contract` |
| A5 | anything under `libs/` depending on a service or on the frontend |
| A6 | a core module (`kernel`, `contracts-core`, `security-core`) depending on a JVM-only library in its shared source set. The rule outlived the Scala.js build it was written for: the shared source set is still where a dependency that does not belong in a portable core gets in |
| A8 | the gateway depending on `libs/kafka`, `libs/kafka-auth`, fs2-kafka or kafka-clients |
| A9 | a service's `application`, `contract` or `api` depending on that service's own `infrastructure` module |
| A10 | `libs/kafka`, `libs/kafka-auth`, fs2-kafka or kafka-clients on the classpath of any module that is not a service's `infrastructure` or `app`, `libs/kafka*` itself, `libs/config` or `libs/testkit` |
| A11 | a service (other than the gateway, which A4 covers more strictly) depending on any module of another service other than its `contract` and its `client` |

A3 is scoped to services that own a `domain`, and the scoping is mechanical: a service is
domain-owning when a `services/<name>/domain` module is declared in the build. The gateway
declares none, so `services.gateway.application → libs/contracts-core` and `→ libs/http` are
legal — the wire is the gateway's subject matter (ADR-041 §1a). Its real constraints are A4 and
A8 instead. For every other service the original rule stands: `application` owns the types it
returns and the `api` layer maps them to wire DTOs.

A11 arrived with M2's first service-to-service dependency (ADR-041 Amendment 4, ADR-046). Until
M2 no service called another one, so nothing said what a service may see of its neighbour; A4 said
it for the gateway alone. The allow-list names `client` explicitly — the shared cluster-profile
consumer of ADR-046 — so a second such module has to be argued in the commit that adds it. A4 is
not widened to match: the gateway holds no Kafka client and has no profile to resolve, so it stays
at `contract` only, and one gateway edge is reported once, under A4, rather than twice.

A9 and A10 arrived with M1's first adapter module (ADR-041 Amendment 3). A9 is the dependency rule
pointing inward: a layer that can see an adapter will eventually call one, and the port becomes
decoration. A10 is A8 generalised from the gateway to everyone — `org.apache.kafka` must be
importable in exactly the places that adapt it. Its allow-list has five named entries plus any
service's `infrastructure` and `app` modules, and `KafkaAllowListSuite` asserts each entry on its
own so that a sixth has to be argued in the commit that adds it. `libs/config` is on the list
because the Kafka metadata-store adapter lives there (ADR-042 §5); that exception is deliberate and
named, which is what makes a second one visible.

A7 (the shell holding no static reference to a feature) is not checkable from module metadata. It is
enforced by `frontend/scripts/bundle-shape.mjs`, which reads the Vite manifest `pnpm build` writes and
fails if a feature chunk is reachable from the shell's entry without an `import()`; CI runs it as
`pnpm bundle-shape` in the frontend job. It is **not** enforced by BUILD-006 — that was
`build-tests`'s `BundleShape.scala`, which parses Scala.js linker output ADR-048 deleted, and no
`checkBundleShape` task remains for it to run under (`TECH_DEBT.md` records the dead pair).

## 4. Shared libraries and their public APIs

Shared libraries hold no business rule of a single context. Sketches below are
the *shape*; exact signatures are finalized in the M0 tasks. Scala 3, opaque types for ids,
`F[_]` at port boundaries, `IO` only in `app`.

| Module | Content | ADR |
| --- | --- | --- |
| `libs/kernel` (`kui-kernel`) | shared-kernel types below, `KuiError` hierarchy, paging/sorting primitives, `Validated` helpers. Pure. | ADR-004, ADR-034 |
| `libs/contracts-core` | error envelope DTO, `Page` DTOs, `Section[A]` envelope, SSE event DTOs, capability DTOs, Tapir codecs for kernel types. JVM/JS. | ADR-003, ADR-007, ADR-034, ADR-035 |
| `libs/kafka` (+ `libs/kafka-auth`) | `KafkaAdminPort` family over fs2-kafka `KafkaAdminClient`, consumer/producer factories, `KafkaErrorMapper`, batching, client property assembly from `ClusterProfile`; cloud SASL handlers as optional runtime modules | ADR-006, ADR-022, ADR-030 |
| `libs/serde` (+ `libs/serde-confluent`) | `Serde[F]` SPI, built-ins, registry/resolution, Kafbat bridge; Confluent wire-format serializers isolated | ADR-028, ADR-014 |
| `libs/filter` | `MessageFilterPort[F]` over cel-java | ADR-017 |
| `libs/cache` | `Ref`+TTL `SnapshotCell`, Caffeine wrapper, `SeriesWindow` retention ring, metrics hooks | ADR-016 |
| `libs/observability` | otel4s bootstrap, log4cats structured logger with MDC bridge, Tapir interceptors, metric names | ADR-008, ADR-009 |
| `libs/security-core` | `Principal`, `Rbac.decide`, `PrincipalCodec`, masking rule model. Pure; JVM/JS. | ADR-020, ADR-021, ADR-023 |
| `libs/http` | Netty server setup, error interceptor, health/ready/capabilities endpoints, sttp client factory with failover/retry/circuit breaker/bulkhead, SSE helpers | ADR-003, ADR-037 |
| `libs/config` | Ciris loaders, YAML + env mapping, `Secret[A]`, `ConfigStore[F]` port with Kafka (default) and file adapters, `StoreRecord` envelope, AES-GCM envelope encryption | ADR-013, ADR-036, ADR-042 |
| `libs/testkit` | Testcontainers topology, fake ports, ScalaCheck generators, Tapir stubs, golden files | ADR-018 |

### 4.1 `kui-kernel` shared-kernel types

```scala
package kui.kernel

opaque type ClusterId = String          // slug of the configured name (ADR-031)
opaque type KafkaClusterId = String     // reported by describeCluster
opaque type TopicName = String
opaque type PartitionId = Int
opaque type Offset = Long
opaque type BrokerId = Int
opaque type GroupId = String
opaque type Subject = String
opaque type SchemaId = Int
opaque type ConnectName = String
opaque type ConnectorName = String
opaque type CorrelationId = String
opaque type ServiceId = String          // "cluster", "topic", ... used by the capability registry

object ClusterId:
  def from(raw: String): Either[ValidationError, ClusterId]
  extension (id: ClusterId) def value: String

final case class TopicPartition(topic: TopicName, partition: PartitionId)
final case class OffsetRange(from: Offset, until: Offset)   // half-open, from <= until

enum SortOrder { case Asc, Desc }
final case class PageRequest(page: PositiveInt, pageSize: PageSize)
final case class Page[A](items: List[A], page: Int, pageSize: Int, totalItems: Option[Long], nextPageToken: Option[PageToken])

sealed trait KuiError { def code: ErrorCode; def message: String }
sealed trait DomainError         extends KuiError
sealed trait ApplicationError    extends KuiError   // NotFound, Conflict, Forbidden, Unsupported(feature), InvalidState, Invalid(fields)
sealed trait InfrastructureError extends KuiError   // Unreachable, Timeout, AuthFailed, Upstream(status, body), CircuitOpen
```

### 4.2 `KafkaAdminPort` family (`libs/kafka`)

One narrow port per context (`research/kafka/admin-capabilities.md` DC-D1), all implemented by
one adapter over fs2-kafka 4 with the raw `Admin` escape hatch where needed (ADR-006). Services
never import `org.apache.kafka.*`.

```scala
package kui.kafka.admin

final case class BatchResult[K, A](values: Map[K, A], skipped: Map[K, SkipReason])   // never silent drops

trait ClusterAdmin[F[_]]:
  def describeCluster(profile: ClusterProfile): F[Either[KuiError, ClusterDescription]]
  def describeQuorum(profile: ClusterProfile): F[Either[KuiError, Option[QuorumInfo]]]
  def capabilities(profile: ClusterProfile): F[Set[ClusterFeature]]
  def brokerConfigs(profile: ClusterProfile, broker: BrokerId, docs: Boolean): F[Either[KuiError, List[ConfigEntry]]]
  def alterBrokerConfig(profile: ClusterProfile, broker: BrokerId, ops: NonEmptyList[ConfigOp]): F[Either[KuiError, Unit]]
  def describeLogDirs(profile: ClusterProfile, brokers: Set[BrokerId]): F[Either[KuiError, BatchResult[BrokerId, List[LogDir]]]]
  def alterReplicaLogDir(profile: ClusterProfile, replica: TopicPartitionReplica, path: LogDirPath): F[Either[KuiError, Unit]]

trait TopicAdmin[F[_]]:
  def listTopics(profile: ClusterProfile, includeInternal: Boolean): F[Either[KuiError, List[TopicListing]]]
  def describeTopics(profile: ClusterProfile, names: List[TopicName]): F[Either[KuiError, BatchResult[TopicName, TopicDescription]]]
  def describeConfigs(profile: ClusterProfile, names: List[TopicName], docs: Boolean): F[Either[KuiError, BatchResult[TopicName, List[ConfigEntry]]]]
  def listOffsets(profile: ClusterProfile, spec: OffsetSpec, partitions: Set[TopicPartition], onNoLeader: NoLeaderPolicy): F[Either[KuiError, BatchResult[TopicPartition, Offset]]]
  def createTopic(profile: ClusterProfile, spec: NewTopicSpec, validateOnly: Boolean): F[Either[KuiError, Unit]]
  def deleteTopic(profile: ClusterProfile, name: TopicName): F[Either[KuiError, Unit]]
  def alterConfig(profile: ClusterProfile, name: TopicName, ops: NonEmptyList[ConfigOp], validateOnly: Boolean): F[Either[KuiError, Unit]]
  def createPartitions(profile: ClusterProfile, name: TopicName, total: PositiveInt): F[Either[KuiError, Unit]]
  def reassign(profile: ClusterProfile, plan: ReassignmentPlan): F[Either[KuiError, Unit]]
  def deleteRecords(profile: ClusterProfile, before: Map[TopicPartition, Offset]): F[Either[KuiError, Map[TopicPartition, Offset]]]
  def describeProducers(profile: ClusterProfile, partitions: Set[TopicPartition]): F[Either[KuiError, BatchResult[TopicPartition, List[ProducerState]]]]

trait GroupAdmin[F[_]]:
  def listGroups(profile: ClusterProfile, states: Set[GroupState]): F[Either[KuiError, List[GroupListing]]]
  def describeGroups(profile: ClusterProfile, ids: List[GroupId]): F[Either[KuiError, BatchResult[GroupId, GroupDescription]]]
  def committedOffsets(profile: ClusterProfile, ids: List[GroupId]): F[Either[KuiError, BatchResult[GroupId, Map[TopicPartition, Option[Offset]]]]]
  def alterOffsets(profile: ClusterProfile, id: GroupId, offsets: Map[TopicPartition, Offset]): F[Either[KuiError, Unit]]
  def deleteOffsets(profile: ClusterProfile, id: GroupId, partitions: Set[TopicPartition]): F[Either[KuiError, Unit]]
  def deleteGroup(profile: ClusterProfile, id: GroupId): F[Either[KuiError, Unit]]

// PLANNED, not built: no SecurityAdmin exists in libs/kafka (it belongs to the unbuilt security service)
trait SecurityAdmin[F[_]]:
  def describeAcls(profile: ClusterProfile, filter: AclFilter): F[Either[KuiError, List[AclBinding]]]
  def createAcls(profile: ClusterProfile, bindings: NonEmptyList[AclBinding]): F[Either[KuiError, Unit]]
  def deleteAcls(profile: ClusterProfile, filters: NonEmptyList[AclFilter]): F[Either[KuiError, List[AclBinding]]]
  def describeQuotas(profile: ClusterProfile, filter: QuotaFilter): F[Either[KuiError, List[QuotaEntry]]]
  def alterQuotas(profile: ClusterProfile, alterations: NonEmptyList[QuotaAlteration]): F[Either[KuiError, Unit]]
```

Batching (200 topics / 50 groups / 200 partitions, parallelism 4), no-leader filtering before
`listOffsets`, client invalidation on reconnect-class errors and the exhaustive
`KafkaErrorMapper` are adapter concerns (DC-D3..D5); they are configured per cluster through
`ClusterProfile.admin`.

### 4.3 `MessageBrowsePort` (`libs/kafka`, consumed by `kui-message-service`)

```scala
package kui.kafka.consume

enum SeekMode:
  case Beginning, Latest
  case Offset(perPartition: Map[PartitionId, Offset] | Offset)
  case Timestamp(at: Instant)
enum Direction { case Forward, Backward }
enum IsolationLevel { case ReadUncommitted, ReadCommitted }
final case class PollBudget(maxRecords: Int, maxBytes: Long, deadline: FiniteDuration, throttle: Option[BytesPerSecond])

final case class RawRecord(tp: TopicPartition, offset: Offset, timestamp: Instant, timestampType: TimestampType,
                           key: Option[Array[Byte]], value: Option[Array[Byte]], headers: List[RawHeader],
                           keySize: Int, valueSize: Int)

trait MessageBrowsePort[F[_]]:
  /** Describes the topic, drops empty and leaderless partitions, resolves seek positions,
    * clamps into [begin, end], applies the compacted-topic beginning fallback. */
  def resolveRanges(profile: ClusterProfile, topic: TopicName, seek: SeekMode, direction: Direction,
                    partitions: Option[NonEmptySet[PartitionId]], isolation: IsolationLevel): F[Either[KuiError, Map[TopicPartition, OffsetRange]]]
  /** One consumer per stream; assign/seek/pause; terminates when position >= until for all. */
  def pollRanges(profile: ClusterProfile, ranges: Map[TopicPartition, OffsetRange], budget: PollBudget, isolation: IsolationLevel): Stream[F, PollEvent]
  def tail(profile: ClusterProfile, partitions: NonEmptySet[TopicPartition], budget: PollBudget): Stream[F, PollEvent]
  def produce(profile: ClusterProfile, record: RawProducerRecord): F[Either[KuiError, RecordMetadata]]

enum PollEvent:
  case Polled(records: Chunk[RawRecord], stats: PollStats)   // stats: bytes, records, elapsed
  case Exhausted(tp: TopicPartition)
```

`kui-message-service` composes this port with `SerdeRegistry`, `MessageFilterPort` and the
masking rules into the browse use case; the merge-sort by timestamp with per-partition offset
order, page limits, cursor creation and the SSE envelope are application logic there.

### 4.4 `SerdeRegistry` (`libs/serde`)

```scala
package kui.serde

enum Target { case Key, Value }
enum PayloadKind { case Text, Json }
final case class DeserializeResult(text: String, kind: PayloadKind, properties: Map[String, Json])
final case class DeserializeFailure(serde: SerdeName, cause: String)

trait Serde[F[_]]:
  def name: SerdeName
  def describe: SerdeDescription
  def canDeserialize(topic: TopicName, target: Target): F[Boolean]
  def canSerialize(topic: TopicName, target: Target): F[Boolean]
  def preferable(topic: TopicName, target: Target): F[Boolean]
  def schema(topic: TopicName, target: Target): F[Option[SchemaDescription]]
  def parameters(topic: TopicName, target: Target): F[List[SerdeParameter]]
  def deserializer(topic: TopicName, target: Target): F[Deserializer[F]]
  def serializer(topic: TopicName, target: Target, params: Map[String, String]): F[Serializer[F]]

trait Deserializer[F[_]]:
  def deserialize(headers: List[RawHeader], bytes: Array[Byte]): F[Either[DeserializeFailure, DeserializeResult]]
trait Serializer[F[_]]:
  def serialize(input: String, headers: List[RawHeader]): F[Either[SerializeFailure, Array[Byte]]]

trait SerdeRegistry[F[_]]:
  def forCluster(profile: ClusterProfile): Resource[F, ClusterSerdes[F]]   // built in config order, closed on profile change

trait ClusterSerdes[F[_]]:
  def all: List[Serde[F]]
  /** pattern rules -> explicit config -> default serde -> String; Fallback String serde always exists. */
  def resolve(topic: TopicName, target: Target, explicit: Option[SerdeName]): F[Either[KuiError, Serde[F]]]
  def suggest(topic: TopicName, target: Target, use: SerdeUse): F[List[SerdeSuggestion]]
```

Deserialization failures never abort a stream: the record is emitted with the fallback
`String` result and a `deserializeError` (ADR-035).

### 4.5 `CapabilityRegistry` (gateway `application`)

The shapes below live in two places on purpose (ADR-041): the registry's own types belong to
the gateway's `application` layer, and the wire DTOs of the same name belong to
`libs/contracts-core`, where they carry Tapir schemas and Circe codecs. `services/gateway/api`
maps between them. The two differ where the wire needs it: the DTO spells durations as
milliseconds, because JSON has no duration type.

```scala
package kui.gateway.application.capability

final case class CapabilityKey(service: ServiceId, cluster: Option[ClusterId])   // None = cluster-independent

enum CapabilityState:
  case Available
  case Degraded(reason: DegradedReason)                       // structured (ADR-032)
  case Unavailable(reason: ReasonCode, message: String, since: Instant)
  case NotConfigured                                          // e.g. no schema registry on this cluster

final case class DegradedReason(code: ReasonCode, message: String, suggestedPollInterval: Option[FiniteDuration], p95: Option[FiniteDuration])

trait CapabilityRegistry[F[_]]:
  def snapshot: F[Map[CapabilityKey, CapabilityState]]
  def state(key: CapabilityKey): F[CapabilityState]
  def changes: Stream[F, CapabilityChange]                    // fed to /api/v1/capabilities/stream
  def report(key: CapabilityKey, state: CapabilityState): F[Unit]   // from readiness pollers and circuit breakers
  def probeNow(service: ServiceId): F[Unit]                   // POST /api/v1/capabilities/{service}/probe
```

The wire counterparts in `libs/contracts-core` (`package kui.contracts.capability`, task
KERN-005) mirror these case for case, with `ServiceId`/`ClusterId` carried through the Tapir
codecs from KERN-004 rather than degraded to bare `String`, and with
`suggestedPollIntervalMs: Option[Long]` / `p95Ms: Option[Long]` in place of the two
`FiniteDuration`s.

Inputs: each service's `GET /capabilities` (which lists, per cluster, the features it
currently supports and whether the cluster has the upstream configured), readiness polling,
the gateway's circuit breaker state per upstream (§6), and observed p95 latency. How the four
combine — the precedence order, the sticky `since`, the asymmetric debounce, and the rule that
a business error never dims a capability — is ADR-039.

### 4.6 `Rbac.decide` and the signed principal (`libs/security-core`)

```scala
package kui.security

enum Resource { case ApplicationConfig, ClusterConfig, Topic, ConsumerGroup, Schema, Connect, Connector, Ksql, Acl, Audit, ClientQuotas }
sealed trait Action { def resource: Resource; def implies: Set[Action]; def isAlter: Boolean }
final case class Permission(resource: Resource, pattern: Option[CompiledPattern], actions: Set[Action])   // actions expanded
final case class Role(name: RoleName, clusters: Set[ClusterId], subjects: List[Subject], permissions: List[Permission])
final case class RbacPolicy(roles: List[Role], defaultRole: Option[List[Permission]]) { def enabled: Boolean }

enum PrincipalKind { case Anonymous, Session, Bearer, System }
final case class Principal(name: UserName, roles: Set[RoleName], kind: PrincipalKind)

final case class ResourceAccess(resource: Resource, name: Option[String], actions: Set[Action], fallback: Option[ResourceAccess])
final case class AccessRequest(cluster: Option[ClusterId], resources: List[ResourceAccess], operation: OperationName, flags: ClusterFlags)
enum Decision { case Allowed; case Denied(reason: DenyReason) }

object Rbac:
  def resolveRoles(attributes: IdentityAttributes, roles: List[Role]): Set[RoleName]              // login time
  def effectivePermissions(policy: RbacPolicy, p: Principal, cluster: Option[ClusterId]): List[Permission]
  def decide(policy: RbacPolicy, p: Principal, req: AccessRequest): Decision                    // pure; every JVM caller decides from this one function
  def visible[A](policy: RbacPolicy, p: Principal, cluster: ClusterId, resource: Resource)(name: A => String): List[A] => List[A]

final case class PrincipalClaims(subject: UserName, roles: Set[RoleName], kind: PrincipalKind, sessionRef: Option[SessionRef],
                                 issuedAt: Instant, expiresAt: Instant, audience: ServiceId, requestDigest: RequestDigest)
final case class RequestDigest(method: String, path: String, bodySha256: Sha256)

trait PrincipalCodec[F[_]]:
  def sign(claims: PrincipalClaims): F[SignedPrincipal]                                          // compact JWS, HS256, kid
  def verify(token: SignedPrincipal, expected: ServiceId, request: RequestDigest, now: Instant): F[Either[PrincipalError, Principal]]
```

Read-only clusters are enforced inside `decide` through `ClusterFlags.readOnly` and
`Action.isAlter` (ADR-021), so no URL-pattern filter exists anywhere.

**What exists today (M0, task KERN-006).** `Principal`, `PrincipalKind`, `RequestDigest`,
`SessionRef`, `PrincipalClaims`, `SignedPrincipal`, `PrincipalError` and `PrincipalCodec[F]`
are implemented in `libs/security-core/src/kui/security/`, with the HS256 JWS implementation in
`libs/security-core/src-jvm/kui/security/JwsPrincipalCodec.scala`. The JOSE library lives in the
JVM source set alone, so the module still cross-compiles to the browser; `./mill
checkArchitecture` rule A6 fails the build if it ever moves. The RBAC half was written here as an M6
sketch; M6 has since delivered it (`libs/security-core/src/kui/security/rbac/Rbac.scala`,
`Policy.scala`), so the signatures above are the design shape, not the exact code.

Two details differ from the sketch above and are decided by KERN-006. `RequestDigest` travels in
the claims as an object (`req: {m, p, b}`) rather than as one further hash, because hashing the
triple would put a SHA-256 implementation in the shared source set for the benefit of a caller — the
browser, which shared this code at the time — that never signs anything; comparing the three fields binds a token to a
call exactly as tightly. And building a digest from a body is `RequestDigests.of` in the JVM
source set for the same reason, while `RequestDigest.ofRequestLine` (the streaming case, which
hashes no body) stays shared.

## 5. Inter-service contracts, headers and principal propagation

- Every service publishes exactly one `contract` module; the gateway derives routes from
  endpoint definitions (`research/kafbat/architecture.md` D1). Hand-written path lists are
  forbidden.
- Contract version `/api/v1` on the gateway; service-internal base path `/internal/v1`. Only
  additive changes inside a major; consumers ignore unknown fields.
- Headers on every gateway → service call:

| Header | Content | Required |
| --- | --- | --- |
| `X-Kui-Principal` | compact JWS from `PrincipalCodec.sign` (ADR-020); omitted in all-in-one, where the principal is passed in-process | yes (distributed) |
| `X-Kui-Correlation-Id` | gateway request id, always generated by the gateway and never taken from an inbound request (ADR-040); echoed in every log, span and error envelope | yes |
| `traceparent`, `tracestate` | W3C trace context from otel4s | yes |
| `X-Kui-Cluster-Id` | the `ClusterId` from the path, for adapters that log/metric outside the route layer | when cluster-scoped |
| `Accept: text/event-stream` | for streaming endpoints; the gateway re-streams | streams |

Services strip any inbound `X-Kui-*` header that did not pass `PrincipalCodec.verify`; the
gateway strips every inbound header whose name begins `X-Kui-` at the edge, by prefix rather
than by an enumerated list, before routing, authentication or logging (ADR-040). `traceparent`
and `tracestate` are outside that family and are handled by otel4s.

- Synchronous calls carry per-service timeout, bounded concurrency (bulkhead), retry only
  for idempotent reads, and a circuit breaker with half-open probing (`libs/http`, ADR-037).
- Streaming: FS2 over HTTP chunked `text/event-stream` service → gateway, re-streamed to the
  browser; cancellation propagates browser → gateway → service → consumer close (fiber
  cancellation, `KafkaConsumer.resource`).
- Services may call each other directly on the callee's published `/internal/v1` contract,
  under the four conditions of **ADR-043** (published contract, cached last-known fallback,
  capability reporting, one hop and no chains). This is the rule on direct service-to-service
  calls that ADR-043 settles; the gateway does not relay internal traffic. The edge list below is closed — adding
  an edge amends ADR-043:
  every Kafka-facing service → cluster-service (`ClusterProfile`, §10), and
  metrics-service → topic/consumer snapshot endpoints (30 s cadence, tolerant).
  The cluster-service half of the first edge ships in M1:
  `GET /internal/v1/clusters/{clusterId}/profile` is declared in
  `services/cluster/contract/.../ProfileEndpoints.scala` (ETag = the store record's version,
  `If-None-Match` answers 304), and the change stream
  `GET /internal/v1/clusters/stream` in `services/cluster/api/.../ClusterStreamEndpoint.scala`
  (an fs2 body cannot be described in a browser-compiled module). Both are **redacted in M1** —
  no credential leaves either — because M1 has no consumer that builds a Kafka client from them;
  M2's first consumer decides how it receives credentials.
- Asynchronous internal events (`kui.internal.events` topic) stay `RESEARCH` for M6+; nothing
  in M0–M5 depends on them.

## 6. Capability registry and the degraded-response envelope

Each service exposes `GET /health/live`, `GET /health/ready` and `GET /capabilities`:

The response shape is the committed sample
`libs/contracts-core/test/resources/golden/service-capabilities.json`, which
`CapabilityDtosSuite` decodes on both platforms:

```
GET /capabilities ->
{ "service": "schema",
  "clusters": { "prod-eu": { "configured": true,  "features": ["SCHEMA_REGISTRY"], "status": "available" },
                "staging": { "configured": false, "features": [], "status": "not_configured" } } }
```

The gateway folds readiness polling (every 10 s, configurable), the per-upstream circuit
breaker and these reports into `CapabilityState` per `(service, cluster)`. The per-cluster half
is **real as of M1**: readiness and the circuit are written to the service key
`(service, None)`, and what a service reports about one of its clusters is written to that
cluster's key. **A cluster's state is never folded into its service's.** The M0 code took the
worst of a service's clusters as the service's own verdict, which meant one unreachable Kafka
cluster dimmed the cluster feature for every other cluster's users — the failure ADR-039 §6 and
the M1 plan's decision D4 both exist to prevent. A cluster the service stops reporting is
retired as `not_configured` rather than left behind. The gateway publishes them at
`GET /api/v1/capabilities` and `GET /api/v1/capabilities/stream` (SSE, full snapshot on
connect, deltas afterwards); accepts `POST /api/v1/capabilities/{service}/probe`.

Aggregated responses are partial by design. The envelope in `contracts-core`:

```scala
enum Section[+A]:
  case Ok(data: A, fetchedAt: Instant)
  case Stale(data: A, fetchedAt: Instant, reason: ReasonCode)   // last known snapshot served while upstream is down
  case Unavailable(reason: ReasonCode, message: String, since: Option[Instant])
  case Forbidden
  case NotConfigured
```

Aggregations that must return partial results (`research/kafbat/api-analysis.md` "Proposed
KUI /api/v1 mapping"): `GET /clusters`, `GET /clusters/{id}/dashboard`,
`GET /clusters/{id}/brokers/{brokerId}` (metrics section), `GET /topics/{topic}/overview`,
`GET /consumer-groups/{groupId}` page aggregation, `GET /connects?withStats`,
`GET /capabilities`. A section failure never fails the response.

`GET /clusters` is **implemented as of M1** by
`services/gateway/application/.../cluster/ClusterOverviewUseCase.scala`, served by
`ClusterOverviewRoutes`. It carries two independent section levels: the outer one says whether
the cluster service could be reached at all — `stale` means the rows are the last that arrived,
with the time they did — and each row's own `summary` says whether that Kafka cluster could be
reached. `GET /clusters/{id}/dashboard` is deliberately **not** built in M1: with metrics and
topic counts out of scope, what remains of it is exactly `GET /clusters/{id}`, which the cluster
service already serves.

`GET /clusters/{clusterId}/topics/{topicName}/overview` is **implemented as of M2** by
`services/gateway/application/.../topic/TopicOverviewUseCase.scala`, served by
`TopicOverviewRoutes`. It is the second aggregation, and with two of them the pattern is now a
pattern rather than a one-off, so it is worth stating what the two share and where they differ.

Both never fail because an upstream did not answer: every path returns a document, a transport
failure is still reported to the capability signals, and the failing part is a section rather
than a status code. The topic overview adds the rule for a section whose service **does not
exist in this build at all**: it is `not_configured`, which a screen hides, and not
`unavailable`, which a screen shows with a reason so somebody can go and fix it (ADR-032). In M2
that applies to four of its five sections — consumer groups, connectors, ACLs and schemas — and
`unavailable` there would put four permanent red panels on every topic page of every
installation, which trains operators to ignore the colour that matters.

It also draws the line the cluster overview did not have to: a topic that does not exist is a
**404**, not a document with an empty topic section. "No such topic" and "the topic service could
not answer" have different remedies, and a page that renders an empty topic for one that was
deleted an hour ago looks like an answer.

Adding a section is a registration — a `SectionSource` under the section's name, plus the client
it calls — and `TopicOverviewSuite.addingASectionIsAMapEntry` proves it by registering one in a
test. That is how M4's consumer-groups section arrives without this file changing.

## 7. Streaming (SSE) envelope

Applies to message browsing, event tracking, KSQL responses, capability changes, live
metrics and topic-analysis progress (ADR-035). Named events, JSON `data`, cursor in `id`:

```
event: phase      data: {"name": "Polling partitions [0,1]"}
event: message    data: {"partition":0,"offset":42,"timestamp":"...","timestampType":"CREATE_TIME",
                          "key":{"text":"...","kind":"JSON","serde":"SchemaRegistry","properties":{...}},
                          "value":{...}, "headers":{"k":"v"}, "keySize":12,"valueSize":340,"headersSize":8,
                          "deserializeErrors":[{"target":"value","serde":"Avro","cause":"..."}]}
event: consumed   data: {"bytes":1048576,"records":250,"elapsedMs":812,"filterErrors":0,"budget":{"bytesLeft":...,"msLeft":...}}
event: done       data: {"reason":"limit"|"exhausted"|"budget"|"cancelled","cursor":"<signed cursor or null>"}
id: <signed cursor>
event: error      data: {"code":"KUI-UPSTREAM-UNAVAILABLE","message":"...","retryable":true,"correlationId":"..."}
event: heartbeat  data: {}      (every 15 s while idle; keeps proxies and EventSource alive)
```

Rules: exactly one terminal event (`done` or `error`) unless the client cancels; validation
and permission failures before the stream starts are ordinary HTTP error envelopes (§15);
`Last-Event-ID` on reconnect is accepted as a cursor; tailing emits no `done`; UI-facing
tailing throughput is rate-limited (default 20 events/s) in the service.

**What exists today (M0, task HTTP-004).** `libs/http`'s `kui.http.sse.Sse` implements the kernel
of this: heartbeat discipline, the at-most-one-terminal-event rule, a bounded drop-oldest buffer,
cancellation, and the `kui.stream.active`/`kui.stream.events` metrics. The normative example of the
wire format — byte for byte, including field order (`event`, then `id`, then `data` last) — is
`SseSuite.goldenWireFormat` in `libs/http/test/src/kui/http/sse/SseSuite.scala`; the browser-side
parser in `@kui/kernel` (`packages/kernel/src/data/sse/`) is tested against the same bytes, so the
two halves cannot drift apart. `phase`, `message` and `consumed` are domain events added by the services that own them
(M3); this module only guarantees that whatever a caller emits behaves the way every stream must.

## 8. Paging: offset pages and signed cursors

- Lists with in-memory sort (topics, consumer groups, schemas, connectors): offset paging
  `page`/`pageSize`, `sort=<field>:<asc|desc>`, `q`, `mode=plain|fts`, response
  `Page{items, page, pageSize, totalItems}`. The reference `pageCount` bug (computed before
  the internal-topic filter) is not reproduced.
- Naturally ordered data (message browsing, event tracking, audit): opaque signed cursors
  (ADR-026), never a process-local cache:

```
cursor = base64url(payload) "." base64url(HMAC-SHA256(payload, cursorKey))
payload = { v:1, c:<clusterId>, t:<topic>, d:"fwd"|"bwd", p:{"0":43,"1":17}, f:<filterId?>,
            ks:<keySerde?>, vs:<valueSerde?>, l:<limit>, i:"ru"|"rc", exp:<epoch s> }
```

Any replica of `kui-message-service` can continue a page. Smart filter sources are not
embedded; the browse request accepts `filterId` **and** `filterSource`, and the client sends
both whenever it has a smart filter, so a replica that has not compiled the filter compiles
it on demand (ADR-017). Cursors expire (default 1 h) and are bound to the cluster and topic.
- Kouncil-style table paging is a separate non-streaming endpoint
  (`GET /topics/{topic}/messages/page`) with per-partition newest-first pages; it needs no
  server state at all (ADR-026, `research/kouncil/architecture.md` D2).

## 9. State, caching and refresh schedulers per service

Kafbat's single `Statistics` snapshot is split by context (ADR-027); every snapshot carries
`status: Initializing | Online | Offline(lastError)`, `scrapedAt`, is replaced atomically, and
is served for reads with `Section.Stale` when the upstream is currently failing. All refresh
loops run under a `Supervisor`, are cancellable and emit `kui.cache.*` and
`kui.kafka.admin.duration` metrics (ADR-016).

| Service | Snapshot / cache | Refresh | Invalidation | Staleness contract |
| --- | --- | --- | --- | --- |
| cluster | `ClusterProfile` registry (replayed from `__kui_config`, then tail-following); `ClusterDescription`, quorum, brokers, log dirs, capability set per cluster | metadata every 30 s; capabilities every 1 h and on reconnect | a new `__kui_config` record for `cluster/<id>`; `POST /clusters/{id}/refresh` | reads ≤ 30 s old; profile version bump propagates within one poll interval; store unreachable means last known state plus `Degraded` |
| topic | per-cluster topic snapshot: descriptions, configs, begin/end offsets, name index | every 30 s (chunked admin calls) | partial update after create/update/delete of a topic; refresh endpoint | list/search ≤ 30 s old; detail pages re-describe the one topic live |
| message | serde registry per cluster (`Resource`, rebuilt on profile change); compiled CEL filters (content-hash id, bounded, TTL 1 h); SR schema-by-id cache (size-bounded), subjects (TTL 60 s) | on demand | profile change | payloads are never cached |
| consumer | per-cluster group snapshot: listings, descriptions, committed offsets; end offsets for committed partitions via its own `listOffsets` (no cross-service call) | every 30 s (50 groups × 4 parallel) | after reset/delete of a group | lag on the list page ≤ 30 s old; group detail page computes live |
| security (**planned, not built**) | none (ACL/quota lists are live, 5 s bounded) ; ACL capability probe | probe hourly | — | — |
| schema | subject list (TTL 60 s), latest-version cache (size-bounded, TTL 60 s) | on demand | after register/delete | — |
| connect | per-connect state: connectors + statuses via `?expand=status&expand=info` | every 30 s | after any action | list ≤ 30 s old; detail live |
| ksql | query pipes (TTL 1 min, single use) | — | — | — |
| metrics | scraped broker metrics, inferred metrics from topic/consumer snapshot endpoints | every 30 s | — | `/metrics` exposition is the last scrape |
| metrics | per-(cluster, metric) `SeriesWindow` ring (`libs/cache`), one bucket per `step`, bounded by `maxAge` and by `maxSamples` | the newest scrape of each `step`-wide bucket; every read evicts first, against an `Instant` the caller supplies | `clear` on a cluster-profile change, which restarts the coverage clock | a never-sampled bucket is absent, never `0`; a window collecting for less than the period asked for answers `None` rather than four minutes labelled "the last 24h"; a window whose samples have all been evicted counts a `miss`, so a dead collector shows on `kui.cache.misses` |
| alerts | per-cluster durable event/rule projection in `alerts/<clusterId>` plus per-principal read watermark in `settings/<clusterId>-<principal digest>`; bounded in-memory fallback only when no metadata store is configured | evaluation interval and store tail | rule evaluation, acknowledgement, mark-read | with a Kafka store, restarts preserve event identity, acknowledgement and unread state; without one startup logs the explicit in-memory degradation |
| identity | `RbacPolicy` (compiled once, hot-reloaded from the `rbac/roles` key of `__kui_config` or from a file watcher), sessions, OIDC state entries (5 min, single use) | on change | new store record, file change, session expiry | store unreachable means last known policy plus `Degraded`; writes rejected |
| gateway | capability registry; `sessionId → Principal` (TTL 30 s); OpenAPI merge | readiness every 10 s | logout, role reload event | — |

Cache discipline: TTL, invalidation trigger, bound, hit/miss metrics and a named
staleness contract, all recorded in the table above. Secrets and message payloads are never
cached. Small caches use `Ref` + TTL (`libs/cache.SnapshotCell`); bounded large caches
(schema by id, compiled filters) wrap Caffeine `AsyncCache` in `IO` (no Scaffeine); a history
of one value over time — every chart and sparkline this product draws — uses
`libs/cache.SeriesWindowCell`, which refuses on each of the three counts its row states
rather than interpolating across them.

The browser has a narrower cache tier. Non-sensitive appearance preferences use `localStorage`
for synchronous first paint and reconcile with the principal-scoped backend record. Unsynced
appearance snapshots use opaque principal-and-cluster-scoped keys, so one operator's retry cannot
be uploaded into another operator's settings after a shared-browser session change. The alert feed
may warm from IndexedDB only after `/auth/me` has established a principal: its key is a SHA-256
digest of cluster, principal and the current authorization grants; entries expire after five
minutes, are capped at sixteen, are decoded through the network decoder, and are always labelled
stale until the backend replaces them. A refusal removes that scope and sign-out/session expiry
clears the store. Authentication material, mutation responses and message payloads are never put in
browser storage. Hashed frontend assets use the ordinary immutable HTTP cache; `index.html` is
`no-store`, and API/SSE responses are never proxy-cached.

Search: an in-memory prefix/substring/trigram index inside each snapshot (`libs/kernel`
`NameIndex`); Lucene only if a benchmark on ≥ 50 k names shows p95 > 50 ms (ADR-038).

**Cross-entity search sits one level above those indexes and holds no state of its own** (ADR-049).
`GET /api/v1/search?q=<1..200>&limit=<1..50>` is answered by the gateway itself, beside the two
aggregations it already serves — not by a seventh service and not by an index at the edge. It fans
out one request per searched service per cluster and no request per result: the topic service's
unpaged `topics/names`, and the consumer and schema list endpoints, which take the caller's `q` and
narrow on their own side over the 30-second snapshots this table describes. It adds no cache row of
its own, so a search is exactly as fresh as the rows above it and never fresher.

Its one piece of vocabulary is `partial`: the service ids the gateway could not ask, de-duplicated
and sorted so that two identical requests produce identical bytes. A service reaches it in four ways
and they are one fact to whoever is looking — this deployment routes no such service, the call
failed, the caller may not list that kind of thing, or the service answered with a freshness section
carrying no rows (`Unavailable`, `Forbidden`, `NotConfigured`; `Stale` carries rows and is read
exactly like `Ok`, because a search over slightly old names beats a search that says a cluster holds
nothing). It is a list of ids rather than a boolean because the remedy differs per service, and it
is never empty by accident: losing the cluster list names `cluster` and all three searched services,
and a deployment holding no clusters at all names the three, because an empty document with an empty
`partial` would be byte-identical to "everybody answered and nothing matched". `limit` caps each
kind separately and is spent in rounds across the clusters, so one busy cluster cannot make another
cluster's matches unreachable from the field.

## 10. Configuration ownership and distribution without restart

Typed configuration is loaded by Ciris (ADR-013) from CLI flags → env → YAML
files → defaults, with Kafbat-compatible env keys mapped explicitly. Ownership (ADR-036):

| Section | Owner (single writer) | Readers | Distribution |
| --- | --- | --- | --- |
| `kui.clusters[]` | cluster-service (`ClusterConfigStore`, backed by the `cluster/<id>` keys of `__kui_config`) | every Kafka-facing service via `GET /internal/v1/clusters/{id}/profile` (ETag = profile version) + `GET /internal/v1/clusters/stream` (SSE change notifications, cached fallback = last known profile) | no restart; services rebuild clients/serdes when the version changes |
| `kui.auth`, `kui.rbac` | identity-service (`RolePolicySource`, reading `rbac/roles` from `__kui_config`, or a file watcher) | gateway (`/auth/me`, role reload SSE) | no restart for roles; auth adapter changes need an identity-service restart (documented) |
| `kui.store.*` | static only (file/env), never in the store | the two store-connected services | restart of that process (§10.1) |
| `kui.gateway`, `kui.server`, `kui.telemetry` | each process | — | restart of that process |

**What exists today (M0, task CFG-001).** The `kui.server`, `kui.gateway` and `kui.telemetry`
sections are implemented in `libs/config`, with the precedence chain (CLI → environment → YAML →
defaults), accumulated problem reporting, unknown-key rejection, `env:`/`file:` secret references
and the §14 outbound URL policy. `kui.auth` accepts only `type: disabled`, and `kui.clusters[]` and
`kui.rbac` are recognised placeholders that nothing reads: they arrive with the cluster registry in
M1 and the authorization model in M6. The key table for operators is
`docs/operations/configuration.md` and the reference file is `deployment/compose/kui.yaml`.

`ClusterProfile` is a value object of the Cluster Registry context and the *published
language* every other context translates from: bootstrap servers, typed security (ADR-022),
optional registry/connect/ksql/metrics endpoints with their auth, serde declarations,
masking rules, audit settings, polling limits and admin batching knobs. Secrets travel as
`Secret[A]` (redacted in `toString`, Circe encoders and logs) and, for keystores, as inline
bytes so a distributed service never depends on a shared filesystem; adapters materialize
them into a private tmpfs path when the Kafka client insists on a path.

The config wizard (validate/apply/test-connection, cluster CRUD, related file upload) is a
cluster-service use case with a throwaway client built from the candidate profile; remote
validation of arbitrary URLs is gated by `kui.clusters.remoteValidation.enabled` and an
allow-list (SSRF, §14). Concurrent writers are rejected with `KUI-CONFIG-VERSION-CONFLICT`
(optimistic `version` in the record envelope, §10.1). The `ConfigStore` port has a Kafka
adapter (default) and a file adapter (dev, bootstrap, read-only); no relational database is
introduced (ADR-036, ADR-042).

### 10.1 The metadata store: internal compacted Kafka topics

KUI keeps its own metadata in Kafka (ADR-042). There is no relational database and no shared
filesystem. The topics live on a **store cluster** configured statically under
`kui.store.kafka.*`, because the connection strings of the managed clusters are themselves in
the store and cannot bootstrap it. The store cluster may be one of the managed clusters or a
separate one.

**Bootstrap order** is one-directional and must not be reordered:

```
static config (Ciris: CLI -> env -> YAML -> defaults)
  -> store Kafka client (kui.store.kafka.*)
    -> replay __kui_config to the end of the log
      -> managed clusters known -> admin clients built -> service reports Ready
```

**Topics** (prefix `kui.store.topicPrefix`, default `__kui_`):

| Topic | Shape | Key | Value |
| --- | --- | --- | --- |
| `__kui_config` | compacted, **single partition**, RF `kui.store.replicationFactor` (default 3; 1 in dev) | section path: `cluster/<clusterId>`, `settings/<clusterId>-<principal digest>`, `alerts/<clusterId>`, `rbac/roles`, `masking/<clusterId>` | `StoreRecord` JSON (Circe, ADR-007); principal names never appear in settings keys |
| `__kui_files` | compacted, single partition, same RF | file id | binary payload in the same envelope, capped by `kui.store.maxFileBytes` (default 4 MiB) |
| `__kui_audit` | **not** compacted, retention-based, partitioned by cluster id | cluster id | `AuditRecord` JSON (ADR-023) |

Exact topic configuration, and the `max.message.bytes` implication of `__kui_files`, are in
`docs/operations/metadata-store.md`. KUI creates missing topics at startup and validates
existing ones; an existing topic with an incompatible `cleanup.policy`, partition count or
`max.message.bytes` fails the service at startup with a message naming the topic, the setting,
the expected value and the found value. It never silently rewrites operator topic settings.

**Consistency.** A single partition gives total order. Each owning service replays the log into
memory and then follows the tail, so every replica converges on the same state in the same
order. Writes are produced with `acks=all` and `enable.idempotence=true`, and the writer waits
to read its own record back from the tail before acknowledging the HTTP call, which is what
gives an operator read-your-writes: the `PUT` that returns 200 is already visible to every
replica that has caught up. Each entry carries a `version`; a writer produces only when its base
version matches the state it replayed, and after read-back it checks whether another record for
the same key with the same base version landed first. If one did, the later writer lost the race
and fails with `KUI-CONFIG-VERSION-CONFLICT` (ADR-034). Deletion is a tombstone (null value).
This is correct with several replicas of the same service because the partition, not a lock, is
the serialization point. ADR-036's single-writer-per-section ownership still holds: it keeps two
*contexts* from writing one section, and the version check keeps two *processes* from clobbering
each other.

**Secrets at rest.** Records are readable by anyone with read access to the topic, so every
secret field (SASL passwords, JAAS material, keystore and truststore bytes, OAuth client
secrets) is encrypted with AES-GCM before it is produced, under a key from
`kui.store.encryptionKey` (env or mounted secret, never in the store). The envelope carries the
`keyId` so keys can be rotated by writing new records under a new id while old records stay
readable (`research/scala/security-research.md` §5). Restrictive ACLs on `__kui_*` are an
operator requirement, documented in `docs/operations/metadata-store.md`.

**Who reads the topic.** The **cluster** and **identity** services connect to the store
directly, because they own sections and must write them. Every other Kafka-facing service
(topic, message, consumer, schema, connect, ksql, metrics, alerts) receives the resolved,
redacted `ClusterProfile` over the internal contract instead: they need the profile, not the raw
sections, they must work without store-cluster credentials, and one extra hop is cheaper than one
more store connection and one more holder of the encryption key **per service in that list** —
count them there rather than trusting a number written here, which is how the previous figure came
to be one out. The **gateway never
touches the store** (ADR-040).

**Failure behavior.** When the store cluster is unreachable, the owning service keeps serving
from its last replayed state, reports the affected capability as `Degraded(reason)` into the
fold (ADR-039) so responses carry the degraded envelope (§6), and rejects writes. It never
starts empty on a replay failure and it never falls back to the file adapter silently.

```scala
package kui.config.store

/** One record in the metadata log. `version` is the optimistic-concurrency counter for `key`. */
final case class StoreRecord(
    envelopeVersion: Int,           // format version of this envelope itself, currently 1
    key: StoreKey,                  // e.g. StoreKey("cluster/prod-eu")
    version: Long,                  // 1 for the first record of a key, +1 per successful write
    updatedAt: Instant,
    updatedBy: Option[String],      // principal id, when the write came from the UI
    payload: Json,                  // section body; secret fields replaced by EncryptedField
    encryption: Option[EncryptionInfo]
)

final case class EncryptionInfo(keyId: String, algorithm: String) // algorithm = "AES-GCM-256"
final case class EncryptedField(keyId: String, nonce: Base64, ciphertext: Base64)

/** The port from ADR-036, unchanged in shape. Adapters: Kafka (default), file (dev/read-only). */
trait ConfigStore[F[_]]:
  def get(key: StoreKey): F[Option[StoreRecord]]
  def list(prefix: String): F[List[StoreRecord]]
  /** Fails with KuiError.VersionConflict when `expected` is not the current version. */
  def put(key: StoreKey, expected: Option[Long], payload: Json): F[Either[KuiError, StoreRecord]]
  def delete(key: StoreKey, expected: Long): F[Either[KuiError, Unit]]
  /** Every change after the initial replay, in log order. Never completes. */
  def changes: fs2.Stream[F, StoreRecord]
  def health: F[StoreHealth]

enum StoreHealth { case Ready(lastOffset: Long); case Degraded(reason: String, since: Instant) }

object KafkaConfigStore:
  /** Creates or validates the topics, replays the log, then follows the tail.
    * The Resource does not complete until the initial replay reaches the end offset. */
  def resource[F[_]: Async](
      settings: StoreSettings,     // bootstrap, security, prefix, RF, size caps
      crypto: FieldCrypto[F],      // AES-GCM over the fields a codec marks secret
      clock: Clock[F]
  ): Resource[F, ConfigStore[F]]
```

## 11. All-in-one composition

**Implemented (AIO-001).** `apps/allinone` depends on every service's `application`,
`infrastructure` and `api` modules plus the gateway's. One composition root (ADR-010) builds one
`IO` runtime, one Netty server, one otel4s provider:

- The gateway's per-service client is `InProcessServiceClient`, which builds a transport out of
  Tapir's stub interpreter over the service's own routes *and its own interceptors*, and hands it
  to the very same `SttpServiceClient` the distributed deployment uses. So the gateway code path
  (routing, RBAC pre-check, capability registry, aggregation) is not merely equivalent to the
  distributed one — it is the same objects running the same lines, and only the backend underneath
  differs. `InProcessServiceClientSuite` asserts that by asking both transports the same questions
  and comparing the answers, failures included.
- `PrincipalCodec` is `PrincipalCodec.inProcess` (no signing); `Principal` is passed as a value.
  `kui.gateway.principalKeys` and `kui.gateway.services` are both ignored, each with its own
  warning naming the key and saying what to do instead.
- Services do not open listeners of their own, and their routes are not mounted on the gateway's
  listener either: they are reachable only through the gateway's proxied routes, which is the same
  rule a distributed deployment enforces with a network policy (§14). One consequence is worth
  recording because the alternative design does not have it — the identical `/health/live`,
  `/health/ready` and `/capabilities` paths of every service in §1's diagram never share a router,
  so no prefixing scheme is needed and none was invented.
- Session store, `RbacPolicy` and the capability registry are single in-memory instances. The
  config store and audit sink are the real Kafka adapters pointed at the single dev broker when
  `kui.store.kafka.*` is set, and the file adapter otherwise; all-in-one works either way (§10.1).
- Fault isolation still holds at the code level: a failing use case returns a `KuiError`
  that the capability registry records exactly as it would from a remote 5xx.

### 11.1 What all-in-one does not give you

**It is one failure domain, by construction.** Everything above is about the *code*: a use case
that fails degrades one feature and the rest of the UI keeps working, and a developer can
reproduce that on a laptop in seconds (`FaultIsolationSuite`). None of it is process isolation. One
JVM holds the gateway and every service, so an out-of-memory kill, an unhandled error in the
runtime, or a `docker stop` takes all of it down together.

Anyone who needs a service to survive another service's death runs the distributed deployment
(`deployment/compose/docker-compose.yml`). Nobody should read the shared capability registry and
the graceful degradation in this shape as evidence of more than that.

## 12. Frontend shell and microfrontends

`@kui/api` (the generated contract seam: schema types, `createApiClient`, `ApiError`, `Section`),
`@kui/kernel` (design tokens, primitives, the query cache, the SSE wrappers, kernel signals),
`@kui/shell` (Solid Router route table, layout, `FeatureGate`, capability banner, lazy feature
loader) and one package per feature. It is a pnpm workspace built by Vite, and it is **not built by
Mill**: it ships as its own container image and reaches the gateway over HTTP (ADR-048). Vite splits
at each feature's dynamic `import()`; the shell references a feature only through the `load` thunk in
`packages/shell/src/features/registry.ts`, keyed by `FeatureId` (ADR-012 as amended). Cross-feature
panels (topic page → consumers tab) go through a kernel slot, never a direct import
(`research/kafbat/ui-analysis.md` DC-H6).

Navigation state per feature and cluster (ADR-032):

```
FeatureState = Ready | Degraded(reason) | Unavailable(reason, since) | Forbidden | NotConfigured
```

`NotConfigured` entries are hidden; `Forbidden` entries are shown disabled with a permission
tooltip; `Unavailable` entries are shown dimmed and **clickable**, landing on the feature's
fallback panel (reason, `since`, retry, "what still works"); `Degraded` shows an amber dot.
Stale data stays on screen greyed with its timestamp; actions are disabled. The frontend
runs the same `Rbac.decide` on the pre-expanded permission list from `/api/v1/auth/me`.

**Implemented**, and re-read against the tree on 2026-09-12 — the four `kui.ui.*` names this
paragraph carried were the Scala.js implementation ADR-048 deleted, and `git grep` finds no
occurrence of any of them outside this document. What exists now:
`frontend/packages/shell/src/nav/navigation.ts` decides which entries exist,
`frontend/packages/shell/src/chrome/NavDrawer.tsx` and `NavItem.tsx` apply the rendering
rules, and `frontend/packages/shell/src/features/FeatureGate.tsx` decides between the feature and
its fallback and is the one place that starts a dynamic import. The fourth,
`kui.ui.kernel.component.ActionPermissionWrapper`, has **no successor under that name**: the
permission state it read is `frontend/packages/kernel/src/data/permissions/store.ts`, and no
component in `frontend/packages/` is the one place both reasons are merged. The clause saying the
RBAC half is *"always `true` until M6"* is superseded: M6 closed, sign-in and role-based
authorization are built, and what makes them inert on a default deployment is
`kui.auth.type: disabled` rather than an unwired frontend. See `docs/frontend/README.md` for the
rendering-rule and reason-code tables.

## 13. Observability standard

The observability standard applies unchanged: otel4s (`oteljava` backend, ADR-009) for traces and metrics,
log4cats structured logs over Logback/logstash JSON with MDC bridged from the span context
(ADR-008). Every request/stream/refresh emits `correlation.id`, `user.id` (hashed when
`kui.telemetry.hashUserIds`), `cluster.id`, `service.name`, `operation`. Metric names are
that same standard list plus `kui.stream.events {service, stream, event}`,
`kui.stream.active {service, stream}`, `kui.cursor.rejected {reason}`,
`kui.principal.rejected {reason}`, `kui.config.version {section}`. Tapir interceptors in
`libs/observability` apply them to every server; the sttp client factory in `libs/http` to
every client. Health endpoints are unauthenticated and allow-listed; everything under
`/api/v1` is authenticated unless `kui.auth.type = disabled`.

## 14. Security boundaries

Three concerns, never conflated: application authentication (identity-service,
ADR-015, ADR-019), Kafka cluster authentication (`libs/kafka-auth`, typed per cluster,
ADR-022), authorization (`libs/security-core`, enforced at the gateway and re-checked in every
service from the signed principal, ADR-020, ADR-021).

Boundary rules:

- Browser → gateway: opaque session cookie `kui_session` (`HttpOnly; Secure; SameSite=Lax`),
  id rotated at login, idle 30 min / absolute 12 h; `X-Csrf-Token` double-submit header plus
  `Sec-Fetch-Site` check on every cookie-authenticated mutation; `POST /auth/logout`. Bearer
  tokens for API clients are stateless and exempt from CSRF.
- Gateway → services: `X-Kui-Principal` JWS (HS256, `kid` rotation, 60 s expiry, `aud` per
  service, request digest). A service reachable without the gateway still refuses unsigned
  requests. Services must not be exposed outside the cluster network.
- Read-only clusters and audit are decided from the same `AccessRequest` (`Action.isAlter`),
  so authorization, read-only enforcement and audit never disagree (ADR-021, ADR-023).
- Secrets: `Secret[A]` everywhere; `GET /api/v1/config` returns a redacted view derived
  from the same model; JAAS strings are rendered from typed fields with escaping; nothing
  secret appears in logs, traces, errors or the frontend.
- Metadata store: every secret field is AES-GCM encrypted before it is produced to `__kui_*`
  (§10.1, ADR-042), because a Kafka record is readable by anyone with topic read access. The
  encryption key comes from `kui.store.encryptionKey` and is never written to the store.
  Operators must restrict `__kui_*` to KUI's own principal; guidance is in
  `docs/operations/metadata-store.md`, and the default RBAC policy keeps its deny rule on the
  audit topic (ADR-023).
- Outbound URLs (registry, connect, ksql, Prometheus, JMX, OIDC, LDAP) are config-time
  inputs: strict URL type, `http`/`https` only, deny link-local and metadata ranges by
  default, no cross-host redirects, upstream bodies never echoed into error messages.
- Regex patterns from config (RBAC values, masking, topic patterns) are compiled once at load
  and linted for catastrophic backtracking; user input never becomes a regex except the
  event-tracking `regex` operator, which runs with a match timeout.
- Threat model document, planned for M6 as `docs/security/threat-model.md` — **not written**.
  M6 closed without it and `git ls-files docs/security` is empty; nothing else in this repository is
  that document. It is stated here rather than left as a commitment a reader would assume was
  kept, and the controls above are the security decisions that were actually made.

## 15. Errors

`KuiError` (§4.1) is mapped in each `api` module to one envelope (ADR-034). The exact shape is
the committed sample `libs/contracts-core/test/resources/golden/error-envelope-validation.json`,
which the encoder is asserted against byte for byte on both platforms; the second sample,
`error-envelope-upstream.json`, shows the no-details case. `ErrorEnvelope.of` builds one and
`ErrorEnvelope.statusOf` gives the HTTP status, so the code-to-status mapping exists once.

Timestamps are RFC 3339 in UTC with exactly three fractional digits (`2026-09-03T10:11:12.000Z`)
so that the same instant always serialises the same way; `details` is always present and is `[]`
when empty; and an unrecognised `code` still decodes, so an older browser keeps working against a
newer gateway.

**The single mapping point (M0, task HTTP-001).** `kui.http.ErrorInterceptor` in `libs/http` is
where a failure becomes a response, and it is the only such place. It covers all four paths at
once: a route that matched nothing (`KUI-ROUTE-NOT-FOUND`), an input that would not decode
(`KUI-VALIDATION`, with `details[0].field` naming the input), an exception nobody expected
(`KUI-INTERNAL`, with the stack trace in the log and never in the body), and — through the pure
`ErrorInterceptor.render`, which each service's `api` layer calls — an error the endpoint's own
logic returned. Every one of them takes its status from `ErrorCode.httpStatus`, the same table
`ErrorEnvelope.statusOf` reads, so a second code-to-status mapping cannot appear. Every error
response also carries the correlation id both as the `X-Kui-Correlation-Id` header and in the body,
and the two always agree.

Codes are stable strings, namespaced `KUI-<AREA>-<NAME>`; the frontend renders by code.
Stack traces never leave a service. Kafka client exceptions are translated by
`KafkaErrorMapper` in `libs/kafka` (total over the class list in
`research/kafka/admin-capabilities.md` §1–§5, property-tested); upstream REST errors are
translated by each client's sealed `UpstreamError` (ADR-037).

## 16. Repository layout

This is a compact map of the current tracked tree. To inspect the authoritative top-level roster,
ask Git rather than the working directory:

```bash
git ls-files | cut -d/ -f1 | sort -u
```

```
kui/
├── build.mill  mill  .mill-version  .tool-versions  .gitignore
│             .scalafmt.conf  .scalafix.conf  .scalafix-pure.conf  .scalafix-tests.conf
├── README.md  ARCHITECTURE.md  CAPABILITY-MAP-prometheus-observability.md
│             SPEC-prometheus-query-core.md  LICENSE  repo.txt
├── libs/        kernel/ contracts-core/ kafka/ kafka-auth/ serde/ serde-confluent/ filter/
│                cache/ observability/ security-core/ http/ config/ testkit/
├── services/    gateway/ cluster/ topic/ message/ consumer/ schema/ connect/ ksql/
│                metrics/ alerts/ identity/
│                (each that owns a domain: domain application infrastructure contract api app;
│                 cluster/ adds client/, and gateway/ has no domain and no infrastructure)
├── frontend/    packages/ api/ kernel/ shell/ feature-clusters/ feature-topics/
│                feature-messages/ feature-consumers/ feature-schemas/ feature-alerts/
│                feature-connect/ feature-ksql/ · e2e/ is the Playwright suite
│                (a pnpm/TypeScript/Vite workspace — its own build, its own image)
├── apps/        allinone/
├── build-tests/ the suites that read build.mill, ci.yml and the Dockerfiles themselves
├── deployment/  compose/ quickstart/ demo/ secured/ examples/ frontend/ metrics/
│                storybook/
├── .github/     workflows/ci.yml and actions/setup-build/
├── .agents/     repository-local Playwright skill
├── scripts/     run-tests.sh, check-action-pins.sh
├── screens/     tracked design captures
├── docs/        API, frontend and operations documentation
├── tasks/       current planning and work tracking
├── tools/       build-time generators
└── mill-build/  Mill build support
```

`mill` is the committed launcher script, not a directory. Browser end-to-end tests live under
`frontend/e2e/`; the repository does not ship a top-level `e2e/`, Helm chart, or benchmark tree.
Mill task names follow the project's build-command conventions.

### Naming key

The same thing has a different form in different places; each form has one job.

| Form | Where it is used | Example |
| --- | --- | --- |
| `kui-<name>-service` | prose and code comments about a deployable service | `kui-topic-service` |
| `kui-<name>` | Docker image and Compose service name | `kui-topic` |
| `<name>` | `ServiceId` values, capability keys, metric and log labels, OpenAPI tags | `topic` |
| `services/<name>/<layer>` | directory path | `services/topic/api` |
| `services.<name>.<layer>` | Mill module id (`.` for directory nesting; a module that used to cross-compile keeps a `.jvm` suffix) | `services.topic.api` |
| `libs/<name>` … `libs.<name>` | same rule for libraries; `kui-<name>` appears in prose only | `libs/kernel`, `libs.kernel.jvm`, "kui-kernel" |
| `frontend/packages/<name>` … `@kui/<name>` | frontend packages. Directory and package name agree; there is no Mill module | `frontend/packages/feature-topics`, `@kui/feature-topics` |

The `<name>` form is the identifier of record: a `ServiceId` in code, the `service` label in
every metric and log line, and the key the capability registry and the frontend `FeatureId`
share, so a capability maps to a feature without a lookup table.
