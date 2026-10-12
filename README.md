<div align="center">

# KUI

### Kafka ops, minus the tab chaos.

See the cluster. Find the event. Fix the lag. Keep moving.

[![Apache 2.0](https://img.shields.io/badge/license-Apache--2.0-7c3aed.svg)](LICENSE)
[![Scala 3](https://img.shields.io/badge/backend-Scala%203-dc322f.svg)](https://www.scala-lang.org/)
[![TypeScript](https://img.shields.io/badge/frontend-TypeScript-3178c6.svg)](https://www.typescriptlang.org/)
[![Docker](https://img.shields.io/badge/run_with-Docker-2496ed.svg)](https://www.docker.com/)

KUI is a modern web console for operating Apache Kafka. Explore records, watch brokers and
consumer lag, manage topics and schemas, run Kafka Connect and ksqlDB, and follow live cluster
health without bouncing between five different tools.

[Website & documentation](https://worxbend.github.io/kui/) · [Quick start](#quick-start) · [Features](#everything-in-one-control-room) · [Showcase](#showcase) · [Gallery](#the-captured-product-tour) · [Deployment](#built-to-run-your-way)

</div>

[![A 37-page and state animated tour of KUI](docs/frontend/screenshots/kui-walkthrough.gif)](docs/frontend/screenshots/kui-walkthrough.gif)

> The walkthrough above is a 37-screen capture of the real quickstart stack—not a mockup. It
> covers every navigable unauthenticated quickstart page plus selected record formats, loading
> modes, error pages, and optional-service states.

## Everything in one control room

| Area | What you get |
| --- | --- |
| **Cluster pulse** | Multi-cluster navigation, broker topology, storage, throughput, p99 latency, handler utilization, producer traffic, and record-size views. |
| **Topics without guesswork** | Search and inspect topics, partitions, consumers, and configuration. Create, reconfigure, expand, empty, or delete topics with previewed destructive actions. |
| **Find the event, not the needle** | Stream records from earliest, latest, offset, or timestamp; paginate or infinite-scroll; filter strings and decoded fields; publish and resend messages. |
| **Serde-aware payloads** | Read String, JSON, numeric, UUID, Base64, and Hex values plus Schema Registry-backed Avro, JSON Schema, and Protobuf. |
| **Lag without the spreadsheet** | Inspect consumer state, members, assignments, and per-partition lag; preview and apply offset resets from a guided workflow. |
| **Schema Registry workspace** | Browse subjects and versions, inspect definitions, register schemas, and manage or test compatibility. |
| **Kafka ecosystem** | See connector and task health, pause/resume/restart connectors, explore ksqlDB streams/tables/queries, and execute statements. |
| **Alerts that explain themselves** | Filter the live alert feed, acknowledge events, and see cluster notifications without leaving the current workflow. |
| **Safer operations** | Read-only mode, form or OIDC authentication, RBAC, CSRF protection, audit records, server-side masking, and plan-token confirmation for destructive changes. |

## Why KUI

- **Fails in sections, not as a whole.** Each domain can run independently. When an optional
  service is unavailable, the rest of the console stays useful and the affected screen tells you
  what happened.
- **Streams instead of stockpiling.** Record browsing, live queries, metrics, and alerts flow to
  the browser without buffering entire topics in memory.
- **Keeps risky clicks intentional.** Topic deletion, offset resets, and destructive ksqlDB
  statements use plan → token → confirm flows, with read-only and authorization checks at the
  edge.
- **One contract from server to screen.** Scala 3 services publish OpenAPI contracts that generate
  the TypeScript client used by the SolidJS interface.

## Showcase

| Cluster health without the archaeology | Filter decoded records down to the field |
| --- | --- |
| [![KUI cluster overview with broker health, topic count, uptime, throughput and consumer lag](docs/frontend/screenshots/current/05-cluster-overview.png)](docs/frontend/screenshots/current/05-cluster-overview.png) | [![KUI message browser filtering a decoded JSON field](docs/frontend/screenshots/current/26-filter-json.png)](docs/frontend/screenshots/current/26-filter-json.png) |
| **Your cluster's pulse, at a glance.** | **The event you need, minus the scroll marathon.** |

| Consumer lag, members, and assignments | Schemas and compatibility in the same flow |
| --- | --- |
| [![KUI consumer group detail with state, members, assignments and partition lag](docs/frontend/screenshots/current/18-consumer-group-detail.png)](docs/frontend/screenshots/current/18-consumer-group-detail.png) | [![KUI Schema Registry subjects and compatibility controls](docs/frontend/screenshots/current/22-schemas.png)](docs/frontend/screenshots/current/22-schemas.png) |
| **Debug the group, then reset offsets with a preview.** | **Browse, validate, and register without leaving KUI.** |

| Kafka Connect operations | ksqlDB workspace |
| --- | --- |
| [![KUI Kafka Connect screen showing connector and task state](docs/frontend/screenshots/current/20-connect.png)](docs/frontend/screenshots/current/20-connect.png) | [![KUI ksqlDB workspace showing streams, tables, queries and statement execution](docs/frontend/screenshots/current/21-ksql.png)](docs/frontend/screenshots/current/21-ksql.png) |
| **Connector health and controls, right where you need them.** | **Inspect the topology, run a statement, keep the context.** |

## The captured product tour

These 37 screenshots are captured at 1440×900 from the seeded quickstart stack by
[`frontend/e2e/capture-readme.ts`](frontend/e2e/capture-readme.ts). The capture fails on browser
errors, verifies the frontend and gateway are the same build, and rebuilds the 37-frame walkthrough
GIF automatically.

<details>
<summary><strong>Start, settings, clusters, and dashboards — screens 01–09</strong></summary>

| | |
| --- | --- |
| [![Landing dashboard](docs/frontend/screenshots/current/01-landing.png)](docs/frontend/screenshots/current/01-landing.png) **Landing dashboard** | [![Appearance and interface settings](docs/frontend/screenshots/current/02-settings.png)](docs/frontend/screenshots/current/02-settings.png) **Settings** |
| [![Registered clusters](docs/frontend/screenshots/current/03-clusters.png)](docs/frontend/screenshots/current/03-clusters.png) **Clusters** | [![Cluster configuration overview](docs/frontend/screenshots/current/04-manage-clusters.png)](docs/frontend/screenshots/current/04-manage-clusters.png) **Manage clusters** |
| [![Cluster health overview](docs/frontend/screenshots/current/05-cluster-overview.png)](docs/frontend/screenshots/current/05-cluster-overview.png) **Health overview** | [![Cluster traffic metrics](docs/frontend/screenshots/current/06-cluster-traffic.png)](docs/frontend/screenshots/current/06-cluster-traffic.png) **Traffic** |
| [![Cluster storage and replica health](docs/frontend/screenshots/current/07-cluster-storage.png)](docs/frontend/screenshots/current/07-cluster-storage.png) **Storage** | [![Kafka broker list](docs/frontend/screenshots/current/08-brokers.png)](docs/frontend/screenshots/current/08-brokers.png) **Brokers** |
| [![Kafka broker detail](docs/frontend/screenshots/current/09-broker-detail.png)](docs/frontend/screenshots/current/09-broker-detail.png) **Broker detail** | |

</details>

<details>
<summary><strong>Topics and messages — screens 10–16</strong></summary>

| | |
| --- | --- |
| [![Kafka topic list and statistics](docs/frontend/screenshots/current/10-topics.png)](docs/frontend/screenshots/current/10-topics.png) **Topics** | [![Kafka topic overview](docs/frontend/screenshots/current/11-topic-overview.png)](docs/frontend/screenshots/current/11-topic-overview.png) **Topic overview** |
| [![Topic partition layout](docs/frontend/screenshots/current/12-topic-partitions.png)](docs/frontend/screenshots/current/12-topic-partitions.png) **Partitions** | [![Topic consumer groups](docs/frontend/screenshots/current/13-topic-consumers.png)](docs/frontend/screenshots/current/13-topic-consumers.png) **Topic consumers** |
| [![Topic configuration and administration](docs/frontend/screenshots/current/14-topic-settings.png)](docs/frontend/screenshots/current/14-topic-settings.png) **Topic settings** | [![Kafka message browser](docs/frontend/screenshots/current/15-messages.png)](docs/frontend/screenshots/current/15-messages.png) **Browse messages** |
| [![Cross-topic message tracker](docs/frontend/screenshots/current/16-message-tracker.png)](docs/frontend/screenshots/current/16-message-tracker.png) **Message tracker** | |

</details>

<details>
<summary><strong>Consumers, alerts, Connect, ksqlDB, and schemas — screens 17–23</strong></summary>

| | |
| --- | --- |
| [![Consumer group list and lag](docs/frontend/screenshots/current/17-consumer-groups.png)](docs/frontend/screenshots/current/17-consumer-groups.png) **Consumer groups** | [![Consumer group members assignments and offsets](docs/frontend/screenshots/current/18-consumer-group-detail.png)](docs/frontend/screenshots/current/18-consumer-group-detail.png) **Consumer detail** |
| [![Cluster alerts and acknowledgements](docs/frontend/screenshots/current/19-alerts.png)](docs/frontend/screenshots/current/19-alerts.png) **Alerts** | [![Kafka Connect connectors and tasks](docs/frontend/screenshots/current/20-connect.png)](docs/frontend/screenshots/current/20-connect.png) **Kafka Connect** |
| [![ksqlDB streams tables queries and editor](docs/frontend/screenshots/current/21-ksql.png)](docs/frontend/screenshots/current/21-ksql.png) **ksqlDB** | [![Schema Registry subject list](docs/frontend/screenshots/current/22-schemas.png)](docs/frontend/screenshots/current/22-schemas.png) **Schemas** |
| [![Schema Registry subject versions and compatibility](docs/frontend/screenshots/current/23-schema-subject.png)](docs/frontend/screenshots/current/23-schema-subject.png) **Schema subject** | |

</details>

<details>
<summary><strong>Guardrails and record workflows — screens 24–33</strong></summary>

| | |
| --- | --- |
| [![Authorization forbidden page](docs/frontend/screenshots/current/24-forbidden.png)](docs/frontend/screenshots/current/24-forbidden.png) **Forbidden** | [![Not found recovery page](docs/frontend/screenshots/current/25-not-found.png)](docs/frontend/screenshots/current/25-not-found.png) **Not found** |
| [![Decoded JSON field filtering](docs/frontend/screenshots/current/26-filter-json.png)](docs/frontend/screenshots/current/26-filter-json.png) **JSON filter** | [![String payload filtering](docs/frontend/screenshots/current/27-filter-string.png)](docs/frontend/screenshots/current/27-filter-string.png) **String filter** |
| [![Avro payload filtering](docs/frontend/screenshots/current/28-filter-avro.png)](docs/frontend/screenshots/current/28-filter-avro.png) **Avro** | [![JSON Schema payload filtering](docs/frontend/screenshots/current/29-filter-json-schema.png)](docs/frontend/screenshots/current/29-filter-json-schema.png) **JSON Schema** |
| [![Protobuf payload filtering](docs/frontend/screenshots/current/30-filter-protobuf.png)](docs/frontend/screenshots/current/30-filter-protobuf.png) **Protobuf** | [![Expanded record with copy controls](docs/frontend/screenshots/current/31-copy-controls.png)](docs/frontend/screenshots/current/31-copy-controls.png) **Copy value, key, and headers** |
| [![Offset-based message pagination](docs/frontend/screenshots/current/32-pagination.png)](docs/frontend/screenshots/current/32-pagination.png) **Pagination** | [![Infinite-scroll message loading](docs/frontend/screenshots/current/33-infinite-scroll.png)](docs/frontend/screenshots/current/33-infinite-scroll.png) **Infinite scroll** |

</details>

<details>
<summary><strong>Configured and optional-service states — screens 34–37</strong></summary>

| | |
| --- | --- |
| [![Traffic metrics on the staging cluster](docs/frontend/screenshots/current/34-staging-traffic.png)](docs/frontend/screenshots/current/34-staging-traffic.png) **Configured metrics** | [![Schema Registry not configured state](docs/frontend/screenshots/current/35-staging-schemas-not-configured.png)](docs/frontend/screenshots/current/35-staging-schemas-not-configured.png) **Schemas not configured** |
| [![Kafka Connect not configured state](docs/frontend/screenshots/current/36-staging-connect-not-configured.png)](docs/frontend/screenshots/current/36-staging-connect-not-configured.png) **Connect not configured** | [![ksqlDB not configured state](docs/frontend/screenshots/current/37-staging-ksql-not-configured.png)](docs/frontend/screenshots/current/37-staging-ksql-not-configured.png) **ksqlDB not configured** |

</details>

## Quick start

Docker with the Compose plugin is the only prerequisite.

```bash
git clone https://github.com/worxbend/kui.git
cd kui
deployment/quickstart/quickstart.sh
```

Open **http://localhost:8090/ui/**. The quickstart launches Kafka with seeded topics and records,
Schema Registry, Kafka Connect, ksqlDB, broker metrics, a lagging consumer, and KUI.

Want to see the sign-in and role flows too?

```bash
deployment/quickstart/quickstart.sh --with-auth
```

When you are done:

```bash
deployment/quickstart/quickstart.sh down
```

## Built to run your way

KUI uses the same domain modules in two deployment shapes:

- **All-in-one** — one JVM plus the frontend, ideal for a small footprint and the local quickstart.
- **Fault-isolated** — a gateway and independent domain services, so one unhealthy integration does
  not take the whole console with it and each service can be scaled or restarted separately.

Start with the [minimal](deployment/examples/minimal.yaml),
[three-cluster](deployment/examples/three-clusters.yaml), or
[production-shaped](deployment/examples/production.yaml) configuration examples. Each example
documents its deployment and security settings inline. The
[production deployment guide](docs/operations/deployment.md) covers topology selection, image
builds, secrets, ingress, probes, resource boundaries, upgrades, and rollback; the
[metrics-source guide](docs/operations/configuration.md) covers Prometheus exposition and API
sources, authentication, TLS/mTLS, caching, and limits.

## Current scope and security

KUI is currently pre-1.0. Authentication and authorization are implemented but default to
**disabled**, so do not expose a default deployment to an untrusted network. Configure form or
OIDC authentication, RBAC, TLS, and secrets before using KUI beyond a local environment.

ACL and client-quota management are outside the current product scope. Kafka administration that
is available in KUI is still subject to read-only mode and the permissions of the configured Kafka
principal.

## Product docs

| Guide | Covers |
| --- | --- |
| [Production deployment](docs/operations/deployment.md) | Topologies, image builds, secrets, HTTPS ingress, probes, resources, upgrades, and rollback |
| [Metrics sources](docs/operations/configuration.md) | Prometheus exposition and API sources, authentication, TLS/mTLS, caching, and limits |
| [Message masking](docs/operations/masking.md) | Server-side rules for redacting sensitive record fields |
| [Observability](docs/operations/observability.md) | Logs, metrics, traces, health, and readiness |
| [Error codes](docs/api/error-codes.md) | Generated HTTP status, retryability, area, and meaning for every stable API error code |
| [Architecture](ARCHITECTURE.md) | Runtime boundaries, fault isolation, and module design |
| [OpenAPI](docs/api/openapi.json) | The merged HTTP contract |

## License

KUI is available under the [Apache License 2.0](LICENSE).
