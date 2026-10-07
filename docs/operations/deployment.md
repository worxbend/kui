# Deploying KUI in production

KUI is pre-1.0 and its container images are not currently published to a registry. A production
deployment therefore starts from a reviewed source commit, builds the images, publishes them to
your own registry, and pins the resulting immutable tags or digests in your platform. The Compose
files in this repository are executable development and fault-isolation examples; they contain
plain HTTP, development credentials, and published diagnostic ports and are not production
manifests.

This guide describes the runtime contract those examples prove. It does not prescribe Kubernetes,
Helm, or a particular load balancer because the repository ships none of those integrations.

## Choose a topology

The browser interface is a separate nginx image in both supported shapes. It serves `/ui/` and
proxies same-origin `/api/` requests to the gateway.

| Shape | Backend processes | Choose it when | Tradeoff |
| --- | --- | --- | --- |
| All-in-one | One `kui-allinone` process | A smaller operational footprint matters more than independent service recovery | Every backend domain restarts and scales together |
| Fault-isolated | `kui-gateway` plus the nine domain services in [`deployment/compose/docker-compose.yml`](../../deployment/compose/docker-compose.yml) | Domains must restart or scale independently, or one unhealthy integration must not take down the rest of the console | More images, configuration mounts, probes, and internal service addresses |

The all-in-one shape still needs `kui-frontend`. The distributed shape must keep domain services on
a private network: only the gateway calls them, and only the frontend needs to be reachable from
the user-facing proxy.

Run exactly **one gateway replica** today. Browser sessions live in that process's memory, so a
second replica cannot see sessions issued by the first. A gateway restart also invalidates current
sessions and requires users to sign in again. Domain services can be isolated or replicated by the
deployment platform, provided all replicas use the same configuration and signing keys.

The repository examples show the two shapes:

- [`docker-compose.allinone.yml`](../../deployment/compose/docker-compose.allinone.yml) starts the
  all-in-one backend and frontend.
- [`docker-compose.yml`](../../deployment/compose/docker-compose.yml) starts the gateway, every
  domain service, the frontend, and development fixtures. Use its KUI service graph and health
  checks as a reference, not its broker fixtures, credentials, port exposure, or HTTP settings.
- [`production.yaml`](../../deployment/examples/production.yaml) is an annotated configuration
  catalog. It is not a complete orchestrator manifest and still selects disabled authentication;
  production must replace that posture as described below.

### Local-demo upstream policy

The distributed, all-in-one, multi-cluster demo, secured demo, and quickstart Compose
backends explicitly set `KUI_ALLOW_PRIVATE_UPSTREAMS=true`. Docker service names resolve
to private IPs, and KUI checks the address it actually connects to, not just the URL's
hostname. The all-in-one and secured demos also permit private integrations registered
later. This is a demo-only SSRF-policy relaxation, not a TLS-verification switch; it is
not set on nginx frontends, Kafka, or other fixture containers.

Do not copy this environment setting into production. Unset (or `false`) retains the
strict policy, including for DNS names resolving to private, loopback, or link-local
addresses. Production private upstreams require a deliberate security review and network
egress controls; the demo switch broadly permits non-public destinations, not just Docker.

In the distributed demo, standalone alerts and metrics mount `compose/kui-service.yaml`,
whose `kui.clusterProfiles.url` points to `http://kui-cluster:8080`. Keep that authoritative
profile source available: local bootstrap entries are not a replacement for live profile
updates. Other standalone deployments must provide `kui.clusterProfiles.url` (or the
fallback `kui.gateway.services.cluster.url`) in the file actually mounted by each process.

Check all five topologies and the alerts/metrics mounts without starting containers or
loading a local `.env` file:

```bash
python3 scripts/test-deployment.py DemoTopologyPolicy QuickstartBindings ToolPins
```

## Build one release

Backend images are deterministic Mill builds tagged `0.1.0-SNAPSHOT` in the current source tree.
Build only the all-in-one backend with:

```bash
./mill --no-server deployment.docker.allinone.docker.build
```

Build every backend image for the distributed shape with:

```bash
./mill --no-server deployment.docker.__.docker.build
```

Build the frontend from the same clean commit:

```bash
docker build \
  --file deployment/frontend/Dockerfile \
  --tag kui-frontend:0.1.0-SNAPSHOT \
  .
```

Retag these local images for your registry and deploy them by immutable digest or by a tag your
registry does not allow to move. Backend images include OCI version and Git revision labels, and
`GET /api/v1/info` reports the running build. Set the frontend's `KUI_BUILD_VERSION` to the same
release identifier so the UI and API identify one release during incident diagnosis.

Do not copy the `ghcr.io/kui/...:latest` command from an annotated YAML example: the current
repository explicitly builds KUI images locally and has no registry publication workflow.

## Prepare configuration and secrets

Mount configuration read-only and pass it with `--config /etc/kui/kui.yaml`. Configuration
precedence is command-line flags, environment variables, files, then built-in defaults. For
example, `--kui.server.port=9090` overrides `KUI_SERVER_PORT`, which overrides the YAML value.
Unknown keys and invalid combinations fail startup.

Use the examples as starting points, then remove settings that exist only for local development:

- [`minimal.yaml`](../../deployment/examples/minimal.yaml) shows the smallest all-in-one cluster
  configuration.
- [`three-clusters.yaml`](../../deployment/examples/three-clusters.yaml) shows multiple clusters,
  shared cursor signing, SCRAM, and a private CA.
- [`production.yaml`](../../deployment/examples/production.yaml) shows gateway service discovery,
  signing-key rotation, Kafka-backed metadata, and telemetry settings.

Secret-typed fields accept `env:NAME` and `file:/absolute/path` references. Prefer a mounted secret
file where the platform supports one. KUI resolves references at startup and reports a missing,
empty, or unreadable reference without printing its value. Keep at least these values outside the
configuration file:

- Kafka, Schema Registry, Connect, ksqlDB, metrics, OIDC, and metadata-store credentials;
- form-login password hashes;
- every `kui.gateway.principalKeys[].key` in a distributed deployment;
- `kui.streaming.cursorKey` when tokens may be handled by more than one replica or must survive a
  restart;
- `kui.store.encryptionKey`, which must be backed up separately because stored secrets cannot be
  recovered without it.

Generate independent signing keys rather than reusing one secret for several purposes:

```bash
openssl rand -base64 48  # gateway principal signing key
openssl rand -base64 48  # cursor and plan-token signing key
openssl rand -base64 32  # metadata-store encryption key
```

Every gateway and domain-service process in one distributed deployment must receive the same
`kui.gateway.principalKeys` list. To rotate it without breaking internal calls, add a new key to
every process first, give it a future `notBefore`, wait until all processes accept both keys, then
remove the old key only after the new key is active.

Keep the security policy consistent across process-specific configuration files as well. The
gateway performs an RBAC pre-check, and each domain service enforces the signed principal again;
deploying different `kui.rbac`, cluster `readOnly`, or principal-key settings on the two sides can
make the UI offer an action that the service correctly refuses. Treat those sections as one
deployment-owned configuration even when the platform mounts them into separate containers.

### Authentication and authorization

`kui.auth.type` supports `form` and `oidc`; `disabled` is the default and is unsafe on an untrusted
network. For form login, generate each encoded password hash through standard input so the password
does not enter shell history:

```bash
./mill services.identity.app.runMain kui.identity.app.HashPassword
```

Store the result through a reference such as `passwordHash: "file:/run/secrets/admin-password-hash"`.
The executable form-login and RBAC structure is in
[`kui-quickstart-auth.yaml`](../../deployment/quickstart/kui-quickstart-auth.yaml); its accounts and
literal hashes are demo credentials and must not be reused.

For OIDC, configure `issuer`, `clientId`, `clientSecret`, and `redirectUri` under `kui.auth.oidc`.
Register the callback as:

```text
<public-origin><base-path>/api/v1/auth/oidc/callback
```

An empty `kui.rbac` disables authorization and permits all actions. Define explicit roles and
permissions for production, test each role through the real login flow, and set `readOnly: true` on
clusters where KUI must never mutate Kafka. RBAC and read-only mode are separate controls: use both
when the deployment requires defense in depth.

Leave `kui.server.devInsecureCookies` unset or `false`. It exists only for plain-HTTP local stacks;
the default secure cookie requires HTTPS at the user-facing edge.

### Kafka and upstream TLS

Use `SASL_SSL` for broker authentication over TLS and leave `security.ssl.verifyHostname: true`.
Mount a JKS or PKCS12 truststore only when the broker uses a private CA, and supply its password by
secret reference. The secured cluster in
[`three-clusters.yaml`](../../deployment/examples/three-clusters.yaml) is the repository's worked
configuration shape.

Treat Schema Registry, Connect, ksqlDB, and external metrics endpoints the same way: use HTTPS,
configure their supported authentication mode, and mount private trust material read-only. The
[metrics-source guide](configuration.md) documents the metrics-specific TLS, mTLS, authentication,
timeout, cache, and response bounds.

## Put KUI behind HTTPS

Terminate public TLS at your ingress or reverse proxy and route users to `kui-frontend:8082`. Do
not publish domain-service ports. In a production network the gateway should be reachable by the
frontend and the domain services should be reachable by the gateway, without either group being
directly reachable from users.

The frontend container needs:

- `KUI_GATEWAY_URL`, an internal URL such as `http://kui-gateway:8080`;
- `KUI_BASE_PATH`, empty for a root deployment or a prefix such as `/kui`;
- `KUI_BUILD_VERSION`, the release identifier built and deployed with the backend.

When mounting under a prefix, set the gateway's `kui.server.basePath` to the same value as
`KUI_BASE_PATH`. Do not put a trailing slash in `KUI_BASE_PATH`. The resulting public paths are
`<base-path>/ui/` and `<base-path>/api/v1/...`.

Record browsing, live alerts, and push queries use server-sent events. Any proxy in front of the
frontend must preserve streaming responses: disable response buffering and caching for the API
path and set a read timeout long enough for live streams. The shipped frontend nginx configuration
already does this for its hop to the gateway.

Keep the HTTP request-line limit at least 16 KiB on every outer proxy. KUI permits an 8 KiB
browse cursor, and its URL also carries a route, deployment prefix, and other query parameters.
The shipped nginx and backend use a 16 KiB limit; an 8 KiB proxy default rejects otherwise valid
continuations with HTTP 414.

The quickstart and its frontend overlay publish only on `127.0.0.1` by default. For an explicitly
network-accessible disposable demo, set `KUI_QUICKSTART_BIND_ADDRESS` to the desired host interface
(or `0.0.0.0` for all IPv4 interfaces). This exposes unauthenticated demo services; do not use it
as a production configuration. Kafka's advertised external address must also be reachable by
remote clients if they connect directly to the demo broker.

Preserve KUI's response security headers, forward the public host and scheme, and keep CORS disabled
for the normal same-origin deployment. If another origin must call the API, list exact origins under
`kui.gateway.cors.origins`; wildcard origins are rejected.

## Probes and rollout gates

Probe each layer for the question it can answer:

| Target | Liveness | Readiness or serving check |
| --- | --- | --- |
| Frontend nginx | `GET /healthz` | Fetch `<base-path>/ui/index.html` and the proxied gateway readiness endpoint |
| Gateway or all-in-one, through the frontend | `GET <base-path>/api/v1/health/live` | `GET <base-path>/api/v1/health/ready` |
| Internal domain service | `GET /health/live` | `GET /health/ready` |

Liveness never depends on Kafka or another upstream and is the restart signal. Readiness may return
503 when an upstream is unavailable; remove that instance from routing without turning the failure
into a restart loop. `/capabilities` is operational state consumed by the gateway, not a probe.
Full response semantics are in [Observing KUI](observability.md#health-endpoints-what-to-probe-and-what-a-503-means).

Before shifting traffic, verify all of the following:

```bash
curl --fail https://kui.example.com/healthz
curl --fail https://kui.example.com/api/v1/health/ready
curl --fail https://kui.example.com/api/v1/info
```

Add the configured base path to these URLs when KUI is not mounted at `/`. The info response is the
release check: compare its Git revision with the reviewed commit and confirm the frontend reports
the same release identifier.

## Observability and resources

Use JSON logs in production and keep `kui.telemetry.hashUserIds: true` unless policy explicitly
requires identifiable usernames. Send OTLP telemetry to a collector on the private network and,
if enabled, expose `kui.telemetry.prometheusPort` only to the monitoring system. This port reports
KUI process telemetry; it is different from Kafka metrics collected by the metrics service. The
[observability guide](observability.md) lists the emitted logs and metrics and explains failure
behavior.

The backend image runs as numeric UID 1001, writes temporary data under `/tmp`, sizes the Java heap
to 75% of the container memory limit, and exits on out-of-memory. Preserve a writable `/tmp` tmpfs
when using a read-only root filesystem. The frontend example also drops Linux capabilities and
uses writable tmpfs mounts for generated nginx configuration.

The repository does not claim universal CPU or memory requests: cluster count, topic count,
consumer-group count, record browsing, and metrics retention change the workload materially. Set a
real container memory limit so the 75% heap rule has a defined input, leave the remaining 25% for
native memory and process overhead, and size CPU and concurrency from a staging workload that
resembles production. Watch request latency, upstream latency, readiness, circuit state, heap, and
container restarts before raising `maxConcurrent` or Kafka admin parallelism.

## Upgrade and rollback

Treat the frontend, gateway, and domain services as one versioned release because the browser API
client is generated from the same contracts the services implement.

1. Build all images from one clean, reviewed commit and publish immutable references.
2. Back up the deployed configuration and every referenced secret, especially the metadata-store
   encryption key. Do not rotate keys as part of an unrelated version upgrade.
3. Exercise the release with production-shaped configuration and dependencies in staging.
4. For the distributed shape, replace internal services and wait for readiness, then replace the
   single gateway, then the frontend. Keep the mixed-version interval short.
5. Verify proxied readiness and `/api/v1/info`, then exercise sign-in, a read-only workflow, a
   streamed record browse, and one authorized mutation appropriate for the environment.

A gateway replacement ends active sessions because the current session store is in memory; plan
the user-visible restart accordingly. An all-in-one replacement restarts every backend domain at
once.

Rollback means redeploying the previous complete image set **and its previous configuration**, then
repeating the same readiness and build checks. A rollback does not undo Kafka mutations that users
performed while the newer release was active. If a release changes metadata-store content, retain
the encryption keys needed to read every record; never delete an old key merely because the image
rollback succeeded.
