## Use an existing Kafka cluster

This is a two-container baseline for running your own published KUI images against an existing broker. It is a local verification setup, not a complete production ingress or authentication configuration. Start with [publishing your images](../publish-images/).

## Prepare configuration

Create `kui.yaml` beside your Compose file. Replace `kafka.example.net:9092` with broker addresses reachable from the backend container, including every advertised broker address.

```yaml
kui:
  server:
    host: "0.0.0.0"
    port: 8080
  auth:
    type: disabled
  clusters:
    - id: local
      name: "Local Kafka"
      bootstrapServers: "kafka.example.net:9092"
      readOnly: true
```

This baseline uses plaintext Kafka and disabled KUI authentication for local verification. Configure SASL/TLS and [KUI authentication](../security/) for a shared environment. `localhost` inside a container points to that container, not your host or broker.

## Create the Compose file

Save this as `compose.yaml`. The frontend is the only published service. nginx proxies `/api/` to the backend on the private Compose network.

```yaml
services:
  kui:
    image: ${KUI_REGISTRY_NAMESPACE:?set registry}/kui-allinone:${KUI_RELEASE_TAG:?set release}
    command: ["--config", "/etc/kui/kui.yaml"]
    volumes:
      - ./kui.yaml:/etc/kui/kui.yaml:ro
    read_only: true
    tmpfs:
      - /tmp
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-fsS", "http://127.0.0.1:8080/api/v1/health/live"]
      interval: 10s
      timeout: 3s
      retries: 10
      start_period: 30s
  frontend:
    image: ${KUI_REGISTRY_NAMESPACE:?set registry}/kui-frontend:${KUI_RELEASE_TAG:?set release}
    ports:
      - "127.0.0.1:8090:8082"
    environment:
      KUI_GATEWAY_URL: http://kui:8080
      KUI_BUILD_VERSION: ${KUI_RELEASE_TAG}
    depends_on:
      kui:
        condition: service_healthy
    read_only: true
    tmpfs:
      - /tmp
    volumes:
      - type: tmpfs
        target: /etc/nginx/conf.d
        tmpfs:
          mode: 0777
    cap_drop:
      - ALL
    security_opt:
      - no-new-privileges:true
    restart: unless-stopped
```

## Pull, start, and check

```bash
export KUI_REGISTRY_NAMESPACE="docker.io/your-dockerhub-user"
export KUI_RELEASE_TAG="0.1.0-your-reviewed-commit"
docker compose config
docker compose pull
docker compose up -d --wait
curl -fsS http://localhost:8090/healthz
curl -fsS http://localhost:8090/api/v1/health/ready
curl -fsS http://localhost:8090/api/v1/info
```

Open **http://localhost:8090/ui/**. Check that the cluster is reachable, read-only mode is enforced, and the frontend and backend identify the same release. Readiness verifies KUI's runtime; inspect the cluster view separately to confirm broker connectivity.

## Add integrations and secure the edge

Use the [production deployment guide](../deployment/) for the supported upstream configuration, authentication, role grants, secret references, secure cookies, HTTPS, probe requirements, and resource boundaries. Private upstream destinations are subject to KUI's SSRF policy. Do not blindly copy the quickstart's `KUI_ALLOW_PRIVATE_UPSTREAMS=true` relaxation into production.

Run one gateway replica today because browser sessions live in its process memory. Publish only the frontend through your HTTPS reverse proxy; keep the backend and distributed domain services private.

## Stop without discarding configuration

```bash
docker compose down
```

The configuration remains in your directory. This Compose file does not manage Kafka's lifecycle or storage.
