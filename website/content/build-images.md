## Choose one source revision

Build the backend and frontend from the same reviewed commit. The current backend version is **0.1.0**. Record the commit and use an immutable release tag when publishing your images.

## Docker-only backend build

The quickstart Dockerfile compiles the backend inside its build stage, so no host JVM is required:

```bash
docker build \
  --file deployment/quickstart/Dockerfile \
  --tag kui-allinone:0.1.0 \
  .
```

Run from the repository root. The initial build downloads the toolchain and dependencies.

## Mill backend build

With the repository's JVM toolchain available, build the deterministic all-in-one image:

```bash
./mill --no-server deployment.docker.allinone.docker.build
```

For the fault-isolated topology, build all backend images:

```bash
./mill --no-server deployment.docker.__.docker.build
```

The modules include gateway, cluster, topic, message, consumer, schema, metrics, alerts, Connect, ksqlDB, and all-in-one images. The frontend is separate.

## Build the frontend

```bash
docker build \
  --file deployment/frontend/Dockerfile \
  --tag kui-frontend:0.1.0 \
  .
```

The build stage installs the pinned frontend toolchain, typechecks, and builds the browser bundle. The runtime image serves it with unprivileged nginx on port **8082**.

## Verify the release

Inspect the local images and version labels. Run the two halves together and compare `/api/v1/info` with the frontend build shown in Settings. Set `KUI_BUILD_VERSION` at frontend runtime to the same release identifier.

These commands build for the local Docker platform. They do not establish a multi-architecture registry release. Use only a platform you have built and verified.

Continue with [publish to a registry](../publish-images/) and [run your own images](../run-images/).
