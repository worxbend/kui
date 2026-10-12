## Prerequisites

Install Git and Docker with the Compose plugin. Start the Docker daemon and check that `docker info` and `docker compose version` work. Allow several minutes for the first build and enough memory for the Kafka ecosystem services.

## Start the playground

Run these commands from a terminal:

```bash
git clone https://github.com/worxbend/kui.git
cd kui
deployment/quickstart/quickstart.sh
```

Open **http://localhost:8090/ui/** when the script reports readiness. The quickstart builds a missing all-in-one image inside Docker and builds the separate frontend. Later starts reuse existing backend images.

You get a local Kafka broker, seeded topics and sample records, a lagging consumer, Schema Registry, Kafka Connect, ksqlDB, metrics, and the KUI console. See [your first session](../first-steps/) for a walkthrough.

## Resolve occupied ports

Keep an existing service running and move the quickstart instead. The backend API defaults to port 8080; the browser defaults to 8090.

```bash
KUI_PORT=18080 KUI_FRONTEND_PORT=18090 \
KUI_QUICKSTART_KAFKA_PORT=19092 \
deployment/quickstart/quickstart.sh
```

Open **http://localhost:18090/ui/** with these overrides. Connect and ksqlDB ports can also be changed with `KUI_QUICKSTART_CONNECT_PORT` and `KUI_QUICKSTART_KSQL_PORT`.

## Try authentication

```bash
deployment/quickstart/quickstart.sh --with-auth
```

The auth override provides demo accounts and roles. Read the accounts in [kui-quickstart-auth.yaml](https://github.com/worxbend/kui/blob/main/deployment/quickstart/kui-quickstart-auth.yaml). These credentials are for the local playground only. Use `--with-auth` before the subcommand when managing that stack.

## Check and stop the stack

```bash
deployment/quickstart/quickstart.sh status
deployment/quickstart/quickstart.sh logs
```

`logs` follows output until you interrupt it. To stop the playground:

```bash
deployment/quickstart/quickstart.sh down
```

> `down` removes the quickstart volumes as well as its containers. Treat its seeded records and any records you add there as disposable.

For the authentication variant, use `deployment/quickstart/quickstart.sh --with-auth down`.

## Rebuild after source changes

The start script reuses existing backend images. To test a new source commit, rebuild explicitly. See [build container images](../build-images/). The quickstart is a development environment; follow the [production guide](../deployment/) for a deployment on a shared network.
