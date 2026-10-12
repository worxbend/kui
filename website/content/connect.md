## Configure the integration

Set a Kafka Connect upstream for the cluster, including the appropriate authentication and TLS settings. KUI communicates with the worker's REST API. A missing integration is shown as not configured rather than as an empty connector list.

## Inspect connector and task health

Open **Kafka Connect** to review connectors and tasks. Inspect the connector configuration and the states of its tasks. A running connector with a failed task still needs investigation.

![Kafka Connect operations](/screenshots/20-connect.webp)

## Operate a connector

Use the supported pause, resume, or restart controls for the selected connector. Wait for the state to update and inspect its tasks after an operation. A connector restart does not fix an invalid configuration or an unreachable external system.

## Investigate failures

Read the task error and correlate it with the Connect worker's own logs. Confirm that its broker credentials, plugin, source or sink system, and network access are valid. KUI's own upstream reachability and permissions are separate from the connector's connections.

The [quickstart](../quickstart/) supplies a local Connect worker and seeded connector for exploration.
