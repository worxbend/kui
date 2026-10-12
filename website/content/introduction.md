KUI is an open-source web console for operating Apache Kafka. It brings cluster health, topic administration, record browsing, consumer groups, Schema Registry, Kafka Connect, ksqlDB, and alerts into one workspace.

## Start with the real product

The [Docker quickstart](../quickstart/) starts Kafka, seeded records, a lagging consumer, Schema Registry, Connect, ksqlDB, broker metrics, and KUI. It builds the KUI images locally when they are missing. No separately installed JVM is required for that path.

![KUI cluster overview](/screenshots/05-cluster-overview.webp)

## Choose your next step

| Your goal                       | Read this guide                                                           |
| ------------------------------- | ------------------------------------------------------------------------- |
| Try KUI on your machine         | [Run locally with Docker](../quickstart/)                                 |
| Find a particular event         | [Filter records](../filtering/)                                           |
| Replay records to another topic | [Copy between topics](../copying/)                                        |
| Diagnose lag and reset offsets  | [Consumer lag & offset plans](../consumers/)                              |
| Ship images to your registry    | [Build images](../build-images/) and [publish images](../publish-images/) |
| Operate a secured deployment    | [Production deployment](../deployment/)                                   |

## How KUI runs

The browser interface is a separate nginx container. It serves `/ui/` and proxies same-origin `/api/` requests to the backend. The backend can run as one all-in-one process or as a gateway with independent domain services.

The application uses Scala 3, Cats Effect, and FS2 behind a SolidJS interface. Generated TypeScript clients share the published OpenAPI contracts with the backend.

## Current scope

KUI is pre-1.0. This repository builds images from source; it does not provide an official registry publication workflow. Local examples disable authentication. Configure authentication, authorization, secrets, and HTTPS before exposing KUI beyond a trusted local environment.

ACL and client-quota management are outside the current product scope. Optional integrations appear only when configured. Available writes depend on KUI permissions, cluster read-only mode, and the Kafka principal's own permissions.
