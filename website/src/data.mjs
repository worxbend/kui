export const repo = "https://github.com/worxbend/kui";
export const sections = [
  {
    name: "Start here",
    pages: [
      [
        "introduction",
        "Introduction",
        "A practical overview of KUI and its deployment model.",
      ],
      [
        "quickstart",
        "Run locally with Docker",
        "Start the complete, seeded Kafka playground.",
      ],
      [
        "first-steps",
        "Your first session",
        "Find your way from cluster health to individual records.",
      ],
    ],
  },
  {
    name: "Operate Kafka",
    pages: [
      [
        "clusters",
        "Clusters & brokers",
        "Navigate environments and understand cluster health.",
      ],
      [
        "topics",
        "Manage topics",
        "Inspect partitions, configure retention, and plan changes.",
      ],
      [
        "messages",
        "Browse & publish records",
        "Decode, seek, inspect, and produce messages.",
      ],
      [
        "filtering",
        "Filter records",
        "Use decoded fields, text predicates, and CEL expressions.",
      ],
      [
        "copying",
        "Copy between topics",
        "Replay an offset range with the original bytes and headers.",
      ],
      [
        "consumers",
        "Consumer lag & offset plans",
        "Inspect groups and preview an offset reset before applying it.",
      ],
      [
        "schemas",
        "Schemas & compatibility",
        "Work with Avro, JSON Schema, and Protobuf subjects.",
      ],
      [
        "connect",
        "Kafka Connect",
        "Inspect and operate connectors and their tasks.",
      ],
      ["ksqldb", "ksqlDB", "Explore streams, tables, queries, and statements."],
      [
        "alerts",
        "Alerts & notifications",
        "Filter the feed and acknowledge cluster events.",
      ],
    ],
  },
  {
    name: "Build & deploy",
    pages: [
      [
        "build-images",
        "Build container images",
        "Build the backend and frontend from one reviewed commit.",
      ],
      [
        "publish-images",
        "Publish to a registry",
        "Tag and push to Docker Hub, GHCR, or a private registry.",
      ],
      [
        "run-images",
        "Run your own images",
        "A two-container setup for an existing Kafka cluster.",
      ],
      [
        "deployment",
        "Production deployment",
        "Topology, secrets, ingress, health probes, and rollback.",
      ],
      [
        "security",
        "Authentication & permissions",
        "Form login, OIDC, RBAC, and read-only operation.",
      ],
      [
        "configuration",
        "Metrics configuration",
        "Prometheus sources, authentication, TLS, caching, and limits.",
      ],
      [
        "masking",
        "Message masking",
        "Server-side redaction rules and their boundaries.",
      ],
      [
        "observability",
        "Observability",
        "Logs, metrics, traces, health, and readiness.",
      ],
      [
        "gateway-authentication",
        "Gateway authentication",
        "Proxy trust, client budgets, and credential persistence.",
      ],
      [
        "troubleshooting",
        "Troubleshooting",
        "Resolve startup, connectivity, and optional-service issues.",
      ],
    ],
  },
  {
    name: "Reference",
    pages: [
      [
        "api",
        "API & architecture",
        "OpenAPI contracts, stable errors, and runtime boundaries.",
      ],
    ],
  },
];
export const guides = sections.flatMap((section) =>
  section.pages.map(([slug, title, description]) => ({
    slug,
    title,
    description,
    group: section.name,
  })),
);
export const features = [
  {
    id: "clusters",
    title: "The whole cluster. In focus.",
    short: "Cluster health",
    description:
      "Move between environments without losing context. Read broker health, replica sync, throughput, storage, and consumer lag from one console.",
    image: "05-cluster-overview",
    guide: "clusters",
    details: [
      "Multi-cluster navigation",
      "Broker topology and configuration",
      "Traffic, storage, and replica health",
    ],
  },
  {
    id: "messages",
    title: "Find the event that matters.",
    short: "Record explorer",
    description:
      "Seek to an offset or timestamp. Decode the payload. Filter down to a field. Follow a record across topics, or publish a new one when you need to test a flow.",
    image: "26-filter-json",
    guide: "filtering",
    details: [
      "String, JSON, Avro, JSON Schema, and Protobuf",
      "Field filters and advanced CEL expressions",
      "Pagination, infinite scroll, and message tracking",
    ],
  },
  {
    id: "consumers",
    title: "Turn lag into a plan.",
    short: "Consumer groups",
    description:
      "See members, assignments, and lag per partition. Preview proposed offset changes, review the impact, and apply the exact plan you approved.",
    image: "18-consumer-group-detail",
    guide: "consumers",
    details: [
      "Group state and partition lag",
      "Guided offset reset with plan tokens",
      "Offset and group administration",
    ],
  },
  {
    id: "topics",
    title: "Know what your topics are doing.",
    short: "Topic operations",
    description:
      "Inspect partitions, consumers, and configuration. Create topics, expand partitions, update settings, and preview destructive operations before confirming.",
    image: "10-topics",
    guide: "topics",
    details: [
      "Topic search and inspection",
      "Retention and partition configuration",
      "Guarded empty and delete operations",
    ],
  },
  {
    id: "copying",
    title: "Replay the original. Keep the context.",
    short: "Copy & replay",
    description:
      "Copy ranges between topics without decoding or transforming records. Original keys, values, and headers stay intact. Read the receipt to see what was actually written.",
    image: "15-messages",
    guide: "copying",
    details: [
      "Explicit per-partition offset ranges",
      "Original bytes and headers",
      "Read and written counts in the receipt",
    ],
  },
  {
    id: "schemas",
    title: "Make the contract visible.",
    short: "Schema Registry",
    description:
      "Browse subjects and versions, inspect definitions, register schemas, and test compatibility in the same workspace as your Kafka records.",
    image: "23-schema-subject",
    guide: "schemas",
    details: [
      "Avro, JSON Schema, and Protobuf",
      "Subject versions and definitions",
      "Compatibility checks and registration",
    ],
  },
  {
    id: "connect",
    title: "Keep the pipeline moving.",
    short: "Kafka Connect",
    description:
      "Read connector and task health, inspect configuration, and pause, resume, or restart connectors without switching tools.",
    image: "20-connect",
    guide: "connect",
    details: [
      "Connector and task states",
      "Configuration inspection",
      "Pause, resume, and restart controls",
    ],
  },
  {
    id: "ksqldb",
    title: "Query with the topology in view.",
    short: "ksqlDB",
    description:
      "Explore streams, tables, and persistent queries. Execute statements and follow results with the context of your cluster still in reach.",
    image: "21-ksql",
    guide: "ksqldb",
    details: [
      "Streams, tables, and queries",
      "Statement execution",
      "Plans for destructive statements",
    ],
  },
  {
    id: "alerts",
    title: "See the signal. Take the next step.",
    short: "Alerts & safety",
    description:
      "Follow live alerts, filter the feed, and acknowledge events. Read-only mode, permissions, masking, and audited operations keep the right guardrails close.",
    image: "19-alerts",
    guide: "alerts",
    details: [
      "Live cluster notifications",
      "Filter and acknowledge events",
      "Form/OIDC, RBAC, CSRF, and audit records",
    ],
  },
];
export const gallery = [
  ["01-landing", "Home dashboard", "Workspace"],
  ["02-settings", "Appearance & settings", "Workspace"],
  ["03-clusters", "Registered clusters", "Clusters"],
  ["04-manage-clusters", "Manage clusters", "Clusters"],
  ["05-cluster-overview", "Cluster overview", "Clusters"],
  ["06-cluster-traffic", "Cluster traffic", "Clusters"],
  ["07-cluster-storage", "Cluster storage", "Clusters"],
  ["08-brokers", "Brokers", "Clusters"],
  ["09-broker-detail", "Broker detail", "Clusters"],
  ["10-topics", "Topics", "Topics"],
  ["11-topic-overview", "Topic overview", "Topics"],
  ["12-topic-partitions", "Topic partitions", "Topics"],
  ["13-topic-consumers", "Topic consumers", "Topics"],
  ["14-topic-settings", "Topic settings", "Topics"],
  ["15-messages", "Message browser", "Records"],
  ["16-message-tracker", "Message tracker", "Records"],
  ["17-consumer-groups", "Consumer groups", "Consumers"],
  ["18-consumer-group-detail", "Consumer group detail", "Consumers"],
  ["19-alerts", "Cluster alerts", "Ecosystem"],
  ["20-connect", "Kafka Connect", "Ecosystem"],
  ["21-ksql", "ksqlDB workspace", "Ecosystem"],
  ["22-schemas", "Schema subjects", "Ecosystem"],
  ["23-schema-subject", "Schema versions & compatibility", "Ecosystem"],
  ["24-forbidden", "Permission denied", "States"],
  ["25-not-found", "Not found", "States"],
  ["26-filter-json", "Decoded JSON filter", "Records"],
  ["27-filter-string", "String filter", "Records"],
  ["28-filter-avro", "Avro payload", "Records"],
  ["29-filter-json-schema", "JSON Schema payload", "Records"],
  ["30-filter-protobuf", "Protobuf payload", "Records"],
  ["31-copy-controls", "Copy key, value & headers", "Records"],
  ["32-pagination", "Message pagination", "Records"],
  ["33-infinite-scroll", "Infinite scroll", "Records"],
  ["34-staging-traffic", "Staging traffic", "Clusters"],
  ["35-staging-schemas-not-configured", "Registry not configured", "States"],
  ["36-staging-connect-not-configured", "Connect not configured", "States"],
  ["37-staging-ksql-not-configured", "ksqlDB not configured", "States"],
].map(([image, title, category]) => ({ image, title, category }));
