# Changelog

## 0.1.0

First tagged source release. Build from this tag using the deployment instructions;
container images and binary release assets are not published by the tag push.

### Security

- Enforce authentication independently of RBAC, bind OIDC callbacks to their initiating browser, and fail closed when stored credentials cannot be read.
- Restrict cluster discovery and events to caller scope; align frontend and backend permission fallbacks.
- Protect session rotation, login throttling, trusted-proxy handling, and authenticated session capacity.
- Validate the actual connected upstream IP while retaining hostname-based TLS verification. Bound finite and streaming connection pools independently.
- Recheck destructive-operation state and entity identity before execution. Keep quickstart ports local by default.

### Fixed

- Preserve message scan ordering, completeness, partial producer acknowledgements, and resend failure details.
- Reject incomplete consumer-offset reset and purge plans rather than inventing missing offsets.
- Correct ksqlDB destructive-statement parsing and command outcomes, Connect restart compatibility, schema compatibility writes, Protobuf decoding, and CEL computed fields.
- Prevent stale asynchronous UI work from crossing cluster, topic, schema, dialog, or session boundaries; recover expired sessions and interrupted streams.
- Reconcile runtime cluster profiles in alert and metrics services; preserve alert acknowledgement and refresh severity correctly.
- Fence stale cache loads and snapshots and release dynamically owned resources safely.
- Serve assets correctly beneath a base path and support valid cursor request lines through both nginx and the backend.
- Pin the Storybook HTTP server and repair invalid generated component metadata.

### Upgrade notes

- Authentication-enabled installations now require login even when RBAC is absent.
- Production upstream URL policy remains strict. Local demo manifests explicitly opt in to private upstream addresses; review policy before connecting private production services.
- Deploy frontend and backend together: generated contracts include permission fallbacks and richer mutation outcomes.
- Build metadata and default local image tags are now `0.1.0`.

### Verification and known limits

- The integrated fixes passed 4,913 Scala tests and 2,134 frontend tests, coverage gates, lint, formatting, architecture and generated-contract checks, application and Storybook builds, and 11 deployment checks. Coverage thresholds were not lowered.
- Browser accessibility checks covered 39 affected stories in both themes, not the entire application.
- No live Kafka/Connect/Schema Registry/ksqlDB stack or real OIDC provider was exercised for this release. Real HTTP, TLS, SSE, socket, proxy, and Storybook checks were exercised.
- Kafka and ksqlDB do not expose atomic preview-and-delete operations. Fresh state and identity checks reduce risk but cannot eliminate every upstream race.
- JVM DNS, masked connection establishment, and noncooperative response callbacks can delay transport cleanup. See `DEPENDENCY_MATRIX.md` for transport constraints.
- The frontend dependency audit still reports existing advisories: 1 critical, 7 high, 6 moderate, and 3 low. This release does not claim a clean dependency audit; those dependency upgrades are separate work.
