# Quality review

Review date: 2026-10-22 (Europe/Kyiv). Baseline: `main`, `c2f245d1d142e31bdb051b7a0e9df982f1f6fc25`; initially clean. No branch was created. Findings describe that baseline unless explicitly marked otherwise.

## Executive summary

- The frontend dependency audit reports 28 vulnerability entries: 2 critical, 8 high, 14 moderate, and 4 low; the website audit flags markdown-it (two advisories, highest severity high). This is dependency evidence, not proof that every vulnerable operation is reachable in the shipped application.
- OIDC discovery trusts the returned issuer without comparing it with the configured issuer; group-claim decoding throws for documented input shapes. Characterization also found a histogram crash when its selected bucket disappears. These behavior changes remain proposals.
- Final backend verification passes all 4,913 tests; four failed at baseline. Browser integration remains unstable: four final full-run failures differ from the 17 baseline failures, all show session-limit interference, and all pass unchanged in focused reruns. No repository-wide green verdict is claimed.
- Final frontend typechecking, production build, all 2,144 unit tests, coverage, package boundaries, full Storybook/site accessibility checks and distributed recovery pass.
- Three charts now share one tested keyboard handler, backed by ten new characterization cases. Shell search ownership and browse-release bookkeeping remain deferred because their modules failed baseline browser tests. Behavior and dependency changes remain proposals.

## Scope, toolchain, and baseline

The tracked inventory includes 1,232 Scala files, 268 TSX files, 211 TypeScript files, shell/Python tooling, and the JavaScript documentation site. These inventory counts include tests and generated files; they are not production-code counts. The review combines repository-wide searches with deeper inspection of churn hotspots, authentication, outbound HTTP, resource ownership, message retention, masking, mutation confirmation, frontend state, and test infrastructure. It does not establish absence of all defects.

Exclusions: ignored `out/`, `node_modules/`, `frontend/dist/`, `frontend/.tsbuild/`, coverage/Storybook/browser outputs, `website/dist/`, vendored fonts, binary captures, lockfiles, `frontend/packages/api/src/schema.d.ts`, `constants.generated.ts`, and generated `docs/api/{openapi.json,openapi.browser.json,error-codes.md}`. Builds may regenerate ignored outputs; source refactoring must not edit excluded files. No applied migration tree was found. No tracked `AGENTS.md`, CONTRIBUTING file, or `.editorconfig` was found. `ARCHITECTURE.md` and its remaining operations guides were read; many referenced ADR files are absent (Q09).

Backend: Scala 3.9.0, Mill 1.1.8, Cats Effect/fs2, Tapir, MUnit, ScalaCheck, Testcontainers, scoverage. Mill ran with its cached JDK 21.0.10; the interactive Java default is Temurin 25.0.4.1. Frontend: SolidJS 2 prerelease, TypeScript 5.9.3, Vite, Vitest/jsdom, Playwright, axe, Storybook; 11 packages. Node is 24.22.0 and the host pnpm executable reports 12.14.0; the manifest pins pnpm 11.25.0, also used by the frontend image build. Docker 29.9.1 is reachable. Website: Node scripts and markdown-it. No new dependency was installed into repository manifests.

Commands run from the repository root unless the directory column says otherwise. Logs are local diagnostics under `/tmp/kui-quality-*.log`; conclusions and failure identities are retained here so they do not depend on those temporary files.

| Command | Directory | Actual baseline result |
| --- | --- | --- |
| `git checkout main` | root | Passed after the initial read-only Git sandbox was lifted; already on `main`. |
| `./mill __.compile` | root | Passed, 12,235 build tasks; no compiler warning lines. Task count is not a test count. |
| `./scripts/run-tests.sh` | root | Exit 1; 81 JUnit reports, 4,913 cases, 4,909 passed, 4 failed, 0 skipped. Script exits before its summary on failure; counts independently read from current JUnit XML. |
| `./mill __.checkFormat` | root | Initial queued invocation lost its Mill RPC worker; sequential retry passed (495 tasks). |
| `./mill __.fix --check` | root | Passed: 10,674 build tasks, no lint violations reported. |
| `./mill checkArchitecture` | root | Passed: 289 modules, 10 rules, no layering violations. |
| `./scripts/check-coverage.sh` | root | Initial queued invocation lost its Mill RPC worker; sequential retry passed: Cobertura line rate 80%, branch rate 76%, above 77%/72% floors. Two notices recommend raising floors; no unmeasured-module warning. |
| `./mill __.openApiCheck` | root | Passed: 2,546 tasks; committed documents match endpoints. |
| `./mill docs.errorCodes --check` | root | Passed: 31 error codes, document current. |
| `./mill frontend.apiConstants --check` | root | Passed: 31 error codes, generated constants current. |
| `pnpm typecheck` | frontend | Passed: project references and E2E TypeScript. |
| `pnpm test:coverage` | frontend | Passed: 104 files, 2,134 tests; statements 88.73%, branches 81%, functions 87.76%, lines 90.38%. |
| `pnpm build` | frontend | Passed. |
| `pnpm lint:boundaries` | frontend | Passed: 457 files, 11 packages, zero violations. |
| `pnpm bundle-shape` | frontend | Passed: all eight feature packages dynamically imported, none statically reachable from entry. |
| `pnpm test:storybook-docgen` | frontend | Passed: 4 tests. |
| `pnpm build-storybook` | frontend | Passed; one Vite chunk-size warning group. |
| `pnpm a11y` | frontend | First invocation ran before the local server was ready (exit 2); after readiness, all 782 stories passed in both themes, zero violations. Server: `pnpm exec http-server storybook-static -p 6017 -s`. |
| `pnpm audit --audit-level high` / `pnpm audit --json` | frontend | Exit 1; 28 vulnerability entries, with severity counts above. No automatic remediation. |
| `python3 scripts/test-deployment.py` | root | Passed: 11 tests. |
| `./scripts/check-action-pins.sh` | root | Passed: 30 immutable remote references; 10 checkout steps discard credentials. |
| `npm audit --prefix website --json` | root | Exit 1: markdown-it has one high and one low advisory (one affected dependency); no remediation applied. |
| `npm run build --prefix website` | root | Passed: 28 pages, 24 guides, 37 screenshots. |
| `npm run check --prefix website` | root | Passed: 29 HTML files, including 404. |
| `PORT=4371 npm run serve --prefix website` then `npm run test:browser --prefix website` | root | Passed: 24 guide routes; desktop/tablet/mobile; no console/page/network errors or reported accessibility violations. |
| `npm run test:a11y --prefix website` | root | Passed: seven routes in each of two themes, zero violations. |
| `bash -n <file>` on all tracked `.sh`; `node --check <file>` on all tracked `.js`/`.mjs` | root | Passed: 18 shell and 15 JavaScript files. ShellCheck is not installed; these are syntax checks, not equivalent linting. |
| `./mill --no-server deployment.docker.allinone.docker.build` | root | Passed: 4,092 tasks. |
| `docker compose --project-directory deployment/quickstart -f deployment/quickstart/docker-compose.quickstart.yml -f deployment/frontend/docker-compose.frontend.yml build frontend` | root | Passed; pinned pnpm 11.25.0 inside the image. |
| `KUI_PORT=18082 KUI_FRONTEND_PORT=18090 KUI_QUICKSTART_KAFKA_PORT=19092 KUI_QUICKSTART_CONNECT_PORT=18083 KUI_QUICKSTART_KSQL_PORT=18088 KUI_QUICKSTART_REGISTRY_PORT=18084 deployment/quickstart/quickstart.sh up` | root | Passed; both direct and proxied `/api/v1/info` identify baseline commit `c2f245d`. Ports avoid unrelated running applications. |
| `KUI_E2E_UI=http://localhost:18090 KUI_E2E_API=http://localhost:18082 pnpm e2e` | frontend | Exit 1: 128 selected, 86 passed, 17 failed, two skipped, 23 did not run after serial-group failures. |
| `./mill --no-server deployment.docker.__.docker.build` | root | Passed: all eleven backend images, 4,912 tasks, 494 seconds under heavy host I/O pressure. |
| `KUI_PORT=18081 KUI_FRONTEND_PORT=18091 ./deployment/compose/smoke.sh` | root | Setup failed because host port 18081 was occupied; all started backend services were healthy. Cleanup completed before retry. |
| `KUI_PORT=18082 KUI_FRONTEND_PORT=18090 ./deployment/compose/smoke.sh` | root | Passed: all nine routed services answered, measurements were present, stopping cluster isolated its failure, and restarting restored reads while the interface stayed available. |

Coverage limits: Scala instrumentation excludes generated BuildInfo and compiler-oversized Tapir schema initializers by the existing build policy; the reported rate does not measure those. Frontend figures are V8 coverage with the committed include/exclude configuration.

Warnings: browser output contains 18 color-environment warnings; frontend unit output contains 20 jsdom “Not implemented” notices. Backend test output contains 409 structured WARN and 10 structured ERROR log lines, including deliberate upstream/TLS failure fixtures; these are not 419 compiler/analyzer findings. No SonarQube cognitive-complexity result or vulnerability reachability result is claimed.

### Exact backend failing set

| Test | Evidence | Excluded module |
| --- | --- | --- |
| `kui.metrics.infrastructure.prometheus.PrometheusQueryClientSuite.OAuth stale fallback allows only transient transport statuses and rejects contract or auth failures` | `services/metrics/infrastructure/test/src/kui/metrics/infrastructure/prometheus/PrometheusQueryClientSuite.scala:457`: seed request returned `Timeout("Prometheus query", 2000)` instead of success. | `services.metrics.infrastructure` |
| `kui.metrics.app.MetricsWiringSuite.a wiring built by MetricsWiring.make measures a configured cluster end to end` | `services/metrics/app/test/src/kui/metrics/app/MetricsWiringSuite.scala:311`: `rates` was empty; expected `List(124800.5)`. | `services.metrics.app` |
| `kui.http.upstream.TransportCancellationSuite.canceling before response headers closes the active socket, not just the waiting fiber` | `libs/http/test/src/kui/http/upstream/TransportCancellationSuite.scala:17`: five-second timeout. | `libs.http` |
| `kui.allinone.AllInOneWiringSuite.theConfiguredMetricsSectionReachesTheMetricsServiceAndNotJustTheSlice` | `apps/allinone/test/src/kui/allinone/AllInOneWiringSuite.scala:275`: thirty-second test timeout. | `apps.allinone` |

These are observed baseline failures. Contention is a plausible contributor, not an established root cause; the Storybook harness measured load 48.5 on 16 cores. A later passing run must not erase this baseline.

### Exact browser failing set

Locations identify complete test declarations in `frontend/e2e/`; titles below are their actual test names. No test was disabled or weakened. Twelve failures explicitly returned `429 KUI-AUTH-RATE-LIMITED` (new-session limit); this is evidence of a harness/deployment interaction, not proof the limiter is incorrect. Bounded service and seed logs were inspected; schema fixtures completed successfully.

| Location | Test | Observed failure |
| --- | --- | --- |
| `brokers.spec.ts:214` | tags an expanded card with the version the cluster's own summary reports | HTTP 429 |
| `brokers.spec.ts:246` | says a broker's share of the leaderships, or says it was not measured | HTTP 429 |
| `brokers.spec.ts:278` | TOTAL LEADERS is the cluster's own count, or a sentence saying it was not read | HTTP 429 |
| `brokers.spec.ts:371` | follows a cluster change into the second cluster's brokers screen | HTTP 429 |
| `connect.spec.ts:125` | the gateway answers a document shaped like the service's own golden | HTTP 429 |
| `connect.spec.ts:196` | every connector the API named is a card, in the state the API reported | HTTP 429 |
| `connect.spec.ts:277` | each card carries the service's task figures and no throughput at all | HTTP 429 |
| `consumers.spec.ts:67` | the count over the table is the server's, not the number of rows drawn | HTTP 429 |
| `consumers.spec.ts:85` | offers paging, and says which rows of how many are on screen | HTTP 429 |
| `consumers.spec.ts:164` | offers forgetting the offsets on each topic the group holds them on | HTTP 429 |
| `features.spec.ts:59` | says whether a subject's level is its own or the registry's | HTTP 429 |
| `features.spec.ts:92` | counts the registry's subjects and not the rows on this page | HTTP 429 |
| `features.spec.ts:109` | selecting a subject changes the address and keeps the list beside it | Expected locator absent after 15 seconds |
| `messages.spec.ts:206` | paints a useful partial page before committing a 100-record burst | Message browser did not render |
| `shell.spec.ts:127` | a deep link renders the page it names, not the root | Expected locator absent after 15 seconds |
| `shell.spec.ts:151` | message browsing defaults persist and drive a real browse | Response/click timed out after 60 seconds |
| `topics.spec.ts:193` | reads the cluster's read-only flag, and the write controls obey it | Expected “This cluster is configured read-only in KUI”; received “This cluster is read-only.” |

Refactoring exclusions therefore also cover the shell and feature modules for brokers, Connect, consumers, schemas, messages and topics. The kernel chart suites passed; none of these failures identifies chart navigation. Search E2E cases passed, but the shell is still excluded as a module.

### Churn and complexity screening

Churn command: `git log --since=12.months --format= --name-only | sort | uniq -c | sort -rn | head -30`. Deleted historical paths were excluded from candidates. The following screen counts `if`, `case`, `catch`, `for`, `while`, `&&`, and `||` after a simple comment/string removal, then multiplies by commit touches. It is a lexical prioritization heuristic, not cyclomatic or Sonar cognitive complexity; JSX/template interpolation and Scala literals limit its precision. Physical lines include comments.

| Current file | Commit touches | Lines | Decision tokens | Product |
| --- | ---: | ---: | ---: | ---: |
| `libs/config/src/kui/config/KuiConfigSource.scala` | 21 | 3,544 | 294 | 6,174 |
| `frontend/packages/shell/src/App.tsx` | 24 | 1,528 | 63 | 1,512 |
| `frontend/packages/feature-messages/src/session.ts` | 8 | 895 | 107 | 856 |
| `build.mill` | 96 | 4,535 | 4 | 384 |
| `frontend/packages/kernel/src/components/charts/LineChart.tsx` | 2 | 262 | 22 | 44 |
| `frontend/packages/kernel/src/components/charts/Histogram.tsx` | 1 | 301 | 25 | 25 |
| `frontend/packages/kernel/src/components/charts/BarChart.tsx` | 1 | 236 | 14 | 14 |

Raw duplicate-block screening found identical chart navigation, service OpenAPI command drivers, interceptor construction, and frontend test mounting helpers. Generated schemas were excluded from findings. Duplication severity below reflects actual change coupling, not just textual similarity.

## Architecture

```text
Browser -> nginx -> gateway API/auth/routing -> service contracts
Solid shell -> lazy feature packages -> kernel -> generated/handwritten API client
Service app (composition root)
  -> API (Tapir/contracts) -> application (use cases) -> domain (ports/policy)
  -> infrastructure adapters -----------------------> domain ports
Infrastructure -> Kafka/HTTP/config/cache/security/telemetry libraries
Domain -> shared kernel and Cats; contracts -> wire codecs and shared contract types
All-in-one composes the same application/adapters in one JVM.
Website build -> Markdown and operations guides -> independent static site.
```

Entry points are `apps/allinone/.../AllInOne.scala`, each `services/*/app` main, `frontend/packages/shell/src/App.tsx`, and `website/scripts/build.mjs`. The gateway owns edge concerns rather than business entities, so its contract dependencies are intentional. Domain import screening found no HTTP/DB/vendor-framework import; the alert domain's `java.security.MessageDigest` computes a deterministic identifier, not network I/O. The configured architecture gate remains the authoritative module-edge check; its result is recorded above. Q05 concerns ownership inside the shell, not a demonstrated package cycle.

## Findings

Severity uses the requested categories: Critical includes confirmed bugs/security issues; High means a change-blocking hotspot. Test coverage means relevant existing tests, not that they catch the reported gap.

| ID | Severity | Category | Location | Evidence | Principle | Proposed refactoring | Risk | Effort | Test coverage | Source |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Q01 | Critical | Security/dependencies | `frontend/package.json:42`, `frontend/package.json:50`, `website/package.json:16` | Audit reports critical Storybook preview origin validation and Seroval deserialization advisories, plus eight high entries; website markdown-it adds high/low advisories. Development and runtime dependency paths both occur; application exploitability was not established. | Dependency risk management | Approval required: targeted upgrades and lockfile regeneration, not a refactoring. | High: framework compatibility | M | partial | tool:pnpm audit; tool:npm audit |
| Q02 | Critical | Security/OIDC | `services/identity/infrastructure/src/kui/identity/infrastructure/OidcRelyingParty.scala:160`, `:304` | Discovery accepts any returned issuer; token issuer is checked against that returned value, not `config.issuer`. A misdirected discovery document can replace the configured trust identity. No exploitation was attempted. | Validate trust boundaries | Approval required: compare discovery issuer with configured issuer before caching/using it. | Medium: rejects previously accepted providers | S | partial | manual |
| Q03 | Critical | Reliability/OIDC | `services/identity/infrastructure/src/kui/identity/infrastructure/OidcRelyingParty.scala:293`, `:325`, `:332` | Nimbus 10.9.1 probe: string and comma-separated groups throw `ParseException` at `getStringListClaim`; an empty array reaches `getStringClaim` and also throws. Claim checks are outside `catchNonFatal`, so these escape the typed result. | Total boundary decoding; exception handling | Approval required: decode claim shapes explicitly and map malformed claims to a typed refusal. | Medium: changes failed-login behavior | S | partial | manual; tool:java |
| Q04 | High | Test reliability | `services/metrics/infrastructure/test/src/kui/metrics/infrastructure/prometheus/PrometheusQueryClientSuite.scala:457`; remaining locations in baseline table | Four backend tests fail; metrics observation and socket cancellation currently lack a reproducibly green baseline. Timeout/load correlation does not establish correctness. | Feathers: preserve the safety net | Investigate existing failures separately; use synchronization/controlled time where justified. No changes to failing modules. | High | M | yes | tool:MUnit |
| Q05 | Medium | Long Function / ownership | `frontend/packages/shell/src/App.tsx:324` | Debounce timer, request cancellation, search generation, state and cleanup occupy roughly 80 lines inside the application composition function. Search changes share the 24-touch shell hotspot and require reading unrelated startup/overview orchestration. | Fowler Extract Function; SRP | Extract the cohesive search controller into a shell-internal module; keep rendering and health reporting at the call site. | Medium: cleanup and stale responses | S | yes | manual |
| Q06 | Medium | Duplicated Code | `frontend/packages/feature-messages/src/session.ts:606`, `:615` | Live and bounded release branches duplicate six reference-count/state-write statements. Each ownership bookkeeping change must remain identical while list ordering differs. | Fowler Slide Statements / Consolidate Duplicate Conditional Fragments | Compute each mode's next arrays, then perform the shared release/retain/update sequence once. | Low: preserve ordering and byte ownership | S | yes | manual |
| Q07 | Medium | Duplicated Code | `frontend/packages/kernel/src/components/charts/BarChart.tsx:86`, `Histogram.tsx:153`, `LineChart.tsx:97` | Three identical keyboard move/dispatch blocks implement Arrow/Home/End/Escape. An interaction correction must be applied three times. Existing tests only partially cover boundaries/default prevention. | Fowler Extract Function; Beck no duplication | Characterize each chart's navigation, then extract one kernel-internal keyboard handler. | Low: empty-data behavior must remain | S | partial | manual |
| Q08 | Low | Duplicated tooling | `services/alerts/api/src/kui/alerts/api/OpenApiDocument.scala:29`, `services/topic/api/src/kui/topic/api/OpenApiDocument.scala:29` | The same file-write/check/exit driver is repeated across service generators. Diagnostic changes require multiple edits. It is short, deliberate, and guarded by generated-document checks. | Fowler Extract Function | Consider a shared build-tool driver in a separately reviewed change; retain exact exit codes and messages. | Medium: generator command compatibility | M | partial | manual |
| Q09 | Low | Documentation reliability | `ARCHITECTURE.md:3`, `:72`, `:108` | Claims ADRs live in missing `docs/adr/`/`DECISIONS.md`, names a removed Scala E2E suite and a security service absent from the shipped catalog. Readers can mistake historical design for delivered functionality. | Accurate documentation | Update scope/status and obsolete links against current modules and CI. | Low | S | no | manual |
| Q10 | High | Integration-test reliability | `frontend/e2e/fixtures.ts:26`, `frontend/e2e/messages.spec.ts:206`, `frontend/e2e/topics.spec.ts:284`; full set above | 17 baseline browser failures; authentication fixture reads a body without checking sign-in status, and 12 requests later fail with the session limiter. Serial groups leave 23 cases unexecuted. Other failures include missing screens and a text-contract mismatch. | Feathers: trustworthy safety net | Investigate fixture lifecycle/rate limits and individual UI contracts separately; do not weaken assertions or change product behavior in a refactoring. | High | M | yes | tool:Playwright; manual |
| Q11 | Critical | Reliability / invalid cursor | `frontend/packages/kernel/src/components/charts/Histogram.tsx:240`, `:245` (after extraction; baseline `:262`, `:267`) | Discovered during characterization, before production edits: select the last bucket and shrink to fewer nonempty buckets. Tooltip effects cast a missing bucket and read `to`/`count`, throwing and halting unguarded reactive updates. | Validate collection-index lifetimes | Approval required: choose clear/clamp behavior when data removes the selected bucket, and guard tooltip reads. Current failure is preserved in a characterization test. | Medium: changes selection behavior | S | partial | tool:Vitest; manual |

Q02's required issuer binding is specified by [OpenID Connect Discovery 1.0, section 4.3](https://openid.net/specs/openid-connect-discovery-1_0.html#ProviderConfigurationValidation). Q03 was reproduced with the repository's cached Nimbus JAR using a temporary Java program that calls the same two getters; this proves the getter behavior, not an end-to-end login test. Existing `OidcRelyingPartySuite` has 16 cases, with a nonempty list as the normal groups fixture.

Q01's critical advisory identifiers are [GHSA-p6vx-979v-rg4c](https://github.com/advisories/GHSA-p6vx-979v-rg4c) and [GHSA-f2ph-cmg5-wxc8](https://github.com/advisories/GHSA-f2ph-cmg5-wxc8). Audit reports fixes at Seroval 1.6.1+ and Storybook 10.6.2+ respectively; compatibility and reachability still require assessment. High frontend findings involve undici, brace-expansion, Seroval, source-map-js and Vite. Website markdown-it 15.0.2 is affected by [GHSA-gfr2-rv44-r23c](https://github.com/advisories/GHSA-gfr2-rv44-r23c) and [GHSA-vfgh-jcmw-g37q](https://github.com/advisories/GHSA-vfgh-jcmw-g37q); npm offers 15.0.3. It processes repository Markdown during the build, not visitor input at runtime. Do not equate development-tool advisories with a remotely exploitable production server.

## Leave as-is

- Scala sealed ADT matching, immutable records and explicit `Either` errors express domain alternatives; replacing them with mutable class hierarchies would add complexity.
- `KuiConfigSource` is large and heavily changed, but its section decoders already separate rules and accumulate errors intentionally. File size alone does not justify changing precedence, secret resolution, or introducing a generalized schema DSL. Review section-specific edits with existing precedence/configuration suites.
- `build.mill` is mostly declarative module wiring. Its high churn and length warrant attention, not a framework/build migration in this review.
- Effect `Resource`/fs2 lifetimes, scoped Kafka consumers, bounded caches/queues, socket-address validation in `HttpTls`, constant-time token MAC comparisons, and masking before message delivery are intentional controls. No replacement is justified by surface syntax alone.
- Java interop casts/null values have narrow, documented Scalafix exceptions. Do not expand suppressions or remove boundary assertions for cosmetic conformity.
- Plan-token codecs differ in signed payload and domain semantics; deduplication must not change their existing wire formats. Service interceptor scaffolding and per-package test fixtures also preserve boundaries; a shared abstraction needs an explicit owner.
- Browser layout/accessibility checks complement jsdom; the configured timeout headroom is documented. Do not weaken assertions or skip failing cases to obtain a green run.

## Open questions

- Which approved dependency upgrade window should address Q01, including Solid prerelease compatibility and development-tool advisories?
- Should discovery reject every issuer mismatch, including trailing-slash aliases, as the protocol requires (Q02)? Which real providers and group-claim shapes must Q03 support?
- Are the four backend failures repeatable with isolated resources? Any root-cause fix needs its own behavior-focused change and regression evidence.
- How should browser fixtures respect the new-session limit while retaining independent principals? Which tooltip wording is the intended contract, and what causes the missing browser screens (Q10)?
- Should a histogram clear or clamp selection when updated data removes its selected bucket (Q11)?
- Should the historical architecture document be revised to the release scope, or clearly separated into current versus planned architecture?

The build and required local tools/services are available. Baseline test failures invoke the module exclusions above, rather than the build/toolchain stop condition. Before the review commit, `./mill __.compile` and frontend `pnpm typecheck`, `pnpm build`, `pnpm test` passed again (104 files, 2,134 tests, 20 existing jsdom notices).

## Phase 2 — Refactoring plan

Priority is severity, then hotspot evidence, then effort, subject to eligibility. Q01–Q03 require behavior/dependency changes; Q04/Q10 identify failing safety nets. The higher-churn medium findings Q05/Q06 are excluded by those failures. Q07 is consequently the only safely addressable medium finding in this run. Q08/Q09 remain untouched because their files are outside the eligible batch.

### B1 — Share chart keyboard navigation (Q07)

- Goal: maintain one implementation of the three byte-identical, 25-line keyboard blocks without changing visible or accessible behavior.
- Scope: five files maximum under `frontend/packages/kernel/src/components/charts/`: `keyboard.test.tsx`, `keyboard.ts`, `BarChart.tsx`, `Histogram.tsx`, `LineChart.tsx`. No public barrel/manifest changes.
- Safety net first: add component-level characterization for both arrows and their bounds, Home/End, Escape, ignored keys, browser default prevention, changing data length, and empty data. Keep existing announcements, even where empty-data behavior differs between charts. Existing chart render/accessibility tests remain intact.
- Named refactoring: **Extract Function**. Move the existing navigation closure to a kernel-internal factory receiving the current count/cursor/setter; replace each duplicate with a call. Preserve branch order, lazy reads, default prevention, and the empty-data index values.
- Planned commits: `test(charts): characterize keyboard navigation`; then `refactor(charts): extract keyboard handler to remove duplication`. Each is independently verified and below 200 changed lines. Documentation stays in separate commits.
- Before every commit: root `./mill __.compile`; frontend `pnpm typecheck`, `pnpm build`, `pnpm test`. These follow CI's independent backend/frontend build boundary; the entire frontend suite runs for each step. Backend sources remain unchanged and the full backend suite runs at baseline and final verification.
- At batch end: frontend `pnpm lint:boundaries`, `pnpm bundle-shape`; root `./mill __.checkFormat`, `./mill __.fix --check`, `./mill checkArchitecture`. Final verification repeats the baseline checks, including coverage, Storybook accessibility, current-source images, browser integration and distributed smoke. No new failures are accepted relative to the recorded baseline; differences are investigated, not silently relabeled.
- On a failing refactoring step: at most two fix-forward attempts, otherwise restore only that uncommitted step and document the failure. Never weaken existing tests or lint rules.

### Deferred refactoring batches

Q05 would extract a shell-internal search controller after tests for pending debounce and in-flight cancellation on disposal. Q06 would compute live/bounded row arrays separately and consolidate the six shared release/retain/state assignments, backed by the existing pause/budget/ordering tests. Neither batch runs while its module has baseline browser failures. No API or wire-format change is needed for either eventual refactoring.

### Proposals requiring approval

| Finding | Separate proposed change | Failing check or regression test that would prove the need |
| --- | --- | --- |
| Q01 | Assess reachability, upgrade affected dependencies to reviewed compatible releases, and regenerate excluded lockfiles. No upgrades here. | Existing `pnpm audit --audit-level high` fails; website `npm audit --prefix website --json` reports the markdown-it advisories. Require clean applicable audit gates plus compatibility/build/browser tests after upgrades. |
| Q02 | Bind discovery issuer exactly to configured issuer before accepting provider metadata. | Add an OIDC fixture with discovery issuer B for configured issuer A and an otherwise valid token from B; expect typed refusal. Current discovery accepts B. Cover trailing-slash mismatch explicitly. |
| Q03 | Decode supported groups shapes explicitly; convert malformed claims into typed refusal instead of an effect exception. | Add cases for scalar groups, comma-separated groups, empty arrays, and malformed values. Current Nimbus getter sequence throws on the first three; assert their intended group sets and typed refusal for malformed input. |
| Q04 | Diagnose metrics/cancellation failures; change synchronization or product behavior only with established cause. | The four exact backend failures above are the reproductions. Require repeatability under controlled load before choosing a repair; do not increase timeouts merely to conceal failure. |
| Q10 | Establish fixture session lifecycle/rate-limit policy and intended UI contracts; repair the harness or product in separate changes. | The 17 browser failures above reproduce current gaps. Add an explicit failed-sign-in fixture test that refuses non-2xx before protected reads; retain the real-session limiter test. The tooltip assertion must follow an explicitly chosen wording contract. |
| Q11 | Choose whether shrinking data clears or clamps selection, then make histogram rendering total for a removed bucket. | Select the last of two buckets, replace data with one bucket, and assert the chart remains rendered with no error-boundary fallback. That desired-behavior assertion currently fails; `keyboard.test.tsx` instead pins the current failure until a separate fix is approved. |

No new runtime/test dependency, schema change, API change, migration, CI edit, or excluded-path edit is planned. Questions and proposals are recorded for later decisions; none blocks the eligible behavior-preserving batch.

## Phase 4 — Refactoring results

### Delivered batch

B1 completed in `8ed9f626` (characterization) and `f63e4e0f` (Extract Function). Ten new component-level cases pin all keyboard branches, bounds, default prevention, dynamic data, empty announcements, and the pre-existing histogram failure. Three identical blocks now call one internal 30-line helper; package exports and behavior are unchanged. No production fix-forward was needed.

During test authoring, the first dynamic-shrink expectation exposed Q11, before any production edit. The final test uses a test-only error boundary and accepts either tooltip field as the first failed read; both effects dereference the removed bucket. The initial boundary message expected `to`, but the boundary reported `count`; typechecking also required converting its `unknown` error safely. These were corrections to new characterization tests, not changes to existing tests or production behavior. An initial focused command from the repository root could not locate Vitest; the command ran successfully from `frontend`.

| Commit | Purpose | Verification before commit |
| --- | --- | --- |
| `2257ab75` | Review only | `./mill __.compile`; frontend `pnpm typecheck`, `pnpm build`, `pnpm test`: passed, 104 files / 2,134 tests. Full baseline failures retained above. |
| `f2b51afc` | Plan only | Same four commands passed; 104 files / 2,134 tests. |
| `8ed9f626` | Chart characterization only | Same four commands passed; 105 files / 2,144 tests. Focused `pnpm exec vitest run packages/kernel/src/components/charts/keyboard.test.tsx` passed 10 cases. |
| `f63e4e0f` | Shared keyboard handler only | Same four commands passed; 105 files / 2,144 tests. `pnpm lint:boundaries` passed (459 files / 11 packages); `pnpm bundle-shape` passed (eight lazy features); `./mill __.checkFormat`, `./mill __.fix --check`, `./mill checkArchitecture` passed (289 modules / 10 rules). |
| `docs: add refactoring results` | Results documentation only | `./mill __.compile`; frontend `pnpm typecheck`, `pnpm build`, `pnpm test`: passed again before commit; 105 files / 2,144 tests. |

Each implementation commit changes at most four files; the complete batch touches five. The test commit adds 150 lines; the refactoring changes 108 lines (36 additions / 72 deletions). Documentation is separate. No existing test was removed, skipped or weakened; no lint suppression or dependency was added.

### Before / after maintenance measurements

The same lexical screen described above is used here; these are not Sonar scores. Navigation duplication is measured by direct comparison of the exact source blocks.

| Production file | Physical lines before → after | Decision tokens before → after |
| --- | --- | --- |
| `BarChart.tsx` | 236 → 214 | 14 → 8 |
| `Histogram.tsx` | 301 → 279 | 25 → 19 |
| `LineChart.tsx` | 262 → 240 | 22 → 16 |
| New `keyboard.ts` | 0 → 30 | 0 → 6 |
| Total | 799 → 763 | 61 → 49 |

The three byte-identical 25-line navigation blocks became one implementation. This removes two copies and 36 net production lines; it does not reduce the number of supported navigation cases.

### Full final verification

Final commands use the same directories and configuration as the baseline. The successful final backend build used `./mill --no-server __.compile` after the cache recovery described below. Commands grouped in a row were each run; a passing build is not being substituted for tests.

| Check / exact command | Before | After |
| --- | --- | --- |
| `./mill __.compile` (final: add `--no-server`) | Pass | Pass: 12,235 tasks; no compiler warnings |
| `./scripts/run-tests.sh` | 4,909 passed / 4 failed / 0 skipped | 4,913 passed / 0 failed / 0 skipped; 81 current JUnit reports independently counted |
| `./scripts/check-coverage.sh` | Lines 80%, branches 76% | Lines 80%, branches 76%; same two floor-raise notices |
| `./mill __.checkFormat`; `./mill __.fix --check`; `./mill checkArchitecture` | Pass; zero reported violations | Pass; zero reported violations, 289 modules / 10 architecture rules |
| `./mill __.openApiCheck`; `./mill docs.errorCodes --check`; `./mill frontend.apiConstants --check` | Pass | Pass; 31 error codes |
| `pnpm typecheck`; `pnpm build` | Pass | Pass |
| `pnpm test:coverage` | 104 files / 2,134 passed | 105 files / 2,144 passed; no failures |
| Frontend statements / branches / functions / lines | 88.73 / 81.00 / 87.76 / 90.38% | 88.89 / 81.37 / 87.78 / 90.55% |
| `pnpm lint:boundaries`; `pnpm bundle-shape` | 457 files / 11 packages; eight lazy features | 459 files / 11 packages; eight lazy features; zero violations |
| `pnpm test:storybook-docgen`; `pnpm build-storybook`; `pnpm a11y` | Four docgen tests; 782 stories × two themes passed | Same counts, all passed; zero accessibility violations |
| `python3 scripts/test-deployment.py`; `./scripts/check-action-pins.sh` | 11 tests; 30 action references / 10 credential-discarding checkouts passed | Same counts, all passed |
| `npm run build --prefix website`; `npm run check --prefix website` | 28 pages / 29 checked HTML files | Same counts, passed |
| `npm run test:browser --prefix website`; `npm run test:a11y --prefix website` | 24 guide routes / seven routes × two themes passed | Same counts, no reported browser errors or accessibility violations |
| `bash -n <file>`; `node --check <file>` over tracked scripts | 18 shell / 15 JavaScript files passed | Same counts, passed |
| `pnpm audit --audit-level high`; `npm audit --prefix website --json` | Fail: 28 frontend entries; one affected website dependency | Same findings/severity counts; no remediation applied |
| `./mill --no-server deployment.docker.__.docker.build`; frontend Compose build command from baseline | Eleven backend images and frontend built | All rebuilt successfully; backend image build 238 seconds |
| Quickstart startup and `KUI_E2E_UI=http://localhost:18090 KUI_E2E_API=http://localhost:18082 pnpm e2e` | 86 passed / 17 failed / 2 skipped / 23 not run | Startup passed; 97 passed / 4 failed / 2 skipped / 25 not run. Full-run failure set changed; see comparison below. |
| `KUI_PORT=18082 KUI_FRONTEND_PORT=18090 ./deployment/compose/smoke.sh` | Pass | Pass: measured traffic, signed reads for all routed services, isolated cluster failure, recovery without intervention, and interface availability throughout |

The four backend failures did not recur, but their causes remain unresolved: no backend source was changed. Baseline exclusions were retained throughout. Warning counts: jsdom notices 20 → 20; Storybook chunk-size groups 1 → 1; backend structured WARN lines 409 → 339 and ERROR lines 10 → 10. These runtime fixture counts vary with timing and are not compiler/analyzer violations.

### Browser failure comparison

Direct and proxied `/api/v1/info` both identified code commit `f63e4e0f3d150c42670748cb51104101a257418f`; `gitDirty=true` reflected only the pending review-document update. All four final full-run failures were new test names relative to the baseline. Their saved traces each show `429` from `/api/v1/auth/me` followed by rate-limited screen requests, the same Q10 failure mode seen at baseline. Bounded service logs were inspected. This is evidence of session-limit interference, not proof that the complete integration gate is reliable.

| Final full-run failure | Trace evidence | Unchanged focused rerun |
| --- | --- | --- |
| `frontend/e2e/dashboard.spec.ts:89` — nests the topic tree under Topics, and its rows lead to the list | 17 HTTP 429 responses, including session creation | Passed |
| `frontend/e2e/dashboard.spec.ts:135` — a drawer prefix row lands on a filtered topics list | 17 HTTP 429 responses, including session creation | Passed |
| `frontend/e2e/messages.spec.ts:187` — a browse with no predicate registers nothing | 15 HTTP 429 responses, including session creation | Passed |
| `frontend/e2e/topics.spec.ts:174` — a ?q= in the address filters the list | 15 HTTP 429 responses, including session creation | Passed |

Exact focused command, from `frontend`: `KUI_E2E_UI=http://localhost:18090 KUI_E2E_API=http://localhost:18082 pnpm e2e e2e/dashboard.spec.ts:89 e2e/dashboard.spec.ts:135 e2e/messages.spec.ts:187 e2e/topics.spec.ts:174`. All four passed in 3.2 seconds, without source, configuration or assertion changes. This focused run does not replace the full-run result. Color-environment warnings: 18 at baseline, five in the final full run, one in the focused run.

Fifteen baseline failing browser cases passed in the final full run. The other two (`messages.spec.ts:206`, `topics.spec.ts:193`) were blocked by earlier serial-group failures, so they were not verified again. Twenty-five serial-dependent cases remain unverified in that full run. No persistent new failure was reproduced in the focused checks, but the full browser gate remains red and timing-sensitive; an unqualified repository-wide green verdict is not claimed.

| Touched production file | Line coverage before → after | Branch coverage before → after |
| --- | --- | --- |
| `BarChart.tsx` | 96.46 → 100% | 78.82 → 86.95% |
| `Histogram.tsx` | 86.25 → 93.75% | 76.57 → 89.47% |
| `LineChart.tsx` | 88.05 → 95.65% | 72.35 → 83.17% |
| New `keyboard.ts` | Not present → 100% | Not present → 100% |

The helper also has 100% statement/function coverage. Percentages reflect both added tests and removal of duplicated executable statements; they are not a claim that unrelated code improved.

Additional generated-contract verification: from `frontend/packages/api`, `pnpm exec openapi-typescript ../../../docs/api/openapi.browser.json --output /tmp/kui-quality-schema.d.ts --immutable --alphabetize`, then `cmp src/schema.d.ts /tmp/kui-quality-schema.d.ts`, both passed. Generation targeted a temporary file, leaving the excluded committed artifact untouched.

Verification recovery: the initial final compile was interrupted while its gateway coverage serializer wrote an unexpectedly large generated metadata file (11,696,111,616 bytes observed). A thread dump located the active write in `dotty.tools.dotc.coverage.Serializer`; this does not establish the cause of the growth. `./mill --no-server clean services.gateway.api.scoverage.compile services.gateway.api.scoverage.data` passed; the fresh compile passed and produced 628,798 bytes of metadata. The full backend tests and coverage gate then passed. A subsequent `clean --help` probe was rejected as an invalid selector and changed no source. No build configuration, compiler version or coverage exclusion changed. Incremental metadata growth remains an unresolved tool/cache concern, rather than a claimed source fix.

### Deferred findings and residual risks

Q07 is resolved. Q01–Q03 and Q11 require behavior/dependency changes and remain proposals. Q04/Q10 remain baseline safety-net failures. Q05/Q06 are deferred because their modules failed baseline browser tests; passing unit/search cases do not override that exclusion. Q08/Q09 remain low-priority findings in untouched files. The histogram crash remains deliberately reproducible; the helper extraction does not claim to fix it. Dependency exploitability, real-provider OIDC compatibility and production deployment were not verified by this review.

Cleanup: the temporary quickstart and distributed stacks were removed, including their test volumes; the website and Storybook verification servers were stopped. Unrelated pre-existing containers were left running. Build caches/images remain normal local build artifacts; no tracked generated file or lockfile changed.

Commit hygiene: work stayed on `main`, using the existing Git identity, without pushing, creating branches, rebasing or rewriting pre-session history. Messages were inspected after each commit with `git log -1 --format=%B` and across the session with `git log --format=%B 2257ab75^..HEAD`; no prohibited attribution was found. The final report is a separate `docs: add refactoring results` commit.

## Phase 5 — R1 follow-up fixes (2026-10-25)

Each finding landed on its own `fix/` branch. An independent reviewer checked each branch before merge.

| Finding | Branch | Outcome |
| --- | --- | --- |
| Q04 `TransportCancellationSuite` | `fix/q04a-transport-cancellation` | Test-only. The fake peer's blocking `accept`/reads ignored cancellation, so a failed run hung cleanup until the 30s munit timeout. They now close on cancel. Its 3s `SoTimeout` raced the test's 5s guard and was removed. Reproduced 2 times in about 75 runs, only under load average 44-126. Remaining risk: cold-JVM starvation can still exhaust the test's own 5s guard. |
| Q04 `PrometheusQueryClientSuite` | `fix/q04b-metrics-tests` | Test-only. Only the first plain case and the first OAuth case failed. Cause: first-request class loading and JIT ran inside the 2s call timeout. A suite fixture now warms both paths once, unasserted. Before: 5 failures in 11 loaded runs. After: 0 in 20. |
| Q04 `MetricsWiringSuite` | none | Already fixed by `8e46f6fc` (polls for the first bucket instead of a fixed sleep). 21/21 green. |
| Q04 `AllInOneWiringSuite` | `fix/q04c-admin-pool-close` | Product fix. `AdminClientPool.closeAll` waited the full 5s `CloseTimeout` per pool for calls abandoned against an unreachable broker. Four pools spent about 20s of the 30s budget. It now aborts pending calls first. New `AdminClientPoolSuite` case: red at 5000 ms before, green after. Release still runs if the abort throws (`IsolatedProbeSuite`). |
| Q05 | `fix/q05-shell-search` | Characterization tests first (in-flight abort and pending debounce on disposal), then Extract Function into `shell/src/data/searchController.ts`. |
| Q06 | `fix/q06-session-release` | Characterization tests first (ordering, byte ownership, reference release), then one shared release/retain/update sequence. |
| Q08 | `fix/q08-openapi-driver` | `kui.http.OpenApiDocumentFile` replaces ten copies. Messages and exit codes unchanged. One behaviour change, committed separately: a target path with no directory part now writes instead of throwing an NPE. This matches the gateway, and Mill never passes such a path. |

Integration verification on `integration/r1`: `./mill __.compile`, `__.checkFormat`, `__.fix --check`, `checkArchitecture` and `__.openApiCheck` passed. `./mill -k __.test`: 492 suites, 4,922 tests, 0 failed. The four Q04 suites passed 5 consecutive runs. Frontend `pnpm typecheck`, `test` (105 files, 2,156 tests), `build`, `lint:boundaries` and `bundle-shape` passed.

Browser E2E against a current-source quickstart: the first full run had 2 failures. One was the traffic stat cards: `net::ERR_NETWORK_CHANGED` from the host. The other was the topic-delete auto-create warning: the topic was reported as not existing right after creation. After those 2 failures, 14 later tests in the same serial groups did not run. Both specs then passed 3 times in isolation. A second full run passed 126 with 2 deployment-shape skips. Treat the topic-delete case as a possible metadata-propagation flake to watch.

The DESIGN.md R1 exit criterion is still open: at least 5 consecutive green CI runs.
