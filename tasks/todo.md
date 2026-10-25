# R1 tasks (see tasks/plan.md)

## Task 1: Q04a — TransportCancellationSuite
**Acceptance:** root cause established with evidence; fix in product or test sync, not timeout bump; suite green 5x in a row incl. under load.
**Verify:** `./mill libs.http.test.testOnly kui.http.upstream.TransportCancellationSuite` x5
**Branch:** fix/q04a-transport-cancellation · **Scope:** S

## Task 2: Q04b — metrics trio
**Acceptance:** cause of empty `rates`, Prometheus seed timeout, allinone 30s timeout established; fixed without timeout bumps; each suite green 5x.
**Verify:** `./mill services.metrics.infrastructure.test`, `services.metrics.app.test`, `apps.allinone.test` x5
**Branch:** fix/q04b-metrics-tests · **Scope:** M

## Task 3: Q06 — session release dedupe
**Acceptance:** six shared release/retain/state statements run once; ordering and byte ownership unchanged; existing pause/budget/ordering tests pass.
**Verify:** `pnpm test`, `pnpm typecheck` (frontend)
**Branch:** fix/q06-session-release · **Scope:** S

## Task 4: Q05 — shell search controller
**Acceptance:** characterization tests for debounce + in-flight cancel on dispose added first; controller extracted shell-internal; behavior unchanged.
**Verify:** `pnpm test`, `pnpm typecheck`, `pnpm lint:boundaries`, `pnpm bundle-shape`
**Branch:** fix/q05-shell-search · **Scope:** S

## Task 5: Q08 — shared OpenAPI driver
**Acceptance:** one driver; exit codes and messages byte-identical; all generated docs unchanged.
**Verify:** `./mill __.openApiCheck`, `./mill checkArchitecture`
**Branch:** fix/q08-openapi-driver · **Scope:** M

## Checkpoint: Integration
- [x] Adversarial review passed per branch
- [x] Integration branch: backend compile/test/arch/openapi; Q04 suites 5x green
- [x] Frontend gates + one E2E run
- [ ] Human review before merge to main

## Task 6: Docs status refresh (DESIGN.md, QUALITY_REVIEW.md)
- [x] R0 delivered, R1 results recorded
