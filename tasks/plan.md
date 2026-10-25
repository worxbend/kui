# Implementation Plan: R1 trustworthy safety net (QUALITY_REVIEW Q04, Q05, Q06, Q08)

## Overview

R0 (Q01, Q02/Q03, Q09, Q10, Q11) is merged on `main`. This plan delivers DESIGN.md roadmap R1:
diagnose and fix the four flaky backend tests (Q04), then the deferred refactors Q05/Q06, and the
optional Q08 OpenAPI driver dedup. Each task lands on its own `fix/qNN-*` branch, is reviewed
adversarially, and is merged only after the integration checkpoint.

## Architecture Decisions

- No architecture change. DESIGN.md ADR-D01 stands (one module graph, two deployment shapes).
- Q04: fix only an established root cause. Never raise timeouts or skip tests to get green (DESIGN R-3).
- Q05/Q06/Q08 are behavior-preserving refactors (Extract Function / Consolidate Duplicate Fragments).
  No public API, wire format, exit code, or message change.
- Agents run targeted module builds/tests only. Docker stacks and Playwright run once, serially, at
  the integration checkpoint, to avoid port collisions and host load that skews Q04.

## Task List

### Phase 1: Safety net (Q04)
- [x] Task 1: Q04a — `libs.http` `TransportCancellationSuite` socket cancellation failure
- [x] Task 2: Q04b — metrics trio: `PrometheusQueryClientSuite`, `MetricsWiringSuite`, `AllInOneWiringSuite`

### Phase 2: Refactors
- [x] Task 3: Q06 — consolidate live/bounded release bookkeeping in `feature-messages/src/session.ts`
- [x] Task 4: Q05 — extract shell search controller from `shell/src/App.tsx`
- [x] Task 5: Q08 — shared OpenAPI document write/check driver for the 9 service generators

### Checkpoint: Integration (after Tasks 1-5)
- [x] Each branch passed an independent adversarial review
- [x] Merge into an integration branch; `./mill __.compile`, `./mill __.test`, `checkArchitecture`, `__.openApiCheck`
- [x] Q04 suites green 5 runs in a row, test counts unchanged
- [x] Frontend `pnpm typecheck`, `pnpm test`, `pnpm build`, `pnpm lint:boundaries`, `pnpm bundle-shape`
- [x] Browser E2E once against quickstart
- [ ] Human review before merging to `main`

### Phase 3: Docs
- [x] Task 6: Update DESIGN.md/QUALITY_REVIEW.md status (R0 delivered, R1 results)

## Risks and Mitigations
| Risk | Impact | Mitigation |
|------|--------|------------|
| Q04 is load-only, no code cause | Med | Report evidence; change nothing rather than hide it |
| Parallel worktree builds raise host load | Med | Targeted module builds; Q04 repeat runs re-done serially at checkpoint |
| Q05 cleanup/stale-response regression | Med | Characterization tests for debounce and in-flight cancel on dispose first |
| Q08 changes generator exit codes/messages | Low | Byte-identical messages and exit codes; `__.openApiCheck` |

## Open Questions
- None blocking. Q08 is "Could": drop it if the review finds the shared driver adds more than it removes.
