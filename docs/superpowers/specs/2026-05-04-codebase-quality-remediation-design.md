# Codebase Quality Remediation Design

Date: 2026-05-04

## Goal

Restore trustworthy validation first, then clean up static-analysis noise, then reduce memory and operational risk in the request/data paths. Work proceeds in A, B, C order so each phase gives the next phase a safer verification baseline.

Engineering review decisions:

- D1: Implement A, B, and C in one pass rather than separate PRs. Keep the A -> B -> C order inside the branch and verify after each phase so failures remain attributable.
- D2: Include PUT streaming/spooling in Phase C rather than deferring it behind a separate design gate.
- D3: Add a repository-owned `golangci-lint` configuration and wire it into `make lint`.
- D4: Require complete PUT streaming regression coverage for ETag, versioning, EC, failure cleanup, and allocation behavior.
- D5: Require both allocation and latency regression gates for changed memory paths.

## Context

The review found three classes of issues:

- Validation blockers: `go vet` reports a `lostcancel` in `cmd/grainfs/serve.go`; `go test ./...` fails in `tests/e2e.TestAutoSnapshot_CreatesSnapshotAutomatically`; `go test -race ./internal/raft` fails in HeartbeatCoalescer tests because the test fake reads and writes `sender.sends` without synchronization.
- Cleanup debt: `golangci-lint` reports unused code, unchecked production errors, ineffectual assignments, and lower-value test/bench unchecked errors.
- Memory and operational risk: large object paths still use whole-body or whole-file allocation in HTTP PUT, cluster backend PUT, Range GET, and NFS fallback RMW paths.

## Phase A: Validation Recovery

Phase A makes the repository's quality signals trustworthy before broad cleanup or memory-path refactors.

### Scope

- Fix `cmd/grainfs/serve.go` `lostcancel` by ensuring the `context.WithTimeout` cancel function is used on every path. The preferred shape is to create the timeout context only inside the `!joinMode` branch that uses it.
- Investigate `tests/e2e.TestAutoSnapshot_CreatesSnapshotAutomatically`, which currently produces `list objects: forward: no reachable peer` and creates zero snapshots. The fix must address the root cause or add a deterministic readiness gate. Do not add arbitrary sleeps unless investigation proves the failure is purely timing and the wait condition is still explicit.
- Fix the HeartbeatCoalescer race-test failure by making the test fake expose lock-protected send snapshots. Do not change production coalescer behavior unless investigation finds a production race.

### Success Criteria

- `go vet ./...` passes.
- `go test ./tests/e2e -run TestAutoSnapshot_CreatesSnapshotAutomatically -count=1` passes.
- `go test -race ./internal/raft -run 'TestCoalescer_(BatchFlush_AllReplies|PartialReply)' -count=1` passes.
- Touched package tests pass.

## Phase B: Cleanup And Refactor

Phase B reduces static-analysis noise and removes stale code after Phase A makes failures meaningful.

### Scope

- Add a checked-in `.golangci.yml` so the cleanup baseline is reproducible across machines.
- Update `make lint` so the repository lint command runs `go vet`, `gofmt`, and the configured `golangci-lint` check.
- Triage `golangci-lint run --timeout=5m` findings in this order:
  1. Production unused code, unchecked errors, and ineffectual assignments.
  2. Test and benchmark unchecked errors that obscure real failures.
  3. Style-only simplifications.
- Delete dead code only when it has no production or test caller and no documented near-term role.
- Keep ambiguous code if it has a clear pending purpose; add a short comment or TODO rather than speculative deletion.
- Refactor only around code touched by actual fixes. Avoid broad file splits in this phase.

### Candidate Cleanup Areas

- Unused production candidates reported by lint: `internal/incident/ports.go` `realClock`, `internal/nbd/handshake.go` `extendedHeaders`, `internal/raft/raft_conn.go` `drainSeconds`, `internal/cluster/ring_store.go` ring helpers, `internal/cluster/shard_placement.go` placement apply helpers, and `cmd/grainfs/receipt_wiring.go` `setupLocalReceipt`.
- Production unchecked errors: packblob rotation/delete paths, volume `ReadFull`, volume metadata writes, and QUIC close calls.
- Ineffectual assignments in cluster, raft, scrubber, and NFSv4 code.

### Success Criteria

- `make lint` runs the repository-owned lint baseline.
- `golangci-lint run --timeout=5m` has no unresolved production-severity findings from the reviewed set, or remaining findings are explicitly documented as accepted.
- Touched package tests pass.
- No unrelated architectural rewrite is included.

## Phase C: Memory And Operational Risk

Phase C reduces peak RSS and long-lived resource risk in hot data paths without weakening consistency or ETag semantics.

### Scope

- Inventory and classify large allocation paths:
  - HTTP PUT body handling.
  - `DistributedBackend.PutObject` and `PutObjectAsync`.
  - Range GET allocation.
  - NFS fallback RMW paths.
- Add focused tests or benchmarks for representative allocation-heavy paths before refactoring them.
- Prefer low-risk response-path improvements first, such as streaming Range GET with bounded readers where Hertz supports it cleanly.
- Implement PUT streaming/spooling in this phase. It must preserve ETag calculation, EC shard splitting, replication, rollback, and version metadata. Likely implementation options are tempfile spooling or a bounded-buffer abstraction.
- Add or verify metrics for goroutines, open file descriptors, and RSS only if the current metrics surface does not already cover them.

### Success Criteria

- Focused memory tests or benchmarks show reduced allocation for changed paths.
- Latency benchmarks or targeted regression tests show small-object PUT/GET paths do not regress materially.
- Existing S3, NFS, and cluster behavior tests still pass.
- PUT streaming/spooling tests cover ETag/size/content-type parity, version listing and versioned reads, EC shard metadata and reconstruction, partial reader or peer failure cleanup, and bounded allocation under a large body.
- Any PUT streaming/spooling change preserves ETag, versioning, EC placement, and rollback behavior.

## Non-Goals

- Do not use the one-pass implementation decision as permission for unrelated rewrites.
- Do not split the largest files purely for style.
- Do not change public protocol behavior while doing cleanup.
- Do not treat ignored test or benchmark errors as production blockers unless they hide a real failure.

## Execution Order

1. Phase A: validation recovery.
2. Phase B: cleanup and local refactor.
3. Phase C: memory and operational risk reduction.

Each phase should be independently reviewable and should leave the repo in a testable state before moving on.

## What Already Exists

- `Makefile` already provides `test`, `test-race`, `test-e2e`, and `lint`; Phase B should extend this rather than introduce a parallel validation script.
- `README.md` already documents the core test and lint commands.
- `TODOS.md` already tracks the auto-snapshot e2e failure, predictive resource warnings, and the bucket replication race. This plan should fix or reference those items rather than duplicate them.
- `--pprof-port` and `/debug/pprof` already exist for profiling, and `/metrics` is already served through Prometheus. Phase C should reuse these surfaces before adding new operational metrics.
- Existing Range GET tests cover HTTP 206 behavior and range edge cases. Phase C should add allocation/performance coverage instead of replacing the behavior tests.

## NOT In Scope

- CI provider setup is not included unless implementation discovers an existing CI convention to extend.
- Broad file splits of `backend.go`, `raft.go`, `serve.go`, `compound.go`, or `volume.go` are not included unless a touched fix needs a local extraction.
- NFS fallback RMW redesign is not required before the S3/cluster PUT and Range GET memory paths are fixed.
- Predictive resource warnings remain tracked in `TODOS.md`; this plan only adds or verifies metrics if current surfaces are missing.

## Test Plan

```text
CODE PATHS                                              REQUIRED COVERAGE
Phase A
  cmd/grainfs/serve.go rebalance timeout ctx
    |-- joinMode=false executes plan with cancel          go vet + touched package test
    |-- joinMode=true does not create unused cancel       go vet
  auto snapshot e2e
    |-- server port ready                                 existing helper
    |-- routing/data-group readiness before assertions    focused e2e readiness wait
    |-- >=2 snapshots eventually created                 focused e2e assertion
  heartbeat coalescer fake
    |-- SendHeartbeatBatchWithCorrID appends under lock   existing test path
    |-- tests read lock-protected send snapshots          race test

Phase B
  lint baseline
    |-- .golangci.yml checked in                          make lint
    |-- production findings fixed/documented              golangci-lint

Phase C
  Range GET
    |-- 206/header/body behavior                          existing tests
    |-- large range avoids full allocation                 allocation benchmark/test
  PUT streaming/spooling
    |-- ETag/size/content-type parity                      unit/integration regression
    |-- latest and old version reads                       versioning regression
    |-- EC shard metadata + reconstruction                 cluster/EC regression
    |-- partial reader or peer failure cleanup             failure-injection regression
    |-- bounded allocation under large body                allocation benchmark/test
    |-- small-object latency does not materially regress   latency benchmark/regression
```

## Failure Modes

- Auto-snapshot startup can race routing readiness and return `forward: no reachable peer`; the focused e2e test must wait on an explicit readiness condition.
- HeartbeatCoalescer tests can report a race if the fake exposes mutable slices without locking; the fake must return snapshots.
- Lint cleanup can remove code that is intentionally reserved for near-term work; ambiguous removals need comments or TODOs rather than speculative deletion.
- Range GET streaming can return wrong `Content-Length` or truncate if the bounded reader is mis-sized; existing 206 tests plus allocation coverage must catch this.
- PUT streaming can leave temp files, stale shard metadata, partial peer writes, or wrong ETags after a reader/peer failure; failure-injection tests must cover cleanup and visible object state.

## Parallelization

Sequential implementation is preferred inside the single branch because Phase B depends on Phase A's trustworthy validation baseline, and Phase C depends on the tests and lint baseline from A/B. Independent investigation can still happen in parallel, but code changes should land in A -> B -> C order.

| Step | Modules touched | Depends on |
|------|-----------------|------------|
| Phase A validation recovery | `cmd/`, `tests/e2e/`, `internal/raft/` | - |
| Phase B lint cleanup | `Makefile`, lint config, selected `cmd/` and `internal/` packages | Phase A |
| Phase C memory paths | `internal/server/`, `internal/cluster/`, storage-facing tests | Phase A, Phase B |

Lane A: Phase A -> Phase B -> Phase C (sequential, shared validation baseline)

## GSTACK REVIEW REPORT

| Review | Trigger | Why | Runs | Status | Findings |
|--------|---------|-----|------|--------|----------|
| CEO Review | `/plan-ceo-review` | Scope & strategy | 0 | - | Not run for this backend quality remediation plan |
| Codex Review | `/codex review` | Independent 2nd opinion | 0 | - | Not run |
| Eng Review | `/plan-eng-review` | Architecture & tests (required) | 1 | CLEAR | 5 issues reviewed and resolved into plan decisions; 0 unresolved, 0 critical gaps |
| Design Review | `/plan-design-review` | UI/UX gaps | 0 | - | Not applicable; no UI scope |
| DX Review | `/plan-devex-review` | Developer experience gaps | 0 | - | Not run |

- **UNRESOLVED:** 0
- **VERDICT:** ENG CLEARED - ready to implement against this plan.
