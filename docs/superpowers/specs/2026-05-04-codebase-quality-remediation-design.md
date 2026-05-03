# Codebase Quality Remediation Design

Date: 2026-05-04

## Goal

Restore trustworthy validation first, then clean up static-analysis noise, then reduce memory and operational risk in the request/data paths. Work proceeds in A, B, C order so each phase gives the next phase a safer verification baseline.

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
- Treat PUT streaming as a separate design inside this phase because it interacts with ETag calculation, EC shard splitting, replication, rollback, and version metadata. Likely implementation options are tempfile spooling or a bounded-buffer abstraction.
- Add or verify metrics for goroutines, open file descriptors, and RSS only if the current metrics surface does not already cover them.

### Success Criteria

- Focused memory tests or benchmarks show reduced allocation for changed paths.
- Existing S3, NFS, and cluster behavior tests still pass.
- Any PUT streaming/spooling change preserves ETag, versioning, EC placement, and rollback behavior.

## Non-Goals

- Do not combine all phases into one mega-refactor.
- Do not split the largest files purely for style.
- Do not change public protocol behavior while doing cleanup.
- Do not treat ignored test or benchmark errors as production blockers unless they hide a real failure.

## Execution Order

1. Phase A: validation recovery.
2. Phase B: cleanup and local refactor.
3. Phase C: memory and operational risk reduction.

Each phase should be independently reviewable and should leave the repo in a testable state before moving on.

