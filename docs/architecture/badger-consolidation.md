# Per-Node BadgerDB Consolidation (C2)

**Status:** Draft / Design proposal
**Owner:** TBD
**Trigger:** PR #128 (`perf/badger-compactor`) ship — C1 cheap-win delivered, but
idle CPU floor still ~26% per node. C2 is the structural change required to push
idle floor closer to single-digit percent.

## Problem statement

Idle 5-node cluster, N=8 raft groups (default), measured on macOS 14:

| metric | value |
|---|---|
| CPU per node (avg) | 26.4% (host total ~135%) |
| Goroutines per node | 286 |
| HeapAlloc per node | ~1.47 GB |
| CPU profile dominant | `runtime.kevent` 36% + `pthread_cond_wait` 21% + `pthread_cond_signal` 15% = 72% scheduler |
| Goroutine pprof dominant | BadgerDB compactor + housekeeping > 50% of total |

Goroutines parked are cheap; **wake-up frequency** is what drives the scheduler
cost. Each BadgerDB instance contributes ~10 housekeeping goroutines that wake
on short tickers (compactor, valueLog GC, watermark, monitorCache, publisher,
flushMemtable, vlogThreshold, doWrites, updateSize). C1 reduced compactor pool
from 4 → 2; the other ~8 housekeepers per instance are unchanged. Therefore the
only meaningful path to lower idle cost is **reducing the number of BadgerDB
instances**.

## Current architecture

Per raft group, GrainFS opens **2** BadgerDB instances:

| File | Purpose | sync writes |
|---|---|---|
| `internal/cluster/group_lifecycle.go:64` (`groupDir/badger`) | FSM application state | no |
| `internal/raft/store.go:128` (`groupDir/raft`, via `NewBadgerLogStore`) | raft WAL + state + snapshot | **yes** |

Per-process counts at default `--seed-groups=8`:

```
8 groups × 2 BadgerDB instances = 16 group-level BadgerDB
+ 1 process-level metadata BadgerDB (cmd/grainfs/serve.go)
= 17 BadgerDB per process
× 5 nodes = 85 BadgerDB cluster-wide
```

At default `--seed-groups`, larger clusters seed even more. The current `--seed-groups=N`
policy (`max(8, cluster_size×4)`) means the scaling is linear with cluster size, and
goroutine count is super-linear when cluster_size > 2.

## Why C1 alone isn't enough

C1 (compactor pool reduction) measured impact on idle-N8:

| metric | baseline | C1 | Δ |
|---|---|---|---|
| Goroutines/node | 307 | 286 | -7% |
| CPU/node | 27.5% | 26.4% | -4% |

The remaining ~286 goroutines and ~26% CPU floor cannot be brought down
without removing instances themselves. Compactors are roughly half of the
parked goroutines but a smaller fraction of wake-up cost; the housekeepers
(valueLog GC tickers, etc.) wake regardless of compactor count.

## Goal

Reduce per-node BadgerDB instance count from `2N + 1` to **2 + 1** (state
shared, raft-log shared, metadata as today) without changing any raft safety
property or losing per-group operational isolation.

Target metrics (idle-N8):

| metric | C1 | C2 target |
|---|---|---|
| Goroutines/node | 286 | < 100 |
| CPU/node | 26.4% | < 10% |
| Group teardown latency | ~ms (Close) | comparable (DropPrefix + cancel) |
| Crash recovery | per-group dir | shared DB recovery, group prefix replay |

## Why two shared BadgerDBs, not one

| | merged into 1 | separate state + raft-log |
|---|---|---|
| state writes | forced sync (raft log requires sync) → **5-10× write amplification** on FSM commits | unsynced FSM writes, fast |
| compaction interference | state write storm stalls raft log fsync chain | independent LSMs, raft log latency stable |
| recovery scoping | one DB | independent — log can recover without touching state |

Conclusion: keep state and raft-log as separate BadgerDBs but each shared
across all groups within a node. Net: 2 BadgerDB instances per node for
cluster work + 1 for process metadata = 3 total.

## Key namespacing scheme

```
state DB (no sync):
  <groupID>/<originalKey>          // e.g. "group-3/buckets/foo/objects/bar"

raft-log DB (sync writes):
  <groupID>/raft:state              // current term, votedFor, last applied
  <groupID>/raft:snapshot           // most recent snapshot bytes
  <groupID>/raft:snapshot:meta      // index, term
  <groupID>/raft:log:<index>        // log entry by index
  <groupID>/raft:bootstrap          // bootstrap marker
```

`<groupID>` is a fixed-width or length-prefixed encoding that does not collide
with any group ID character. Recommendation: length prefix (`varuint(len) ||
groupID || keytail`) — supports arbitrary group IDs and gives O(1) prefix
extraction during scans.

The `keyManagedMode` flag currently DB-global moves to a process-level marker
file (`<dataDir>/managed-mode`) since it now applies to a node-shared DB.

## Required code changes

### `internal/raft/store.go`

`BadgerLogStore` becomes the actual log layer:

```go
type BadgerLogStore struct {
    db *badger.DB    // shared, opened by the node, not the group
    managedMode bool
    // optional refcount for safe Close() when last group teardown
}

// NewBadgerLogStore stays for single-DB backward compat (tests, single-node).
// New constructor for shared mode:
func OpenSharedLogStore(path string, opts ...BadgerLogStoreOption) (*BadgerLogStore, error)

// Per-group view returned by NamespacedLogStore.For(groupID):
type NamespacedLogStore struct {
    inner   *BadgerLogStore
    groupID string
    prefix  []byte  // varuint(len(groupID)) || groupID
}

// Implements the LogStore interface raft.Node consumes
func (n *NamespacedLogStore) AppendEntries(...)
func (n *NamespacedLogStore) GetEntries(low, high uint64) ...
func (n *NamespacedLogStore) TruncateBefore(idx uint64) ...
func (n *NamespacedLogStore) TruncateAfter(idx uint64) ...
func (n *NamespacedLogStore) SaveState(term uint64, vote string) ...
func (n *NamespacedLogStore) SaveSnapshot(meta SnapshotMeta, payload []byte) ...
func (n *NamespacedLogStore) LoadSnapshot() ...
func (n *NamespacedLogStore) LastIndex() uint64
```

Every existing global key (`raft:state`, `raft:snapshot`, etc.) becomes
prefixed. Tests must verify `TruncateBefore/After` only scan the group's prefix
range.

### `internal/cluster/group_lifecycle.go`

`instantiateLocalGroup` no longer opens its own DBs. It receives the shared
`*BadgerLogStore` and shared FSM `*badger.DB` from the caller and constructs:

```go
logStore := sharedLogStore.For(entry.ID)
fsmView  := NewNamespacedFSMView(sharedFSMDB, entry.ID)
node     := raft.NewNode(rcfg, logStore)
```

### `internal/cluster/apply.go` (FSM Snapshot/Restore — critical)

Today (`apply.go:660` Snapshot): iterates the entire DB.
Today (`apply.go:683` Restore): deletes the entire DB then writes new state.

Required:

```go
func (f *FSM) Snapshot() (FSMSnapshot, error) {
    // iterate ONLY f.prefix (groupID prefix), not entire DB
    return f.snapshotPrefix(f.prefix)
}

func (f *FSM) Restore(r io.Reader) error {
    // 1. DropPrefix(f.prefix) to clear THIS group only
    // 2. write incoming snapshot under f.prefix
    // 3. NEVER touch other groups' keys
}
```

### `internal/cluster/group_backend.go`

`Close()` must not close the shared DB. It calls `node.Close()`, then optionally
asks the owning storage layer to `DropPrefix(groupID)` (only on group removal,
not on graceful shutdown).

```go
type GroupBackend interface {
    Close(opts CloseOpts) error
}
type CloseOpts struct {
    DropData bool // true only when the group is being permanently removed
}
```

### Process-level lifecycle

`cmd/grainfs/serve.go` opens the two shared BadgerDBs once at startup and
passes them to the cluster wiring. Closes them once at shutdown after all
groups have closed.

## Migration

Per project memory rule: no `migrate` CLI; auto-handle on startup.

Strategy:

1. On startup, scan `<dataDir>/groups/*/badger` and `<dataDir>/groups/*/raft`.
2. If found, run a one-shot migration:
   - Open each group's old DB read-only.
   - Iterate keys, write prefixed versions into the new shared DBs.
   - Verify counts, then `os.RemoveAll` the old per-group DB dir.
3. Idempotent: completed groups have a marker file; restart resumes mid-migration.
4. Migration progress logged, surfaced in `/admin/status` or similar so operators see it.

If the dataDir already has the new layout (shared DBs present, no per-group
DBs), skip migration.

## Invariants to preserve (must-test, codex-validated)

Each must have explicit test coverage before merge:

1. **Per-group identity**: `currentTerm`, `votedFor`, `commitIndex`, `lastApplied`
   are never read or written outside the owning group's prefix.
2. **Log range isolation**: `LastIndex(groupA)` reflects only group A's
   entries; `TruncateBefore/After(groupA, idx)` does not touch group B.
3. **Snapshot containment**: A `SaveSnapshot(groupA)` payload never includes
   any group B keys; `Restore(groupA)` never deletes group B keys.
4. **Joint consensus durability**: ConfChange entries committed under group A
   replay correctly across restart and never bleed group B's `Servers` set.
5. **Group teardown safety**: removing group A leaves all other groups
   unaffected (no DB close, no data loss).
6. **Concurrent read-during-truncate**: group A's truncation does not block or
   corrupt group B's read path beyond expected DB-wide compaction stalls.
7. **Migration correctness**: after auto-migration, every key is exactly
   round-tripped, group-prefixed, and old per-group dirs removed only after
   verification.

## Test plan

| Test | Type | What it proves |
|---|---|---|
| `TestNamespacedLogStore_PrefixIsolation` | unit | `TruncateBefore/After`, `GetEntries`, `LastIndex` scoped to group |
| `TestSharedFSM_SnapshotPrefixOnly` | unit | snapshot serializes only own prefix |
| `TestSharedFSM_RestoreDoesNotWipeSiblings` | unit | restore of group A leaves group B keys intact |
| `TestE2E_ClusterTeardown_GroupRemoval` | e2e | removing one group leaves cluster healthy |
| `TestE2E_JointConsensus_SharedDB` | e2e | joint consensus works across restart with shared DB |
| `TestE2E_ConcurrentTruncateRead` | e2e | read latency on group B during heavy truncate on group A stays bounded |
| `TestE2E_AutoMigration_PerGroupToShared` | e2e | start with old layout dir, verify migration + correctness |
| `TestE2E_ClusterPerf_All` (existing harness) | perf | idle goroutines/CPU drop to target |

## Phases

| Phase | Scope | Estimate |
|---|---|---|
| P1 | Design review, finalize key encoding + interface | 0.5d |
| P2 | `NamespacedLogStore` + `BadgerLogStore` shared mode + unit tests | 1d |
| P3 | FSM `Snapshot`/`Restore` prefix-scoping + unit tests | 0.5d |
| P4 | `cmd/grainfs/serve.go` + `cluster` wiring (open once, pass through) | 0.5d |
| P5 | Auto-migration + idempotency tests | 0.5d |
| P6 | E2E invariant tests (the must-test list above) | 1d |
| P7 | Run `cluster_perf_profile_test` matrix, verify idle floor < 10%/node | 0.5d |
| P8 | Operational: backup script, restore drill, runbook update | 0.5d |
| **Total** | | **~5 days** |

## Open questions

1. **Single-node bootstrap path**: `cmd/grainfs/serve.go:289` opens
   `BadgerLogStore` for the meta-raft. Does meta-raft also share with the
   group log DB, or does it stay separate? Sharing means group-meta interaction
   on the same DB; separating preserves bootstrap simplicity. Recommendation:
   keep meta-raft separate (3 BadgerDBs total: shared state, shared group-log,
   meta-log).

2. **Compaction interference under load**: a single shared DB serializes
   compaction across all groups. Need a load test (load-N32 or higher) to
   confirm that this does not introduce write stalls under aggressive writes.
   May require tuning `NumCompactors` back up for the shared state DB.

3. **Backup/restore semantics**: the existing recover-cluster workflow
   (`docs/recover-cluster.md`) operates on per-group dirs. Will need updating
   for shared layout — likely simpler (one snapshot per node covers everything).

4. **fdatasync amortization**: with shared raft-log DB, all groups' AppendEntries
   sync writes hit a single DB. Badger amortizes via the value log; verify
   under multi-group write storm that `WithSyncWrites(true)` still yields
   acceptable per-group commit latency.

## Risk and rollback

- **Pre-merge**: behind a feature flag `--shared-badger=false` (default off
  initially). Feature flag is removed in a later release once validated.
- **Post-merge regression**: revert is full-revert of the shared-DB code path
  + auto-migration in reverse (keep code paths for both layouts during the
  transition release).
- **Worst case data corruption**: any prefix-scoping bug can wipe sibling
  groups. The unit test suite for `Restore` and `TruncateBefore/After` is the
  primary safety net; merge gate.

## References

- C1 PR: #128 `perf/badger-compactor` (cheap compactor reduction baseline)
- Codex review of this design: see PR review thread linked from #128
- TODOS Phase 19 entry: "BadgerDB 인스턴스 통합" (deferred from C1)

---

**Decision needed**: approve scope and target metrics, then assign owner +
phase ordering. Do not start P2 implementation before P1 design review.
