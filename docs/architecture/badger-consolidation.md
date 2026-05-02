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

## P0a — Baseline measurements (current per-group layout)

Captured 2026-05-02 on macOS 14, branch `perf/badger-compactor` HEAD `5034931`
(C1 applied: state DB `NumCompactors=2`, raft-log DB `NumCompactors=2 +
NumVersionsToKeep=1`). Host had concurrent `fix-e2e-tests` worktree e2e suite
running (5+ extra grainfs procs); all numbers are under contention.

| scenario | boot | RSS/node | heap/node | CPU/node | gor/node | PUT ok / err | GET ok / err |
|---|---|---|---|---|---|---|---|
| idle-N8 | 20 s | 286 MB | 1469 MB | 26.4% | 286 | — | — |
| idle-N16 | 15 s | 399 MB | 2396 MB | 30.0% | 443 | — | — |
| idle-N64 | partial (2 nodes died mid-run, host OOM-adjacent) | — | — | — | 1942 (survivors) | — | — |
| load-N8 | 21 s | 529 MB | 1887 MB | 49.2% | 306 | 1026 / 1196 (54% err) | 242 / — |
| load-N16 | 17 s | 741 MB | 2725 MB | 61.0% | 456 | 605 / 78 (11% err) | 235 / 116k |
| load-N32 | **FAIL** (no leader within 120 s; ~22/32 groups instantiated; peer connection eviction at boot) | — | — | — | — | — | — |
| load-N64 | not attempted (cliff already at N=32) | — | — | — | — | — | — |

Top CPU functions on `node-0` for `idle-N8` (representative of all idle): 87%
in `runtime.kevent` + `pthread_cond_wait` + `pthread_cond_signal` +
`findRunnable` — scheduler-dominated, application code <5%. Profile pattern
unchanged from N=8 to N=16; idle CPU is goroutine-wakeup-bound, not raft-work-bound.

### Observations

1. **Goroutine scaling is sub-linear**: N=8→N=16 doubles groups but
   goroutines grow only +55% (286 → 443). Per-instance fixed overhead
   dominates over per-group scaling. C2's win comes from reducing fixed
   per-instance overhead × 2N+1 → 2.

2. **N=32 boot cliff**: 5 nodes × 32 groups × 2 BadgerDB = 320 BadgerDB
   instances cluster-wide. Bootstrap fails before any user request — peers
   evict each other's connections under the resource storm
   (`evicted: stale connection`, `default bucket creation failed: forwardPropose:
   ... read header: EOF`). This is the existing-layout failure mode that C2
   directly addresses by collapsing 2N+1 → 3 instances per node.

3. **load-N16 PUT error rate (11%) < load-N8 (54%)**: counterintuitive
   improvement at higher N. Hypothesis: 16 group leaders distribute write
   pressure better than 8. Larger N is *helpful for write throughput* up to
   the host saturation point. C2, by raising that saturation point, lets the
   cluster realize this benefit at higher N.

4. **No compaction-stall signature observed**: A1's worst-case (shared LSM
   compaction → write stall → leader churn) never appeared at idle or load.
   Evidence is limited (couldn't run sustained-write tests on contended host),
   but the failure mode that *did* appear (boot saturation at N=32) is one
   that C2 makes *better*, not worse.

### Decision: PROCEED to P0b prototype

Premise validation: the design's central worry (shared-DB compaction stall)
is unconfirmed. The actual current-layout failure mode (per-instance overhead
breaks bootstrap at N≥32) is *what C2 is designed to fix*. C2 should
demonstrably let the cluster sustain N=32 or higher; if a quick prototype
can show this on the same host, the design's primary value proposition is
proven. Compaction-stall risk remains a P0b focus, to be measured directly
via prototype `load-N16` sustained writes.

P0b spec: minimal `NamespacedLogStore` + per-node-shared `*badger.DB` for
state and log, behind a `--shared-badger` flag (**default true**). Skip
migration; refuse startup when legacy per-group raft dirs are detected.
Just enough to run `cluster_perf_profile_test` matrix end-to-end.
Compare: idle-N8/16 goroutines & CPU; load-N8/16 PUT error rate;
load-N32 (does it boot now?).

## P0b — Shared raft-log prototype (code landed, measurements pending)

Code wired in commit `<TBD>`:

- `internal/raft/store.go`: `BadgerLogStore` gains `prefix []byte` + `shared bool`
  fields. New `OpenSharedLogStore(db *badger.DB, groupID string, ...)` constructor
  views an externally-managed DB with all keys prefixed by 4-byte big-endian
  `len(groupID)` followed by `groupID` (no separator needed — fixed-width
  length is unambiguous and avoids the wraparound that a 1-byte length would
  hit at `len(groupID) >= 256`). `checkManagedMode` retries on
  `badger.ErrConflict` so concurrent same-group opens succeed. `Close()` is
  a no-op for shared stores; the caller owns DB lifecycle. Existing
  `NewBadgerLogStore` callers see zero behavior change (prefix is empty).
- `cmd/grainfs/serve.go`: new `--shared-badger` cobra flag (**default true**).
  When enabled, opens one `*badger.DB` at `<dataDir>/shared-raft-log/` with
  the same options as per-group log DBs (`SyncWrites=true`, `NumCompactors=2`,
  `NumVersionsToKeep=1`). On startup, refuses to proceed when any
  `<dataDir>/groups/*/raft` directory exists from a pre-P0b deployment —
  failing closed instead of silently abandoning the legacy raft state.
  Forwards `--badger-managed-mode` and other `BadgerLogStoreOption`s into
  every group's `OpenSharedLogStore`.
- `cmd/grainfs/serve.go`: `InstantiateLocalGroup` call site passes
  `LogStore: raft.OpenSharedLogStore(sharedRaftLogDB, entry.ID)` when shared
  mode is on.
- `internal/cluster/group_lifecycle.go`: pre-existing `GroupLifecycleConfig.LogStore`
  field carries the shared view through; no new wiring required there.
- `tests/e2e/cluster_perf_profile_test.go`: forwards `GRAINFS_PERF_SHARED_BADGER=1`
  env to `--shared-badger`; bumped `waitForLeaderAndCreateBucket` timeout from
  120s to 240s to absorb host contention.

What's NOT in P0b:

- FSM state DB consolidation (still per-group at `groupDir/badger/`).
  Defers half the win until full C2 P3 lands.
- Migration / auto-conversion: `--shared-badger=false` lets legacy per-group
  layouts continue. Mixing modes on the same data dir is unsupported.
- `Snapshot`/`Restore` prefix-scoping: perf test uses
  `--snapshot-interval 0`, so unsafe Restore can't fire. Full C2 must fix.
- Group teardown safety: prototype assumes cluster runs to completion.

### Status: smoke run blocked by host load

idle-N8 smoke (with and without `--shared-badger`) failed to elect a meta-raft
leader within 240s on the test host. Concurrent `fix-e2e-tests` worktree
e2e suite was driving load-average to 15+ and using ~75% CPU on grainfs
processes from that worktree, starving the perf cluster's bootstrap.

Empirically the failure is host-state, not P0b code:
- Both shared (`--shared-badger`) and non-shared baselines fail the same way
- Failure mode is meta-raft `ProposeWait: not the leader` for >30s, identical
  pattern to load-N32 baseline under similar contention
- Raft unit-test suite passes after refactor (`go test ./internal/raft/`)
- Prior idle-N8 runs on the same code (commit `317f991`) succeeded under
  *lighter* contention earlier today (boot 15-23s)

Measurement deferred to next session when the host is idle. Run with:

```bash
GRAINFS_PERF=1 GRAINFS_PERF_SHARED_BADGER=1 GRAINFS_PERF_SCENARIO=idle-N8 \
  GRAINFS_PERF_DIR=/tmp/grainfs-perf-p0b \
  go test -run '^TestE2E_ClusterPerf_All$' -count=1 -timeout 600s -v ./tests/e2e/
```

Then sweep `idle-N8,idle-N16,load-N8,load-N16,load-N32` and update this
section with the comparison table.

## P3 — FSM state DB consolidation (implementation design)

P0b consolidates the raft-log half. P3 consolidates the **other** half — the
per-group FSM state badger at `groupDir/badger/` — into a single shared
`<dataDir>/shared-fsm/` *badger.DB. Goal: 2N+1 → 3 BadgerDB per node total.

### Scope inventory

The per-group state DB is wired into both `FSM` (apply.go) and
`DistributedBackend` (backend.go); both hold the same `*badger.DB`. Touch
points cluster-wide:

| file | `db.Update/View` calls | iterators |
|---|---:|---:|
| `internal/cluster/apply.go` (FSM) | 19 | 4 |
| `internal/cluster/backend.go` (DistributedBackend) | 15 | many |
| `internal/cluster/shard_placement.go` | 7 | — |
| `internal/cluster/snapshotable.go` | 3 | — |
| `internal/cluster/scrubbable.go` | 1 | — |
| **total** | **45** | **19** |

Key-builder functions in `apply.go`: 9 (`bucketKey`, `bucketPolicyKey`,
`bucketVerKey`, `objectMetaKey`, `objectMetaKeyV`, `latestKey`,
`multipartKey`, `shardPlacementKey`, `pendingMigrationKey`). All return
`[]byte("rawPrefix:" + suffix)`. None are aware of group identity today.

### Approach: key-builder methods on FSM/DistributedBackend

Two refactor approaches were considered:

**A. Wrap badger.Txn with auto-prefix.** A `prefixedTxn` type that mirrors
   the Txn API and applies prefix to every Get/Set/Delete/NewIterator. Pro:
   call sites unchanged. Con: ~10 wrapper methods, every closure signature
   changes from `*badger.Txn` to `*prefixedTxn`, badger internals leak
   through (RawIterator, etc.) — invasive across every call site anyway.

**B. Make key builders methods on FSM/DistributedBackend.** Each
   `objectMetaKey(...)` becomes `f.objectMetaKey(...)` and prepends the
   group prefix. Iterator prefixes use `f.prefixed(rawPrefix)`. No new
   abstraction; the diff is mechanical (sed-shape) and visible. Con:
   more lines changed (every call site touches one identifier). **Recommended.**

### Code shape

```go
// apply.go
type FSM struct {
    db     *badger.DB     // shared across all groups when shared-fsm enabled
    prefix []byte         // 4-byte BE len(groupID) || groupID; nil for legacy
    rings  *ringStore
    // ...existing fields
}

func NewFSM(db *badger.DB, opts ...FSMOption) *FSM { /* prefix from opts */ }

func (f *FSM) prefixed(rawKey []byte) []byte {
    if len(f.prefix) == 0 {
        return rawKey
    }
    out := make([]byte, len(f.prefix)+len(rawKey))
    copy(out, f.prefix)
    copy(out[len(f.prefix):], rawKey)
    return out
}

// Each free key-builder becomes a method:
func (f *FSM) bucketKey(b string) []byte    { return f.prefixed([]byte("bucket:" + b)) }
func (f *FSM) objectMetaKey(b, k string) []byte { return f.prefixed([]byte("obj:" + b + "/" + k)) }
// ... 7 more
```

Same shape for `DistributedBackend` (which uses the same set of keys).
Best to factor the key builders into one place that both FSM and
DistributedBackend reference, sharing a `prefixedKeys` helper struct.

### Snapshot / Restore — the codex hotspot

`FSM.Snapshot()` (apply.go:660) currently iterates the whole DB
unconditionally. `FSM.Restore()` (apply.go:683) currently deletes every
key in the DB before reloading. **Both are fatal in shared mode** —
group A's snapshot would include group B state; restoring group A's
snapshot would wipe every other group on the node.

Required changes:

```go
func (f *FSM) Snapshot() ([]byte, error) {
    state := make(map[string][]byte)
    err := f.db.View(func(txn *badger.Txn) error {
        opts := badger.DefaultIteratorOptions
        opts.Prefix = f.prefix    // scoped iteration
        it := txn.NewIterator(opts)
        defer it.Close()
        for it.Rewind(); it.ValidForPrefix(f.prefix); it.Next() {
            item := it.Item()
            // Strip the group prefix before serialization — snapshot
            // bytes must be portable across group renames.
            key := string(item.Key()[len(f.prefix):])
            // ...
        }
        return nil
    })
    // ...
}

func (f *FSM) Restore(data []byte) error {
    // 1. Drop ONLY this group's prefix
    if len(f.prefix) > 0 {
        if err := f.db.DropPrefix(f.prefix); err != nil {
            return err
        }
    } else {
        // legacy path: keep existing whole-DB drop
    }
    // 2. Re-encode keys with prefix on write
    state, err := unmarshalSnapshotState(data)
    // ...
    return f.db.Update(func(txn *badger.Txn) error {
        for k, v := range state {
            if err := txn.Set(f.prefixed([]byte(k)), v); err != nil {
                return err
            }
        }
        return nil
    })
}
```

`badger.DropPrefix` is a documented API; it walks the LSM and drops
matching keys atomically. Brief stall during the operation is acceptable
for the snapshot install path.

### Group teardown

`GroupBackend.Close()` currently calls `db.Close()` (backend.go path).
On shared mode the DB is process-owned; group close becomes:

```go
// no-op for shared DB (caller owns Close)
// optional DropPrefix when the group is being PERMANENTLY removed
//   (not on graceful shutdown)
```

Same `wrapped` flag approach as P0b's BadgerLogStore.

### Migration (legacy → shared)

Same pattern as P0b: at startup, refuse to enable shared-fsm when
`<dataDir>/groups/*/badger/` directories exist from a pre-P3 deployment.
Operator must wipe (test cluster) or run an explicit migration tool
(production — out of scope for prototype).

### Test plan (must-pass before merge)

| test | why |
|---|---|
| `TestSharedFSM_PrefixIsolation` | two FSM views over shared DB, all read/write paths stay scoped |
| `TestSharedFSM_SnapshotPrefixOnly` | group A snapshot serializes only group A keys |
| `TestSharedFSM_RestoreDoesNotWipeSiblings` | restoring group A leaves group B keys intact |
| `TestSharedFSM_DropPrefixOnTeardown` | removing group A only drops group A keys |
| `TestSharedFSM_RestartPersistence` | shared DB close + reopen preserves all groups |
| `TestE2E_ClusterPerf_SharedFSM_LoadN32` | end-to-end: load-N32 boots cleanly with both shared log + shared FSM (the C2 win) |

The first four are unit; the last two are e2e.

### Phase ordering

This is C2 P3, sequenced AFTER P0b measurement confirms the shared-log
direction works. If P0b shows shared-log alone unblocks load-N32 boot
and reduces idle CPU meaningfully, P3 is the natural follow-up — it
roughly doubles the win.

If P0b shows shared-log alone is enough, P3 becomes optional /
deprioritized.

### Estimate (revised from original 5d total)

| step | est |
|---|---|
| Refactor key builders to methods on FSM (apply.go) | 1d |
| Refactor key builders for DistributedBackend (backend.go + others) | 1.5d |
| Snapshot/Restore prefix-scoping + unit tests | 1d |
| Iterator prefix adjustment (19 sites) | 0.5d |
| Migration refusal + serve.go wiring + flag | 0.5d |
| E2E test (perf matrix on shared-fsm) + bug fixing | 1d |
| Codex review iteration | 0.5d |
| **total** | **~6d** |

Plus the original P0b 1.5d (now ~done) → **C2 full ≈ 7-8d** vs original
5d estimate. The extra time is mostly the refactor breadth and
Snapshot/Restore safety tests.

## References

- C1 PR: #128 `perf/badger-compactor` (cheap compactor reduction baseline)
- Codex review of this design: see PR review thread linked from #128
- TODOS Phase 19 entry: "BadgerDB 인스턴스 통합" (deferred from C1)
- P0a plan + measurements: this section + `tests/e2e/cluster_perf_profile_test.go` matrix

---

**Decision needed**: approve scope and target metrics, then assign owner +
phase ordering. Do not start P2 implementation before P1 design review.
