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

### Approach: shared `stateKeyspace` abstraction

Two refactor approaches were considered:

**A. Wrap badger.Txn with auto-prefix.** A `prefixedTxn` type that mirrors
   the Txn API and applies prefix to every Get/Set/Delete/NewIterator. Pro:
   call sites unchanged. Con: ~10 wrapper methods, every closure signature
   changes from `*badger.Txn` to `*prefixedTxn`, badger internals leak
   through (RawIterator, etc.) — invasive across every call site anyway.

**B (revised after codex review). Introduce a `stateKeyspace` object** that
   both `FSM` and `DistributedBackend` reference, with three primitives:

   ```go
   type stateKeyspace struct { prefix []byte }
   func (ks *stateKeyspace) Key(raw []byte) []byte         // prefix || raw
   func (ks *stateKeyspace) Prefix(rawPrefix []byte) []byte // for iterators
   func (ks *stateKeyspace) Strip(fullKey []byte) []byte    // remove prefix
   ```

   Plus all the semantic builders (`bucketKey`, `objectMetaKey`, ...) become
   methods on `stateKeyspace` so they auto-prefix. **`backend.go`,
   `shard_placement.go`, `snapshotable.go`, `scrubbable.go` also need to
   use this** — they currently do raw literal prefix scans like
   `it.Seek([]byte("obj:"))` and `string(item.Key())` extraction
   (e.g. `backend.go:676,748,2020,2036,2490`, `snapshotable.go:24`,
   `scrubbable.go:73`, `shard_placement.go:91`). Mechanical "free function
   becomes method" alone misses these and ships cross-group leaks. Every
   iterator caller must use `ks.Prefix("obj:")` (not `[]byte("obj:")`)
   and `ks.Strip(item.Key())` (not `string(item.Key())`).

   **Recommended.** Approach A's hidden auto-prefix would make the leaks
   above silent; B with explicit `ks.Strip` makes prefix-stripping a code
   review checkpoint.

   Originally framed as "key-builder methods on FSM" — that under-scoped
   the work; backend.go and the others have raw-literal call sites that
   wouldn't be caught by renaming free functions to methods.

### Code shape

```go
// internal/cluster/keyspace.go (new file)
type stateKeyspace struct {
    prefix []byte // 4-byte BE len(groupID) || groupID; nil for legacy/single-group
}

// Use the same encoder as P0b BadgerLogStore for consistency:
//   prefix = binary.BigEndian.PutUint32(uint32(len(groupID))) || groupID
func newStateKeyspace(groupID string) (*stateKeyspace, error) { /* ... */ }

func (ks *stateKeyspace) Key(raw []byte) []byte                 { /* prefix || raw */ }
func (ks *stateKeyspace) Prefix(rawPrefix []byte) []byte        { /* prefix || rawPrefix */ }
func (ks *stateKeyspace) Strip(fullKey []byte) ([]byte, bool)   { /* remove prefix or false */ }

// Semantic builders become methods on the keyspace:
func (ks *stateKeyspace) BucketKey(b string) []byte             { return ks.Key([]byte("bucket:" + b)) }
func (ks *stateKeyspace) ObjectMetaKey(b, k string) []byte      { return ks.Key([]byte("obj:" + b + "/" + k)) }
// ... ObjectMetaKeyV / LatestKey / MultipartKey / ShardPlacementKey / PendingMigrationKey / BucketPolicyKey / BucketVerKey

// apply.go
type FSM struct {
    db     *badger.DB     // shared across all groups when shared-fsm enabled
    keys   *stateKeyspace // owned, captures group prefix
    rings  *ringStore
    // ...existing fields
}

func NewFSM(db *badger.DB, ks *stateKeyspace) *FSM { /* ks may have empty prefix */ }
```

`DistributedBackend` holds the same `*stateKeyspace` (passed in via
`GroupBackendConfig`). Every iterator (15+ in backend.go) becomes:

```go
// before
opts.Prefix = []byte("obj:")
for it.Seek([]byte("obj:")); it.ValidForPrefix([]byte("obj:")); it.Next() {
    rest := string(item.Key()[len("obj:"):])
    // ...
}

// after
objPfx := f.keys.Prefix([]byte("obj:"))
opts.Prefix = objPfx
for it.Seek(objPfx); it.ValidForPrefix(objPfx); it.Next() {
    rawKey, ok := f.keys.Strip(item.Key())
    if !ok { continue }
    rest := string(rawKey[len("obj:"):])
}
```

`Strip` returning `bool` makes leak detection observable — callers can
assert/log when a key arrives without the expected prefix.

### Snapshot / Restore — the codex hotspot

`FSM.Snapshot()` (apply.go:660) currently iterates the whole DB
unconditionally. `FSM.Restore()` (apply.go:683) currently deletes every
key in the DB before reloading. **Both are fatal in shared mode** —
group A's snapshot would include group B state; restoring group A's
snapshot would wipe every other group on the node.

Required changes (revised after codex review for restore-order safety
and snapshot format versioning):

```go
// Format header for shared-FSM snapshots. v1 = legacy whole-DB
// (current pre-P3 format), v2 = group-scoped (P3 and later).
const (
    snapshotMagic = "GFSMSNAP"
    snapshotV2    = uint8(2)
)

func (f *FSM) Snapshot() ([]byte, error) {
    state := make(map[string][]byte)
    err := f.db.View(func(txn *badger.Txn) error {
        opts := badger.DefaultIteratorOptions
        ksPrefix := f.keys.Prefix(nil) // group prefix (nil for legacy)
        opts.Prefix = ksPrefix         // scoped iteration
        it := txn.NewIterator(opts)
        defer it.Close()
        for it.Rewind(); it.ValidForPrefix(ksPrefix); it.Next() {
            item := it.Item()
            raw, ok := f.keys.Strip(item.Key())
            if !ok {
                continue // defensive: shouldn't happen given prefix scope
            }
            // Snapshot bytes are portable across renames — store the
            // group-relative key, not the encoded one.
            err := item.Value(func(v []byte) error {
                state[string(raw)] = append([]byte(nil), v...)
                return nil
            })
            if err != nil {
                return err
            }
        }
        return nil
    })
    if err != nil {
        return nil, err
    }
    // marshalSnapshotState v2 emits magic + version + state
    return marshalSnapshotStateV2(state)
}

func (f *FSM) Restore(data []byte) error {
    // 1. Decode + validate FIRST. A corrupt snapshot must not wipe state.
    state, ver, err := unmarshalSnapshotStateAny(data)
    if err != nil {
        return fmt.Errorf("snapshot decode: %w", err)
    }
    // 2. Defend against misencoded payloads — keys in state must be
    //    raw (group-relative). v2 enforces this; v1 (legacy) keys are
    //    raw by definition. Reject keys that already start with our
    //    own prefix (would double-prefix on write).
    for k := range state {
        if f.keys.HasPrefix([]byte(k)) {
            return fmt.Errorf("snapshot contains pre-prefixed key %q — refusing to install (suspected migration mismatch)", k)
        }
    }
    // 3. Now safe to drop. Scoped to this group only — see the
    //    DropPrefix concurrency note below before assuming this is cheap.
    ksPrefix := f.keys.Prefix(nil)
    if len(ksPrefix) > 0 {
        if err := f.db.DropPrefix(ksPrefix); err != nil {
            return err
        }
    } else {
        // legacy single-group path: existing whole-DB delete loop
    }
    // 4. Re-encode keys with prefix on write.
    return f.db.Update(func(txn *badger.Txn) error {
        for k, v := range state {
            if err := txn.Set(f.keys.Key([]byte(k)), v); err != nil {
                return err
            }
        }
        return nil
    })
    _ = ver // future: version-specific replay paths
}
```

#### `badger.DropPrefix` is NOT group-local in cost

Per badger documentation
([pkg.go.dev/.../badger/v4#DB.DropPrefix](https://pkg.go.dev/github.com/dgraph-io/badger/v4#DB.DropPrefix)),
DropPrefix is a **DB-wide** operation: it stops accepting new writes,
stops memtable flushes and compaction, compacts affected levels,
**then resumes**. Implications for shared-FSM:

- `DropPrefix(groupA)` blocks **every** group's writes for the duration
  (10s of ms to seconds, scaling with the prefix's data volume).
- Read latency on other groups can spike during the operation.
- This is acceptable on snapshot install (rare) and group destruction
  (rare). It is **not** acceptable as part of routine teardown — see
  "Group teardown" below.

Operations that trigger DropPrefix must therefore be:
- bounded in frequency (per-group, raft-driven only),
- coordinated with metrics so cluster-wide latency spikes surface,
- excluded from any hot path.

### Group teardown — separate "shutdown" from "destroy"

Codex flagged this: bolting `DropPrefix` onto `Close()` makes it impossible
to distinguish a transient shutdown (process restart, leadership transfer,
voter swap) from a permanent group removal — the wrong call once means
data loss across restart.

Two distinct paths:

| operation | trigger | shared-DB behavior |
|---|---|---|
| `GroupBackend.Close()` | graceful shutdown, leadership transfer, voter step-down, process exit | no-op for shared DB (process owns Close); raft node stopped |
| `GroupBackend.DestroyGroupData(ctx)` | meta-Raft commits permanent group removal AND local raft node already stopped | `DropPrefix` after committing intent; idempotent |

`DestroyGroupData` is called only from the data-group plan executor's
"group permanently removed" code path, never from a shutdown handler.
The intent must be persisted (e.g., a `<dataDir>/destroyed-groups.log`)
so a crash mid-DropPrefix can resume on restart without re-evaluating
"is this group really gone?".

Same explicit pattern as P0b's `BadgerLogStore.Close` no-op for shared
stores; the new piece is the dedicated `DestroyGroupData` entry point.

### Migration (legacy → shared)

Same pattern as P0b but stricter: detect every mixed-layout permutation
explicitly. A simple "absent dir" check is too loose — partial migrations,
ungraceful crashes, or operator mistakes can produce mixed states that
slip through "no per-group dirs, fresh start".

| Layout state | Action |
|---|---|
| no `groups/*/badger/`, no `shared-fsm/` | fresh — proceed shared-fsm enabled |
| no `groups/*/badger/`, `shared-fsm/` exists | continuing previous shared-fsm install — proceed |
| any `groups/*/badger/`, no `shared-fsm/` | legacy — refuse with `--shared-badger=false` instruction |
| any `groups/*/badger/`, `shared-fsm/` exists | **mixed** — fail closed, refuse to start (corruption likely; don't guess) |
| `groups/*/badger/` removed but `groups/<id>/blobs/` lingering | suspicious; require explicit operator confirmation flag (out of scope) |

Group-lifecycle code that eagerly creates `groups/<id>/badger/`
(`group_lifecycle.go:60`) must be conditional on shared-fsm mode being
*off* — otherwise the prototype trips its own refusal on first boot.

Note: even in shared-fsm mode, the per-group blob storage at
`groups/<id>/blobs/` still exists — only the metadata DB moves. Blob
dir lifecycle stays on the existing per-group ownership model and
must be audited alongside DB keys for any future "destroy group"
flow (codex noted this).

### Test plan (must-pass before merge)

Unit tests:

| test | why |
|---|---|
| `TestSharedFSM_PrefixIsolation` | two FSM views over shared DB, all read/write paths (including backend.go iterators) stay scoped |
| `TestSharedFSM_PathologicalGroupIDs` | mirror P0b's pathological IDs (>255 chars, colons, NULs, prefix-of-prefix) for FSM keys |
| `TestSharedFSM_SnapshotPrefixOnly` | group A snapshot serializes only group A keys |
| `TestSharedFSM_RestoreRejectsCorruptBytes` | corrupt snapshot must fail BEFORE any DropPrefix runs (state preserved) |
| `TestSharedFSM_RestoreRejectsPrefixedKeys` | snapshot containing already-prefixed keys is rejected (suspected migration mismatch) |
| `TestSharedFSM_RestoreLegacySnapshotIntoShared` | a v1 (whole-DB) snapshot taken pre-P3 restores correctly under v2 group-prefix on a shared FSM |
| `TestSharedFSM_DestroyGroupData` | `DestroyGroupData(A)` drops only A; B and C unaffected |
| `TestSharedFSM_RestartPersistence` | shared DB close + reopen preserves all groups + their bootstrap markers |
| `TestSharedFSM_MigrationRefusal` | every legacy/mixed/fresh layout permutation produces the right pass/fail decision |

Concurrency tests (catch the codex-flagged DropPrefix DB-wide stall + races):

| test | why |
|---|---|
| `TestSharedFSM_ConcurrentSnapshotAndApply` | `Snapshot(A)` while `Apply(B)` is committing — both succeed, no key bleed |
| `TestSharedFSM_ConcurrentRestoreAndApply` | `Restore(A)` running while `Apply(B)` writes — B's writes either complete or queue cleanly; no partial-state visible to readers |
| `TestSharedFSM_ConcurrentDropPrefixAndReadList` | `DropPrefix(A)` while group B does `List` / `Get` / iterator — bounded latency, no false-empty reads |
| `TestSharedFSM_KillDuringDropPrefix` | subprocess SIGKILL mid-DropPrefix(A) — restart cleans up, B's data intact, A's destruction either completes or restarts safely |

E2E:

| test | why |
|---|---|
| `TestE2E_ClusterPerf_SharedFSM_LoadN8` | parity with P0b matrix; no regression on the established win |
| `TestE2E_ClusterPerf_SharedFSM_LoadN32` | the C2 boot-ceiling lift; cluster forms leader and serves PUTs at N=32 with <1% error rate |

### Phase ordering

This is C2 P3, sequenced AFTER P0b measurement confirms the shared-log
direction works. If P0b shows shared-log alone unblocks load-N32 boot
and reduces idle CPU meaningfully, P3 is the natural follow-up — it
roughly doubles the win.

If P0b shows shared-log alone is enough, P3 becomes optional /
deprioritized.

### Estimate (revised after codex review of P3 design)

| step | est |
|---|---|
| Build `stateKeyspace` abstraction + unit tests | 0.5d |
| Refactor `FSM` (apply.go, 19 db ops, 9 key builders, 4 iterators) | 1d |
| Refactor `DistributedBackend` (backend.go, 15 db ops + 5+ raw iterator literals identified by codex) | 2d |
| Refactor `shard_placement.go` / `snapshotable.go` / `scrubbable.go` raw literal sites | 0.5d |
| Snapshot/Restore prefix-scoping + format v2 marker + unit tests | 1d |
| `DestroyGroupData` separation from Close + persisted-intent log | 0.5d |
| Migration refusal (5 layout permutations) + serve.go wiring + flag | 0.5d |
| Concurrency tests (Snapshot+Apply / Restore+Apply / DropPrefix+List / kill-mid-drop) | 1d |
| E2E test (perf matrix on shared-fsm) + bug fixing | 1d |
| Codex review iteration + cross-model reconciliation | 0.5d |
| **total** | **~8.5d** |

Plus the original P0b 1.5d (now ~done) → **C2 full ≈ 10d** vs original
5d estimate. The extra time is the refactor breadth (backend.go's
iterator literals, not just key-builders), the Snapshot/Restore safety
tests, and the new concurrency suite.

### Design v2 second-pass review — open issues

A second codex pass on the v2 design (post-rewrite-of-12-issues) found
13 more, several architectural. **The design is not yet ready for
implementation; this section is the working punchlist.**

Architecture:

1. **Live Raft snapshot install bypasses `FSM.Restore`.**
   `HandleInstallSnapshot` (`internal/raft/raft.go:2087,2121`) wraps the
   snapshot bytes in a `LogEntryCommand` and pushes it onto `applyCh`;
   `RunApplyLoop` (`internal/cluster/backend.go:323`) then calls
   `fsm.Apply(entry.Command)` — not `fsm.Restore`. So the prefix-scoped
   Restore design above only fixes direct `SnapshotManager.Restore`,
   not the live follower catch-up path. The Apply path also drops
   the snapshot send when `applyCh` is full (`raft.go:2121`). Fixing
   this means either routing snapshot installs through a new code
   path that calls `Restore`, or making `Apply` prefix-aware for
   snapshot-typed entries.

2. **Restore is still crash-unsafe.** Validate-first fixes
   corrupt-input wipe, but `DropPrefix` followed by a separate
   `Update` is non-atomic. A crash between the two leaves the group
   empty. Need a staging area + commit marker, or a transactional
   delete+set strategy. (`docs/architecture/badger-consolidation.md`
   restore sketch lines 622, 633.)

API and coverage:

3. `Strip` API in this doc is inconsistent: returns `[]byte` once,
   `([]byte, bool)` elsewhere. `HasPrefix` is referenced but not
   defined. Pin one signature.

4. `Strip(..., bool)` paired with the example's `if !ok { continue }`
   silently hides leaks. Replace with a panic-or-error path when the
   iteration scope guarantees the prefix should be present, plus an
   explicit "leak counter" metric.

5. **Inventory still incomplete.** Codex pass 2 found additional raw
   scan / unprefixed-key sites:
   - `internal/cluster/backend.go:2129` (`WalkObjects` body)
   - `internal/cluster/backend.go:2141` (`WalkObjects` body)
   - `internal/cluster/backend.go:729` (`GetBucketVersioning` raw `txn.Get`)
   - `internal/cluster/snapshotable.go:142,209` (snapshotable restore + blob lookup)
   - `internal/cluster/scrubbable.go:73` (already known)
   - `internal/cluster/apply.go:558` (`RecoverPending`)
   - `internal/cluster/shard_placement.go:160,243` (fallback scans)

6. **Snapshot v2 magic `GFSMSNAP` is not ambiguity-free** vs v1
   FlatBuffers payload. Low-probability, but a v1 snapshot whose
   first 8 bytes happen to spell those characters would parse as v2.
   Use a longer or self-describing framing (e.g., embed FlatBuffer
   magic + explicit version table).

Concurrency / lifecycle:

7. **DropPrefix won't starve heartbeats but will stall apply.**
   Heartbeats run from `runLeader`/`replicateToAll`, independent of
   FSM apply. The real risk is `RunApplyLoop` blocking in
   `fsm.Apply` while raft `applyLoop` eventually blocks on
   `applyCh`. Test plan must cover committed-but-not-applied state
   under DropPrefix, not just leader churn.

8. **Migration table cannot distinguish legitimate continuation
   from operator-nuked `groups/`.** "No `groups/*/badger`,
   `shared-fsm` exists → proceed" also accepts orphaned shared
   metadata after a partial start + manual dir deletion. Need a
   shared-FSM manifest at `<dataDir>/shared-fsm/manifest.json` with
   layout version + bootstrapped group IDs.

9. **`group_lifecycle.go` wiring is not solved.**
   `GroupLifecycleConfig` has no shared-FSM field
   (`internal/cluster/group_lifecycle.go:21`). `instantiateLocalGroup`
   always opens `groups/<id>/badger` (line 60). `serve.go` threads
   shared raft-log via `glc.LogStore` but no shared-FSM equivalent.
   Plus `GroupBackend.Close` closes the embedded DB
   (`internal/cluster/group_backend.go:119`) — shared-FSM needs an
   explicit ownership semantic similar to P0b's `BadgerLogStore.shared`.

10. **`DestroyGroupData` intent persistence failure unspecified.**
    What happens when meta-Raft has committed permanent removal,
    local raft is stopped, but the intent log fsync fails? Correct
    sequence: fail BEFORE `DropPrefix`, retry from durable meta
    state, use pending/complete markers in the log.

11. **SIGKILL test is not deterministic.** Badger has no
    "inside DropPrefix" hook. The deterministic substitute is
    injecting kill points around P3 phases (after intent fsync,
    before/after `DropPrefix`, before completion marker), not
    actual SIGKILL during DropPrefix.

Estimate / regression:

12. Estimate 8.5d does not cover live `InstallSnapshot` semantics,
    restore crash recovery, manifest design, DB ownership wiring,
    or deterministic fault injection. Realistic: **11-14d**.

13. Doc regressions: unreachable `_ = ver` statement after `return`
    in the Restore sketch; silent `continue` on `Strip` failure;
    `DestroyGroupData` priced at 0.5d despite needing durable
    lifecycle semantics.

### Decision: pause P3 design, run P0b measurement first

Each design pass surfaces deeper architectural issues (live snapshot
install bypass, restore crash safety, manifest design). Continuing to
iterate on the design without empirical data is high risk: P3 may not
be necessary if P0b alone unblocks the load-N32 boot ceiling, or P3's
real shape may differ depending on what the measurement reveals.

Next step is **not** more design iteration. The next concrete action
is to run the P0b perf matrix (idle-N8/16, load-N8/16/32) on an idle
host and measure:

- goroutine count: P0b vs C1-baseline at each scenario
- per-node CPU: same comparison
- load-N32 boot status: does shared raft-log alone allow it?
- workload PUT error rate at each scenario

The result determines whether P3 is the next priority or whether
something else (e.g., raft tick rate tuning, badger compaction
parameter tuning, different scaling axis) shows more leverage.

If after measurement P3 is still the right move, address the 13
issues above before any code lands.

## References

- C1 PR: #128 `perf/badger-compactor` (cheap compactor reduction baseline)
- Codex review of this design: see PR review thread linked from #128
- TODOS Phase 19 entry: "BadgerDB 인스턴스 통합" (deferred from C1)
- P0a plan + measurements: this section + `tests/e2e/cluster_perf_profile_test.go` matrix

---

**Decision needed**: approve scope and target metrics, then assign owner +
phase ordering. Do not start P2 implementation before P1 design review.
