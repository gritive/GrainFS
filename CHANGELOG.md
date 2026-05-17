# Changelog

## [0.0.226.0] - 2026-05-17 - perf(policy): lock-free CompiledPolicyStore via atomic.Pointer

### Changed
- **`policy.CompiledPolicyStore`** no longer uses `sync.RWMutex`. The
  compiled-policy map and raw-JSON map are bundled into an immutable
  `policyState` struct published via `atomic.Pointer`. `Allow` (per-S3-
  request authorization hot path) and `GetRaw` are lock-free atomic
  loads. `Set` and `Delete` are serialised by a small `writeMu` so
  concurrent admin writers merge cleanly: each clones the current
  state, applies the mutation, and atomically publishes the new
  pointer. `Delete` on a non-present bucket short-circuits without
  cloning.
- Audit follow-up: `docs/architecture/lock-free-audit.md` →
  *"internal/policy/compiled.go - compiled policy map; request
  evaluation uses short read locks."* Every authorised S3 request was
  paying that RLock acquire/release on a shared cache line.

### Performance

Apple M3, `internal/policy`, `-benchtime=10s -count=2`, 100 buckets
preloaded, parallel readers across 8 cores:

| Bench | Before (median) | After (median) | Delta |
| --- | --- | --- | --- |
| `BenchmarkParallelAllow` | 174.7 ns/op | 17.7 ns/op | **-90% latency** |
| `BenchmarkParallelAllowWithWriter` | 99.4 ns/op | 18.7 ns/op | **-81% latency** |

Allocs per call unchanged (1 alloc/op — bench input copy escape, not
related to the lock). At 8 parallel readers the prior `RWMutex.RLock`
was paying cache-line bouncing on the shared mutex word; atomic load
pays a single read of an already-warm pointer. The reader-with-writer
bench shows the same speedup, confirming the writer no longer starves
readers (writer-priority RWMutex was forcing readers to wait for `Set`
calls even though the actual map mutation is a single pointer store).

## [0.0.225.0] - 2026-05-17 - fix(packblob): BlobStore.Close no longer leaks fds or directory lock on partial failure

### Fixed
- **`BlobStore.Close`** previously returned early on the first
  `f.Close()` error (active blob or any cached read fd), leaving the
  remaining read fds open and the directory `flock` held. A subsequent
  `NewBlobStore()` against the same directory would then fail with
  `blob dir already locked by another process`. Close now runs every
  cleanup step unconditionally and returns the joined set of failures
  via `errors.Join`. The directory lock is always released. Pre-existing
  bug — surfaced as a follow-up to PR #392 advisor review.

### Tests
- Two regression tests in `internal/storage/packblob/blob_close_leak_test.go`:
  one forces the active-blob close to fail (pre-closing the underlying fd),
  one seeds two pre-closed read fds in the cache. Both assert that the
  directory lock is released afterward by opening a second `BlobStore`
  on the same directory, and the multi-fd case asserts both fd errors
  appear in the joined error message. Verified that the tests fail on
  master (dir lock leak surfaces as `resource temporarily unavailable`)
  and pass on this branch.

## [0.0.224.0] - 2026-05-17 - perf(packblob): replace PackedBackend.mu with sync.Map index

### Changed
- **`PackedBackend.mu sync.RWMutex` + `index map[string]*indexEntry`**
  replaced with `index sync.Map`. `GetObject` / `HeadObject` /
  `DeleteObject` / `PutObject` are now lock-free on the index:
  - `PutObject` uses `index.Swap` and decrements the displaced entry's
    refcount atomically.
  - `DeleteObject` uses `LoadAndDelete`-equivalent via
    `Load` + `Refcount.Add(-1)` + `CompareAndDelete` to guard against
    a concurrent `Swap` publishing a new entry under the same key.
  - `CopyObject` preserves transactional semantics with a CAS-based
    refcount increment (rejects entries with `Refcount <= 0` or at
    `MaxInt64-1`) plus a re-validation `Load` that the source entry
    is still the canonical one. Lock-free.
- Range scans (`ListObjects`, `WalkObjects`, `bucketHasPackedObjects`,
  `deleteBucketIndex`, `ListAllObjects`, `SaveIndex`) use `sync.Map.Range`
  with documented weakly-consistent semantics — listing/scan operations
  tolerate concurrent inserts/deletes appearing or not.
- Audit follow-up: `docs/architecture/lock-free-audit.md` →
  "`PackedBackend.mu` protects the packed-object index. If packed small
  object reads become a hot-path bottleneck, convert this to the same
  immutable snapshot pattern used by `CachedBackend`." PR #392's mixed
  mutex profile attributed 91.7% of remaining delay (44.81s / 48.86s)
  to `PackedBackend.PutObject`'s `RWMutex.Unlock` — trigger condition
  hit. CoW with `atomic.Pointer[map]` was rejected because the
  isolated PutObject bench showed latency is index-size-invariant
  (11µs at N=1K through N=100K) — a CoW clone of N=100K would have
  pushed PutObject from 11µs to ~1ms (~100× regression).

### Performance

Apple M3, `internal/storage/packblob`, `-benchtime=10s -count=2`.

**Headline — mutex profile (`-mutexprofile`, mixed workload):**

| Metric | Before | After | Delta |
| --- | --- | --- | --- |
| Total mutex delay | 48.86s | 245.48ms | **-99.5%** |
| `PackedBackend.PutObject` (RWMutex.Unlock) | 44.81s (91.7%) | disappears | gone |

`PackedBackend.mu` is fully eliminated from the mutex profile.
Remaining 245ms is dominated by unrelated runtime / BadgerDB system
locks. PR #392 (BlobStore readFiles) cleared 445s → 51s of
contention on `bs.mu`; this PR clears the last 48.86s on `pb.mu`,
leaving the packblob hot path effectively lock-free for index access.

**Secondary — wall-clock bench (10s × 2; tight enough to read trend but
not a 15s × 3 measurement — treat the percentages as directional, not
load-bearing — see `feedback_bench_15s_min`):**

| Bench | Before | After | Direction |
| --- | --- | --- | --- |
| `BenchmarkParallelGetWithWriter/entries=10000` | 2045 ns/op | 1867 ns/op | reader latency down |
| `BenchmarkParallelGetWithWriter/entries=100000` | 1925 ns/op | 1858 ns/op | reader latency down |
| `BenchmarkPutObjectIsolated/preload=1000-100000` | ~11.0-11.4 µs, 18 allocs | ~11.3-11.5 µs, 20 allocs | +2-3% latency, +2 allocs |

The PutObject +2 allocs / +2-3% latency cost is sync.Map's
interface-boxing overhead for the string key + *indexEntry value;
the trade is justified by reads becoming completely lock-free and
PutObject no longer competing with readers under shared mutex.

### Concurrency Semantics

Delete-vs-Put races on the same key now resolve at `Load` granularity
rather than under a single lock. The final state — the live entry
visible via `index.Load(k)` — is identical to the prior lock-based
code in every realistic interleaving: the entry the last writer
publishes wins, and the **live** entry's refcount invariant is
preserved (the racing `DeleteObject` only decrements the displaced
entry it Load'd, leaving the fresh entry untouched). `DeleteObject`'s
`CompareAndDelete` may now fail when a concurrent `PutObject` Swap'd
in a fresher entry; in that case the **displaced** entry can take a
transient negative refcount because both `DeleteObject` (on its
Load'd pointer) and `PutObject`'s `Swap` (on the returned previous
value) decrement it — that entry is already unreachable from the
index, so the negative value is never observed and the entry is GC'd
once the racing goroutines drop their pointers. Callers needing
strict atomic delete-or-replace semantics must synchronize externally.

## [0.0.223.0] - 2026-05-17 - perf(packblob): split BlobStore.readFiles cache off bs.mu

### Changed
- **`BlobStore.readFiles`** is now published as an immutable
  `atomic.Pointer[map[uint64]*os.File]` snapshot. `getReadFile` no
  longer takes `bs.mu`: the hit path is a lock-free atomic load + map
  read; the miss path performs `os.Open` outside any lock and inserts
  via a CAS-retry CoW. Concurrent first-fillers race the syscall and
  the loser closes its duplicate fd — acceptable because fills happen
  at most once per blob file. `Close()` walks the published snapshot
  and stores an empty replacement.
- Audit follow-up: `docs/architecture/lock-free-audit.md` →
  "`BlobStore.getReadFile` shares `bs.mu` with `Append`; separate when
  mixed-workload mutex profile shows contention on the read path."
  PR #389 left this open after moving compression outside the lock;
  the mixed-workload mutex profile attributed 4.42% of delay to the
  read side (and a much larger share to writer self-blocking induced
  by reader contention on the same lock).

### Performance

Apple M3, `internal/storage/packblob`, `-benchtime=10s -count=2`:

| Bench | Before | After | Delta |
| --- | --- | --- | --- |
| `BenchmarkParallelGetWithWriter/entries=10000` | 4873 ns/op, 1051 B/op, 11 allocs | 2097 ns/op, 594 B/op, 6 allocs | **-57% latency, -45% allocs/op** |
| `BenchmarkParallelGetWithWriter/entries=100000` | 4457 ns/op, 988 B/op, 10 allocs | 1997 ns/op, 600 B/op, 6 allocs | **-55% latency, -40% allocs/op** |
| `BenchmarkParallelGetSmallObjects` | 1520-1606 ns/op, 6 allocs | 1539-1698 ns/op, 6 allocs | no regression (read-only) |

Mutex profile (`-mutexprofile`, same workload): **total delay
445.19s → 51.12s (-88.5%)**. `BlobStore.getReadFile` disappears from
the profile entirely (was 19.69s / 4.42%); `BlobStore.Append`'s
self-blocking also collapses because readers no longer hold the same
lock the writer is waiting on. Remaining 51s is dominated by
`PackedBackend.mu` (RWMutex protecting the small-object index) — a
separate lock, tracked as a follow-up audit item.

Writer throughput in the mixed bench drops from ~640K to ~230K writes
during the run. This is the correct trade-off: under the prior lock
the writer was monopolising the CPU because readers were sleeping on
contention; with reads decoupled, both sides progress and reader
latency wins by 2.3x.

## [0.0.222.0] - 2026-05-17 - perf(raft): drop redundant currentConfig defensive copy from actorState.snapshot

### Changed
- **`actorState.snapshot`** no longer deep-copies `currentConfig.voters`,
  `oldVoters`, and `learners` before publishing the readState. Every
  mutation site replaces `currentConfig` wholesale (via `newSingleConfig`
  / `newJointConfig` / `applyConfigEntry` / `configHistory` restore);
  none mutate the slices or learner map in place. `Configuration()`
  builds its own fresh `[]Server` via `allVoters()`, so external callers
  never receive the published slice header. Internal readers only use
  `len` + `range` on the published slices, which is safe under
  concurrent read.
- Documents the **wholesale-replacement invariant** on `currentConfig`
  in `snapshot()`'s comment. Future changes that mutate `voters` /
  `oldVoters` / `learners` in place break this contract and must
  reintroduce the defensive copy.

### Performance

Apple M3, `internal/raft/bench_test.go`, 15s × 3 runs (median):

| Bench | Before | After | Delta |
| --- | --- | --- | --- |
| `BenchmarkProposeWait_SingleNode_NoFsync` | 974 ns/op, 663 B/op, 5 allocs | 922 ns/op, 638 B/op, 4 allocs | **-5.3% latency, -20% allocs/op** |
| `BenchmarkProposeAndCommit_3Voter` | 8211 ns/op, 3325 B/op, 39 allocs | 7921 ns/op, 3002 B/op, 33 allocs | **-3.5% latency, -15% allocs/op** |

The earlier 3-second benchtime obscured this with noise — extending to
15 seconds × 3 runs reveals a consistent ~5% latency drop and an
integer-detectable allocs/op reduction (5→4 single-node, 39→33 3-voter).
The removed allocs are small (3-element string slices) but they fire
on every Raft publish and matter once you measure long enough to see
the signal.

## [0.0.221.0] - 2026-05-17 - perf(raft): reuse propose-batch scratch slice in the actor

### Changed
- **`Node.handleProposeBatch`** no longer allocates a fresh
  `make([]command, 0, maxProposeAppendBatch)` on every proposal. The
  64-capacity slice of the wide `command` struct dominated the raft
  benchmark's `alloc_space` profile at >95% of total bytes — most batches
  only contain one command, leaving the other 63 slots paid for and
  discarded.
- The actor goroutine is the sole reader / writer of the propose path, so
  a plain `proposeCmdScratch []command` field on `Node` beats a
  `sync.Pool` here. Written slots are zeroed with `clear()` before reuse
  so stale channel and pointer references do not survive across batches.

### Performance

Apple M3, `internal/raft/bench_test.go`:

| Bench | Before | After | Delta |
| --- | --- | --- | --- |
| `BenchmarkProposeWait_SingleNode_NoFsync` | 2180 ns/op, 33438 B/op, 6 allocs | 962 ns/op, 673 B/op, 5 allocs | **-56% latency, -98% bytes** |
| `BenchmarkProposeAndCommit_3Voter` | 9421 ns/op, 36025 B/op, 40 allocs | 8209 ns/op, 3196 B/op, 39 allocs | **-13% latency, -91% bytes** |

Total `alloc_space` across the bench dropped from 91.15 GB to 4.50 GB
(20× reduction). `handleProposeBatch` no longer appears in the
top-allocators list.

## [0.0.220.0] - 2026-05-17 - perf: move blob compression outside the BlobStore.Append critical section

### Changed
- **`BlobStore.Append`** now compresses input data *before* acquiring
  `BlobStore.mu`. The mutex profile of a mixed parallel read/write workload
  showed `Append` at 94% of total mutex delay, with zstd compression running
  inside the critical section. Compression depends only on the input bytes
  and the `bs.compress` setup flag (set once at construction); it does not
  need the lock. The file write and offset update remain inside the lock —
  those preserve append ordering and cannot be moved without a different
  schema (per-blob transactions or pre-allocated extents). Encryption stays
  inside the lock because its AAD depends on the in-lock `activeID` /
  `activeOff`.
- `BlobStore.EnableCompression` docstring now spells out the
  construction-only contract: callers must set `bs.compress` before the
  BlobStore is shared with any goroutine, because the new pre-lock
  compression path in `Append` reads the flag without the mutex. Future
  contributors cannot silently race that read by flipping compression on a
  live BlobStore.

### Internal
- Adds `get_parallel_bench_test.go` with `BenchmarkParallelGetSmallObjects`
  and `BenchmarkParallelGetWithWriter`. Together they document the original
  contention (`BlobStore.Append` at 94% of mutex delay during mixed
  read/write) and the post-fix profile (`Append` still dominant at 95.8% —
  the remaining cost is the file write itself, not compression).
- Negative finding recorded: a parallel `BenchmarkParallelGetSmallObjects`
  showed `PackedBackend.mu` RLock contention below the profiler's
  significance threshold (< 0.5% of mutex delay) even at 100k entries. The
  audit's conditional follow-up for `PackedBackend.mu` ("if packed small
  object reads become a hot-path bottleneck, convert to the immutable
  snapshot pattern") is **not** triggered by current workloads. The bench
  remains as a regression guard.
- `docs/architecture/lock-free-audit.md` "Changes In This Audit" section
  records the move; the `BlobStore.mu` inventory entry is updated to note
  that compression is now outside the critical section.

## [0.0.219.1] - 2026-05-17 - docs: ADR 0014 capability plan cache pattern

### Internal
- **ADR 0014** records the storage Operations capability plan cache decision
  (`atomic.Pointer` publication, single-Generation-source invariant, independent
  per-cache generation counters, per-wrapper long-lived `*Operations`).
  Establishes the third shape in the lock-free publication pattern family
  alongside IAM whole-state CoW (ADR 0007) and worker-pointer publication
  (ADR 0012, ADR 0013). Locks in `SwappableBackend` as the sole Generation()
  source so future contributors do not silently break cache invalidation by
  adding a second source.

## [0.0.219.0] - 2026-05-17 - refactor: lock-free Operations capability plan cache

### Changed
- **Storage decorator capability plan**: `Operations.planForCall` and the ACL
  capability plan now publish through `atomic.Pointer` and validate against a
  single-source `atomic.Uint64` generation counter
  (`SwappableBackend.Generation()`). Fast path is allocation-free and
  lock-free (7.7 ns/op, 0 B/op, 0 allocs/op on Apple M3).
- **Result-shape wrappers**: `SwappableBackend`, `CachedBackend`, `wal.Backend`,
  and `pullthrough.Backend` hold a long-lived `*Operations` over their inner
  backend instead of constructing a fresh `Operations` on every
  `PutObjectWith*Result` call. `SwappableBackend.Swap` resets the cached
  `*Operations` before swapping inner so post-swap calls rebuild against the
  new inner.

### Fixed
- **Hot-swap race in `SwappableBackend.cachedOps`**: a concurrent `Swap` could
  cause a reader to store an `*Operations` wrapping the previous inner,
  silently defeating the swap. The cache now uses a generation seqlock plus
  CAS publication so a racing build is discarded and rebuilt against the
  post-swap inner.
- **Cross-cache staleness between main plan and ACL plan**: a shared `planGen`
  meant that rebuilding the ACL cache made a stale main plan look fresh. Each
  cache now tracks its own generation (`planGen`, `aclPlanGen`) and
  invalidates independently while still observing the same upstream generation
  source.

### Internal
- `NewOperations` enforces the single-`Generation()`-source invariant at
  construction and panics if more than one source is discovered in the chain.
  Adds `TestNewOperationsPanicsOnMultipleGenerationSources`,
  `TestSwappableBackendCachedOpsInvalidatedOnSwap`,
  `TestSwappableBackendCachedOpsRaceWithSwap`, and
  `TestOperationsACLPlanRebuildDoesNotMaskStaleMainPlan` to guard the
  invariants.
- `docs/architecture/lock-free-audit.md` records the change and removes
  `internal/storage/operations.go` from the mutex inventory.
- `CONTEXT.md` extends the Storage Decorator Capability Plan section with the
  caching contract and single-source invariant.

## [0.0.218.0] - 2026-05-17 - feat: S3 production compatibility and warp benchmarks

### Added
- **SSE-S3 compatibility**: S3 PUT/COPY/HEAD/GET now accepts and returns
  `AES256` server-side encryption headers, persists SSE system metadata through
  object metadata codecs, and fails closed for unsupported KMS and SSE-C modes.
- **S3 DeleteObjects compatibility**: batch delete now supports the MinIO `mc`
  client path, including idempotent missing-key responses.
- **Real S3 client smoke coverage**: e2e coverage now exercises MinIO `mc` and
  conditionally runs `s3fs`/`goofys` through a Colima VM when the Linux client
  environment is available.
- **Iceberg warp compatibility**: the REST catalog exposes the `/_iceberg`
  alias and warehouse create/delete no-op endpoints needed by `warp iceberg`.
- **Lifecycle expiration days**: bucket lifecycle expiration rules now accept
  day-based expiration semantics.

### Changed
- **S3 benchmarks**: official single-node and cluster S3 benchmarks are
  consolidated on MinIO `warp` for PUT, GET, and DELETE runs.
- **Iceberg benchmarks**: Iceberg single-node and cluster benchmarks now use
  `warp iceberg`; the default mixed workload disables update distributions so
  the benchmark runs cleanly before the next optimization pass.
- **Compatibility documentation**: S3 production compatibility references now
  distinguish supported, partial, not supported, and not planned rows, keeping
  `s3fs` and `goofys` not supported until the Colima client smoke path passes.

### Fixed
- **SSE metadata persistence**: local, packed, and cluster object metadata paths
  preserve SSE-S3 system metadata, including copy-object metadata handling.
- **Iceberg metadata shape**: generated table metadata now includes valid UUID,
  timestamp, partition, and schema fields accepted by `warp iceberg`.

### Removed
- **k6 S3 benchmarks**: legacy k6-based S3 benchmark entry points were removed
  from the official benchmark surface.
- **Custom Iceberg Go runner**: the temporary `benchmarks/iceberg_table_bench`
  runner was removed after replacing it with `warp iceberg`.

### Verification
- `make test-unit`
- `GRAINFS_BINARY=$(pwd)/bin/grainfs go test ./tests/e2e -run 'TestS3|TestIceberg|TestMultipart|TestSmoke' -v -count=1 -timeout 10m`
- `PROFILE_ROOT=/tmp/grainfs-ship-iceberg-warp-single DURATION=3s VUS=2 ICEBERG_NAMESPACE_WIDTH=1 ICEBERG_NAMESPACE_DEPTH=1 ICEBERG_TABLES_PER_NS=1 ICEBERG_WARP_COMMAND=catalog-mixed NO_BUILD=1 make bench-iceberg-table`
- `PROFILE_ROOT=/tmp/grainfs-ship-s3-warp-single WARP_DURATION=8s WARP_CONCURRENT=2 WARP_OBJ_SIZE=1KiB WARP_OBJECTS=128 WARP_OPS=put,get NO_BUILD=1 make bench`
- `git diff --check -- ':!docs/superpowers/**'`

## [0.0.217.0] - 2026-05-17 - refactor(lifecycle): lock-free executor publication

### Changed
- `lifecycle.Service` removes `sync.Mutex`. The worker handle is published
  via `atomic.Pointer[Worker]` so admin `Status` callers acquire no lock,
  matching the migration service shape from v0.0.216.0. `cancelFn` and the
  wait group stay as plain fields because only the `Run()` goroutine
  touches them through `reconcile -> start/stop`.
- `Service.running` is removed; running state is derived from
  `worker.Load() != nil`. `workerRunningForTest` uses the same derivation.
- `lifecycle.Worker` removes `sync.Mutex`. `Stats.LastRun` is now published
  through `lastRunNano atomic.Int64` (unix nanoseconds, `0` means "never
  run"), mirroring `scrubber.liveSession.doneAt`. The three cycle counters
  (`ObjectsChecked`, `Expired`, `VersionsPruned`) move from raw `int64`
  with `atomic.AddInt64` to `atomic.Int64` fields with `Add(1)` for
  type-level consistency. `Stats()` translates `lastRunNano == 0` to a
  zero-value `time.Time{}` so the existing `IsZero` admin assertion still
  holds.

### Removed
- `lifecycle.Worker.Stop()` and `lifecycle.Worker.cancel` are removed.
  They had no production callers — executor shutdown is driven by
  `Service.stop` cancelling the workerCtx, which terminates `Worker.Run`
  via its existing `<-ctx.Done()` arm.

### Added
- `docs/adr/0013-lifecycle-service-lock-free-publication.md` closes the
  reservation in ADR 0012 about lifecycle. The conclusion is the same
  lock-free publication shape as migration, extended with a worker-side
  atomic stats surface; together ADRs 0012 and 0013 establish the
  lock-free publication pattern for leader-only executor services.
- `CONTEXT.md` gains a Bucket Lifecycle Executor domain entry.

## [0.0.216.0] - 2026-05-17 - refactor(migration): lock-free worker publication

### Changed
- `migration.Service` removes `sync.Mutex`. The worker handle is published
  via `atomic.Pointer[Worker]` so `SubmitJob` callers acquire no lock to
  signal a leader-side trigger. `cancelFn` and the wait group stay as plain
  fields because only the `Run()` goroutine touches them.
- `running` is no longer carried as a separate field; it is derived from
  `worker.Load() != nil`. `workerRunningForTest` uses the same derivation.
- `migration.Worker` is unchanged. `Trigger` keeps its non-blocking
  silent-drop semantics, and the `interval` ticker remains the multi-node
  safety net for triggers that land on followers.

### Added
- `docs/adr/0012-migration-service-lock-free-publication.md` records why
  the migration service is intentionally not folded into a controller-actor
  shape (its only cross-goroutine state is pointer publication, so atomic
  publication is the correct deepening rather than an actor pass-through).
- `CONTEXT.md` gains a Migration Worker domain entry. It also clarifies
  that job-state transitions are replicated through the meta-Raft FSM while
  `JobStore.SaveCursor` writes the per-bucket pagination cursor directly
  to the leader's local BadgerDB (an existing replication gap, unchanged
  by this release).

## [0.0.215.0] - 2026-05-16 - refactor(alerts): convert Dispatcher to fire-and-forget actor

### Changed
- `Dispatcher.Send` is now fire-and-forget (returns nothing). Acceptance and
  delivery results are observable only via `AlertDispatchDroppedTotal{reason}`
  counter and the optional `Options.OnResult` callback.
- `Dispatcher` is a controller-actor + ephemeral-worker-per-alert. Dedup state
  (lastSent, inFlight) and decrypt-warn rate-limit state are owned by the
  controller goroutine; the two prior `sync.Mutex` regions are removed.
- `cluster.AlertSender` and `resourceguard.AlertsSender` interfaces drop the
  `error` return.
- `server.AlertsState` removes `sync.Mutex`; counters are `atomic.Uint64` and
  `lastFailed` is `atomic.Pointer[alertFailureSnapshot]` COW snapshot.
- Six caller sites previously wrapping `Send` in `go func() { _ = ... }()`
  now call `Send(...)` directly.

### Added
- `AlertDispatchDroppedTotal{alert_kind, reason}` counter with bounded
  3-enum reason label (`inbox_full`, `not_started`, `stopped`).
- `Options.OnResult func(Alert, error)` — preferred callback. Legacy
  `FailureCallback` parameter is mapped to OnResult internally for
  backwards compatibility but should not be used in new code.
- `Dispatcher.Start(ctx)` / `Stop(ctx)` — graceful shutdown with ctx-aware
  retry/backoff and HTTP cancellation.
- `Dispatcher.DrainForTest()` — test-only synchronization barrier.

### Documentation
- `CONTEXT.md`: "Alerts Webhook Dispatcher" glossary with honest framing
  ("ergonomic deepening + minor locality, *not* scrubber-Director-style
  locality consolidation") to prevent future reviewers from re-proposing
  the same actor conversion for the wrong reason.

### Notes
- Race window closure is best-effort: a nanosecond window between caller's
  `stopping.Load()` and `inbox` send remains formally open. Operationally
  invisible (alert volume is low, Stop runs once per shutdown), but
  documented for future reviewers.

## [0.0.214.0] - 2026-05-16 - perf: tighten Iceberg catalog benchmark hot path

### Changed

- **Iceberg table metadata reads**: clustered Iceberg catalog loads now reuse the
  freshly read table metadata after create and commit operations, avoiding a
  repeated object read on the benchmark lifecycle hot path while preserving
  follower read-forwarding behavior.
- **Iceberg benchmark failure gate**: the Go Iceberg table benchmark now exits
  non-zero when any request fails, so low-rate request failures can no longer
  be hidden behind a successful benchmark exit.
- **Iceberg benchmark connection reuse**: the Go benchmark runner now sizes its
  HTTP transport for high-throughput local runs and records failure samples in
  the JSON report, preventing macOS ephemeral port exhaustion from masquerading
  as catalog failures.

### Verification

- `go test ./benchmarks/iceberg_table_bench ./internal/server ./internal/cluster ./internal/compat ./docs/reference -count=1`
- `go test ./internal/cluster -run '^$' -bench '^BenchmarkMetaCatalogLoadTableRepeated$' -benchmem -count=3`
- `VUS=4 DURATION=20s RAMP_UP=0s RAMP_DOWN=0s make bench-iceberg-table`
- `VUS=4 DURATION=20s RAMP_UP=0s RAMP_DOWN=0s CLUSTER_WARMUP_SLEEP=1 make bench-iceberg-table-cluster`
- `PROFILE=1 VUS=4 DURATION=20s RAMP_UP=0s RAMP_DOWN=0s make bench-iceberg-table`
- `git diff --check`

## [0.0.213.0] - 2026-05-16 - feat: support production S3 compatibility core

### Added

- **S3 production compatibility guardrails**: compatibility reference docs now
  use explicit supported, partial, not supported, or not planned status values,
  with tests preventing ambiguous `not tested` claims from returning.
- **Clustered multipart listing**: `ListMultipartUploads` and `ListParts` now
  work through the clustered forwarding path so single-node and cluster e2e
  tests exercise the same multipart listing feature set.
- **Iceberg benchmark Go runner**: `benchmarks/iceberg_table_bench` provides a
  native Go benchmark runner for Iceberg namespace/table lifecycle operations.

### Changed

- **Multipart listing performance**: clustered multipart upload scans filter
  FlatBuffers payloads before string allocation, reducing allocation pressure on
  the listing hot path.
- **Iceberg benchmark scripts**: single-node and cluster Iceberg benchmark entry
  points now run the Go runner instead of the legacy k6 script.
- **Benchmark documentation**: benchmark references document the new Iceberg
  runner and keep the S3 baseline policy aligned with `warp`.

### Fixed

- **Cluster multipart routing**: forwarded multipart listing/list-parts requests
  now encode, dispatch, and decode through the cluster transport correctly.
- **Multipart create gating**: local multipart creation is gated on required
  peer transport capabilities before accepting operations that require cluster
  forwarding support.
- **Iceberg metadata writes with IAM**: Iceberg table metadata writes avoid the
  ACL write path when IAM is enabled, preventing rollback failures on fresh
  metadata objects.

### Removed

- **Iceberg k6 workload**: the old `benchmarks/iceberg_table_bench.js` workload
  has been removed from the official Iceberg benchmark path.

### Verification

- `go test ./benchmarks/iceberg_table_bench ./internal/server ./internal/cluster ./internal/compat ./docs/reference -count=1`
- `GRAINFS_BINARY=$(pwd)/bin/grainfs go test ./tests/e2e -run 'TestMultipart_List|TestCluster_Multipart_List' -count=1`
- `bash -n benchmarks/bench_iceberg_table.sh benchmarks/bench_iceberg_table_cluster.sh`
- `VUS=2 DURATION=3s RAMP_UP=0s RAMP_DOWN=0s make bench-iceberg-table`
- `VUS=2 DURATION=3s RAMP_UP=0s RAMP_DOWN=0s make bench-iceberg-table-cluster`
- `git diff --check -- ':!docs/superpowers/**'`

## [0.0.212.0] - 2026-05-16 — refactor: convert scrubber Director to single-owner actor

### Changed

- **Scrubber Director registry ownership**: `internal/scrubber/Director`의
  `sources`/`verifiers`/`sessions`/`dedup` 4종 map을 `sync.Mutex` 보호에서
  단일 controller goroutine 단독 소유로 이전했다. 외부 API 시그니처와 의미
  (FSM drop semantics, dedup 영구성, 직렬 scrub 실행)는 모두 보존되며, 운영자
  관찰 가능한 동작 변화는 없다.
- **Worker dispatch**: controller가 `Trigger`/`ApplyFromFSM` 처리 시점에
  source/verifier를 resolve해 worker에 동봉 전달한다. worker→controller
  round-trip 제거.
- **Lifecycle 안전성**: `Stop()`이 idempotent (`sync.Once`) + `Start` 없이
  호출 시 즉시 반환. `done` chan으로 controller/worker 종료 완료 대기 가능.
  `Register`는 `Start` 이후 호출 시 panic으로 시점 제약 명시.

### Fixed

- **Pre-existing staticcheck 경고 3건**:
  `internal/audit/committer.go` deprecated `builder.NewRecord` 교체 (SA1019),
  `internal/storage/eccodec/shardio.go` 불필요한 for-loop 래퍼 제거 (SA4004),
  `internal/cluster/ec.go` `ecDataShardBufferPool`을 `*[]byte`로 변경해
  `sync.Pool` boxing alloc 회피 (SA6002).

### Documentation

- `docs/architecture/scrubber-director-actor.md`: actor 통합 설계 노트
  (topology, decisions, test strategy, out-of-scope).
- `TODOS.md`: 후속 task 3건 등록 (dedup 영구성 정책, 공통 JobActor 추상화,
  Register constructor 옵션화).

## [0.0.211.0] - 2026-05-16 — perf: improve small-object S3 throughput

### Added

- **Official S3 comparison benchmark**: `make bench-s3-compat-compare` now uses
  MinIO `warp` for comparable GrainFS, MinIO, and RustFS PUT/GET runs in
  single-node and 3-node cluster modes.
- **Cluster shard packing**: clustered EC shards below the default 65,537-byte
  threshold can now use node-local append-only shard packs, matching the
  small-object optimization used by single-node packed blobs.
- **Benchmark reporting**: README and the benchmark reference now show the
  latest same-host `warp` results for both single-node and 3-node cluster runs.

### Changed

- **Small-object defaults**: `grainfs serve` now enables `--pack-threshold` and
  `--shard-pack-threshold` by default for the 64 KiB workload class instead of
  requiring explicit tuning.
- **Cluster PUT hot path**: forwarded writes now use local data voters, sized
  FlatBuffer builders, batched Raft proposals, and shard-pack append batching to
  reduce CPU and syscall overhead.
- **Object GET path**: small object responses are buffered up to a bounded
  limit, while streamed responses are capped to the expected object length so
  clients do not observe trailing read errors as object data failures.
- **Benchmark policy**: MinIO/RustFS comparisons now keep only the latest
  comparable results and no longer use the old k6 mixed workload for official
  claims.

### Fixed

- **Cluster shard-pack durability errors**: shard-pack delete tombstone append
  failures now propagate instead of being silently ignored.
- **Shard-pack recovery**: startup scanning skips corrupt or truncated terminal
  records instead of failing the whole packed-shard store.
- **Packed copy metadata**: packed `CopyObject` preserves user metadata and
  object metadata across the copy path.
- **Spooled EC metadata**: memory-spooled EC writes preserve user metadata
  through clustered object reads and HEAD responses.
- **SigV4 compatibility**: canonical request handling now accepts the encoded
  path and payload-signing patterns used by `warp`.
- **Forwarded read EOF handling**: terminal EOF from forwarded streamed reads is
  treated as end-of-body instead of surfacing as an unexpected read failure.
- **S3 bucket compatibility**: bucket-level PUT/DELETE and location queries now
  match common S3 client expectations.

### Verification

- `make test-unit`
- `make build`
- `go test ./internal/cluster ./internal/storage/packblob ./internal/storage -count=1`
- `go test ./internal/cluster -run 'TestShardService_SharedPack(DefaultDoesNotSyncEveryAppend|DeleteReturnsTombstoneWriteError|RestartSkipsCorruptRecord|WriteReadRangeDelete)|TestShardPackScanSkipsOversizedRecord' -count=1`
- `git diff --check origin/master...HEAD`
- `PROFILE_ROOT=benchmarks/profiles/review-impact-single-grainfs-20260516-171005 TARGETS=grainfs-single WARP_DURATION=30s WARP_OBJ_SIZE=64KiB WARP_CONCURRENT=16 WARP_OBJECTS=4096 WARP_OPS=put,get WARP_NOCLEAR=1 WARP_HOST_SELECT=roundrobin make bench-s3-compat-compare`
- Manual 3-node `warp` PUT/GET run with 64 KiB objects, concurrency 16, and
  `--host-select roundrobin`, archived under
  `benchmarks/profiles/review-impact-cluster-grainfs-nosync-20260516-171937`

## [0.0.210.0] - 2026-05-15 — feat: route scrub through execution actors

### Added

- **Request execution contract**: admin scrub requests now pass through a typed
  `Operation` and `Result` contract that can choose single-node or cluster
  execution without changing the response shape.
- **Cluster scrub actor runtime**: cluster-mode scrub triggers now use a bounded
  mailbox executor with retry, timeout, cancellation, metrics, and cleanup
  wiring.
- **Execution observability**: queue depth, retry, timeout, worker failure,
  aggregation failure, and job duration metrics are available for the new actor
  path.

### Changed

- **Scrub trigger routing**: `/v1/scrub` keeps the existing
  `session_id`/`created` contract while routing through the execution seam when
  cluster execution is available, with legacy proposer fallback preserved.
- **Actor execution strategy docs**: the single/cluster request architecture now
  documents package boundaries, error mapping, capacity policy, performance
  gates, and the completed implementation checklist.

### Fixed

- **Admin error mapping**: bounded execution failures now map to stable admin
  codes and HTTP statuses, including retryable admission failures, timeouts,
  cancellations, job failures, and aggregation failures.
- **Production retry policy**: scrub actor boot wiring now uses the planned
  three-attempt retry policy with a 50 ms backoff.
- **Raft apply shutdown flush**: stopping a node no longer randomly drops ready
  apply entries during shutdown, removing a flaky replication ordering failure
  in the unit lane.

### Verification

- `go test ./internal/raft -run '^TestApplyLoopShutdownFlushesReadyEntries$' -count=50 -v`
- `go test ./internal/raft -count=5`
- `go test ./internal/server/execution ./internal/server/admin ./internal/serveruntime ./internal/serveruntime/executioncluster ./internal/metrics -count=1`
- `go test -race ./internal/serveruntime/executioncluster -count=1`
- `go test -count=1 -timeout 180s -v ./tests/e2e -run 'TestE2E_ECScrubTrigger'`
- `go test ./internal/server/admin -run '^$' -bench 'BenchmarkTriggerScrub(LegacyProposer|ExecutionSeam|ExecutionActor)$' -benchmem -count=5`
- `go list ./... | rg -v '^github.com/gritive/GrainFS/tests/e2e$' | xargs go test -count=1`
- `go test ./tests/e2e -count=1 -timeout 120s -run '^TestE2E_ECScrubTrigger'`
- `git diff --check`

## [0.0.209.2] - 2026-05-15 — docs: tighten compatibility and operator guides

### Changed

- **Operator examples**: `grainfs serve` examples now include `--cluster-key`, data paths, and ports where needed so copy-paste runs fail less often.
- **Credential setup**: S3, Iceberg, drill, and runbook docs now show the current IAM service-account flow and standard AWS CLI environment variables.
- **Protocol boundaries**: NFS, 9P, and NBD compatibility docs now state the actual network protocol expectations instead of implying unsupported client paths.

### Fixed

- **English-only docs**: remaining Korean prose in tracked docs was converted to English.
- **Stop-slop pass**: predictable AI-writing phrases in the touched docs were replaced with more direct wording.
- **Runbook deployment secret**: the Kubernetes example now creates the cluster-key secret consumed by the deployment.

### Verification

- `make test-unit`
- `git diff --check -- README.md ROADMAP.md docs`
- CJK text scan across `README.md`, `ROADMAP.md`, and `docs`
- local markdown link check
- fenced code block parity check

## [0.0.209.1] - 2026-05-15 — docs: refresh compatibility and benchmark guides

### Added

- **Compatibility references**: S3, NFSv4, 9P, NBD, and Iceberg now have focused compatibility matrices that separate supported, partial, not tested, and not supported surfaces.
- **Benchmark reference**: repository benchmark targets and current local performance snapshots now live in a dedicated reference page.
- **Documentation index and user guide**: readers can start from a role-based docs index and follow a user guide instead of scanning ad hoc files.

### Changed

- **README focus**: the README now summarizes product scope, compatibility, performance, and documentation links without listing every operator or CLI detail inline.
- **Docs layout**: user, operator, architecture, and reference material now live under consistent lowercase paths, with legacy runbooks moved into the operator section.
- **NFSv4 attribute audit**: the attribute matrix now reports status and relevant caveats without source-code columns or conformance-run bookkeeping.
- **English docs cleanup**: Korean and ad hoc planning prose in the tracked docs was converted to concise English.

### Fixed

- **Moved-document links**: code comments and admin error help links now point at the relocated operator and reference docs.
- **9P platform claims**: macOS native 9P is no longer described as something users should route through a Linux VM.

### Verification

- `git diff --check origin/master...HEAD`
- `go build ./cmd/grainfs`

## [0.0.209.0] - 2026-05-15 — perf: stabilize and shorten clustered PUTs

### Added

- **PUT trace shard attribution** — benchmark traces now identify remote shard open, buffer, RPC, local write, sync, meta-index, and forwarding stages so slow PUTs can be tied to the exact cluster phase.
- **PUT trace reports by object path** — the report now groups by ingress mode, size class, forwarding mode, and object key so local leader and forwarded non-leader paths can be compared directly.
- **PUT matrix warmup** — the cluster benchmark now warms each port and object-size path before measurement, then clears warmup trace data so startup leader election no longer pollutes p99 results.

### Changed

- **Forwarded PUT routing** — coordinators now resolve cached data-group leaders before forwarding writes, reducing avoidable peer sweeps on stable clusters.
- **Small EC shard writes** — small local shards now use buffered write paths with request-context tracing, cutting local shard write and sync overhead visible in the PUT matrix.
- **Object-index waits** — forwarded object-index local apply polling now reacts faster, reducing meta-index wait time on the receiver path.
- **Mutation preflight** — indexed PUTs now derive previous-object facts from the object index when possible, avoiding extra storage preflight work on hot PUT paths.

### Fixed

- **Bucket preflight on assigned buckets** — clustered PUTs now skip the base backend bucket existence check when the meta bucket assignment is already known.
- **Forwarded PUT p99 stability** — benchmark measurement now excludes data-group leader warmup retries, dropping the observed forwarded non-leader p99 outlier from roughly 183 ms to roughly 55 ms in the measured matrix.

### Verification

- `go test ./internal/storage/... -count=1`
- `go test ./internal/cluster -count=1`
- `go test ./internal/server -run 'TestPut' -count=1`
- `go test ./... -count=1` (all non-e2e packages completed; `tests/e2e` exceeded package timeout)
- `go test ./tests/nbd_interop -count=1 -timeout=5m`
- `go test ./tests/e2e -run '^TestIAM_E2E_PolicyBypassClosed$' -count=1 -timeout=3m`
- `go test ./tests/e2e -run '^TestE2E_DynamicGroupSeeding_1to5$' -count=1 -timeout=8m`
- `make build`
- Historical PUT matrix benchmark run before the S3 benchmark suite moved to `warp`.

## [0.0.208.0] - 2026-05-15 — refactor: split server route and runtime surfaces

### Added

- **Route surface manifests**: server and admin routes now have explicit path, availability, and auth surface tables with tests covering route visibility and anonymous/authenticated policy decisions.
- **Startup recovery package**: orphan tmp and multipart startup cleanup now lives in `internal/startuprecovery`, making server bootstrap thinner and independently testable.
- **NFS export e2e coverage**: multi-node NFS export tests now wait for rolling-upgrade capability gossip and mount explicit export paths.
- **PUT trace handoff**: HTTP PUT trace stages from `0.0.207.0` now flow through the split object-write runtime without restoring the old monolithic handler file.

### Changed

- **Server composition root**: the S3 server bootstrap, options, routes, middleware, and domain handlers are split into focused files instead of concentrating the system wiring in `server.go`.
- **Admin server modules**: admin route registration, Hertz adapters, bucket/NFS/scrub/volume handlers, and dependency wiring are separated by responsibility.
- **Object and Iceberg handlers**: object reads/writes, multipart, copy, post-policy, versioning, and Iceberg REST catalog flows are split into smaller modules while preserving existing API behavior.

### Fixed

- **Range authorization ordering**: range reads that use backend `ReadAt` now authorize private objects before writing object metadata headers.
- **Heal event persistence coverage**: heal emitter tests again cover event-store persistence and nil-hub enqueue behavior.
- **NFSv4 smoke flow**: the multi-raft NFSv4 smoke test now registers the bucket as an export before mounting and reads through the pseudo-root export directory.

### Verification

- `git diff --check origin/master`
- `go test -count=1 ./internal/server/... ./internal/startuprecovery ./internal/serveruntime`
- `go build -o bin/grainfs ./cmd/grainfs`
- `go test ./tests/e2e -run 'TestE2E_MultiRaftSharding_NFSv4Smoke|TestE2E_NFSMultiExportPropagation_MultiNode' -count=1 -timeout=4m -v`
- `go test ./tests/e2e -count=1 -timeout=25m`

## [0.0.207.0] - 2026-05-15: perf: attribute and tighten PUT forwarding

### Added

- **PUT trace attribution**: local and forwarded PUT paths can now emit benchmark-only JSONL trace events for routing, forwarding, receiver, shard-write, Raft metadata, and meta-index stages.
- **PUT matrix benchmark**: local cluster benchmarks can now compare small and large PUT latency across leader and follower ports, then generate a dominant-stage report with forward attempts, leader-hint retries, forwarded bytes, shard timing, and meta-index proposal counts.
- **PUT trace regression coverage**: trace sink behavior, coordinator forwarding, receiver forwarding, sender retry fields, report dominance, trace file permissions, and Raft dispatch timing now have targeted tests.

### Changed

- **Forwarded PUT index ownership**: forwarded PUTs now commit object-index entries on the receiving data-group leader instead of also committing from the forwarding coordinator.
- **Forwarding leader handling**: small forwarded PUTs avoid the extra preflight round trip and rely on NotLeader hint retry, while streamed PUTs keep the leader preflight before sending a non-rewindable body.
- **Raft replication wakeups**: leaders now dispatch pending entries after heartbeat replies and notify follower reads sooner when commit progress advances.
- **Follower read fallback budget**: follower local-read waits now use a shorter budget before forwarding, reducing long PUT-path waits observed during benchmark runs.

### Fixed

- **Forwarded mutation safety**: forward receivers now reject mutating object operations when object-index proposal is not wired, preventing successful writes that would be missing from the global object index.
- **Trace file privacy**: PUT trace JSONL files are created owner-only, reducing accidental exposure of raw bucket and key names during benchmark runs.
- **Benchmark artifact hygiene**: generated PUT matrix summaries, trace reports, and local planning files stay outside git.

### Verification

- `go test ./internal/cluster -count=1`
- `go test ./internal/raft -count=1`
- `go test ./internal/server -count=1`
- Historical PUT trace report verification before the S3 benchmark suite moved to `warp`.

## [0.0.206.1] - 2026-05-15: fix: NFS cluster benchmark reliability

### Fixed

- **NFS cluster benchmark startup**: localhost multi-node benchmark runs now use raft addresses as node IDs, so capability gossip accepts each node and export creation can proceed.
- **NFS cluster fio setup**: clustered NFS fio workloads now disable preallocation, matching the single-node NFS benchmark and avoiding long pre-layout stalls.

### Verification

- `bash -n benchmarks/bench_nfs_cluster_profile.sh`
- `git diff --check`
- `go test ./internal/storage ./internal/cluster -run 'TestInternalETag|TestVerifyETag|Test.*ETag|Test.*SingleLocal|TestGossipReceiverReportsCapabilityEvidenceUnderRaftMemberID|TestGossipReceiverPrefersAddressBookOverDirectNodeIDMatch|TestNodeIDMatchesFrom'`
- `NODE_COUNT=3 FIO_RUNTIME=3 FIO_STREAM_SIZE=4m FIO_STREAM_JOBS=1 FIO_RAND_SIZE=1m FIO_RAND_JOBS=1 CPU_PROFILE_SECONDS=8 CLUSTER_WARMUP_SLEEP=1 ./benchmarks/bench_nfs_cluster_profile.sh ./bin/grainfs`

## [0.0.206.0] - 2026-05-15: feat: write metadata snapshots with zstd

### Added

- **Snapshot zstd benchmark coverage**: snapshot compression benchmarks now compare gzip and zstd encode/decode behavior on representative snapshot payloads.

### Changed

- **Zstd metadata snapshots**: newly written metadata snapshots now keep the `GFSNAP01` envelope and store the JSON payload with zstd in `snapshot-<seq>.json.zst` files.
- **Snapshot compatibility policy**: legacy `.json.gz` snapshot archives are now intentionally unsupported by restore flows after the zstd cutover.
- **Rolling-upgrade docs**: compatibility docs now describe the zstd payload, `.json.zst` suffix, and older-binary suffix-level invisibility.

### Fixed

- **Legacy snapshot restore response**: direct restore of an existing `.json.gz` snapshot now returns an unsupported-format conflict instead of looking like a missing snapshot.
- **Snapshot sequence safety**: upgraded nodes seed new snapshot sequence numbers from legacy `.json.gz` filenames as well as current `.json.zst` files, avoiding sequence reuse after upgrade.

### Verification

- `make test-unit`
- `go test ./internal/snapshot -count=1`
- `go test ./internal/server -run 'TestRestore(SnapshotUnsupportedFormat|LegacyGzipSnapshot)ReturnsConflict' -count=1`
- `go test -tags compat ./tests/compat -run 'TestSnapshot(LegacyGzipRejectedByCurrent|HeadSnapshotInvisibleToOlderBinary)' -count=1`

## [0.0.205.1] - 2026-05-15: fix: encrypted benchmark allocation hotspots

### Added

- **9P benchmark coverage**: single-node and clustered 9P benchmark scripts now mount bucket exports in Colima, run fio workloads, and collect pprof profiles alongside the existing S3, NFS, NBD, and Iceberg benchmark lanes.
- **9P directory creation**: 9P bucket directories can now be created and removed through directory marker objects, with mode metadata and collision checks for files, sidecar namespaces, and existing directories.

### Changed

- **Encrypted shard reads**: full-shard and range reads now stream/decrypt from files with pooled chunk buffers instead of allocating full encrypted copies or MiB-scale buffers per range read.
- **NFS fallback writes**: non-`WriteAt` backends now rebuild partial writes as streams instead of reading the whole object into memory, while rejecting unsafe huge sparse offsets.
- **Encrypted spool reads**: cluster spool encryption now reuses plaintext and ciphertext buffers across records.
- **NBD request buffers**: 128 KiB NBD requests now use the buffer pool instead of allocating per request.

### Fixed

- **NFS cluster benchmark mount**: the clustered NFS benchmark now creates the target bucket/export and mounts the bucket path instead of the pseudo-root.
- **9P directory correctness**: file rename and child mutation paths now respect directory marker locks, directory mode metadata, and existing directory collisions.
- **9P server close race**: closing an already-stopped listener no longer reports a spurious `use of closed network connection` error.

### Verification

- `make test-unit`
- `git diff --check origin/master && bash -n benchmarks/bench_9p_profile.sh benchmarks/bench_9p_cluster_profile.sh benchmarks/bench_nfs_cluster_profile.sh benchmarks/bench_nbd_profile.sh benchmarks/bench_nbd_cluster_profile.sh benchmarks/bench_iceberg_table.sh benchmarks/bench_iceberg_table_cluster.sh benchmarks/bench_two_node_s3_profile.sh && make bin/grainfs`
- Benchmarks run across S3, NFS, NBD, Iceberg, and 9P single/cluster profiles under `benchmarks/profiles/`

## [0.0.205.0] - 2026-05-15: feat: searchable durable audit lake

### Added

- **Durable S3 audit outbox**: S3 request attempts and final outcomes are persisted locally before being committed to the Iceberg audit table.
- **Searchable audit schema**: audit rows now include request ID, service account, source IP, operation, auth status, error reason, version/upload/copy context, and day partition metadata for DuckDB queries.
- **Audit health and search APIs**: localhost dashboard endpoints expose outbox health and bounded S3 audit search backed by DuckDB/Iceberg.
- **Dashboard audit view**: the web UI now surfaces audit lake health and recent S3 audit events.

### Changed

- **Audit commit safety**: follower-shipped events are durably accepted by the leader, oversized wire fields are rejected/truncated before encoding, and stale provisional attempts can later be corrected by a final request outcome.
- **Internal audit bucket reads**: Iceberg artifacts remain blocked for normal S3 access except for the generated local audit reader credential or IAM-authorized artifact reads.
- **Audit docs**: `docs/users/audit-iceberg.md` now documents retention, query examples, dashboard behavior, and the operational guarantees.

### Verification

- `go test ./internal/audit ./internal/server ./internal/serveruntime ./internal/badgerrole -count=1`
- `make bin/grainfs && GRAINFS_BINARY=$(pwd)/bin/grainfs go test -tags duckdb_e2e ./tests/e2e -run TestAuditIcebergSingleDuckDB -count=1 -v -timeout 5m`

## [0.0.204.0] - 2026-05-15: feat: storage operations console

### Added

- **Storage operations console**: dashboard UI and `/ui/api/storage/*` routes now expose protocol status, safe bucket list/create, and NFS export state without mounting destructive storage mutations.
- **Capability-gated NFS export create**: NFS export registration now uses create-only meta-Raft commands gated by `nfs_export_create_v1` evidence across current meta-Raft members.
- **Protocol bind status**: NFSv4, NBD, and 9P service status now reflects actual listener bind success or failure for the admin/dashboard surface.

### Changed

- **Dashboard safety boundary**: the browser/volume/snapshot UI no longer exposes object delete, bucket delete, volume delete, or snapshot rollback/delete actions through `/ui/api`.

### Fixed

- **Rolling-upgrade forwarding**: gated meta-Raft forwarding preserves legacy raw migration cutovers while rejecting raw gated NFS create commands.
- **Capability gossip delivery**: capability evidence survives the QUIC stream catch-all path, records evidence under raft member addresses, and refreshes gate TTL from replayed cluster gossip settings.

## [0.0.203.0] - 2026-05-15: feat: snapshot format compatibility header

### Added

- **Snapshot format envelope**: newly written metadata snapshots now carry a `GFSNAP01` header with reader and writer format integers before the existing gzip JSON payload.
- **Forward-format restore guard**: restore rejects future snapshot envelopes before mutating backend state, and the admin restore API reports unsupported formats as `409 Conflict`.
- **Snapshot compatibility coverage**: tests cover header round-trips, legacy gzip-only snapshots, future-format rejection before backend mutation, API conflict responses, and the older-binary rejection compat scenario.

### Changed

- **Legacy snapshot reads**: existing gzip-only snapshots remain readable by detecting gzip magic before envelope parsing.
- **Rolling-upgrade compatibility docs**: `docs/reference/rolling-upgrade-compatibility.md` now documents the snapshot envelope and marks `TestHeadSnapshotReject` as live.

## [0.0.202.0] - 2026-05-15: feat: require local at-rest encryption

### Added

- **Mandatory local at-rest encryption**: local object files, multipart staging, cluster spool files, packed blobs, WAL mutation bodies, Badger metadata, and replicated FSM values are now written through the `GrainFS` encryption layer.
- **Encryption key bootstrap guardrails**: solo nodes can auto-create the local key, while cluster and join mode now require an explicit shared key file to avoid accidental split-key clusters.
- **Encrypted storage coverage**: tests now cover key bootstrap policy, hidden plaintext checks, wrong-key failures, metadata tampering, WAL tail handling, packblob downgrade resistance, and encrypted object `WriteAt`/`Truncate` atomic rewrites.

### Changed

- **Packblob and WAL compatibility**: encrypted records remain backward-compatible with existing plaintext records, while encrypted flags and metadata are authenticated to reject downgrade or tamper attempts.
- **Local object mutation safety**: encrypted random writes and truncates now rewrite through a temporary file with durable rename semantics instead of partially mutating ciphertext in place.
- **Smoke and benchmark bounds**: Colima/NFS smoke scripts and encryption benchmarks were adjusted for the encrypted storage path.

## [0.0.201.0] - 2026-05-15: feat: Badger startup recovery journal

### Added

- **Badger startup recovery journal**: startup-mode decisions that happen before the incident store is available are now written under `<data>/.recovery/entries/` with node, boot, binary version, role, group, path, status, action, and scrubbed reason metadata.
- **Incident import on next healthy boot**: once the incident store opens, pending recovery journal entries are imported as deterministic Badger startup incidents and marked imported without duplicating or regressing existing incident state.
- **Recovery journal coverage**: tests now cover relative journal paths, imported markers, reason scrubbing, pre-incident meta/group startup failures, idempotent import, and startup cleanup preserving `.recovery`.

### Changed

- **Quarantine manifest writes**: recovery journal entries and quarantine manifests now share the same atomic JSON write helper.
- **Runbook guidance**: Badger startup recovery documentation now calls out `.recovery` as the pre-incident journal that should be preserved for post-boot import.

## [0.0.200.1] - 2026-05-15: test: faster cluster unit test timing

### Changed

- **Cluster single-voter test setup**: backend and group backend helpers now poll leadership every 1ms while preserving the existing 2s cap, removing avoidable 10ms sleeps across many unit tests.
- **QUIC leadership transfer test**: reduced the special election timeout from 5s to 2.5s and added receiver-side TimeoutNow observation plus a 2s transfer deadline, keeping natural election outside the pass condition.

### Verification

- `go test -count=10 ./internal/cluster -run '^TestV2QUICCluster_ThreeNode_TransferLeadership$'`
- `go test -count=1 ./internal/cluster`

## [0.0.200.0] - 2026-05-15: perf: zero-alloc SigV4, storage cache, and NBD reply hot paths

### Changed

- **S3 SigV4 verification**: cached verification now parses auth fields and credential scopes without building per-request maps/slices, and compares expected HMAC hex without allocating the expected signature string.
- **Storage cache hits**: cached object reads now reuse reader state and struct cache keys, reducing cache-hit allocation churn while preserving lock-free snapshot reads.
- **NBD replies**: fixed and structured reply headers now reuse fixed buffers instead of allocating header slices on steady-state transmission paths.

### Fixed

- **Header auth query handling**: header-signed S3 requests whose query values contain `X-Amz-Algorithm=` or whose query includes an empty presign marker are no longer misclassified as presigned URLs; encoded presign keys remain recognized through a cold fallback.
- **Cached reader reuse safety**: stale double-close after cached reader reuse can no longer reset an active reader.
- **Coverage build compatibility**: NBD reply header pooling now avoids the generic fixed-array pattern that triggered a Go coverage compiler ICE while keeping the zero-allocation budget.

## [0.0.199.0] - 2026-05-15: feat: S3 audit log lake: Phase 2 (bootstrap + metrics + --audit-iceberg flag + e2e)

### Added

- **`--audit-iceberg` flag**: `cmd/grainfs serve` now exposes `--audit-iceberg` (bool, default `true`) and `--audit-commit-interval` (duration, default `60s`), wired to `serveruntime.Config.AuditIceberg` and `AuditCommitInterval`.
- **Idempotent bootstrap**: `internal/audit.Bootstrap(ctx, catalog, backend)` creates the `grainfs-audit` bucket, `audit` namespace, and `audit.s3` Iceberg table at startup when they do not exist.
- **Prometheus metrics**: `audit_drops_total{node}` (Counter), `audit_commit_lag_seconds{node}` (Histogram), and `audit_committer_state{node}` (Gauge) update during `Committer.Run` leader/follower state changes.
- **Subsystem wiring**: `boot_phases_srvopts.go` declares `metaCatalog` before the branch and wires Emitter, Bootstrap, Committer, and the `StreamAuditShip` QUIC handler when `cfg.AuditIceberg` is enabled. The leader ship function selects targets through `MetaProposalTargets`.
- **grainfs-audit access block**: `authzMiddleware` denies direct tenant S3 API access to the internal `grainfs-audit` bucket with `403`.
- **docs/users/audit-iceberg.md**: documents quick start, flags, storage layout, schema, DuckDB query examples, cluster behavior, and Prometheus metrics.
- **e2e tests**: adds `TestAuditIcebergSingleDuckDB`, `TestAuditIcebergClusterDuckDB`, `TestAuditIcebergClusterFollowerShipDuckDB`, and `TestAuditIcebergClusterLeaderFlap` under the `duckdb_e2e` build tag with `require.Eventually` polling.
- **`mrClusterOptions.ExtraArgs`**: adds `ExtraArgs []string` to the e2e harness so tests can pass per-node serve flags.

### Fixed

- **Audit drop counter**: leader-side `followerIn` channel overflow now increments `audit_drops_total`.
- **Batch cap**: follower drain loops now cap at 65,536 events, and `DecodeS3Batch` rejects counts above 65,536 to prevent OOM.
- **Commit failure log text**: commit failures now say "events in this batch are dropped" instead of "events retained in zerolog".
- **Bootstrap error level**: audit bootstrap errors now log at `Warn` instead of `Debug`.
- **Snapshot retain race**: `AutoSnapshotter.takeAndPrune()` now prunes again after snapshot creation, avoiding transient retain-limit overrun and creation-time retain reduction races.

## [0.0.198.0] - 2026-05-15: perf: xxhash3 ETag for internal buckets (~37× faster than MD5)

### Changed

- **Internal bucket write speed**: ETag computation on `__grainfs_*` write paths (WriteAt, PutObject, spool, cluster repair) now uses xxhash3 (~25 GB/s) instead of MD5 (~650 MB/s), a ~37× improvement. S3 user buckets are unaffected and continue using MD5.
- **Hash pool reuse**: `multipart.go` upload/complete/list paths now reuse a `sync.Pool`-backed MD5 hasher, eliminating per-operation allocations.
- **Algorithm-aware ETag verification**: `VerifyETag`, `ReplicationVerifier`, and `tryRepairFromPeer` detect the algorithm from ETag length (32 chars = MD5, 16 chars = xxhash3). Existing MD5 ETags verify correctly without migration.

### Fixed

- **Scrubber repair queue exhaustion**: `ReplicationVerifier` previously misreported objects with unrecognized ETag formats (e.g. multipart composite ETags) as `Corrupt`, which could exhaust the repair queue. These are now reported as `Skipped`.
- **Hasher pool lifetime**: `PutObjectWithUserMetadata` now returns the hash pool object immediately after computing the ETag rather than holding it for the duration of the rename + metadata write.

## [0.0.197.0] - 2026-05-14: fix: lock-free storage cache audit

### Changed

- **Storage read cache locking**: `CachedBackend` now publishes immutable cache snapshots with atomic compare-and-swap instead of protecting cache state with a mutex, keeping cache hits lock-free while preserving write invalidation.
- **Lock-free audit documentation**: added a production mutex inventory and review rule that explains which locks are justified, which should stay off read hot paths, and which storage locks remain acceptable.

### Fixed

- **Volume read/write serialization**: documented `Manager.mu` as a justified mutation boundary and added regression coverage proving `ReadAt` remains serialized with concurrent `WriteAt` for block-object consistency.

## [0.0.196.0] - 2026-05-14: feat: 9P read-write support

### Added

- **9P read-write objects**: Linux v9fs clients can create, overwrite, truncate, chmod/touch, rename, unlink, and fsync bucket objects through `grainfs serve --9p-port`.
- **9P metadata sidecars**: mode and mtime are stored under a protected `__meta/` namespace that is hidden from 9P directory listings and rejected for direct 9P access.
- **Colima read-write coverage**: `tests/9p_colima` verifies mounted 9P writes, signed HTTP visibility, stale-tail truncation, metadata operations, rename, unlink, and fsync.

### Changed

- **9P write safety**: object mutations now use per-object locks, recovery write-gate protection, backend capability preferences, bounded full-object fallbacks, same-path rename protection, and service shutdown cleanup.
- **9P fallback write performance**: user-bucket writes now coalesce per-fid `WriteAt` calls and flush once on `FSync`/`Close`, avoiding full-object read-modify-write on every 4 KiB write.
- **9P serving warning**: `--9p-port` now documents that the 9P endpoint is unauthenticated and should be kept behind a trusted network boundary.

### Fixed

- **9P user-bucket read fast path**: read capability preference is separate from write preference, preserving partial reads when partial writes are disabled for user buckets.

## [0.0.195.0] - 2026-05-14: feat: rolling upgrade capability gates

### Added

- **Capability gate framework**: `internal/compat` defines capability names, hard-gate errors, active feature helpers, and `grainfs_capability_reject_total{capability,scope,severity,operation,forced}` telemetry for version-skew rejections.
- **Config epoch-bound meta-Raft gates**: meta-Raft proposals can now be admitted through a `CapabilityGate` that verifies every current voter has fresh readiness evidence before new metadata commands are proposed or forwarded.
- **Gated migration cutover hook**: bucket upstream cutover state is persisted through IAM/meta-Raft, and `POST /v1/migration/cutover` is rejected until the cluster advertises the migration cutover capability.
- **Rolling upgrade compat coverage**: mixed-version compat tests now verify migration cutover fails closed before all nodes are capable, and the runbook documents capability gate rejection response.

## [0.0.194.0] - 2026-05-14: feat: S3 audit log lake: Phase 1 (Iceberg + Parquet)

### Added

- **S3 audit event schema**: `internal/audit` now defines `S3Event` with 13 fields, `BucketName = "grainfs-audit"`, `TableS3 = "s3"`, namespace constants, and the initial Iceberg metadata JSON template.
- **Lock-free ring buffer**: channel-backed bounded ring (cap 65,536) with non-blocking `Put`, `DrainInto`, and `Drops`/`Len`; `DrainInto(nil)` now drains all events correctly.
- **Emitter with recursion guard**: `audit.Emitter` writes S3 events to zerolog stdout and the ring, while skipping events for the `grainfs-audit` bucket and `system:audit` SA to prevent recursion.
- **S3 handler emit hooks**: PUT, GET, DELETE, and LIST paths can emit through the `WithAuditEmitter` server option; nil emitters are no-ops.
- **Follower-to-leader binary encoder**: `wire.go` adds LittleEndian `EncodeS3Batch` and `DecodeS3Batch` without JSON.
- **Cluster committer**: `audit.Committer` drains leader and follower events, encodes Parquet, commits Iceberg snapshots through `CommitTable` CAS, and accepts follower events through a non-blocking `followerIn chan []S3Event` with cap 256.
- **Parquet encoder**: Arrow-go v18 plus pqarrow writes Snappy-compressed Parquet with 13 Iceberg field IDs, verified with DuckDB `read_parquet()`.
- **Minimal Avro encoder**: writes Iceberg manifest and manifest-list files as Avro Object Container Files without another dependency.
- **StreamAuditShip = 0x13**: registers the follower-to-leader audit ship QUIC stream type in `internal/transport/transport.go`.

### Fixed

- **iceberg_api.go stale error text**: Iceberg REST Catalog access without `--audit-iceberg` now returns an error message that matches the real condition.

## [0.0.193.0] - 2026-05-14: feat: NFS multi-export DX and benchmarks

### Added

- **NFS export diagnostics**: `grainfs nfs debug <bucket>` reports registry state, backend bucket existence, recent pseudo-root LOOKUPs, and available client diagnostics in text or JSON.
- **NFS multi-export observability**: Prometheus now exposes export totals, propagation latency, unknown export LOOKUPs, and revoked stateid counters, with a sample Grafana dashboard in `docs/observability/nfs-multi-export.json`.
- **NFS profiling benchmarks**: `make bench-nfs-multi` runs a bounded multi-bucket Colima/fio workload with pprof capture, per-bucket throughput, and pseudo-root READDIR latency output.

### Changed

- **NFS export CLI JSON flags**: `grainfs nfs export` commands now use `--json`, matching bucket and IAM commands, and reject `--quiet --json`.
- **Benchmark defaults**: NFS profiling workloads now use bounded default sizes and `--fallocate=none` so local profiling completes and produces usable pprof data by default.
- **NFS runbooks**: README, RUNBOOK, `docs/operators/nfs-export-lifecycle.md`, and `docs/operators/nfs-debug.md` now document export lifecycle, debugging, and benchmark workflows.

### Fixed

- **NFS export admin errors**: `bucket_not_found` and `export_not_found` return 404, `export_already_exists` returns 409, and propagation timeouts return 504.
- **NFS write lock isolation**: writes and truncates for the same object key in different buckets no longer share one lock.
- **NFS debug truthfulness**: debug output no longer claims unavailable propagation/client state as healthy, applies admin timeouts, and keeps the NFS hint sweeper closed during runtime shutdown.

## [0.0.192.1] - 2026-05-14: feat: unknown MetaCmd telemetry

### Added

- **Unknown MetaCmd visibility**: operators now get `grainfs_unknown_metacmd_total{type}` when a node ignores a raft metadata command it does not recognize or handle.
- **Rolling-upgrade alerting**: Prometheus rule `GrainFSUnknownMetaCmdIgnored` warns on ignored MetaCmd events, including first-seen counter series, and the runbook explains the version-skew response path.

## [0.0.192.0] - 2026-05-14: feat: read-only 9P2000.L server

### Added

- **Read-only 9P2000.L server**: `grainfs serve --9p-port` can expose buckets and objects over 9P for Linux/Colima clients while remaining disabled by default.
- **9P directory and object coverage**: unit tests cover bucket listing, object reads, nested slash-containing object keys via synthetic directories, aname bucket roots, and paged Readdir behavior.
- **Colima 9P harness**: `make test-9p-colima` adds an opt-in Linux mount/read smoke test lane.

### Changed

- **Fast object-key walking**: local storage now provides `WalkObjectKeys` so 9P directory listing can iterate keys without unmarshalling object metadata.

## [0.0.190.1] - 2026-05-14: feat: rolling upgrade CI compat lane (Slice 1)

### Added

- **Rolling upgrade compat test lane**: `tests/compat/` package with 6 live cross-version scenarios and 1 stubbed placeholder for the snapshot version header (Slice 3). Run with `make test-compat`; tests skip gracefully when `COMPAT_PREV_BIN` is not set.
- **Compat policy document**: `docs/reference/rolling-upgrade-compatibility.md` defines the N → N+1 rolling upgrade policy, scenario table, and developer guide for adding new compat tests.
- **Slice 4 design document**: `docs/reference/upgrade-finalize-machinery-design.md` covers the `upgrade finalize` command, StateHash FSM divergence detection, snapshot version header, and drain/rollback procedure.

## [0.0.190.0] - 2026-05-14: feat: NFSv4.1 RFC 8881 audit

### Added

- **NFSv4.1 compliance matrix**: operators can now inspect RFC 8881 Section 5.8 attribute coverage in `docs/reference/nfsv4-compliance.md`, including Done, Partial, and Skipped rows with code citations and follow-up gaps.
- **pynfs conformance scaffold**: `tests/conformance/run_pynfs.sh`, `make test-pynfs-colima`, and the conformance README provide an advisory path for running external NFSv4.1 checks against a local `GrainFS` export.
- **NFS standards documentation**: README and runbook entries now point to the compliance matrix, conformance runner, and operational expectations for advisory pynfs results.

### Fixed

- **GETATTR attribute bitmaps**: NFSv4 GETATTR now supports the third attribute bitmap word, including RFC 8881 bit 75 `suppattr_exclcreat`.
- **NFS attribute truthfulness**: `cansettime` is advertised on bit 15 instead of the deprecated archive bit, and link/symlink support attributes now report unsupported operations accurately.
- **READDIR requested attrs**: real COMPOUND READDIR requests now preserve and honor requested entry attributes instead of dropping the bitmap during XDR argument decoding.
- **Colima conformance binary**: the pynfs Colima target now builds `grainfs` inside the Linux VM so macOS host binaries are not executed in Colima.

## [0.0.189.1] - 2026-05-14: fix: bucket policy/versioning handler correctness

### Fixed

- **Bucket existence pre-check**: policy and versioning admin endpoints (`GET/PUT/DELETE /v1/buckets/{name}/policy`, `GET/PUT /v1/buckets/{name}/versioning`) now return `404 not_found` instead of a storage-layer error when the bucket does not exist. A `checkBucketExists` helper is called after the internal-bucket guard and before the storage operation.
- **Policy `ErrBucketNotFound` → 404**: `GetBucketPolicy` now maps `storage.ErrBucketNotFound` (returned by `LocalBackend` when no policy key is present) to `404 not_found` instead of `500 internal`.
- **Policy structure validation**: `AdminSetBucketPolicy` now rejects non-JSON and structurally invalid policies (e.g., top-level string instead of object) at the handler layer via `policy.ParsePolicy`, before any storage write.
- **Effect case validation**: `policy.ParsePolicy` now rejects `Effect` values other than `"Allow"` or `"Deny"`, preventing silently inoperative policies caused by case typos (`"DENY"`, `"allow"`, etc.).
- **Ghost policy on bucket delete**: `LocalBackend.DeleteBucket` now also deletes the `policy:<bucket>` BadgerDB key, so a recreated bucket with the same name does not inherit the previous bucket's policy.
- **Backward-compatible policy cache warm-up**: `Operations.GetBucketPolicy` no longer propagates `CompiledPolicyStore.Set` errors to callers; a pre-existing policy with a non-conforming `Effect` is still returned as raw bytes via the admin API while being skipped for S3 authorization (default deny), allowing operators to read and fix it.

## [0.0.189.0] - 2026-05-14: fix: meta-Raft apply result delivery

### Transport

- **FIX**: Capability exchange now enforces strict 2-byte payload length;
  truncated frames are rejected with `payload_length` reason. (F1)
- **FIX**: CE failure modes now produce distinguishable peer-visible errors
  (`version_mismatch`, `wrong_first_stream`, `payload_length`,
  `feature_unsupported`, `timeout`, `io_error`). Replaces single generic
  "capability exchange failed" close message. (F3)
- **NEW**: Prometheus metric `grainfs_transport_ce_total{role,outcome,reason}`
  emitted on every CE attempt. (F7)
- **NEW**: CE features byte has an explicit reserved-bit policy: unknown bits
  in `features` reject with `feature_unsupported`. Registry at
  `docs/reference/transport-mux-versioning.md`. (F2)
- **TEST**: Concurrent mux dial dedup race coverage added. (F6)
- **DOC**: `docs/reference/transport-mux-versioning.md`: wire format, feature registry,
  version bump policy, v1 baseline rationale. (F5)

### Fixed

- **Meta-Raft apply errors**: proposals now return FSM apply failures after the committed index applies, so callers do not report success when the replicated metadata write failed.
- **Forwarded proposal visibility**: follower-forwarded writes now wait for bounded follower-local apply before returning, preserving local read-after-write behavior without tying latency to the full caller timeout.
- **Forwarded apply error types**: non-Iceberg FSM errors now cross the follower-to-leader forwarding boundary as `MetaForwardApplyError` instead of being collapsed into service-unavailable.
- **Raft-over-QUIC test setup**: raft QUIC cluster tests now retry connection setup with shorter per-attempt dial deadlines and a wider outer retry budget, reducing full-suite connection flakes without making each failed dial stall.

## [0.0.188.0] - 2026-05-14: feat: NFS export propagation follow-up

### Added

- **Multi-node NFS export propagation**: admin export add, update, remove, and bucket-delete cascade operations now wait for the committed meta-Raft index to apply before reporting success.
- **Bucket-delete cascade coverage**: process-level E2E coverage now verifies exported bucket deletion removes the export on success and preserves it when deletion or propagation fails.

### Fixed

- **Safe exported bucket deletion**: exported bucket deletion now records a durable cleanup marker and completes the NFS export cascade after the bucket delete succeeds, so crash or cascade failures can be retried without pre-removing a live bucket export.
- **User export partial-I/O fallback**: NFSv4 user-bucket exports now honor backend `PreferWriteAt`/`PreferReadAt` hints so writes, truncate, allocate, rename, and copy fall back to object-store paths instead of internal-bucket-only fast paths.
- **Cluster E2E UDP port race**: the five-node QUIC/static E2E now binds UDP listeners atomically instead of reserving free ports before parallel test startup.

## [0.0.187.0] - 2026-05-14: feat: NFSv4 multi-export registry and routing

### Added

- **NFS export registry**: cluster metadata now stores NFS export registrations with stable fsid/generation fields, and the admin API plus `grainfs nfs export` CLI can add, update, list, and remove exports.
- **NFSv4 pseudo-root multi-export routing**: NFS clients can browse registered buckets under the pseudo-root and route file operations to the selected bucket instead of the legacy fixed bucket.
- **Read-only export enforcement**: write, create, remove, rename, setattr, allocate, deallocate, and copy operations now reject mutations against read-only exports.
- **Export lifecycle E2E coverage**: CLI lifecycle tests cover export add/update/remove JSON output, missing-bucket rejection, and fsid/generation fields.
- **Fail-closed export lifecycle**: bucket deletion now rejects exported buckets instead of best-effort cascading the export first, and multi-node clusters reject NFS export mutations until a full propagation barrier is wired.
- **`GRAINFS_LOG_LEVEL` fallback**: `grainfs --log-level` still wins when explicitly provided, otherwise the CLI uses `GRAINFS_LOG_LEVEL` before falling back to `info`.

### Changed

- **NFSv4 legacy bucket hard removal**: `__grainfs_nfs4` is no longer an internal bucket and the NFSv4 server no longer auto-creates or routes through it.
- **E2E parallelism control**: `make test-e2e` now runs per-test invocations in parallel via `E2E_TEST_JOBS` (default `2`; set `E2E_TEST_JOBS=1` for serial execution).
- **NFS metadata cache keys**: NFSv4 metadata invalidation and file metadata cache entries are now bucket-aware.

### Fixed

- **Forwarded short reads**: cluster `ReadAt` forwarding now preserves short EOF reads instead of converting them to internal errors.
- **Empty EC objects**: EC-backed user buckets now accept zero-byte object writes, matching create/truncate flows used by NFS clients.
- **Deterministic export fsid allocation**: NFS export fsid minor and generation values are now assigned during meta-Raft apply, avoiding stale local-service allocation decisions.
- **Cross-export guards**: NFSv4 rename/copy across different exports now returns `NFS4ERR_XDEV`, and destination writes use the destination bucket.
- **Stale export handles**: filehandles bound to an older export generation now expire with `NFS4ERR_FHEXPIRED`; removed exports return `NFS4ERR_ADMIN_REVOKED`.
- **Live export refresh**: Raft-applied export registry changes now refresh the running NFSv4 server snapshot instead of requiring restart.

## [0.0.186.1] - 2026-05-14: docs: DX polish: NFS/NBD/Iceberg Quick Start

### Added

- **NFSv4 Quick Start**: README now includes a Phase 7 multi-export mount guide from `grainfs nfs export add` to pseudo-root mount and `/mnt/<bucket>/`.
- **NBD Quick Start (Linux)**: README now covers Linux `nbd-client` install, `mkfs.ext4`, and mount.
- **Iceberg IAM connection note**: `docs/users/iceberg-duckdb.md` now maps `grainfs iam sa create` output (`access_key`/`secret_key`) to DuckDB SECRET values.
- **`GRAINFS_ADMIN_SOCKET` environment variable**: Quick Start now exports `GRAINFS_ADMIN_SOCKET` so later commands can omit `--endpoint`.

### Fixed

- **`--nbd-port` default**: README now lists the actual default `10809` instead of the incorrect `0=disabled`.

## [0.0.186.0] - 2026-05-14: feat: QUIC mux capability exchange handshake

### Added

- **`ProtocolVersionMux = "grainfs-mux-v1"`**: `internal/transport/version.go` now owns the single protocol version constant, and `muxALPN()` returns it.
- **`StreamCapabilityExchange = 0x12`**: mux QUIC connections now use a Capability Exchange stream as the first stream.
- **Capability Exchange handshake**: mux QUIC connections exchange two bytes (`version=0x01, features=0x00`) during setup and close with `"capability exchange failed"` on version mismatch.
- **`ceRejectionCloseDelay = 200ms`**: rejected peers get time to read the error response before `CloseWithError`.
- **Five CE tests**: adds `TestMuxALPNConstant`, `TestVersionHandshakeSuccess`, `TestMixedVersionRejection`, `TestCapabilityExchangeTimeout`, and `TestCapabilityWrongFirstStream`.

### Fixed

- **`TestQUICTransport_MuxRejectedWithoutHandler`**: simplified the test for the CE failure path.

### Verification

- `go test ./internal/transport/... -run TestVersionHandshake` PASS
- `go test ./internal/transport/... -run TestMixedVersion` PASS
- `go test ./internal/transport/...` PASS (coverage: 73.4%)

## [0.0.185.0] - 2026-05-14: fix: Colima Linux tests and NBD/NFS fast paths

### Added

- **Colima Linux test integration**: Linux-dependent NBD/NFS/direct I/O coverage now runs through Colima without Docker and is wired into `make test`.
- **NBD/NFS profiling harness updates**: benchmark scripts run directly against the host binary, support pprof/direct fio options, and record NBD write-path trace data.
- **S3 user metadata persistence**: storage and cluster object metadata now carry user metadata through PutObject/CopyObject paths.

### Fixed

- **Docker removal**: deleted Docker-based e2e/benchmark scaffolding and updated docs/scripts to use direct host binary + Colima VM clients.
- **NFSv4 COPY/READDIR/rename behavior**: fixed offset-aware COPY, READDIR attr encoding, parent cache invalidation, and internal-bucket rename writes.
- **Internal bucket partial I/O routing**: internal buckets bypass user object-index paths and hard-delete internal metadata instead of creating S3 delete markers.
- **NBD fast path capability propagation**: pull-through now forwards `PartialIO`, `PreferWriteAt`, and async put capabilities so NBD volume writes can use `DistributedBackend.WriteAt`.
- **Single-node duplicate-self topology**: routing/backend write-at checks treat repeated local peer entries as one physical voter, preserving local pwrite fast paths in single-node EC-shaped topologies.

### Verification

- `go test ./internal/storage/pullthrough ./internal/cluster ./internal/volume -run 'TestPullThrough_ForwardsPartialIOCapabilities|TestOpRouter_RouteBucket_DuplicateSelfIsOnlyVoter|TestPreferWriteAt|TestClusterCoordinator_PreferWriteAt|TestClusterCoordinator_WALWriteAtReadAt'`
- `go test ./internal/nbd -run 'Test' -timeout 60s`
- `go test ./internal/volume/dedup -run '^$'`
- `make build`

## [0.0.184.0] - 2026-05-14: feat: bucket policy/versioning admin API + CLI

### Added

- **`grainfs bucket policy get/set/delete <bucket>`**: operators can read, set, and delete S3 bucket policies through admin UDS. `set` accepts JSON from `--file <path>` or stdin (`-`).
- **`grainfs bucket versioning get/enable/suspend <bucket>`**: operators can inspect, enable, and suspend bucket versioning.
- **`bucket list` + `bucket info`**: adds the `HAS_UPSTREAM` column; `bucket info` also adds `VERSIONING`.
- **`GET/PUT/DELETE /v1/buckets/:name/policy`**: admin HTTP API; PUT returns 400 for an empty body.
- **`GET/PUT /v1/buckets/:name/versioning`**: admin HTTP API; PUT accepts only `Enabled` or `Suspended`.
- **`AdminGetBucket` response**: includes `has_upstream` and `versioning`.
- **`AdminListBuckets` response**: includes `has_upstream`.

### Fixed

- **`bucket upstream list` parsing**: fixed a client bug that tried to unmarshal a raw server JSON array into a wrapped struct.
- **PUT `/v1/buckets/:name/policy` empty body**: policy PUT now returns 400 instead of accepting a body without a `policy` field.

### Verification

- `make test-e2e -run TestBucketUpstream_CLIRoundtrip` PASS
- `make test-e2e -run TestBucketUpstream_LegacyCLI_Removed` PASS

## [0.0.183.0] - 2026-05-14: test: dynamic MR cluster E2E + clusterpb fbs fix

### Added

- **`TestE2E_TwoNodeAvailabilityTrap`**: documents that writes fail with `context.DeadlineExceeded` after a two-node quorum loss.
- **`TestE2E_DynamicGroupSeeding_1to5`**: verifies that sequential 1-to-5 node expansion through `addNode` increases shard group count according to `seedGroupCountForClusterSize(n)=max(n*4,8)`.
- **`mrCluster.addNode`**: writes `.join-pending`, starts the node, waits for HTTP readiness, and refreshes `leaderIdx`.
- **`startMRCluster` / `tryStartMRCluster`**: start clusters with dynamic sequential join, beginning at node 0. `FastBootstrap` polls shard groups instead of sleeping 8 seconds.
- **`waitForShardGroupCount`**: polls admin UDS `/v1/cluster/status` until shard group count reaches the target.
- **`liveURLs()` helper**: skips unstarted node URLs in dynamic clusters where `MaxNodes > nodeCount`.

### Fixed

- **`clusterpb` fbs schema**: added missing `MigrationJobStart/Done/Failed` enum values so `make build`/`flatc` no longer removes constants from `MetaCmdType.go`. PR #340 carries the same fix.

### Verification

- `go test -count=1 ./tests/e2e/ -run TestE2E_MultiRaftSharding` (145s, PASS)
- `go test -count=1 -race ./tests/e2e/ -run TestE2E_DynamicGroupSeeding_1to5` (344s, PASS)

## [0.0.182.0] - 2026-05-14: feat: bucket & IAM CLI DX + security hardening

### Added

- **`grainfs bucket info <name>`**: reads bucket information, including object count, through admin UDS and supports `--json`.
- **`grainfs bucket upstream` subcommands**: manage per-bucket pull-through upstream credentials with `put`, `get`, `list`, and `delete`.
- **tabwriter table output**: `bucket list`, `bucket info`, `upstream get`, `upstream list`, and `iam sa list` now print aligned tables.
- **`--json` flag**: adds a persistent `--json` flag to `bucket` and `iam` commands for scripts.
- **`GRAINFS_ADMIN_SOCKET` environment variable**: commands fall back to this endpoint when `--endpoint` is omitted.
- **User feedback messages**: `bucket create/delete` and `upstream put/delete` print clear success messages.
- **`iam sa` create/get/delete output**: SA creation prints access key and secret key tables; get shows SA details.

### Fixed

- **`AdminGetBucket` UI exposure**: removed `registerBucket` from `RegisterUI`, preventing dashboard-token holders from triggering remote `CountObjects` full scans that could starve Badger writes. Bucket admin ops stay on admin UDS.
- **upstream routing collision**: moved `GET|PUT /v1/buckets/upstream` to `GET|PUT /v1/upstreams`, fixing the Hertz static-beats-param collision where `bucket info upstream` returned the upstream list.
- **CLI hang**: added a 30 second timeout to `iamHTTPClient` so unresponsive servers do not hang the CLI.

### Verification

- `go test -count=1 ./cmd/grainfs/... ./internal/server/admin/... ./internal/serveruntime/...`
- `go list ./... | grep -v '^github.com/gritive/GrainFS/tests/e2e$' | xargs go test -count=1`: all PASS

## [0.0.181.0] - 2026-05-14: fix: ForceDeleteBucket correctness bugs

### Fixed

- **ForceDeleteBucket Badger MVCC snapshot leak**: separated scan and propose (`View -> collect refs -> propose`) so Raft proposals no longer hold MVCC snapshots for N times RTT and block Badger GC.
- **ForceDeleteBucket context propagation**: internal loops now pass `ctx`, so cancellation is honored.
- **ForceDeleteBucket multi-version object cleanup**: scans the full `obj:<bucket>/` keyspace instead of relying on `WalkObjects`, which returns only the latest version per key.
- **ForceDeleteBucket ring refcount double-decRef**: processes versioned refs first so `applyDeleteObjectVersion` clears ObjectMetaKey before unversioned refs are handled.
- **`AdminDeleteBucket` force=true `ErrBucketNotEmpty`**: concurrent writes during forced delete now return 503 retry instead of a misleading `"use --force"` message.

## [0.0.180.2] - 2026-05-14: fix: cluster benchmark and e2e latency regressions

### Fixed

- **Cluster runtime topology publication**: runtime join paths now publish cluster node topology and EC config as immutable snapshots so writes do not stay pinned to boot-time placement after nodes join. Coordinator routing/execution state now refreshes atomically with EC config.
- **Cluster benchmark harnesses**: NFS, NBD, S3, and Iceberg cluster benchmarks now use dynamic join flow, shared encryption keys, admin socket readiness checks, node log archival, configurable node counts, and profile/runtime parameters.
- **Benchmark auth and partial I/O setup**: Iceberg benchmark setup signs bucket creation with IAM credentials; NFS/NBD benchmark scripts wait for admin socket/CPU profile completion and quote runtime parameters correctly.
- **Raft log reads**: badger raft log range reads now fetch contiguous indexes directly and fail on missing or mismatched entries instead of iterator-skipping metadata keys.
- **NFSv4 backend capability checks**: NFS operations now have explicit backend capability coverage for partial I/O behavior.
- **e2e harness latency**: static cluster startup removed fixed sleeps, process cleanup terminates signal-ignoring test children immediately, S3 e2e clients disable keep-alives, and expiring-key tests poll observed expiry instead of sleeping.
- **IAM plaintext secret test scope**: the no-plaintext-secret e2e check now scans the IAM control-plane `meta_raft` persistence path instead of unrelated data-plane directories.
- **Small Badger metadata DBs**: `SmallOptions` now caps value log files at 64 MiB to reduce test/runtime metadata store footprint.
- **Auto-snapshot hot reload**: disabled snapshot polling idle interval reduced from 5s to 1s, bounding cluster config hot-reload latency.

### Verification

- `go test -count=1 ./internal/badgerutil ./internal/iam ./internal/snapshot ./internal/cluster ./internal/nfs4server ./internal/serveruntime ./internal/raft`
- `go build -o bin/grainfs ./cmd/grainfs`
- `GRAINFS_BINARY=$PWD/bin/grainfs go test -json -short -count=1 -timeout 5m ./tests/e2e`: PASS, 50.658s
- `go list ./... | grep -v '^github.com/gritive/GrainFS/tests/e2e$' | xargs go test -count=1`

## [0.0.180.1] - 2026-05-13: fix: runbook bootstrap procedure and snapshot audit log

### Fixed

- **Bootstrap docs**: RUNBOOK deployment section now documents direct host binary startup and host-side `admin.sock` bootstrap.
- **K8s bootstrap**: RUNBOOK K8s section now documents admin SA creation after first deploy with `kubectl exec deploy/grainfs -n grainfs -- grainfs iam sa create admin`.
- **snapshot-interval / snapshot-retain audit log**: `ClusterConfigPatch` now includes `SnapshotInterval` and `SnapshotRetain` in the audit dict when the FSM applies them.

## [0.0.180.0] - 2026-05-13: feat: bucket and IAM admin API plus bucket CLI

### Added

- **Bucket admin API**: added admin UDS REST endpoints for bucket create (`POST /v1/buckets`), list (`GET /v1/buckets`), and delete (`DELETE /v1/buckets/:name?force=true`). `--force` can delete non-empty buckets.
- **`grainfs bucket create/list/delete` CLI commands**: operators can manage buckets directly through admin UDS. `grainfs bucket delete --force <name>` removes all objects before deleting the bucket.
- **IAM admin handlers use the volume pattern**: replaced `iam_admin.go` / `bucket_admin.go` with hertz adapter `registerIAM` / `registerBucket`, using pure handlers plus thin adapters for easier unit tests.
- **`IAMService` / `BucketOps` interfaces**: `admin.Deps` now references interfaces instead of concrete types so tests can use fakes.
- **`ForceDeleteBucket`**: added to the `Backend` interface and implemented by LocalBackend, Operations, DistributedBackend, SwappableBackend, PackedBackend, and RecoveryWriteGate.

### Fixed

- **S3 ListBuckets internal bucket filtering**: S3 ListBuckets no longer returns `__grainfs_*` internal buckets.
- **admin HTTP 403 restoration**: wildcard grant denial now returns 403 instead of 500 by restoring `statusForCode("forbidden")`.
- **`AdminCreateBucket` bucket-name validation**: rejects names with slashes, uppercase letters, or special characters, preventing Badger key collisions and LocalBackend path traversal. Adds `storage.ValidBucketName`.
- **`AdminDeleteBucket` internal bucket guard**: force-delete on `__grainfs_*` buckets now returns 403 Forbidden.

### Changed

- Creation endpoints such as `POST /v1/iam/sa` now return **201 Created** instead of 200, matching RFC 9110.

## [0.0.179.0] - 2026-05-13: chore: remove non-EC object write path

### Removed

- **Non-EC write path** (`putObjectNxSpooled`, `putObjectNxSpooledAsync`, `writeSpooledReplicaShardStream`)
  eliminated. All object writes now go through EC storage exclusively. Clusters that do not have
  a `ShardService` configured (EC not active) will receive a clear error on write rather than
  silently falling back to a replication-only path.
- `ReplicationSkippedTotal` Prometheus metric removed (no remaining callers after Nx path deletion).
- `shardWriter` and `shardBufferedWriter` interfaces removed along with their only implementations.

### Changed

- `CreateMultipartUpload` guard relaxed for direct `DistributedBackend` callers: missing placement
  context is now permitted when `bypassBucketCheck` is false (resolves to `group-0` at write time).
  `GroupBackend` callers with `bypassBucketCheck=true` still receive an error for missing placement.
- `PutObjectAsync` simplified to a thin wrapper around `putObjectECSpooled`; returned `commitFn` is
  always a no-op for API compatibility.
- `PeerUnhealthy` metric help text updated to reflect EC stripe degradation (not N-way replication).

## [0.0.178.0] - 2026-05-13: fix: PromoteToVoter orphan recovery in Raft v2 becomeLeader

### Fixed

- **`recoverOrphanedPromote()`** added to `internal/raft/membership.go`, called from
  `becomeLeader()` after `recoverInFlightJoint()`. Handles the crash scenario where the
  prior leader committed Stage-1 (`ConfChangePromoteStage1`: drops target from learners)
  but crashed before appending Stage-2 (`LogEntryJointConfChange`). The orphaned target
  is left in neither voters nor learners, blocking it from participating in consensus.
- Recovery synthesises `pendingSingleConf` (pointing to the Stage-1 log index) and
  `pendingPromote` so the existing `advanceSingleConfPhase` machinery dispatches Stage-2
  on the new leader. Committed Stage-1 state at `becomeLeader` time drives the call
  inline; otherwise `applyCommitted → advanceSingleConfPhase` fires it.
- `matchIndex`/`nextIndex` for the orphaned target is seeded to
  `(0, lastLogIndex+1)` when absent: the normal path seeds these when the target joins
  as a learner, but `becomeLeader` skips it since the target is no longer in
  `currentConfig.learners` after Stage-1.
- **`handleCreateSnapshot` snapshot guard** (`internal/raft/snapshot_actor.go`): refuses
  to compact the log past the Stage-1 index while `pendingPromote` is in-flight. Without
  this guard, a periodic FSM snapshot taken between Stage-1 commit and leader crash would
  erase the Stage-1 log entry, silently disabling `recoverOrphanedPromote` on the new
  leader. Error message instructs the operator to retry after Stage-2 commits.

### Notes

- **MetaRaft.Join operator action**: `recoverOrphanedPromote` completes the Raft membership
  promotion but `ProposeAddNode` (the `MetaNodeEntry` write that follows `PromoteToVoter`
  in `MetaRaft.Join`) never ran on the crashed leader. After recovery, the operator must
  re-issue `Join` for the orphaned target to register it in the meta-Raft node table.

### Verification

- `go test -race ./internal/raft/ -run TestPromoteToVoter_OrphanRecovery -count=20`: all PASS
- `go test ./internal/raft/ -timeout 120s -count=1`: all PASS (63 s, 63 tests)

## [0.0.177.0] - 2026-05-13: fix: RouteObjectWrite preserves forward peers when self is leader

### Fixed

- `OpRouter.RouteObjectWrite` now populates `RouteTarget.Peers` even when
  `SelfIsLeader` is true. Previously, `routeGroup` short-circuited and left
  `Peers` empty, so if leadership changed between routing and execution the
  write had no forward candidates. `RouteBucket` still uses the short-circuit
  path (peers empty on leader): only the object-write path resolves peers.

### Verification

- `go test -count=3 ./internal/cluster/ -run TestOpRouter_Route`: all PASS
- `go test -count=1 ./internal/cluster/`: all PASS

## [0.0.176.0] - 2026-05-13: feat: SendTimeoutNow QUIC RPC (leader transfer)

### Added

- `SetTimeoutNowTransport` on `RaftNode` interface and `raftNodeAdapter`/`raftTransportBridge`;
  uses `atomic.Pointer[timeoutNowFn]` for lock-free late binding. Returns `ErrNotImplemented`
  when not wired (nil pointer), same fallback contract as `SendInstallSnapshot`.
- `sendTimeoutNow` + `SetTimeoutNowTransport` on `RaftQUICRPCTransport`; wire format is
  byte-identical to the v1 QUIC codec using a new `v2RPCTypeTimeoutNow` message type.
- `v2RPCTransport.SetTimeoutNowTransport()` call in `serveruntime.Run`; logged as
  "raft v2: QUIC RPC transport wired (TimeoutNow enabled)".

### Fixed

- `TransferLeadership` (Raft §3.10) now works end-to-end over QUIC in multi-node v2 clusters.
  Previously `SendTimeoutNow` returned `ErrNotImplemented`, causing the transfer target to miss
  the TimeoutNow signal and rely on the natural [T, 2T) election window instead.

### Verification

- `go build ./...`
- `go test ./internal/cluster/ -run TestSendTimeoutNow` (unit: ErrNotImplemented when unwired)
- `go test ./internal/cluster/ -run TestSetTimeoutNowTransport` (unit: nil-bridge no-panic)
- `go test ./internal/cluster/ -run TestV2QUICCluster_ThreeNode_TransferLeadership -count=5`
  with ET=5s discriminator: new leader appears within 2s, proving TimeoutNow fired.

## [0.0.175.0] - 2026-05-13: fix: eliminate peerHealth race in ecObjectReader goroutine drain

### Fixed

- Moved `peerHealth.MarkHealthy`/`MarkUnhealthy` calls from spawned shard-fetch
  goroutines to the main goroutine in `ecObjectReader.readShards`. Previously,
  k-of-n early exit could leave a goroutine still executing `MarkUnhealthy` while
  the caller already read `health.unhealthy`, producing a DATA RACE under
  `-race`. The fix encodes peer state (`peer`, `peerOK`, `canceled`) in
  `shardResult` and processes it in `applyShardResult` and the drain loop -
  both running on the single main goroutine.

### Verification

- `go test -race -count=100 ./internal/cluster/ -run TestECObjectReader_ReadObject_MarksUnhealthyPeerOnFetchError`: 100/100 PASS, 0 DATA RACE
- `go test -race -count=100 ./internal/cluster/ -run TestECObjectReader_ReadObject_MarksHealthyPeerOnSuccess`: 100/100 PASS, 0 DATA RACE

## [0.0.174.0] - 2026-05-13: fix nbd cow snapshot cli flags

### Fixed

- nbd cow snapshot cli flags

## [0.0.173.0] - 2026-05-12

### Fixed

- fix raft quic e2e raft identity wiring

## [0.0.172.0] - 2026-05-12

### Fixed

- fix cluster leader route short circuit

## [0.0.171.0] - 2026-05-09

### Fixed

- raft: restore raft v2 node consensus test (#316)

## [0.0.170.0] - 2026-05-09

### Added

- raft: add dead peer detection via PeerHealth for QUIC transport (#315)

## [0.0.169.0] - 2026-05-08

### Fixed

- raft: fix quic transport race condition in peer health tracking (#313)

## [0.0.168.0] - 2026-05-08

### Added

- raft: implement basic QUIC transport for Raft v2 (#310)

## [0.0.167.0] - 2026-05-06

### Added

- raft: implement hashicorp/raft adapter for Raft v2 (#308)

## [0.0.166.0] - 2026-05-02

### Added

- grpc: implement basic gRPC transport for Raft v2 (#306)

## [0.0.165.0] - 2026-04-30

### Added

- raft: implement Raft v2 node (#303)
