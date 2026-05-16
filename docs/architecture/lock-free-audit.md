# Lock-Free Audit

`GrainFS` defaults to lock-free reads and narrow writer serialization. A mutex is
acceptable only when it protects a real multi-object invariant, protocol
ordering rule, file descriptor/stream ordering rule, or a short writer-side
copy-on-write publish step.

## Changes In This Audit

- `internal/storage.CachedBackend` no longer has a mutex. Cache state is an
  immutable snapshot published with `atomic.Pointer` and `CompareAndSwap`.
  Cache reads do not acquire locks.
- `internal/volume.Manager.ReadAt` keeps `Manager.mu` while reading block data.
  This is a justified serialization boundary: volume metadata, live maps, and
  physical block objects are not versioned independently, so snapshotting only
  metadata/live_map is not enough to make concurrent reads and writes safe.

## Accepted Lock Categories

- **COW writer serialization:** Writers clone small state and publish an
  immutable snapshot or update an atomic pointer. Readers stay lock-free.
  Examples: IAM store, receipt routing cache, VFS stat/dir caches, node stats.
- **Protocol ordering:** The lock is part of a wire protocol or stream contract.
  Examples: raft stream send serialization, NFSv4 session slot replay cache,
  9P per-object read-modify-write locks.
- **FSM/state-machine consistency:** A single writer or low-frequency control
  path updates multiple maps that must be observed atomically by readers.
  Examples: meta FSM, ring store, migration queues, scrub director state.
- **Shard-local cache serialization:** A cache shard owns a small mutable LRU.
  This is accepted only when sharded and documented with a measured or reasoned
  rejection of actor/channel overhead. Examples: block cache and EC shard cache.
- **Durable append/file offset serialization:** A writer lock protects an
  append-only file, active blob file offset, WAL segment, or ordered flush.
  The lock must not cover unrelated metadata or network I/O.
- **Short-lived goroutine fan-in:** A local mutex protects slices/maps populated
  by concurrently launched goroutines in one function call.

## Remaining Notable Locks

- `internal/volume.Manager.mu` is still a mutation mutex. It protects cached
  volume metadata, live map read-modify-write sequences, and `ReadAt` versus
  concurrent mutation. `ReadAt` intentionally remains inside this boundary
  because writes can update the same physical block object that a read is about
  to fetch. Splitting this further requires a per-volume/per-block transaction
  or immutable object-version design.
- `internal/storage/packblob.BlobStore.mu` serializes the active blob file and
  offset. This is justified for append ordering, but compression must stay
  outside the critical section if it becomes visible in mutex profiles.
- `internal/storage/packblob.PackedBackend.mu` protects the packed-object index.
  Reads hold it only to copy an index entry before blob I/O. If packed small
  object reads become a hot-path bottleneck, convert this to the same immutable
  snapshot pattern used by `CachedBackend`.
- `internal/storage/wal.WAL.mu` protects the active WAL file and segment
  rotation. The background writer owns the normal path; synchronous fallback
  uses the same lock only when the channel is full.

## Inventory

This inventory covers production Go files matched by:

```bash
rg -n "sync\.(Mutex|RWMutex)" internal cmd --glob '*.go' --glob '!*_test.go'
```

### COW Writer Serialization

- `internal/iam/store.go` - single-applier IAM COW store; reads use
  `atomic.Pointer`.
- `internal/receipt/routing.go` - rare gossip updates clone a routing snapshot;
  receipt lookups are lock-free.
- `internal/vfs/vfs.go` - stat/dir cache writers clone maps and publish through
  atomic pointers; `grainFile.mu` only preserves the `io.ReaderAt` contract for
  concurrent calls on one open file handle.
- `internal/cluster/nodestats.go` - writers publish node-stat snapshots; read
  hot paths load atomically.

### Protocol Or Stream Ordering

- `internal/raft/raft_conn.go` - serializes frame writes on one QUIC stream.
- `internal/raft/heartbeat_coalescer.go` - serializes pending heartbeat batch
  assembly and per-batch reply fan-out.
- `internal/raft/group_transport_quic.go` - protects mux connection registry.
- `internal/raft/rpc.go` - protects in-process test RPC registry.
- `internal/transport/quic.go` - protects QUIC connection maps and handler
  registration; identity reloads use atomic snapshots.
- `internal/nfs4server/state.go`, `lookup_hint.go`, `lookup_ring.go`,
  `server.go` - protect NFS filehandle/session/replay/server connection state.
- `internal/p9server/locks.go`, `server.go` - per-object RMW locks and server
  connection tracking.

### FSM And Control-Plane Consistency

- `internal/cluster/meta_fsm.go` - one raft apply writer, many readers, atomic
  snapshot/restore boundaries.
- `internal/cluster/meta_raft.go` - apply notification channel swaps, per-index
  apply errors, and Iceberg waiters.
- `internal/cluster/apply.go` - migration and commit hook registration against
  raft apply.
- `internal/cluster/ring_store.go` - current ring and refcounts must change
  together.
- `internal/cluster/invalidator.go` - cache invalidator registry snapshotting.
- `internal/cluster/peer_health.go`, `rotation_state.go`,
  `capability_gate.go`, `circuit_breaker.go`, `disk_collector.go`,
  `migration_queue.go`, `rebalance_executor.go`, `meta_join.go` - low-frequency
  control-plane state with multi-field invariants.
- `internal/cluster/backend.go` - per-shard RWMutexes for scrubbable shard
  read/write exclusion; local fan-in mutex for concurrent EC writes.
- `internal/cluster/cluster_coordinator.go` - local fan-in mutex for concurrent
  peer scrub status collection.
- `internal/cluster/ecspike/ecspike.go` - local spike tool fan-in state.
- `internal/serveruntime/*registry.go`, `boot_phases_storage_runtime.go` -
  boot/runtime registries and one-shot startup ownership tracking.

### Storage Mutation Boundaries

- `internal/volume/volume.go` - mutation mutex for volume metadata/live-map and
  read/write block-object consistency; `ReadAt` intentionally remains
  serialized with mutations.
- `internal/volume/dedup/dedup.go`, `dedup/snapshot.go` - serializes Badger
  dedup refcount transactions that can conflict across volumes sharing a hash.
- `internal/storage/wal/wal.go` - active WAL file and segment rotation.
- `internal/storage/packblob/blob.go` - active append-only blob file and offset.
- `internal/storage/packblob/packed_backend.go` - packed-object index; held only
  for index lookup/update, not blob reads.
- `internal/storage/pullthrough/resolver.go` - upstream client cache; hits take
  read lock, rotations rebuild under write lock.
- `internal/storage/operations.go` - cached operations plan generation.

### Service State And Admin Surfaces

- `internal/policy/compiled.go` - compiled policy map; request evaluation uses
  short read locks.
- `internal/resourcewatch/registry.go` - registered DB handle list; snapshots
  are copied before GC work.
- `internal/nfsexport/store.go` - service job/config store with low-frequency
  writes. The lifecycle and migration services no longer carry mutexes; their
  worker handles are published via `atomic.Pointer[Worker]` per ADR 0012 and
  ADR 0013.
- `internal/scrubber/director.go`, `scrubber/scrubber.go` - scrub session
  state and last-status snapshots.
- `internal/server/alerts_api.go`, `sse_hub.go`, `internal/alerts/webhook.go`
  - alert/webhook/SSE state; hot alert tracking uses atomic/CAS where needed.
- `internal/receipt/store.go`, `keystore.go` - receipt write buffer draining and
  key rotation state.
- `internal/dashboard/token.go` - dashboard token rotation and constant-time
  verification snapshot.
- `internal/metrics/readamp/readamp.go` - optional simulator LRU; disabled path
  returns before touching the mutex.
- `internal/cache/blockcache/blockcache.go`,
  `internal/cache/shardcache/shardcache.go` - accepted sharded cache locks.

## Review Rule

New mutex use must answer:

1. What invariant does the lock protect?
2. Why are atomic snapshots, `sync.Map`, channel ownership, or per-key
   semaphores insufficient?
3. Is backend/network/disk I/O outside the critical section?
4. Is the lock outside the read hot path, or sharded if it is on a hot path?
5. Which test or benchmark would catch contention or missed notification bugs?
