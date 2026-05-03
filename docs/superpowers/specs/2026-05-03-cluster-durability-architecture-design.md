# Cluster Durability Architecture Design

## Context

The current protocol layering contract says S3, NFSv4, NBD, and Iceberg all pass through `storage.Backend`, then through `ClusterCoordinator`, then into the meta-Raft control plane and data Raft groups. Recent protocol-contract work closed several gaps, but three architecture risks remain:

1. Internal bucket partial IO (`WriteAt`, `ReadAt`, `Truncate`) can still bypass data-group Raft and peer replication.
2. Multipart Upload part bodies are stored as leader-local files while MPU metadata is replicated through Raft.
3. The storage interface has no request context, so cancellation, shutdown, and stream deadlines do not propagate consistently.

This design keeps a durability-first product goal, but implements the large context boundary change as a mechanical first slice so behavior changes stay reviewable:

1. Convert the storage boundary to context-first signatures without changing behavior.
2. Make internal bucket partial IO cluster-safe for N x replication.
3. Add EC-safe partial overwrite for internal bucket objects.
4. Move MPU parts to a durable distributed staging path.
5. Add behavioral cancellation coverage for the new paths.

## Goals

- Preserve data across data-group leader failure and leadership transfer.
- Keep NFSv4/VFS/NBD behavior on the same distributed object storage contract as S3.
- Avoid putting large or frequently written object bytes directly into Raft logs.
- Keep implementation incremental enough to ship as a sequence of focused PRs.
- Make request cancellation and shutdown deadlines first-class storage boundaries.

## Non-Goals

- Change user-visible S3 Multipart Upload semantics beyond making part staging durable.
- Add a second control-plane service.

## Approach

Use five focused slices.

1. **Mechanical context migration:** convert `storage.Backend` itself to context-first method signatures so cancellation and deadlines are an architecture boundary. This slice should be behavior-preserving.
2. **Internal partial IO durability for N x replication:** add data-group commands and forwarding for `WriteAt`, `ReadAt`, and `Truncate`. Local leader paths must no longer mutate internal object metadata without Raft.
3. **Internal partial IO durability for EC:** support parity-safe partial overwrite on erasure-coded internal objects.
4. **Durable MPU staging:** store MPU part bodies as internal distributed objects instead of process-local files.
5. **Behavioral context cancellation:** remove remaining request-path background contexts and add cancellation tests for the new durability paths.

## P0: Mechanical Context Migration

### Problem

`storage.Backend` has no context parameter. Cluster paths therefore use `context.TODO()` and `context.Background()` in forwarding, shard IO, EC fanout, and Raft proposals. Client cancellation, shutdown drain, and QUIC stream cancellation do not reliably stop in-flight storage work.

### Design

Convert `storage.Backend` to context-first method signatures:

```go
type Backend interface {
    CreateBucket(ctx context.Context, bucket string) error
    HeadBucket(ctx context.Context, bucket string) error
    DeleteBucket(ctx context.Context, bucket string) error
    ListBuckets(ctx context.Context) ([]string, error)

    PutObject(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*Object, error)
    GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *Object, error)
    HeadObject(ctx context.Context, bucket, key string) (*Object, error)
    DeleteObject(ctx context.Context, bucket, key string) error
    ListObjects(ctx context.Context, bucket, prefix string, maxKeys int) ([]*Object, error)
    WalkObjects(ctx context.Context, bucket, prefix string, fn func(*Object) error) error

    CreateMultipartUpload(ctx context.Context, bucket, key, contentType string) (*MultipartUpload, error)
    UploadPart(ctx context.Context, bucket, key, uploadID string, partNumber int, r io.Reader) (*Part, error)
    CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []Part) (*Object, error)
    AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error
}
```

Optional extension interfaces also become context-first:

```go
type PartialIO interface {
    WriteAt(ctx context.Context, bucket, key string, offset uint64, data []byte) (*Object, error)
    ReadAt(ctx context.Context, bucket, key string, offset int64, buf []byte) (int, error)
    Truncate(ctx context.Context, bucket, key string, size int64) error
}
```

This slice is behavior-preserving. It changes signatures, updates call sites, passes existing request/service contexts where available, and uses explicit process-root contexts only where there is no request scope.

### Required Behavior

- No behavior changes to routing, replication, MPU, snapshots, lifecycle, cache, or protocol semantics.
- No new `context.TODO()` or `context.Background()` is introduced inside request handling, forwarding, shard IO, or Raft proposal paths except at explicit process/service roots.
- Compile-time assertions cover every backend/wrapper after the interface migration.

### Tests

- Existing full non-e2e test suite passes after the interface migration.
- Focused tests cover wrapper context pass-through for WAL, cache, pull-through, packblob, recovery gate, and `ClusterCoordinator`.
- Cancellation tests can be added where existing infrastructure already exposes blocking fake backends, but deeper behavioral cancellation belongs to P4.

## P1: Internal Bucket Partial IO Durability

### Problem

`ClusterCoordinator.WriteAt` and `Truncate` route internal bucket writes to the local group leader when possible. The target `DistributedBackend.WriteAt` and `Truncate` are explicitly single-node fast paths: they write the local file and update Badger metadata without a Raft proposal or peer replication. In cluster mode this means NFSv4/VFS writes can disappear after leader failure or leader transfer.

The non-leader fallback is also too expensive: it reads the whole object, modifies a byte range, and writes the whole object back.

### Design

Add partial IO as first-class data-group operations:

- `CmdWriteAtObject`
- `CmdTruncateObject`
- `ForwardOpWriteAt`
- `ForwardOpTruncate`
- `ForwardOpReadAt`

`CmdWriteAtObject` records:

- bucket
- key
- offset
- size after write
- content type
- version ID, fixed to `current` for VFS/NFS internal buckets
- modification time

`CmdTruncateObject` records:

- bucket
- key
- target size
- version ID, fixed to `current`
- modification time

Bytes do not go into Raft. The leader first writes data through a replicated internal-object path, then commits metadata through Raft. For N x replication, the leader writes the fixed `current` object path locally and forwards the changed bytes to peers before proposing metadata.

Metadata commit requires a **data quorum**: the leader must durably apply the exact partial operation to a majority of the data-group voters before proposing `CmdWriteAtObject` or `CmdTruncateObject`. `WriteAt` quorum replication sends `(bucket, key, versionID=current, offset, bytes)`; `Truncate` quorum replication sends `(bucket, key, versionID=current, size)`. This aligns the data-write acknowledgement set with Raft quorum intersection, so after a leadership transfer at least one node in the new committed majority should have the object bytes. Best-effort peer replication is not sufficient for this path.

The first implementation must not mix range-op replication with full resulting-object replication. Full-state rewrite can be added later as a repair or compaction primitive, but the correctness path uses one operation model so ordering, sparse holes, EOF behavior, and partial failures are testable.

If data quorum cannot be reached, the operation fails before metadata commit. Any partial writes outside the quorum decision are treated as orphaned local data and are cleaned up by retry/overwrite or scrubber-style repair.

`ReadAt` should no longer fetch the full object on non-leaders. Non-leader requests forward `ForwardOpReadAt` to the leader, and the leader returns only the requested byte range plus EOF/size semantics.

Partial IO quorum replication must be bounded. The leader should cap concurrent in-flight partial IO quorum operations and return the existing backpressure-style error when saturated, rather than queueing unbounded NFS/NBD writes. The first implementation should not add write-behind or batching; those need a separate ordering/fsync design. It should include a benchmark gate for representative 4 KiB and 128 KiB internal bucket writes so the regression from the previous local fast path is visible before shipping.

### Required Behavior

- Local leader `WriteAt` and `Truncate` create Raft-visible metadata.
- Non-leader `WriteAt`, `ReadAt`, and `Truncate` forward to the data-group leader.
- The old full-object RMW fallback is removed from the cluster path.
- Direct unreplicated `DistributedBackend.WriteAt` and `Truncate` remain available only for explicit single-node/local mode.
- Restart, Raft snapshot/restore, and leadership transfer preserve internal object size and latest metadata.

### Tests

- Local leader `WriteAt` followed by follower/new-leader `ReadAt` returns the written range.
- `Truncate` updates size metadata and survives Raft snapshot/restore.
- Non-leader `WriteAt` uses `ForwardOpWriteAt`, not `GetObject` plus `PutObject`.
- `ForwardOpReadAt` returns partial data and correct EOF behavior.
- Cluster-mode direct calls cannot silently succeed through the unreplicated single-node fast path.
- Integration/e2e gate: write a partial range through the cluster path on leader A, transfer or kill that data-group leader, then read the range from leader B and verify both bytes and object size.
- Benchmark gate: compare 4 KiB and 128 KiB internal bucket `WriteAt` latency/throughput before and after quorum replication, and document the tradeoff in the PR.

## P2: EC-Safe Internal Partial IO

### Problem

P1 makes internal partial IO durable for N x fixed-version objects, but EC objects need parity-safe updates. A partial overwrite changes one or more data shards and requires corresponding parity shards to be recomputed before metadata can represent the write as committed.

### Design

Support EC partial overwrite with a read-modify-write shard transaction:

1. Resolve the object's current EC placement and version ID.
2. Read the affected stripe range from enough shards to reconstruct the old data for the touched stripe.
3. Apply the partial write or truncate to the reconstructed stripe.
4. Re-encode data and parity shards for only the affected stripe range.
5. Write updated shard ranges to a data quorum that preserves the EC placement's durability requirements.
6. Commit `CmdWriteAtObject` or `CmdTruncateObject` metadata only after the shard update quorum succeeds.

The first implementation may choose stripe-granular rewrite rather than byte-granular parity patching. It must remain explicit in code and tests: EC partial IO is a read-modify-write operation with larger IO amplification than N x replication.

`ReadAt` on EC internal objects should use the existing EC reconstruction path, but it should return only the requested byte range to callers.

### Required Behavior

- EC internal `WriteAt` and `Truncate` do not silently fall back to unsafe local mutation.
- Metadata commit happens only after the affected EC shard updates reach the required write set.
- Partial writes spanning multiple EC stripes update every affected stripe.
- Truncate to smaller and larger sizes updates metadata and shard data consistently.
- Read-after-write works after data-group leader transfer.

### Tests

- Single-stripe EC `WriteAt` updates data and parity, then survives leader transfer.
- Multi-stripe EC `WriteAt` updates every affected stripe.
- EC `Truncate` shrink and grow preserve correct size and data bytes.
- Quorum failure during EC shard update returns an error before metadata commit.
- Benchmark gate: compare EC internal partial writes for 4 KiB, 128 KiB, and stripe-crossing writes, and document amplification.

## P3: Multipart Upload Part Durability

### Problem

Multipart Upload metadata is replicated through Raft, but part bodies are written to leader-local files under the local part directory. If leadership changes between `UploadPart` and `CompleteMultipartUpload`, the new leader can see MPU metadata but not the part bytes.

### Design

Store MPU part bodies as durable internal objects in a per-data-group staging namespace, not as a user-visible S3 bucket:

```text
namespace: __grainfs_mpu
key:       <uploadID>/parts/<partNumber>
```

The staging namespace is owned by the same data group as the target bucket. It is not created through `CreateBucket`, not assigned through the public bucket router, and not visible through user-facing list/head/get APIs. This avoids routing recursion where `UploadPart` calls the coordinator and re-enters the MPU path.

`UploadPart` writes the part body through a data-group internal full-object write primitive, then stores part metadata that references the staging object. The metadata includes:

- upload ID
- target bucket
- target key
- part number
- ETag
- size
- internal staging object key

`CompleteMultipartUpload` reads requested parts from the staging namespace in part-number order and assembles the final object using the existing object write path. It commits final object metadata only after all requested parts are readable and copied successfully.

`AbortMultipartUpload` deletes part metadata and best-effort deletes internal part objects. `CompleteMultipartUpload` also deletes staging objects after the final object commit succeeds. Cleanup failures are logged and left for startup cleanup or scrubber-style orphan cleanup; they do not turn a successful complete into a user-visible failure.

### Required Behavior

- MPU parts survive data-group leadership transfer.
- Internal staging objects are not visible as user bucket objects, bucket versions, lifecycle targets, or ordinary S3 list results.
- Complete fails if any requested part body is missing or unreadable.
- Final object metadata is not committed when part assembly fails.
- Abort and successful complete clean up staging objects best-effort.

### Tests

- Upload part on node A, transfer leadership, complete on node B.
- Abort removes MPU metadata and internal part objects.
- Complete failure leaves no final object metadata.
- Restart preserves MPU metadata and part staging objects.
- Existing S3 MPU ETag and part ordering behavior remains stable.
- Integration/e2e gate: create MPU and upload at least one part while node A leads the target data group, transfer or kill that leader, complete MPU through node B, then verify final object bytes and that staging namespace entries are no longer user-visible.

## P4: Behavioral Context Cancellation

### Problem

P0 makes the storage boundary context-first, but a signature migration alone does not guarantee meaningful cancellation. Forwarding, shard IO, EC fanout, Raft proposals, and streaming reads must actually observe caller cancellation where the underlying operation can stop.

### Design

P1, P2, and P3 should use caller contexts for new forwarding, shard IO, EC fanout, and Raft proposals. P4 adds behavioral cancellation tests and removes remaining request-path background contexts.

For streamed reads, the metadata phase obeys the request context. Once a body is returned, the body lifetime is controlled by the returned `io.ReadCloser`; cancellation must not kill an already-returned body unless the caller closes or cancels the stream.

### Required Behavior

- Forwarded writes respect caller cancellation.
- Shutdown drain can cancel long in-flight forward streams.
- Streamed read metadata cancellation does not break already-returned response bodies.
- Long-running data quorum writes and MPU staging writes stop on caller cancellation where the underlying IO path can be interrupted.

### Tests

- Cancelled forwarded write returns promptly instead of waiting for the default forward timeout.
- `CallRead` still preserves returned body lifetime after metadata response.
- Shutdown context cancels long-running forwarded body writes.
- Cancellation tests cover new P1/P2/P3 paths after the mechanical migration is complete.

## Rollout Plan

1. Ship P0 first as a behavior-preserving mechanical context migration.
2. Ship P1 next because it addresses direct durability risk for NFS/VFS internal writes on N x objects.
3. Ship P2 next to close the EC internal partial overwrite gap.
4. Ship P3 next because S3 MPU part durability has a similar leader-local staging problem.
5. Ship P4 after the touched paths are stable, adding behavioral cancellation coverage for the new context-aware surfaces.

Each slice should update `docs/architecture/protocol-layering.md` when the implementation changes the contract, and should add regression tests at the cluster coordinator, forward receiver/sender, and data-group backend layers.

## What Already Exists

- `ClusterCoordinator` already routes bucket-scoped operations to data-group leaders and has forward sender/receiver wiring. Reuse this path for new partial IO forward ops instead of adding a second forwarding transport.
- `ForwardSender` already has per-call timeout, stream slots, and backpressure patterns. Reuse these for partial IO quorum forwarding.
- `ForwardReceiver` already gates forwarded calls on data-group leadership. Extend that gate for `WriteAt`, `ReadAt`, and `Truncate`.
- `DistributedBackend.WriteAt`, `ReadAt`, and `Truncate` already implement local fixed-version internal bucket file operations. Keep the local primitive, but wrap it in quorum replication and Raft metadata commit for cluster mode.
- Existing MPU metadata commands and tests cover create/complete/abort lifecycle. Reuse the metadata lifecycle and replace only the part body staging path.
- Existing context cancellation tests exist in raft, forward sender, scrubber, and startup cleanup. Use their patterns for P3 instead of inventing new cancellation harnesses.

## NOT In Scope

- Write-behind or batching for NFS/NBD partial writes. The first quorum implementation is synchronous and bounded; batching needs a separate ordering and fsync design.
- User-visible MPU bucket APIs. MPU staging is a per-data-group internal namespace, not a public bucket.
- A new transport protocol for partial IO. New forward ops should use the existing FlatBuffers forward command family.
- A new data repair subsystem. Orphan cleanup can reuse retry/overwrite/startup cleanup/scrubber-style mechanisms until orphan volume proves otherwise.

## Failure Modes

| Codepath | Failure | Test | Error handling | User impact |
|----------|---------|------|----------------|-------------|
| P0 context migration | A wrapper drops ctx and keeps using background context | Wrapper pass-through tests | Compile-time assertions plus focused fake backend tests | Request may keep running after cancel |
| P1 `WriteAt` quorum | Quorum write fails after some peers apply range | Quorum failure unit/integration test | Return error before metadata commit; later overwrite/cleanup handles orphan local bytes | Client sees write failure, not silent data loss |
| P1 `ReadAt` forward | Read goes past EOF or leader changes mid-read | ForwardOpReadAt EOF/range tests | Return partial count/EOF or retryable forward error | NFS/VFS sees normal short read or retryable failure |
| P1 leader transfer | New leader lacks bytes after metadata commit | Leader-transfer integration/e2e gate | Data quorum before metadata commit | Prevents committed metadata pointing at missing bytes |
| P2 EC partial overwrite | Parity shards do not match updated data shard | EC parity/reconstruct tests | Fail before metadata commit on incomplete shard update | Prevents corrupt EC object after partial write |
| P3 staged MPU part | Complete cannot read a staged part | Complete failure test | Do not commit final object metadata | Client sees complete failure, final object absent |
| P3 staging cleanup | Abort/complete cleanup misses staging object | Abort/complete cleanup tests plus startup cleanup follow-up | Log and leave for cleanup path | Hidden capacity leak, not user-visible data corruption |
| P4 cancellation | Caller cancels during forward/quorum write | Cancellation tests for forward and long write | Propagate ctx to forward/shard/propose paths | Client returns promptly instead of hanging |

## Diagrams

Implementation plan should include ASCII diagrams for:

```text
P1 WriteAt
client/NFS -> backend -> ClusterCoordinator -> data-group leader
                                      leader local apply
                                      peer partial apply
                                      data quorum reached
                                      Raft metadata commit
                                      response
```

```text
P2 EC partial write
Read affected EC stripe -> apply byte change -> re-encode affected shards
                              -> write shard updates to required write set
                              -> commit metadata
```

```text
P3 MPU
CreateMultipart -> Raft MPU metadata
UploadPart      -> data-group staging namespace -> part metadata
Complete        -> read staged parts -> final PutObject -> cleanup staging
Abort           -> delete metadata -> cleanup staging
```

Inline comments are useful near the P1 quorum helper and P2 staging helper because both have multi-step durability ordering that is easy to break during future refactors.

## Parallelization Strategy

| Step | Modules touched | Depends on |
|------|-----------------|------------|
| P0 mechanical context migration | `internal/storage`, wrappers, `internal/server`, `internal/cluster`, `cmd/grainfs`, tests | none |
| P1 partial IO durability | `internal/cluster`, `internal/raft/raftpb`, `internal/transport` tests, e2e | P0 |
| P2 EC partial IO durability | `internal/cluster`, EC codec/placement helpers, e2e | P1 |
| P3 MPU staging durability | `internal/cluster`, `internal/storage` tests, `internal/server` MPU tests, e2e | P0 |
| P4 cancellation behavior | `internal/cluster`, `internal/transport`, `internal/server`, e2e | P1, P2, and P3 |

Parallel lanes:

- Lane A: P0 mechanical migration. Must land first because it changes shared interfaces.
- Lane B: P1 N x partial IO durability after P0.
- Lane C: P2 EC partial IO durability after P1.
- Lane D: P3 MPU staging durability after P0. Can run in parallel with Lane B/C only with careful coordination because it shares `internal/cluster`.
- Lane E: P4 cancellation after P1, P2, and P3.

Conflict flags: P1, P2, and P3 all touch `internal/cluster/backend.go` and likely `cluster_coordinator.go`. Prefer sequential execution for these lanes unless separate workers own disjoint helper files and merge frequently.

## Open Decisions

- The exact wire encoding for `ForwardOpReadAt` should mirror existing FlatBuffers forward args and replies rather than introducing a separate ad hoc frame.
- MPU staging namespace cleanup can start as best-effort synchronous cleanup plus startup orphan scanning; a richer lifecycle policy can be added later if orphan volume becomes measurable.
