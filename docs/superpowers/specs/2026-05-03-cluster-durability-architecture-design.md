# Cluster Durability Architecture Design

## Context

The current protocol layering contract says S3, NFSv4, NBD, and Iceberg all pass through `storage.Backend`, then through `ClusterCoordinator`, then into the meta-Raft control plane and data Raft groups. The recent protocol-contract work closed several gaps, but three architecture risks remain:

1. Internal bucket partial IO (`WriteAt`, `ReadAt`, `Truncate`) can still bypass data-group Raft and peer replication.
2. Multipart Upload part bodies are stored as leader-local files while MPU metadata is replicated through Raft.
3. The storage interface has no request context, so cancellation, shutdown, and stream deadlines do not propagate consistently.

This design uses a durability-first order:

1. Make internal bucket partial IO cluster-safe.
2. Move MPU parts to a durable distributed staging path.
3. Add context-aware storage paths incrementally.

## Goals

- Preserve data across data-group leader failure and leadership transfer.
- Keep NFSv4/VFS/NBD behavior on the same distributed object storage contract as S3.
- Avoid putting large or frequently written object bytes directly into Raft logs.
- Keep implementation incremental enough to ship as a sequence of focused PRs.
- Maintain compatibility for legacy single-node/local backends.

## Non-Goals

- Redesign the full `storage.Backend` interface in one change.
- Implement partial erasure-coded overwrite parity updates in this slice.
- Change user-visible S3 Multipart Upload semantics beyond making part staging durable.
- Add a second control-plane service.

## Approach

Use three focused slices.

1. **Internal partial IO durability:** add data-group commands and forwarding for `WriteAt`, `ReadAt`, and `Truncate`. Local leader paths must no longer mutate internal object metadata without Raft.
2. **Durable MPU staging:** store MPU part bodies as internal distributed objects instead of process-local files.
3. **Context propagation:** add optional context-aware backend methods and migrate newly touched cluster paths first.

## P0: Internal Bucket Partial IO Durability

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

Bytes do not go into Raft. The leader first writes data through a replicated internal-object path, then commits metadata through Raft. For N x replication, the leader writes the fixed `current` object path locally and forwards the changed bytes or resulting file state to peer replicas before proposing metadata. If replication fails, the operation fails before metadata commit.

For erasure coding, partial overwrite is not implemented in this slice. Internal bucket partial IO must use the N x fixed-version path until a separate EC partial-update design exists. Full-object internal writes may continue using existing behavior where already supported.

`ReadAt` should no longer fetch the full object on non-leaders. Non-leader requests forward `ForwardOpReadAt` to the leader, and the leader returns only the requested byte range plus EOF/size semantics.

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

## P1: Multipart Upload Part Durability

### Problem

Multipart Upload metadata is replicated through Raft, but part bodies are written to leader-local files under the local part directory. If leadership changes between `UploadPart` and `CompleteMultipartUpload`, the new leader can see MPU metadata but not the part bytes.

### Design

Store MPU part bodies as durable internal objects in a reserved internal bucket:

```text
bucket: __grainfs_mpu
key:    <uploadID>/parts/<partNumber>
```

`UploadPart` writes the part body through the normal distributed object path, then stores part metadata that references the internal staging object. The metadata includes:

- upload ID
- target bucket
- target key
- part number
- ETag
- size
- internal part object key

`CompleteMultipartUpload` reads requested parts from `__grainfs_mpu` in part-number order and assembles the final object using the existing object write path. It commits final object metadata only after all requested parts are readable and copied successfully.

`AbortMultipartUpload` deletes part metadata and best-effort deletes internal part objects. `CompleteMultipartUpload` also deletes staging objects after the final object commit succeeds. Cleanup failures are logged and left for startup cleanup or scrubber-style orphan cleanup; they do not turn a successful complete into a user-visible failure.

### Required Behavior

- MPU parts survive data-group leadership transfer.
- Internal staging objects are not visible as user bucket objects.
- Complete fails if any requested part body is missing or unreadable.
- Final object metadata is not committed when part assembly fails.
- Abort and successful complete clean up staging objects best-effort.

### Tests

- Upload part on node A, transfer leadership, complete on node B.
- Abort removes MPU metadata and internal part objects.
- Complete failure leaves no final object metadata.
- Restart preserves MPU metadata and part staging objects.
- Existing S3 MPU ETag and part ordering behavior remains stable.

## P2: Context Propagation

### Problem

`storage.Backend` has no context parameter. Cluster paths therefore use `context.TODO()` and `context.Background()` in forwarding, shard IO, EC fanout, and Raft proposals. Client cancellation, shutdown drain, and QUIC stream cancellation do not reliably stop in-flight storage work.

### Design

Add optional context-aware interfaces without breaking existing backends:

```go
type ContextBackend interface {
    PutObjectContext(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*Object, error)
    GetObjectContext(ctx context.Context, bucket, key string) (io.ReadCloser, *Object, error)
    DeleteObjectContext(ctx context.Context, bucket, key string) error
    WriteAtContext(ctx context.Context, bucket, key string, offset uint64, data []byte) (*Object, error)
    ReadAtContext(ctx context.Context, bucket, key string, offset int64, buf []byte) (int, error)
    TruncateContext(ctx context.Context, bucket, key string, size int64) error
}
```

`ClusterCoordinator` gets context-aware internal methods first. Existing context-free methods remain wrappers that call the context-aware methods with `context.Background()`.

New forwarding and partial IO paths must use the caller context instead of creating `context.TODO()`. `DistributedBackend` should derive shard RPC deadlines from the caller context rather than from a new background context. HTTP handlers can migrate gradually by checking for context-aware backends and passing `r.Context()`.

For streamed reads, the metadata phase obeys the request context. Once a body is returned, the body lifetime is controlled by the returned `io.ReadCloser`; cancellation must not kill an already-returned body unless the caller closes or cancels the stream.

### Required Behavior

- Forwarded writes respect caller cancellation.
- Shutdown drain can cancel long in-flight forward streams.
- Streamed read metadata cancellation does not break already-returned response bodies.
- Legacy backends continue to work through context-free wrappers.

### Tests

- Cancelled forwarded write returns promptly instead of waiting for the default forward timeout.
- `CallRead` still preserves returned body lifetime after metadata response.
- Shutdown context cancels long-running forwarded body writes.
- A backend without `ContextBackend` still satisfies existing handlers.

## Rollout Plan

1. Ship P0 first because it addresses direct durability risk for NFS/VFS internal writes.
2. Ship P1 next because S3 MPU part durability has a similar leader-local staging problem.
3. Ship P2 after the touched paths are stable, using the new partial IO and MPU paths as the first context-aware surfaces.

Each slice should update `docs/architecture/protocol-layering.md` when the implementation changes the contract, and should add regression tests at the cluster coordinator, forward receiver/sender, and data-group backend layers.

## Open Decisions

- The exact wire encoding for `ForwardOpReadAt` should mirror existing FlatBuffers forward args and replies rather than introducing a separate ad hoc frame.
- Internal partial IO under EC remains out of scope and should fail clearly instead of silently falling back to unsafe local mutation.
- MPU staging bucket cleanup can start as best-effort synchronous cleanup plus startup orphan scanning; a richer lifecycle policy can be added later if orphan volume becomes measurable.
