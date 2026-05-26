# `GrainFS` Technical Roadmap

`GrainFS` is a single-binary distributed object store. It runs as a local
S3-compatible server or as a Raft-backed cluster with object, file, and block
protocol surfaces.

## Product Position

`GrainFS` combines these surfaces in one binary:

- S3-compatible object storage
- NFSv4 and 9P file access
- Linux NBD block volumes
- Iceberg REST Catalog and audit lake workflows
- Custom Raft metadata replication and erasure-coded object placement

The current focus is operator trust: safe defaults, explicit escape hatches,
observable recovery, and compatibility gates for rolling upgrades.

## Architecture

| Layer | Responsibility | Implementation |
| --- | --- | --- |
| Transport | Node-to-node streams and RPC framing | QUIC via `quic-go` |
| Consensus | Leader election, metadata agreement, shard group state | Custom Raft |
| Data plane | EC shards, blob files, packed blobs, volume blocks | Reed-Solomon, BadgerDB metadata, local files |
| API layer | S3, NFSv4, 9P, NBD, Iceberg REST, admin UDS | Hertz, Cobra, protocol-specific servers |

Protocol adapters do not own storage semantics. They translate client behavior
into the shared storage and cluster layers.

## Completed Tracks

### Single-Node S3

- Single `grainfs serve` binary with Hertz and Cobra.
- Bucket and object basics: create, list, put, get, head, delete.
- Multipart upload and SigV4 authentication.
- Local files plus BadgerDB metadata.

### QUIC And Raft

- QUIC transport with stream multiplexing.
- Custom Raft with leader election, log replication, persistence, snapshots, and
  InstallSnapshot.
- Raft-over-QUIC transport, membership changes, leadership transfer, and
  rolling-upgrade compatibility tests.

### Clustered Object Storage

- Runtime conversion from local node to clustered seed.
- Object-level placement through the metadata index.
- EC split/write/read/repair paths.
- Non-leader large-write forwarding over QUIC.
- CRC-backed shard envelopes.
- Reshard manager wiring.
- AppendObject (S3 Express semantics): segments + EC coalesce, owner-routed via
  data-Raft, forward-on-read across owner/peer, range read across stitches, size
  cap + memory budget + 503 SlowDown backpressure, scrubber raw-segment orphan
  sweep.

### Operations

- Admin UDS bootstrap and IAM-only S3 credentials.
- Prometheus metrics, dashboard surfaces, alerts, incident state, and recovery
  drills.
- Badger startup recovery journal.
- Backup, restore, disaster recovery, and offline cluster recovery runbooks.
- Rolling-upgrade compatibility lane and snapshot format guards.

### File And Block Protocols

- NFSv4 server with explicit bucket exports and export propagation.
- 9P2000.L read/write support for trusted local or private networks.
- Linux NBD volumes with multi-node byte replication.
- Shared VFS/cache invalidation paths for cross-protocol visibility.

### Security And Policy

- Bucket policy CRUD and S3 authorization.
- Service accounts, access keys, grants, and bucket-scoped upstream credentials.
- Mandatory local at-rest encryption.
- Rate limiting and no-plaintext-secret checks.

### Performance

- Protobuf/internal encoding cleanup where shipped.
- Cached object reads.
- NFS stat/readdir cache.
- Packed blob format for small objects.
- Benchmark suites for S3, NFS, NBD, 9P, Iceberg, and FUSE-over-S3 clients.
- NFS write coalescing buffer (single-node) — per-key local file absorbs
  consecutive WRITE ops and flushes once on COMMIT / SETATTR truncate /
  idle / shutdown. Sequential write throughput ~7× on bench-nfs streaming.

## Active Roadmap

### 1. Operator Trust

Finish the work that lets an operator understand and recover the system without
reading source code during an incident.

- BadgerDB atomic auto-recovery design.
- Remaining rolling-upgrade safety slices.
- Snapshot-config compatibility status.
- Cluster health with data-group Raft progress.
- Object placement/index orphan and stale reconcile.

### 2. Protocol Correctness

Close the gaps where `GrainFS` advertises a protocol surface but still needs
better conformance or security boundaries.

- NFSv4 `rdattr_error` behavior.
- Scheduled pynfs and nfstest conformance matrix.
- NFSv4 and 9P authentication/access-control design for untrusted networks.
- 9P/NFS shared write-back helper when a concrete bug or conformance gap needs
  it.

### 3. Migration And Cutover

Complete bucket-level migration flows beyond read-through caching.

- Mirror mode.
- Cutover verb and status field on `BucketUpstream`.
- Progress tracking and dashboard surface.
- Upstream `List`, `Head`, and `CopyObject` operations.
- Cancellation semantics on leadership loss.

### 4. Day-2 Cluster Operations

Expose the operations needed for maintenance windows and peer failure handling.

- Negative liveness signal for `cluster remove-peer`.
- Per-data-group leader transfer and drain.
- Persistent drain state that redirects traffic while keeping voters.
- Manual balancer trigger when automatic scheduling is too slow.
- Leader-side `cluster add-voter <addr>` if bulk node-add workflows require it.

### 5. Performance Follow-Ups

Reopen performance work only when a benchmark or production SLO shows the
specific bottleneck.

- S3 Range GET p95/p99 residual latency.
- NBD direct I/O write path and per-block file layout.
- Iceberg REST high-concurrency Raft proposal ceiling.
- EC shard cache sizing.
- QUIC mux pool sizing and meta-mux post-deploy measurement.

## Deferred Ideas

These items stay parked until a user, SLO, or measured bottleneck makes them
concrete.

- Redis protocol.
- TSDB.
- io_uring, SPDK, SIMD, and SoA layout work.
- Control-plane/data-plane process split.
- Hot/cold auto tiering.
- Native FUSE implementation; use existing FUSE-over-S3 clients unless NFSv4
  fails a real workload.

## Design Constants

| Item | Current Value | Notes |
| --- | --- | --- |
| Default shard size | 4 MiB | Balance metadata pressure and I/O size |
| Default EC profile | 4+2 when topology allows it | Runtime chooses smaller profiles for smaller clusters |
| Transport | QUIC | TLS 1.3 and multiplexed streams |
| Metadata KV | BadgerDB | MVCC, LSM-tree storage |
| License | Apache 2.0 | Permissive commercial use |
