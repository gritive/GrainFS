# Durability and Object Placement

`GrainFS` derives object durability from cluster topology and stores the actual
layout of each object version in metadata. Operators do not tune per-object
replication by passing storage flags to `serve`.

## Node Identity

Data Raft membership uses stable node IDs as the canonical peer identity. Raft
addresses are endpoints that can change; node IDs are the durable identity used
in shard group metadata. The metadata address book resolves node IDs to current
Raft addresses when the runtime dials peers.

This keeps join, remove-peer, and observation paths consistent. If a node moves
addresses, update the address book instead of rewriting historical shard group
membership.

## Object-Level Placement

`GrainFS` places object data by object. Bucket lifecycle and namespace operations
remain bucket-routed, but object writes choose a placement group from EC-capable
data groups by hashing `bucket + "/" + key`.

The metadata layer records the placement group for each object version. Reads,
deletes, multipart completion, copy-through-write, range reads, object listing,
and version listing use that index instead of assuming a bucket owns one fixed
data group.

This avoids concentrating a hot bucket on one voter set and lets large buckets
use the available cluster topology.

## Topology-Derived EC

`GrainFS` derives the desired EC profile from configured placement-group voters.
Transient liveness decides whether a write can proceed now; it does not reduce
the durability target. If the desired profile lacks enough writable targets, the
write fails instead of writing a weaker layout.

Each object version records its actual `k`, `m`, and node placement. That allows
mixed layouts during scale-up, repair, and resharding:

- New writes use the current desired profile.
- Existing versions keep their recorded layout until migration rewrites them.
- Scale-up can improve old versions toward the current profile.
- Scale-down does not automatically weaken existing versions.

Reads and repair use the actual per-version layout, while placement policy uses
the current topology for new work.

## Metadata Durability (Phase 3)

Object *data* durability is governed by the EC layout above. Object *metadata*
durability uses a separate mechanism that was changed in Phase 3.

**Pre-Phase-3:** object metadata was committed through the data Raft group
(`data_raft`). This gave strong single-writer ordering but serialized every
PUT through a consensus round, which was the dominant PUT latency bottleneck.

**Phase-3 quorum meta store:** user-bucket object metadata is written directly
to each placement node's local filesystem at
`{dataDir}/.quorum_meta/{bucket}/{key}`, bypassing Raft consensus.

- **K-of-N write quorum**: the write fans out to ECData placement nodes
  (k = 4 in a 4+2 cluster). The PUT succeeds when k nodes acknowledge;
  parity nodes are best-effort.
- **Peer fan-out read**: a parity node that missed the K-of-N write can
  recover metadata by fanning out `ReadQuorumMeta` RPCs to all shard-group
  peers. The reader applies LWW by ModTime and returns the newest entry.
- **Fallback chain**: local file → peer fan-out → BadgerDB (multipart-completed
  objects, pre-Phase-3 objects, scrubber entries) → `ErrObjectNotFound`.
- **Multipart boundary**: `CompleteMultipartUpload` intentionally stays on
  data_raft. `applyCompleteMultipart` writes object meta and deletes the
  multipart manifest key in a single BadgerDB transaction; splitting that
  atomicity would open a window where the manifest leaks.

Internal buckets (`_grainfs_*`) always use raft for metadata; only user buckets
use the quorum meta path.
