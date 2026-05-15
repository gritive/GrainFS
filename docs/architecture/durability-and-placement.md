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
