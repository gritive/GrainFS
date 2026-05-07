# ADR 0004: Object-Level Placement for Hot Buckets

## Status

Accepted.

## Context

The previous Multi-Raft design placed object data by bucket assignment. A hot
bucket could therefore load only one data group and its voter set, even in a
larger cluster with enough nodes and EC shards to spread the traffic.

## Decision

Bucket lifecycle remains bucket-routed. Object data operations and LIST-style
object enumeration route through a meta-Raft global object index:

- New object writes select `placement_group_id` from normal EC-capable data
  groups by hashing `bucket + "/" + key`.
- `group-0` is excluded from normal object placement.
- Data-group Raft elections use the group ID as a deterministic priority key,
  splitting each election timeout window across voters so independent placement
  groups prefer different initial leaders.
- Reads, versioned reads, deletes, multipart completion, copy-through-write,
  Range `ReadAt`, `ListObjects`, `ListObjectVersions`, and `WalkObjects` use
  the object index for object routing/enumeration.
- Cluster object storage uses the EC pipeline. If no EC profile is explicit,
  runtime chooses an effective profile from cluster size.
- Explicit EC profiles fail fast when the current placement group is too small.

## Consequences

Hot buckets can distribute object traffic across normal data groups. Meta-Raft
now carries object-index write pressure, and data/index dual-write mismatches
need explicit reconcile detection instead of silent success.
