# ADR 0007: Topology-Derived Durability Policy

## Status

Accepted.

## Context

GrainFS chooses object EC profiles from cluster topology instead of
operator-supplied `k/m` flags. That makes configured topology the durability
policy, but object versions can outlive the topology that created them. Reads,
repair, remove-peer, rebalancing, and background resharding need one model for
the difference between desired policy and each object's actual shard layout.

## Decision

Topology-derived EC is the desired durability policy. Object-index metadata is
the source of truth for each object version's actual layout:

- Desired EC profile is derived from configured placement group voters, not
  transient liveness. Liveness decides whether a write can execute now; it does
  not lower durability.
- Object index `ECData`, `ECParity`, and `NodeIDs` describe the actual shard
  layout for that object version. `PlacementGroupID` is logical ownership and
  routing namespace.
- New user-bucket writes require all `k+m` placement targets to be writable.
  Missing targets fail fast and surface to S3 as `503 ServiceUnavailable`.
- Scale-up may upgrade existing object versions toward the current desired
  profile. Scale-down does not automatically downgrade existing objects.
- Mixed-profile coexistence is allowed during migration windows. Reads and
  repair use the object-index actual layout; resharding converges lower actual
  layouts toward the desired policy.
- Repair restores the current actual layout. It does not silently move shards to
  new targets. If an actual target is permanently gone, the object enters an
  incident or explicit migration path.
- Group voter rebalancing and object resharding are separate operations. Voter
  moves affect future writes and consensus placement; object `NodeIDs` change
  only after data has actually moved and verified.
- Ring-based object placement and ring-based object resharding do not run under
  this policy.

## Consequences

The operating model becomes:

- configured placement group voters -> desired policy
- object index -> actual layout
- reshard manager -> actual-to-desired convergence
- repair -> actual layout health restoration
- rebalancer/drain -> future target and explicit data movement

This keeps transient liveness changes from silently changing durability, avoids
automatic data-loss posture changes during scale-down, and gives admin tooling a
clear way to explain current, pending-upgrade, downgrade-skipped, unknown, and
repair-needed object layouts.

Admin observability exposes placement summaries and bounded details through
`/v1/cluster/placement` and `grainfs cluster placement`. Normal S3/object APIs
preserve S3 semantics and do not expose EC internals by default.

## References

- ADR 0004: Object-Level Placement for Hot Buckets
- `internal/cluster/topology_policy.go`
- `internal/cluster/placement_report.go`
- `internal/cluster/reshard_manager.go`
- `internal/cluster/cluster_coordinator.go`
