# Remove-Peer Membership Preflight Design

## Context

Cluster peer observation now has a `Cluster Peer Liveness Snapshot` in
`internal/cluster`. The snapshot separates identity state from liveness state
and exposes predicates for display policy and membership-mutation policy.

`POST /api/cluster/remove-peer` still performs quorum preflight in
`internal/server/handlers.go` by calling `LivePeers()`. That legacy Interface
does not carry the snapshot states that matter for safe membership mutation:
`configured`, `live`, `health_cooldown`, `probe_failed`, and
`unresolved_legacy`.

## Goal

Move remove-peer preflight policy into a pure `internal/cluster` Module so the
server handler no longer re-derives membership safety from legacy liveness
strings or raw peer counts.

The Module should preserve a small Interface while concentrating these rules:

- only `self` and resolved `live` peers count as alive for membership mutation;
- `configured` peers are unknown and do not count as alive;
- unresolved legacy rows block membership mutation, except when the requested
  operation removes that exact unresolved legacy row;
- explicit negative liveness states are not alive;
- quorum math is returned as structured facts for HTTP responses and tests.

## Proposed Interface

Add a pure function in `internal/cluster`, for example:

```go
type RemovePeerPreflightInput struct {
    TargetID string
    Voters   []string
    Snapshot []PeerLivenessRow
}

type RemovePeerPreflightResult struct {
    Allowed       bool
    Reason        string
    VotersAfter   int
    AliveAfter    int
    NewQuorum     int
    BlockingPeers []string
}

func EvaluateRemovePeerPreflight(input RemovePeerPreflightInput) RemovePeerPreflightResult
```

The exact names can change during implementation if local naming points to a
clearer term, but the Interface shape should remain pure and snapshot-based.

`Voters` is the current remote voter list from the cluster Interface. The local
self row comes from `Snapshot`, so the preflight can count self through
`IsAliveForMembershipMutation`.

## Policy

The target must be present in the current membership view. A target may match
either a voter ID or a snapshot `PeerID`. This keeps unresolved legacy rows
removable when their `PeerID` is the legacy raft address.

If any unresolved legacy row exists and it is not the target row, the preflight
fails with a blocking reason and the row IDs in `BlockingPeers`. This prevents
membership mutation while identity is ambiguous, without removing the operator's
escape hatch for deleting the bad legacy row itself.

For quorum:

- `votersAfter = currentTotalVoters - 1`
- `newQuorum = max(1, votersAfter/2 + 1)`
- `aliveAfter` counts snapshot rows except the target where
  `IsAliveForMembershipMutation(row)` is true
- the operation is allowed only when `aliveAfter >= newQuorum`

`configured` rows do not count as alive. This is intentionally stricter than
display policy because configured membership is not liveness evidence.

## Server Integration

`handleClusterRemovePeer` should use the new Module when the cluster object
implements `PeerSnapshot() []cluster.PeerLivenessRow`.

The handler remains responsible for:

- request parsing;
- leader checks;
- force override behavior;
- invoking the membership Adapter;
- mapping `RemovePeerPreflightResult` to JSON.

`force=true` should override quorum and unresolved-legacy safety failures, but
should not override structural failures such as the target not being in the
cluster membership view.

If the cluster object has no snapshot Interface, membership mutation is refused.
Legacy `LivePeers()` quorum math is not a safe fallback because it cannot
distinguish `configured`, `live`, and unresolved legacy identity states.

## Tests

Add focused tests in `internal/cluster` for:

- `configured` peers are not counted as alive;
- resolved `live` peers are counted;
- self is counted;
- explicit down states are not counted;
- unresolved legacy rows block unrelated removals;
- the unresolved legacy target itself can be removed;
- quorum failure reports `voters_after`, `alive_after`, and `new_quorum`.

Server tests should stay narrower:

- handler maps a failed preflight to the existing `409` shape;
- `force=true` bypasses quorum or unresolved-legacy policy failures;
- `force=true` does not bypass target-not-in-cluster.

## Out of Scope

This design does not add active metaRaft probing. The snapshot remains a pure
composer of existing signals. A future peer-health monitor should feed
`PeerProbeResult` or equivalent input into the snapshot, then this preflight
Module will pick up the new signal without changing the server handler policy.
