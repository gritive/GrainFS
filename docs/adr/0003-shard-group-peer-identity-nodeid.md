# ADR 0003: Shard Group Peer Identity Uses Node IDs

## Status

Accepted

## Context

Shard group metadata has historically mixed stable node IDs and raft addresses
inside `ShardGroupEntry.PeerIDs`. This makes data Raft group membership harder
to reason about because different parts of the system compare different
identity types. One visible symptom is peer observation: leader identity may be
reported as a node ID while group peers are reported as raft addresses, so role
calculation cannot reliably mark the leader.

Cluster dynamic join also needs a stable identity substrate before it can add
data-group voters safely. Raft addresses are endpoints, not durable node
identity. They may change, and storing them as peer identity makes later
address-book, remove-peer, and dynamic-join behavior ambiguous.

## Decision

`ShardGroupEntry.PeerIDs` uses node IDs as the canonical peer identity for data
Raft group membership. The MetaFSM address book is the canonical resolution
seam between node IDs and raft addresses.

New shard group metadata and membership changes must write node IDs only.
Runtime join, remove, observe, and data-group wiring paths resolve node IDs to
raft addresses through the address book instead of storing raft addresses as
identity.

Legacy raft addresses already stored in `ShardGroupEntry.PeerIDs` are accepted
only as read-time compatibility input. Runtime views normalize legacy addresses
to node IDs when the address book can reverse-resolve them. Existing metadata is
not automatically rewritten or backfilled as part of this decision.

Unresolved legacy peers degrade read and observe paths but block membership
mutation. Existing raft-group attach may continue through the stored raft
address when possible, and admin status should surface the unresolved peer
state. Operations that add, remove, rebalance, or dynamically join data-group
voters must fail until the node mapping is restored or an explicit migration
resolves the legacy identity.

The address book lifecycle is add/update-only for this identity unification
step. Removing a peer does not delete its address book entry; deletion and
garbage collection are separate operator lifecycle concerns because historical
group metadata may still need the mapping for interpretation.

Node identity and node liveness remain separate signals. Address book presence
does not mean a peer is live. Real metaRaft peer-health monitoring is a
follow-up, not part of the identity unification change.

## Consequences

Cluster peer observation presents node ID as the primary peer identity and raft
address as resolved supporting detail. Role calculations compare node IDs to
node IDs, fixing identity mismatches without claiming a live/down health signal.

PR-D remains an identity substrate change. It does not implement dynamic join,
peer health monitoring, metadata rewrite/migration, address book deletion/GC,
joint consensus replacement, or a serve bootstrap UX redesign.

This makes the follow-up dynamic join work possible on top of a stable
membership identity model while keeping existing clusters readable through
read-time legacy normalization.
