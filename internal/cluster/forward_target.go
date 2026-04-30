package cluster

import "errors"

// ErrUnknownGroup is returned when the requested groupID is not found in meta-FSM
// or the entry has no peers.
var ErrUnknownGroup = errors.New("forward_target: unknown group or no peers")

// ShardGroupSource is the interface used by callers (DistributedBackend,
// lookupForwardTarget) to query meta-FSM shard group state. *MetaFSM satisfies
// it; tests use a fake.
type ShardGroupSource interface {
	ShardGroup(id string) (ShardGroupEntry, bool)
	ShardGroups() []ShardGroupEntry
}

// shardGroupSource is the unexported alias used by lookupForwardTarget. Older
// callers can keep the unexported type; new code uses ShardGroupSource.
type shardGroupSource = ShardGroupSource

// lookupForwardTarget returns the first peer of the given group as the forward
// target. Caller forwards the propose RPC; if the target is a follower, raft
// returns NotLeader with leader hint and caller redirects on second try.
//
// We deliberately do NOT cache last-known leaders — cold path 1 RTT loss is
// acceptable, and hot path is local-voter (no forward at all).
func lookupForwardTarget(src shardGroupSource, groupID string) (string, error) {
	entry, ok := src.ShardGroup(groupID)
	if !ok || len(entry.PeerIDs) == 0 {
		return "", ErrUnknownGroup
	}
	return entry.PeerIDs[0], nil
}
