package cluster

// Unit tests verifying the two bug fixes shipped in v0.0.4.2:
//
//  1. selfAddr (raft address) is used for self-detection instead of node.ID() (UUID).
//     Without this fix allNodes entries (addresses) never matched node.ID() (UUID),
//     so every shard was sent to peers and none were written locally.
//
//  2. Placement records are stored/looked up under shardKey = key+"/"+versionID
//     instead of bare key, preventing version collisions in the FSM.

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSelfAddr_SetBySetShardService verifies that SetShardService stores allNodes[0]
// as b.selfAddr. The pre-fix code used b.node.ID() (a human-readable Raft node name)
// for the self-detection comparison, but allNodes entries hold raft peer *addresses*
// — so the comparison always failed and every shard went to a peer.
func TestSelfAddr_SetBySetShardService(t *testing.T) {
	b := newTestDistributedBackend(t)
	svc := NewShardService(b.root, nil)

	allNodes := []string{"addr-self:9001", "addr-peer1:9001", "addr-peer2:9001"}
	b.SetShardService(svc, allNodes)

	// selfAddr must equal allNodes[0] (the "self" convention used by SetShardService).
	assert.Equal(t, "addr-self:9001", b.selfAddr,
		"selfAddr must be the first entry of allNodes (the local node's raft address)")

	// selfAddr must appear in b.allNodes so the self-check in putObjectEC succeeds.
	found := false
	for _, n := range b.allNodes {
		if n == b.selfAddr {
			found = true
			break
		}
	}
	assert.True(t, found, "selfAddr must be present in the allNodes slice")
}

// TestSelfAddr_DifferentFromRaftNodeID confirms that selfAddr (an address like
// "host:port") differs from the Raft node.ID() (a human-readable string like
// "test-node"). This is the root cause of the pre-fix bug: if they were equal,
// the bug would have been invisible.
func TestSelfAddr_DifferentFromRaftNodeID(t *testing.T) {
	b := newTestDistributedBackend(t)
	svc := NewShardService(b.root, nil)

	allNodes := []string{"addr-self:9001", "addr-peer1:9001"}
	b.SetShardService(svc, allNodes)

	raftName := b.RaftNodeID() // returns "test-node" from newTestDistributedBackend
	assert.NotEqual(t, raftName, b.selfAddr,
		"Raft node.ID() (%q) must differ from selfAddr (%q) — if they were equal, the pre-fix bug would have been invisible",
		raftName, b.selfAddr)
}

// TestShardPlacementKey_VersionedStorageAndLookup verifies that placement records
// stored under the versioned shardKey (key+"/"+versionID) can be retrieved under
// the same key, while a lookup with only the bare key returns nothing.
//
// This validates the fix for the placement key skew bug: before the fix,
// putObjectEC wrote placement under shardKey (key+"/"+versionID) but GetObject
// looked up under bare key — so every versioned GET missed the placement record
// and fell back to N× replication.
func TestShardPlacementKey_VersionedStorageAndLookup(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket("bkt"))

	const (
		key       = "myobject"
		versionID = "01JT5BVMZABCDEF12345"
		shardKey  = key + "/" + versionID
	)
	nodes := []string{"addr-a", "addr-b", "addr-c"}

	// Seed placement under the versioned shardKey (the format putObjectEC writes).
	writePlacement(t, b, "bkt", shardKey, nodes)

	// Versioned lookup must succeed.
	got, err := b.fsm.LookupShardPlacement("bkt", shardKey)
	require.NoError(t, err)
	assert.Equal(t, nodes, got.Nodes, "LookupShardPlacement must find a record stored under the shardKey")

	// Bare-key lookup must return zero record — no collision across versions.
	bare, bareErr := b.fsm.LookupShardPlacement("bkt", key)
	assert.NoError(t, bareErr)
	assert.Equal(t, PlacementRecord{}, bare, "LookupShardPlacement with bare key must return zero record when only versioned record exists")
}

// TestShardPlacementKey_MultiVersionNoCollision verifies that two versions of the
// same object carry independent placement records. Before the fix, the second PUT
// would overwrite the first version's placement record (both stored under bare key),
// making the first version's shards unretrievable.
func TestShardPlacementKey_MultiVersionNoCollision(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket("bkt"))

	const key = "shared-key"
	v1Nodes := []string{"node-a", "node-b", "node-c"}
	v2Nodes := []string{"node-x", "node-y", "node-z"}

	writePlacement(t, b, "bkt", key+"/v1", v1Nodes)
	writePlacement(t, b, "bkt", key+"/v2", v2Nodes)

	got1, err1 := b.fsm.LookupShardPlacement("bkt", key+"/v1")
	require.NoError(t, err1)
	assert.Equal(t, v1Nodes, got1.Nodes, "v1 placement must not be overwritten by v2")

	got2, err2 := b.fsm.LookupShardPlacement("bkt", key+"/v2")
	require.NoError(t, err2)
	assert.Equal(t, v2Nodes, got2.Nodes)
}

// TestOwnedShards_WithVersionedPlacement exercises the full OwnedShards path with
// the versioned key construction that was added in the fix. The pre-fix code
// ignored the versionID parameter entirely, making OwnedShards always return nil
// for versioned objects (since the placement was stored under key+"/"+versionID
// but lookup used bare key).
func TestOwnedShards_WithVersionedPlacement(t *testing.T) {
	b := newTestDistributedBackend(t)

	const nodeID = "test-node"
	nodes := make([]string, 0, 6)
	for i := 0; i < 6; i++ {
		nodes = append(nodes, fmt.Sprintf("node-%d", i))
	}
	// seed placement that includes nodeID at positions 0 and 2.
	placement := []string{nodeID, "node-other", nodeID, "node-other", "node-other", "node-other"}
	writePlacement(t, b, "bkt", "obj/vid1", placement)

	owned := b.OwnedShards("bkt", "obj", "vid1", nodeID)
	assert.Equal(t, []int{0, 2}, owned,
		"OwnedShards must use versionID to find the versioned placement record")

	// Different versionID yields no placement record.
	owned2 := b.OwnedShards("bkt", "obj", "vid2", nodeID)
	assert.Nil(t, owned2, "OwnedShards for an unknown version must return nil")
}
