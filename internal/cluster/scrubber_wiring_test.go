package cluster

// Unit coverage for DistributedBackend scrubber contracts: scrubber.ShardOwner
// (NodeID, OwnedShards) and scrubber.ShardRepairer (RepairShardLocal).

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/scrubber"
)

// Compile-time proof that DistributedBackend satisfies the scrubber contracts.
var _ scrubber.ShardRepairer = (*DistributedBackend)(nil)
var _ scrubber.ShardOwner = (*DistributedBackend)(nil)

// writePlacement seeds a placement record directly in the FSM's BadgerDB,
// bypassing the Raft proposal path. Matches the byte layout that
// applyPutShardPlacement writes. Used by tests that call LookupShardPlacement
// directly (not ResolvePlacement).
func writePlacement(t clusterTestTB, b *DistributedBackend, bucket, key string, nodes []string) {
	t.Helper()
	if err := b.db.Update(func(txn *badger.Txn) error {
		rec := PlacementRecord{Nodes: nodes, K: 4, M: 2}
		return txn.Set(shardPlacementKey(bucket, key), encodePlacementValue(rec))
	}); err != nil {
		t.Fatalf("write placement: %v", err)
	}
}

// seedPlacementMeta writes an object metadata record (including EC placement)
// via FSM Apply, so that readPlacementMeta can resolve it.
func seedPlacementMeta(t clusterTestTB, b *DistributedBackend, bucket, key, versionID string, nodes []string, ecData, ecParity uint8) {
	t.Helper()
	raw, err := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket:      bucket,
		Key:         key,
		VersionID:   versionID,
		Size:        1,
		ContentType: "application/octet-stream",
		ETag:        "etag",
		ModTime:     1,
		ECData:      ecData,
		ECParity:    ecParity,
		NodeIDs:     nodes,
	})
	if err != nil {
		t.Fatalf("encode placement meta: %v", err)
	}
	if err := b.fsm.Apply(raw); err != nil {
		t.Fatalf("apply placement meta: %v", err)
	}
}

func TestNodeID_ReturnsSelfAddr(t *testing.T) {
	b := newTestDistributedBackend(t)
	assert.Equal(t, "", b.NodeID(), "NodeID before SetShardService must be empty")
	b.SetShardService(nil, []string{"192.168.1.1:9000", "192.168.1.2:9000"})
	assert.Equal(t, "192.168.1.1:9000", b.NodeID(), "NodeID must return the self (first) address")
}

func TestRaftNodeID_ReturnsRaftNodeID(t *testing.T) {
	b := newTestDistributedBackend(t)
	assert.Equal(t, "test-node", b.RaftNodeID(),
		"RaftNodeID must return the Raft node.ID() set on the underlying Node")
}

func TestOwnedShards(t *testing.T) {
	b := newTestDistributedBackend(t)

	const testVersionID = "any-version"

	tests := []struct {
		name     string
		bucket   string
		key      string
		nodes    []string // nil → skip placement write ("no placement")
		ecData   uint8
		ecParity uint8
		nodeID   string
		want     []int
	}{
		{
			name:   "no placement record yields nil",
			bucket: "b",
			key:    "none",
			nodes:  nil,
			nodeID: "test-node",
			want:   nil,
		},
		{
			name:     "node owns zero shards",
			bucket:   "b",
			key:      "zero",
			nodes:    []string{"other-a", "other-b", "other-c"},
			ecData:   2,
			ecParity: 1,
			nodeID:   "test-node",
			want:     nil,
		},
		{
			name:     "node owns exactly one shard",
			bucket:   "b",
			key:      "one",
			nodes:    []string{"other-a", "test-node", "other-b"},
			ecData:   2,
			ecParity: 1,
			nodeID:   "test-node",
			want:     []int{1},
		},
		{
			name:     "node owns multiple non-contiguous shards",
			bucket:   "b",
			key:      "many",
			nodes:    []string{"test-node", "other-a", "test-node", "other-b", "test-node"},
			ecData:   3,
			ecParity: 2,
			nodeID:   "test-node",
			want:     []int{0, 2, 4},
		},
		{
			name:   "unknown node gets nil",
			bucket: "b",
			key:    "one",
			nodes:  nil, // re-use the one-shard placement seeded above
			nodeID: "not-in-cluster",
			want:   nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.nodes != nil {
				seedPlacementMeta(t, b, tc.bucket, tc.key, testVersionID, tc.nodes, tc.ecData, tc.ecParity)
			}
			got := b.OwnedShards(tc.bucket, tc.key, testVersionID, tc.nodeID)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestRaftNodeID_NilNode(t *testing.T) {
	// DistributedBackend with node == nil must return "" (no panic).
	db := newTestDB(t)
	b := &DistributedBackend{db: db, fsm: NewFSM(db, newStateKeyspaceEmpty())}
	assert.Equal(t, "", b.RaftNodeID())
}

func TestOwnedShards_MetadataOnlyPlacement(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())
	b := &DistributedBackend{db: db, fsm: fsm}

	raw, err := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket:      "b",
		Key:         "obj",
		VersionID:   "v1",
		Size:        1,
		ContentType: "application/octet-stream",
		ETag:        "etag",
		ModTime:     1,
		ECData:      2,
		ECParity:    1,
		NodeIDs:     []string{"test-node", "other", "test-node"},
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(raw))

	got := b.OwnedShards("b", "obj", "v1", "test-node")
	assert.Equal(t, []int{0, 2}, got)
}
