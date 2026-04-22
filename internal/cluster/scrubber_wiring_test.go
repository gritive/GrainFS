package cluster

// Slice 3 of refactor/unify-storage-paths: unit coverage for the
// NodeID / OwnedShards / RepairShardLocal glue consumed by the scrubber
// via scrubber.ShardOwner and scrubber.ShardRepairer.

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/scrubber"
)

// Compile-time proof that DistributedBackend satisfies the optional
// ShardRepairer contract the scrubber uses in cluster mode. ShardOwner is
// deliberately not wired yet — see docstring on RaftNodeID.
var _ scrubber.ShardRepairer = (*DistributedBackend)(nil)

// writePlacement seeds a placement record directly in the FSM's BadgerDB,
// bypassing the Raft proposal path. Matches the byte layout that
// applyPutShardPlacement writes.
func writePlacement(t *testing.T, b *DistributedBackend, bucket, key string, nodes []string) {
	t.Helper()
	require.NoError(t, b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(shardPlacementKey(bucket, key), encodePlacementValue(nodes))
	}))
}

func TestRaftNodeID_ReturnsRaftNodeID(t *testing.T) {
	b := newTestDistributedBackend(t)
	assert.Equal(t, "test-node", b.RaftNodeID(),
		"RaftNodeID must return the Raft node.ID() set on the underlying Node")
}

func TestOwnedShards(t *testing.T) {
	b := newTestDistributedBackend(t)

	tests := []struct {
		name   string
		bucket string
		key    string
		seed   []string // nil → skip placement write ("no placement")
		nodeID string
		want   []int
	}{
		{
			name:   "no placement record yields nil",
			bucket: "b",
			key:    "none",
			seed:   nil,
			nodeID: "test-node",
			want:   nil,
		},
		{
			name:   "node owns zero shards",
			bucket: "b",
			key:    "zero",
			seed:   []string{"other-a", "other-b", "other-c"},
			nodeID: "test-node",
			want:   nil,
		},
		{
			name:   "node owns exactly one shard",
			bucket: "b",
			key:    "one",
			seed:   []string{"other-a", "test-node", "other-b"},
			nodeID: "test-node",
			want:   []int{1},
		},
		{
			name:   "node owns multiple non-contiguous shards",
			bucket: "b",
			key:    "many",
			seed:   []string{"test-node", "other-a", "test-node", "other-b", "test-node"},
			nodeID: "test-node",
			want:   []int{0, 2, 4},
		},
		{
			name:   "unknown node gets nil",
			bucket: "b",
			key:    "one",
			seed:   nil, // re-use the one-shard placement written above
			nodeID: "not-in-cluster",
			want:   nil,
		},
	}

	const testVersionID = "any-version"
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.seed != nil {
				// Placement is now stored under shardKey = key + "/" + versionID.
				writePlacement(t, b, tc.bucket, tc.key+"/"+testVersionID, tc.seed)
			}
			got := b.OwnedShards(tc.bucket, tc.key, testVersionID, tc.nodeID)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestRepairShardLocal_WithoutShardService(t *testing.T) {
	// RepairShardLocal wraps RepairShard; when ShardService is not configured
	// it must surface the "shard service not configured" error rather than
	// panicking. This is the state of a test-only DistributedBackend.
	b := newTestDistributedBackend(t)
	writePlacement(t, b, "b", "k/any-version", []string{"test-node", "other-a"})
	err := b.RepairShardLocal("b", "k", "any-version", 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "shard service not configured")
}
