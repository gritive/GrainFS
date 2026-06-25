package cluster

// Unit coverage for DistributedBackend scrubber contracts: scrubber.ShardOwner
// (NodeID, OwnedShards) and scrubber.ShardRepairer (RepairShardLocal).

import (
	"context"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/badgermeta"
	"github.com/gritive/GrainFS/internal/scrubber"
)

// Compile-time proof that DistributedBackend satisfies the scrubber contracts.
var _ scrubber.ShardRepairer = (*DistributedBackend)(nil)
var _ scrubber.ShardOwner = (*DistributedBackend)(nil)

// writePlacement seeds a placement record directly in the FSM's BadgerDB,
// bypassing the Raft proposal path. Matches the byte layout that
// applyPutShardPlacement writes. Used by tests that call LookupShardPlacement
// directly (not ResolvePlacement).
func writePlacement(t clusterTestTB, b *DistributedBackend, db *badger.DB, bucket, key string, nodes []string) {
	t.Helper()
	if err := db.Update(func(txn *badger.Txn) error {
		rec := PlacementRecord{Nodes: nodes, K: 4, M: 2}
		return txn.Set(shardPlacementKey(bucket, key), encodePlacementValue(rec))
	}); err != nil {
		t.Fatalf("write placement: %v", err)
	}
}

// seedPlacementMeta writes an object metadata record (including EC placement)
// directly into the FSM's DB, so that readPlacementMeta can resolve it.
// CmdPutObjectMeta is a no-op in apply.go (raft-free Slice 2), so we persist
// directly via persistPutObjectMetaUpdate.
func seedPlacementMeta(t clusterTestTB, b *DistributedBackend, bucket, key, versionID string, nodes []string, ecData, ecParity uint8) {
	t.Helper()
	cmd := PutObjectMetaCmd{
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
	}
	if err := b.fsm.db.Update(func(txn MetadataTxn) error {
		return b.fsm.persistPutObjectMetaUpdate(txn, cmd, buildPutObjectMeta(cmd))
	}); err != nil {
		t.Fatalf("seed placement meta: %v", err)
	}
}

func TestRaftNodeID_NilNode(t *testing.T) {
	// DistributedBackend with node == nil must return "" (no panic).
	db := newTestDB(t)
	b := &DistributedBackend{store: badgermeta.Wrap(db), fsm: NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())}
	assert.Equal(t, "", b.RaftNodeID())
}

func TestOwnedShards_MetadataOnlyPlacement(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "b"))

	// Placement (NodeIDs) lives on the latest-only quorum-meta blob — the
	// non-versioned object authority readPlacementMeta resolves.
	seedLatestBlob(t, b, "b", "obj", PutObjectMetaCmd{
		Size:        1,
		ContentType: "application/octet-stream",
		ETag:        "etag",
		ModTime:     1,
		ECData:      2,
		ECParity:    1,
		NodeIDs:     []string{"test-node", "other", "test-node"},
	})

	got := b.OwnedShards("b", "obj", "v1", "test-node")
	assert.Equal(t, []int{0, 2}, got)
}
