// repair_shard_at_shardkey_test.go: RepairShardAtShardKey reconstructs a local
// shard for an arbitrary physical shard key (here a segment-style key) given an
// already-resolved EC placement, bypassing object-version placement resolution.

package cluster

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRepairShardAtShardKey_SegmentKey(t *testing.T) {
	backend := setupECBackend(t)
	svc := backend.shardSvc

	require.NoError(t, backend.CreateBucket(t.Context(), "b"))

	// Build an EC stripe (k=1, m=1) directly at a segment-style shard key,
	// writing each shard locally. All placement nodes are "self" so reads/writes
	// are local. We deliberately do NOT go through PutObject/placement records —
	// RepairShardAtShardKey must work off the supplied PlacementRecord alone.
	const shardKey = "obj/segments/seg-blob-0001"
	cfg := ECConfig{DataShards: 1, ParityShards: 1}
	content := bytes.Repeat([]byte("repair-at-shardkey-segment-"), 256)
	freshShards, err := ECSplit(cfg, content)
	require.NoError(t, err)
	require.Len(t, freshShards, 2)
	for i, s := range freshShards {
		require.NoError(t, svc.WriteLocalShard("b", shardKey, i, s))
	}

	// Drop shard 0 so it must be reconstructed from the surviving shard 1.
	require.NoError(t, os.Remove(svc.getShardPath("b", shardKey, 0)))

	rec := PlacementRecord{Nodes: []string{"self", "self"}, K: 1, M: 1}
	require.NoError(t, backend.RepairShardAtShardKey(t.Context(), "b", shardKey, rec, 0))

	// The rebuilt shard must exist and match the canonical split bytes.
	rebuilt, err := svc.ReadLocalShard("b", shardKey, 0)
	require.NoError(t, err)
	require.Equal(t, freshShards[0], rebuilt)
}

func TestRepairShardAtShardKey_InsufficientSurvivors(t *testing.T) {
	backend := setupECBackend(t)
	svc := backend.shardSvc

	require.NoError(t, backend.CreateBucket(t.Context(), "b"))

	const shardKey = "obj/segments/seg-blob-0002"
	cfg := ECConfig{DataShards: 1, ParityShards: 1}
	content := bytes.Repeat([]byte("repair-insufficient-survivors-"), 256)
	freshShards, err := ECSplit(cfg, content)
	require.NoError(t, err)
	require.Len(t, freshShards, 2)
	for i, s := range freshShards {
		require.NoError(t, svc.WriteLocalShard("b", shardKey, i, s))
	}

	// Delete BOTH shards: repairing shard 0 skips shard 0 and finds shard 1 also
	// gone, leaving 0 survivors < DataShards. The error must carry the substring
	// classifyDataWALStartupRepairFailure matches on.
	require.NoError(t, os.Remove(svc.getShardPath("b", shardKey, 0)))
	require.NoError(t, os.Remove(svc.getShardPath("b", shardKey, 1)))

	rec := PlacementRecord{Nodes: []string{"self", "self"}, K: 1, M: 1}
	err = backend.RepairShardAtShardKey(t.Context(), "b", shardKey, rec, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "other shards readable")
}

func TestRepairShardAtShardKey_NilShardService(t *testing.T) {
	backend := NewSingletonBackendForTest(t)
	backend.shardSvc = nil
	err := backend.RepairShardAtShardKey(t.Context(), "b", "obj/segments/x", PlacementRecord{Nodes: []string{"self", "self"}, K: 1, M: 1}, 0)
	require.Error(t, err)
}
