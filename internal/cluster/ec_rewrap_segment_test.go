package cluster

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rewrapSegmentShard is shared logic for TestECRewrapSegment_MigratesSegmentShard
// and TestECRewrapSegment_MigratesCoalescedShard.
func rewrapSegmentShard(t *testing.T, shardKey string) {
	t.Helper()

	backend, keeper := setupECRewrapBackend(t)
	require.NoError(t, backend.CreateBucket(context.Background(), "b"))

	svc := backend.shardSvc
	content := bytes.Repeat([]byte("segment-rewrap-payload-"), 64)

	// Write an encrypted shard at the given segment/coalesced key.
	// WriteLocalShard always seals via the DEK keeper (GFSENC3 per-shard).
	require.NoError(t, svc.WriteLocalShard("b", shardKey, 0, content))

	// Verify the shard is GFSENC3 at gen 0.
	require.Equal(t, uint32(0), shardGenOnDisk(t, backend, "b", shardKey, 0), "shard must be GFSENC3 at gen 0 after write")

	// Advance the active DEK generation to 1.
	require.NoError(t, keeper.Rotate())
	if _, active := keeper.VersionsAndActive(); active != 1 {
		t.Fatalf("expected active gen 1 after rotate, got %d", active)
	}

	// First call: migrates the stale gen-0 shard to active gen 1.
	did, err := backend.RewrapShardIfStaleAt("b", shardKey, 0, 1)
	require.NoError(t, err)
	assert.True(t, did, "stale shard must be migrated")

	// Shard header must now reflect gen 1.
	assert.Equal(t, uint32(1), shardGenOnDisk(t, backend, "b", shardKey, 0), "shard must be at active gen after rewrap")

	// Plaintext must be preserved after rewrap.
	got, err := svc.ReadLocalShard("b", shardKey, 0)
	require.NoError(t, err)
	assert.Equal(t, content, got, "plaintext preserved across rewrap")

	// Second call is idempotent.
	did, err = backend.RewrapShardIfStaleAt("b", shardKey, 0, 1)
	require.NoError(t, err)
	assert.False(t, did, "already-migrated shard must be a no-op")
}

func TestECRewrapSegment_MigratesSegmentShard(t *testing.T) {
	rewrapSegmentShard(t, "obj/segments/seg-blob-0001")
}

func TestECRewrapSegment_MigratesCoalescedShard(t *testing.T) {
	rewrapSegmentShard(t, "obj/coalesced/co-0001")
}
