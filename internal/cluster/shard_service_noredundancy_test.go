package cluster

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// largeShardPayload returns a payload at/above walPayloadInlineThreshold so
// shardWriteRequiresFsync takes the "large" branch.
func largeShardPayload() []byte {
	return bytes.Repeat([]byte("x"), walPayloadInlineThreshold)
}

// TestShardWriteRequiresFsync_NoRedundancyForcesFsyncForLargeShard asserts that
// on a single-node deployment (ParityShards==0) a large shard write forces a
// direct shard-file + dir fsync: with no parity and no peers, EC reconstruction
// cannot rebuild a page-cache-lost shard, so the file itself must be durable.
func TestShardWriteRequiresFsync_NoRedundancyForcesFsyncForLargeShard(t *testing.T) {
	keeper, clusterID := testDEKKeeper(t)
	svc := NewShardService(t.TempDir(), nil,
		WithShardDEKKeeper(keeper, clusterID),
		withTestWALDEK(t, keeper, clusterID),
		WithNoRedundancy(func() bool { return true }),
	)

	require.True(t, svc.shardWriteRequiresFsync(len(largeShardPayload())),
		"no-redundancy large shard must fsync the shard file directly")
}

// TestShardWriteRequiresFsync_NoRedundancySmallFsynced asserts the S2 flip: a
// small shard is now fsynced directly (its old WAL-inline durability is gone).
func TestShardWriteRequiresFsync_NoRedundancySmallFsynced(t *testing.T) {
	keeper, clusterID := testDEKKeeper(t)
	svc := NewShardService(t.TempDir(), nil,
		WithShardDEKKeeper(keeper, clusterID),
		withTestWALDEK(t, keeper, clusterID),
		WithNoRedundancy(func() bool { return true }),
	)

	require.True(t, svc.shardWriteRequiresFsync(len([]byte("small"))),
		"small shard is fsynced directly after S2 (no WAL)")
}

// TestShardWriteRequiresFsync_RedundantLargeNoFsync asserts the parity>0 case is
// unchanged: a large redundant shard relies on EC reconstruction, so no direct
// shard fsync is forced (keeps the benchmark path fsync-free, S1).
func TestShardWriteRequiresFsync_RedundantLargeNoFsync(t *testing.T) {
	keeper, clusterID := testDEKKeeper(t)
	svc := NewShardService(t.TempDir(), nil,
		WithShardDEKKeeper(keeper, clusterID),
		withTestWALDEK(t, keeper, clusterID),
		WithNoRedundancy(func() bool { return false }),
	)

	require.False(t, svc.shardWriteRequiresFsync(len(largeShardPayload())),
		"with parity, large shard relies on EC reconstruction; no fsync")
}

// TestShardWriteRequiresFsync_NilNoRedundancyLargeNoFsync asserts the default
// (no provider wired) counts as redundant: a large shard is not fsynced.
func TestShardWriteRequiresFsync_NilNoRedundancyLargeNoFsync(t *testing.T) {
	keeper, clusterID := testDEKKeeper(t)
	svc := NewShardService(t.TempDir(), nil, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID)) // no WithNoRedundancy

	require.False(t, svc.shardWriteRequiresFsync(len(largeShardPayload())),
		"nil noRedundancy counts as redundant; large → no fsync")
}
