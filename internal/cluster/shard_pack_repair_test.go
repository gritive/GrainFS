package cluster

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestWriteShardRepairsPackResidentShard reproduces the repair-shadowing bug:
// readShardIntegrity reads pack-first, but the scrubber-repair WriteShard wrote a
// standalone shard_N file, so a pack-resident shard's repair was shadowed by the
// stale pack entry on the next read. After the fix, a pack-resident shard is
// repaired INTO the pack, so the pack-first read observes the repaired bytes.
func TestWriteShardRepairsPackResidentShard(t *testing.T) {
	b := newTestDistributedBackendDEK(t, WithShardPackThreshold(1<<20))
	require.NotNil(t, b.shardSvc.shardPack, "pack must be enabled for this test")

	bucket, key, versionID := "bkt", "obj", "v0000000000000001"
	canonicalKey := ecObjectShardKey(key, versionID)
	total := b.currentECConfig().NumShards()
	path := b.ShardPaths(bucket, key, versionID, total)[0]

	// Seed a STALE entry into the pack (small ⇒ pack-resident).
	stale := bytes.Repeat([]byte("STALE-pack-bytes"), 4)
	require.NoError(t, b.shardSvc.writeLocalShard(context.Background(), bucket, canonicalKey, 0, stale))
	got, err := b.ReadShard(bucket, key, versionID, 0, path)
	require.NoError(t, err)
	require.Equal(t, stale, got, "precondition: shard must be pack-resident and read back as stale")

	// Repair with FRESH bytes via the scrubber-repair write path.
	fresh := bytes.Repeat([]byte("FRESH-repaired"), 8)
	require.NoError(t, b.WriteShard(bucket, key, versionID, 0, path, fresh))

	// The pack-first read MUST now return the repaired bytes, not the stale pack entry.
	got, err = b.ReadShard(bucket, key, versionID, 0, path)
	require.NoError(t, err)
	require.Equal(t, fresh, got, "repair must not be shadowed by the stale pack entry")
}
