package cluster

import (
	"context"
	"testing"

	"github.com/gritive/GrainFS/internal/transport"
	"github.com/stretchr/testify/require"
)

// TestAppendShardDataWAL_RequiresWAL asserts WAL is mandatory: the shard write
// path must reject a write when no WAL is wired, instead of silently signalling
// a direct-fsync fallback (the old requireFsync=true behaviour).
func TestAppendShardDataWAL_RequiresWAL(t *testing.T) {
	keeper, clusterID := testDEKKeeper(t)
	svc := NewShardService(t.TempDir(), nil, WithShardDEKKeeper(keeper, clusterID)) // no WithDataWAL → dataWAL == nil
	_, err := svc.appendShardDataWAL(context.Background(), "bucket", "key", 0, []byte("payload"))
	require.Error(t, err, "shard write without a WAL must be rejected")
	require.Contains(t, err.Error(), "WAL")
}

// TestAppendShardDataWAL_ReplayRequiresFsync guards the surviving requireFsync
// path: during WAL replay the WAL cannot be re-appended, so the caller must
// fsync the shard file directly.
func TestAppendShardDataWAL_ReplayRequiresFsync(t *testing.T) {
	dir := t.TempDir()
	keeper, clusterID := testDEKKeeper(t)
	svc := NewShardService(dir, transport.MustNewTCPTransport("test-cluster-psk"), WithShardDEKKeeper(keeper, clusterID), WithDataWAL(mustTestDataWALDEK(t, dir, keeper, clusterID)))
	svc.replayingDataWAL.Store(true)

	requireFsync, err := svc.appendShardDataWAL(context.Background(), "b", "k", 0, []byte("d"))
	require.NoError(t, err)
	require.True(t, requireFsync, "replay must require direct shard fsync")
}
