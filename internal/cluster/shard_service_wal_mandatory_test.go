package cluster

import (
	"testing"

	"github.com/gritive/GrainFS/internal/transport"
	"github.com/stretchr/testify/require"
)

// S2 moved shard durability off the data WAL (write-time fsync / EC), so the
// regular shard-write path no longer REQUIRES a wired WAL — the old
// TestAppendShardDataWAL_RequiresWAL contract was intentionally removed. The
// stream path still requires a WAL until S4 (see
// shard_service_stream_wal_mandatory_test.go).

// TestShardWriteRequiresFsync_ReplayRequiresFsync guards the surviving
// requireFsync path: during WAL replay the WAL cannot be re-appended, so a
// normal write reached mid-replay must fsync the shard file directly.
func TestShardWriteRequiresFsync_ReplayRequiresFsync(t *testing.T) {
	dir := t.TempDir()
	keeper, clusterID := testDEKKeeper(t)
	svc := NewShardService(dir, transport.MustNewHTTPTransport("test-cluster-psk"), WithShardDEKKeeper(keeper, clusterID), WithDataWAL(mustTestDataWALDEK(t, dir, keeper, clusterID)))
	svc.replayingDataWAL.Store(true)

	require.True(t, svc.shardWriteRequiresFsync(len([]byte("d"))),
		"replay must require direct shard fsync")
}
