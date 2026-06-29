package cluster

import (
	"bytes"
	"context"
	"testing"

	"github.com/gritive/GrainFS/internal/transport"
	"github.com/stretchr/testify/require"
)

// TestWriteLocalShardStream_OverCap_Rejected proves the 64 MiB buffering cap is
// enforced before reading the body.
func TestWriteLocalShardStream_OverCap_Rejected(t *testing.T) {
	tr := transport.MustNewHTTPTransport("test-cluster-psk")
	t.Cleanup(func() { _ = tr.Close() })
	keeper, clusterID := testDEKKeeper(t)
	svc := NewShardService(t.TempDir(), tr, WithShardDEKKeeper(keeper, clusterID))

	over := maxRawShardPayload(false) + 1
	err := svc.WriteLocalShardStreamSizedContext(context.Background(), "b", "k", 0, bytes.NewReader([]byte("x")), over)
	require.Error(t, err, "over-cap stream write must be rejected by the size cap")
}

// TestShardWriteRequiresFsync_Classes pins the fsync decision per shard class
// small => fsync, large+redundant => no fsync (EC), large+no-redundancy =>
// fsync, nil provider => redundant.
func TestShardWriteRequiresFsync_Classes(t *testing.T) {
	small := largeShardFsyncThreshold - 1
	large := largeShardFsyncThreshold + 1

	redundant := &LocalShardStore{noRedundancy: func() bool { return false }}
	require.True(t, redundant.shardWriteRequiresFsync(small), "small shard always fsyncs")
	require.False(t, redundant.shardWriteRequiresFsync(large), "large redundant: EC, no fsync")

	noRed := &LocalShardStore{noRedundancy: func() bool { return true }}
	require.True(t, noRed.shardWriteRequiresFsync(large), "large no-redundancy: fsync")

	nilRed := &LocalShardStore{} // nil provider => redundant
	require.False(t, nilRed.shardWriteRequiresFsync(large), "nil provider counts as redundant")
}
