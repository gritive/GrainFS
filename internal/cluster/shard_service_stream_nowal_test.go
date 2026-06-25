package cluster

import (
	"bytes"
	"context"
	"testing"

	"github.com/gritive/GrainFS/internal/transport"
	"github.com/stretchr/testify/require"
)

// TestWriteLocalShardStream_NoWAL_Succeeds asserts that after S4 the stream
// write path no longer requires a wired WAL — durability is write-time fsync
// (small / no-redundancy) or EC (large redundant). The shard reads back.
func TestWriteLocalShardStream_NoWAL_Succeeds(t *testing.T) {
	tr := transport.MustNewHTTPTransport("test-cluster-psk")
	t.Cleanup(func() { _ = tr.Close() })
	keeper, clusterID := testDEKKeeper(t)
	svc := NewShardService(t.TempDir(), tr, WithShardDEKKeeper(keeper, clusterID)) // no WAL

	plaintext := []byte("streamed shard payload")
	require.NoError(t, svc.WriteLocalShardStreamContext(context.Background(), "b", "k", 0, bytes.NewReader(plaintext)))

	got, err := svc.ReadLocalShard("b", "k", 0)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)
}

// TestWriteLocalShardStream_OverCap_Rejected proves the 64 MiB buffering cap is
// still enforced after the nil-WAL guard is lifted (the cap is WAL-independent;
// readShardPayload rejects streamSize > rawCap before reading the body).
func TestWriteLocalShardStream_OverCap_Rejected(t *testing.T) {
	tr := transport.MustNewHTTPTransport("test-cluster-psk")
	t.Cleanup(func() { _ = tr.Close() })
	keeper, clusterID := testDEKKeeper(t)
	svc := NewShardService(t.TempDir(), tr, WithShardDEKKeeper(keeper, clusterID))

	over := maxRawShardPayload(false) + 1
	err := svc.WriteLocalShardStreamSizedContext(context.Background(), "b", "k", 0, bytes.NewReader([]byte("x")), over)
	require.Error(t, err, "over-cap stream write must be rejected by the size cap, not the WAL")
}

// TestShardWriteRequiresFsync_Classes pins the fsync decision per shard class
// after S4 removed the replay branch: small => fsync, large+redundant => no
// fsync (EC), large+no-redundancy => fsync, nil provider => redundant.
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
