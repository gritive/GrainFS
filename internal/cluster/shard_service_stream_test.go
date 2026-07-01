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

// TestWriteLocalShardStreamSizedContext_ShortBody pins the stricter semantics of
// the sized streaming path: a body that provides fewer plaintext bytes than the
// declared streamSize must error with a "short read" message. This is stricter
// than the removed buffered path, which used io.ReadFull and would silently
// accept any short body by returning whatever bytes were available.
func TestWriteLocalShardStreamSizedContext_ShortBody(t *testing.T) {
	tr := transport.MustNewHTTPTransport("test-cluster-psk")
	t.Cleanup(func() { _ = tr.Close() })
	keeper, clusterID := testDEKKeeper(t)
	svc := NewShardService(t.TempDir(), tr, WithShardDEKKeeper(keeper, clusterID))

	// Declare 10 bytes but provide only 3.
	err := svc.WriteLocalShardStreamSizedContext(context.Background(), "b", "k", 0, bytes.NewReader([]byte("abc")), 10)
	require.Error(t, err, "short body must be rejected by sized streaming path")
	require.Contains(t, err.Error(), "short read")
}

// TestWriteLocalShardStreamSizedContext_OversizedBody pins the stricter semantics
// of the sized streaming path: a body that provides more bytes than the declared
// streamSize must error with an "oversized body" message.
func TestWriteLocalShardStreamSizedContext_OversizedBody(t *testing.T) {
	tr := transport.MustNewHTTPTransport("test-cluster-psk")
	t.Cleanup(func() { _ = tr.Close() })
	keeper, clusterID := testDEKKeeper(t)
	svc := NewShardService(t.TempDir(), tr, WithShardDEKKeeper(keeper, clusterID))

	// Declare 3 bytes but provide 10.
	err := svc.WriteLocalShardStreamSizedContext(context.Background(), "b", "k", 0, bytes.NewReader([]byte("0123456789")), 3)
	require.Error(t, err, "oversized body must be rejected by sized streaming path")
	require.Contains(t, err.Error(), "oversized body")
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
