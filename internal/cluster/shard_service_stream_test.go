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

// TestWriteShardStreamSized_RemoteShortBody asserts that the remote sized write
// path errors when the sender provides fewer bytes than the declared streamSize.
// The HTTP transport detects the body underrun (Content-Length header vs actual
// body bytes) and returns an error client-side before the server responds.
func TestWriteShardStreamSized_RemoteShortBody(t *testing.T) {
	ctx := context.Background()
	keeper, clusterID := testDEKKeeper(t)

	tr1 := transport.MustNewHTTPTransport("test-cluster-psk")
	tr2 := transport.MustNewHTTPTransport("test-cluster-psk")
	require.NoError(t, tr1.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, tr2.Listen(ctx, "127.0.0.1:0"))
	defer tr1.Close()
	defer tr2.Close()

	dir1, dir2 := t.TempDir(), t.TempDir()
	svc1 := NewShardService(dir1, tr1, WithShardDEKKeeper(keeper, clusterID))
	svc2 := NewShardService(dir2, tr2, WithShardDEKKeeper(keeper, clusterID))
	tr2.RegisterShardWriteHandler(svc2.NativeWriteHandler())

	// Declare 10 bytes but provide only 3. The HTTP transport detects the body
	// underrun via Content-Length and errors ("copied N bytes ... instead of M").
	err := svc1.WriteShardStreamSized(ctx, tr2.LocalAddr(), "bkt", "key", 0, bytes.NewReader([]byte("abc")), 10)
	require.Error(t, err, "short body must error through remote sized write path")
}

// TestWriteShardStreamSized_RemoteOversizedBody asserts the remote sized write
// path behaviour when the sender provides more bytes than the declared streamSize.
// Unlike the local path (which returns an "oversized body" error), the HTTP
// transport uses Content-Length to bound the body, so the receiver sees exactly
// streamSize bytes and the write SUCCEEDS — the extra bytes are never sent.
// This is correct: the receiver writes exactly the declared shard size.
func TestWriteShardStreamSized_RemoteOversizedBody(t *testing.T) {
	ctx := context.Background()
	keeper, clusterID := testDEKKeeper(t)

	tr1 := transport.MustNewHTTPTransport("test-cluster-psk")
	tr2 := transport.MustNewHTTPTransport("test-cluster-psk")
	require.NoError(t, tr1.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, tr2.Listen(ctx, "127.0.0.1:0"))
	defer tr1.Close()
	defer tr2.Close()

	dir1, dir2 := t.TempDir(), t.TempDir()
	svc1 := NewShardService(dir1, tr1, WithShardDEKKeeper(keeper, clusterID))
	svc2 := NewShardService(dir2, tr2, WithShardDEKKeeper(keeper, clusterID))
	tr2.RegisterShardWriteHandler(svc2.NativeWriteHandler())

	// Declare 3 bytes but provide 10. The HTTP Content-Length bounds the body
	// to 3 bytes; the receiver writes exactly those 3 bytes. No error.
	err := svc1.WriteShardStreamSized(ctx, tr2.LocalAddr(), "bkt", "key", 0, bytes.NewReader([]byte("0123456789")), 3)
	require.NoError(t, err, "oversized-body write must succeed through remote path (Content-Length truncates naturally)")
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
