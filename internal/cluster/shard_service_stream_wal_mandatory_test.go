package cluster

import (
	"bytes"
	"context"
	"testing"

	"github.com/gritive/GrainFS/internal/transport"
	"github.com/stretchr/testify/require"
)

// TestWriteLocalShardStream_RequiresWAL asserts WAL is mandatory on the stream
// write path too: with no WAL wired the stream write must be rejected rather
// than silently writing a shard file the WAL never observed.
func TestWriteLocalShardStream_RequiresWAL(t *testing.T) {
	svc := NewShardService(t.TempDir(), transport.MustNewQUICTransport("test-cluster-psk")) // no WithDataWAL

	err := svc.WriteLocalShardStreamContext(context.Background(), "b", "k", 0, bytes.NewReader([]byte("payload")))
	require.Error(t, err, "stream shard write without a WAL must be rejected")
	require.Contains(t, err.Error(), "WAL")
}

// TestWriteLocalShardStream_WithWALReadable asserts the stream write goes
// through the WAL/[]byte path and the shard is readable afterward.
func TestWriteLocalShardStream_WithWALReadable(t *testing.T) {
	svc := NewShardService(t.TempDir(), transport.MustNewQUICTransport("test-cluster-psk"), withTestWAL(t))

	plaintext := []byte("streamed shard payload")
	require.NoError(t, svc.WriteLocalShardStreamContext(context.Background(), "b", "k", 0, bytes.NewReader(plaintext)))

	got, err := svc.ReadLocalShard("b", "k", 0)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)
}
