package cluster

import (
	"bytes"
	"context"
	"testing"

	"github.com/gritive/GrainFS/internal/transport"
	"github.com/stretchr/testify/require"
)

// TestWriteLocalShardStream_RequiresWAL asserts the stream write path still
// rejects a missing WAL. NOTE (S2): the regular shard-write path no longer
// requires a WAL (durability is write-time fsync / EC); this stream-path guard
// now only backstops the WAL-sized buffering cap (maxRawShardPayloadForWAL), not
// durability. The guard + this test are removed in S4 along with the WAL wiring.
func TestWriteLocalShardStream_RequiresWAL(t *testing.T) {
	tr := transport.MustNewHTTPTransport("test-cluster-psk")
	t.Cleanup(func() { _ = tr.Close() })
	keeper, clusterID := testDEKKeeper(t)
	svc := NewShardService(t.TempDir(), tr, WithShardDEKKeeper(keeper, clusterID)) // no WithDataWAL

	err := svc.WriteLocalShardStreamContext(context.Background(), "b", "k", 0, bytes.NewReader([]byte("payload")))
	require.Error(t, err, "stream shard write without a WAL must be rejected")
	require.Contains(t, err.Error(), "WAL")
}

// TestWriteLocalShardStream_WithWALReadable asserts the stream write goes
// through the WAL/[]byte path and the shard is readable afterward.
func TestWriteLocalShardStream_WithWALReadable(t *testing.T) {
	tr := transport.MustNewHTTPTransport("test-cluster-psk")
	t.Cleanup(func() { _ = tr.Close() })
	keeper, clusterID := testDEKKeeper(t)
	svc := NewShardService(t.TempDir(), tr, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))

	plaintext := []byte("streamed shard payload")
	require.NoError(t, svc.WriteLocalShardStreamContext(context.Background(), "b", "k", 0, bytes.NewReader(plaintext)))

	got, err := svc.ReadLocalShard("b", "k", 0)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)
}
