package cluster

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestECStreamingShardWrite_LargeRoundTrip is the data-loss gate for the sized
// streaming shard-write path (writeLocalShardStreamContext sized →
// writeLocalShardAADStream → EncodeEncryptedShardStreamToBuffer): a multi-stripe
// EC object must survive PUT → GET byte-for-byte. The sized path no longer
// buffers the plaintext shard as a []byte; the shardCountingReader guard rejects
// a short body, but the real proof that the streamed encode is correct (chunk
// boundaries, AEAD, EC stripe layout) is reading the object back and comparing.
// A 16 MiB body spans many EC stripes and many encrypted chunks per shard.
func TestECStreamingShardWrite_LargeRoundTrip(t *testing.T) {
	bk := newECBenchmarkBackend(t)
	ctx := context.Background()
	require.NoError(t, bk.CreateBucket(ctx, "b"))

	data := make([]byte, 16<<20)
	for i := range data {
		data[i] = byte(i*131 + 7) // deterministic, non-uniform content
	}

	_, err := bk.PutObject(ctx, "b", "k", bytes.NewReader(data), "application/octet-stream")
	require.NoError(t, err)

	rc, _, err := bk.GetObject(ctx, "b", "k")
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	_ = rc.Close()
	require.NoError(t, err)

	require.Equal(t, len(data), len(got), "round-trip length")
	require.True(t, bytes.Equal(data, got), "16 MiB EC object must round-trip through the sized streaming shard write")
}
