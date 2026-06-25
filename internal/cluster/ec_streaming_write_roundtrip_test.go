package cluster

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestECStreamingShardWrite_LargeRoundTrip is the data-loss gate for the sized
// streaming shard-write path (writeLocalShardStreamContext sized →
// writeLocalShardAADStream → atomicShardFileWrite encode-to-file): a multi-stripe
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

// TestAtomicShardFileWrite_SmallShardRoundTrip is the characterization gate for
// the atomicShardFileWrite helper: a small object (each data shard well under
// largeShardFsyncThreshold, so the requireFsync=true write-time-fsync class) must
// round-trip byte-for-byte. It complements the 16 MiB large-shard
// (requireFsync=false EC-durability) case above, exercising the other fsync
// branch.
func TestAtomicShardFileWrite_SmallShardRoundTrip(t *testing.T) {
	bk := newECBenchmarkBackend(t)
	ctx := context.Background()
	require.NoError(t, bk.CreateBucket(ctx, "b"))

	data := bytes.Repeat([]byte("z"), 64<<10)
	_, err := bk.PutObject(ctx, "b", "k", bytes.NewReader(data), "application/octet-stream")
	require.NoError(t, err)

	rc, _, err := bk.GetObject(ctx, "b", "k")
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	_ = rc.Close()
	require.NoError(t, err)

	require.True(t, bytes.Equal(data, got), "small EC object must round-trip through the fsync-class shard write")
}

// TestAtomicShardFileWrite_EncodeErrorLeavesNoFile is the partial-write cleanup
// gate: when the encode callback fails mid-write, atomicShardFileWrite must
// publish NO shard file at path and leave NO orphan .tmp behind. This protects
// the streaming encode-to-file path (a mid-encode AEAD/read error must not strand
// a truncated shard or a leaked tmp).
func TestAtomicShardFileWrite_EncodeErrorLeavesNoFile(t *testing.T) {
	bk := newECBenchmarkBackend(t)
	l := bk.shardSvc.local
	dir, err := l.getShardDir("b", "k", 0)
	require.NoError(t, err)
	require.NoError(t, l.ensureShardDir(dir))
	path := filepath.Join(dir, "shard_0")

	wantErr := errors.New("injected encode failure")
	err = l.atomicShardFileWrite(context.Background(), dir, path, 0, func(w io.Writer) error {
		_, _ = w.Write([]byte("partial"))
		return wantErr
	})
	require.ErrorIs(t, err, wantErr)

	_, statErr := os.Stat(path)
	require.True(t, os.IsNotExist(statErr), "no shard file published on encode error")
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		require.NotContains(t, e.Name(), ".tmp", "no orphan tmp left behind")
	}
}

// TestWriteLocalShardAADStream_ShortBodyRejected is the data-loss guard for the
// streaming encode-to-file path: a body shorter than the committed sizeHint must
// fail loudly (short read) instead of silently writing a truncated shard. The
// shardCountingReader count must still be checked after the encode streams
// straight into the file.
func TestWriteLocalShardAADStream_ShortBodyRejected(t *testing.T) {
	bk := newECBenchmarkBackend(t)
	err := bk.shardSvc.local.writeLocalShardAADStream(context.Background(), "b", "k", "k", 0, bytes.NewReader([]byte("only-8b!")), 1<<20)
	require.Error(t, err)
	require.Contains(t, err.Error(), "short read")
}
