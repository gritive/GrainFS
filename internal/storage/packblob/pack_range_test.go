// Range/ReadAt coverage for PackedBackend.
//
// Locks the Phase 1.6.7 fix: when a chain like
// `clusterCoord → packblob → wal → pullthrough` exists, the wal/pullthrough
// wrappers do `b.Backend.(storage.PartialIO)`. Before this fix that assertion
// failed at the packblob layer and any Range GET (single-node, object above
// pack threshold) surfaced as `wal: inner backend does not support ReadAt`.
package packblob

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

func TestPackedBackend_ImplementsPartialIO(t *testing.T) {
	pb := newTestPackedBackend(t)
	var _ storage.PartialIO = pb
	// Also assert through the interface that the chain wrappers actually use.
	_, ok := storage.Backend(pb).(storage.PartialIO)
	require.True(t, ok, "PackedBackend must satisfy storage.PartialIO so wal/pullthrough ReadAt can delegate through it")
}

type recordingPreparedReadAtBackend struct {
	storage.Backend
	calls int
	obj   *storage.Object
}

func (b *recordingPreparedReadAtBackend) ReadAtObject(ctx context.Context, bucket, key string, obj *storage.Object, offset int64, buf []byte) (int, error) {
	b.calls++
	b.obj = obj
	return copy(buf, "prepared"), nil
}

func TestPackedBackend_ReadAtObjectDelegatesPreparedInner(t *testing.T) {
	inner, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { inner.Close() })

	rec := &recordingPreparedReadAtBackend{Backend: inner}
	pb, err := NewPackedBackend(rec, t.TempDir(), 64*1024)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pb.Close()) })

	reader, ok := any(pb).(storage.PreparedReadAt)
	require.True(t, ok, "PackedBackend must preserve prepared ReadAtObject through wrapper chains")

	obj := &storage.Object{Key: "large", Size: 8, ETag: "etag"}
	buf := make([]byte, 8)
	n, err := reader.ReadAtObject(context.Background(), "test", "large", obj, 0, buf)
	require.NoError(t, err)
	require.Equal(t, 8, n)
	require.Equal(t, []byte("prepared"), buf)
	require.Equal(t, 1, rec.calls)
	require.Same(t, obj, rec.obj)
}

// TestPackedBackend_RangeAcrossSegments exercises the pass-through ReadAt path
// for objects above the pack threshold — the production-breaking case from
// Task 1.9 single-node e2e.
func TestPackedBackend_RangeAcrossSegments(t *testing.T) {
	pb := newTestPackedBackend(t) // 64 KiB threshold
	ctx := context.Background()
	require.NoError(t, pb.CreateBucket(ctx, "test"))

	// 4× a synthetic chunk size so we cross at least one boundary inside
	// the underlying LocalBackend segment dispatch. The threshold here is
	// 64 KiB — anything larger flows through to the inner segment-aware
	// LocalBackend.ReadAt path.
	chunk := 16 << 10 // 16 KiB synthetic "segment" pattern for assertion clarity
	data := make([]byte, 4*chunk)
	for i := range data {
		data[i] = byte(i)
	}
	_, err := pb.PutObject(ctx, "test", "k", bytes.NewReader(data), "application/octet-stream")
	require.NoError(t, err)

	// Range that straddles a chunk boundary — the failure mode behind Task 1.9.
	from := int64(chunk - 1024)
	to := int64(chunk + 1024)
	buf := make([]byte, to-from+1)
	n, err := pb.ReadAt(ctx, "test", "k", from, buf)
	if err != nil && err != io.EOF {
		require.NoError(t, err)
	}
	require.Equal(t, len(buf), n, "Range must return the requested number of bytes")
	require.Equal(t, data[from:to+1], buf, "Range across boundary must match")
}

// TestPackedBackend_ReadAtPackedInline covers the small-object (packed) path:
// the object lives in a blob entry, ReadAt slices from the blob payload.
func TestPackedBackend_ReadAtPackedInline(t *testing.T) {
	pb := newTestPackedBackend(t) // 64 KiB threshold
	ctx := context.Background()
	require.NoError(t, pb.CreateBucket(ctx, "test"))

	data := []byte("the quick brown fox jumps over the lazy dog")
	_, err := pb.PutObject(ctx, "test", "small", bytes.NewReader(data), "text/plain")
	require.NoError(t, err)

	// Confirm it really went through the packed path (would be invisible
	// otherwise — the helper-test threshold guarantees this).
	_, packed := pb.index.Load(packedKey{bucket: "test", key: "small"})
	require.True(t, packed, "small object must be stored inline in the pack blob")

	// Read a slice from the middle.
	buf := make([]byte, 9)
	n, err := pb.ReadAt(ctx, "test", "small", 4, buf)
	require.NoError(t, err)
	require.Equal(t, 9, n)
	require.Equal(t, []byte("quick bro"), buf)

	// Past-end → io.EOF (matches LocalBackend.ReadAt / os.File.ReadAt).
	short := make([]byte, 8)
	n, err = pb.ReadAt(ctx, "test", "small", int64(len(data)-3), short)
	require.Equal(t, io.EOF, err)
	require.Equal(t, 3, n)
	require.Equal(t, data[len(data)-3:], short[:n])

	// Offset >= size → (0, io.EOF).
	n, err = pb.ReadAt(ctx, "test", "small", int64(len(data)), short)
	require.Equal(t, io.EOF, err)
	require.Equal(t, 0, n)
}
