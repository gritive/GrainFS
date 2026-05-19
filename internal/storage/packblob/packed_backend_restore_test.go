package packblob

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// Regression for the in-memory index recovery path. Without LoadIndex() in
// NewPackedBackendWithOptions, the second PackedBackend opens with an empty
// pb.index and HeadObject returns NotFound for any small object PUT before
// the restart, even though its bytes are still in blob_*.bin.
func TestPackedBackend_NewPackedBackend_LoadsExistingIndex(t *testing.T) {
	dir := t.TempDir()
	blobDir := dir + "/blobs"
	innerDir := dir + "/local"

	inner1, err := storage.NewLocalBackend(innerDir)
	require.NoError(t, err)
	pb1, err := NewPackedBackend(inner1, blobDir, 64*1024)
	require.NoError(t, err)
	require.NoError(t, pb1.CreateBucket(context.Background(), "b"))

	_, err = pb1.PutObject(context.Background(), "b", "small.txt", strings.NewReader("hello"), "text/plain")
	require.NoError(t, err)
	require.NoError(t, pb1.Close())
	require.NoError(t, inner1.Close())

	// Re-open over the same on-disk state.
	inner2, err := storage.NewLocalBackend(innerDir)
	require.NoError(t, err)
	pb2, err := NewPackedBackend(inner2, blobDir, 64*1024)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pb2.Close() })

	head, err := pb2.HeadObject(context.Background(), "b", "small.txt")
	require.NoError(t, err)
	require.Equal(t, int64(5), head.Size)
}

// Regression for the snapshot restore path. Before the fix, RestoreObjects
// wiped pb.index and delegated everything to inner. Packed-fast-path objects
// only live in blob_*.bin (inner cluster meta-FSM never received them), so
// the restore returned 0 and packed objects became invisible to LIST/GET.
func TestPackedBackend_RestoreObjects_RebuildsPackedFromDisk(t *testing.T) {
	pb := newTestPackedBackend(t)
	ctx := context.Background()
	require.NoError(t, pb.CreateBucket(ctx, "b"))

	_, err := pb.PutObject(ctx, "b", "small.txt", strings.NewReader("hello"), "text/plain")
	require.NoError(t, err)

	snap, err := pb.ListAllObjects()
	require.NoError(t, err)
	require.Len(t, snap, 1)

	// Drop the in-memory index; the blob and the snapshot are the only
	// surviving evidence of the PUT.
	pb.index.Range(func(k, _ any) bool { pb.index.Delete(k); return true })

	restored, stale, err := pb.RestoreObjects(snap)
	require.NoError(t, err)
	require.Empty(t, stale)
	require.Equal(t, 1, restored)

	head, err := pb.HeadObject(ctx, "b", "small.txt")
	require.NoError(t, err)
	require.Equal(t, int64(5), head.Size)
}
