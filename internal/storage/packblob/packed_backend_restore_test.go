package packblob

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
)

// Regression for the in-memory index recovery path. Without LoadIndex() in
// NewPackedBackendWithOptions, the second PackedBackend opens with an empty
// pb.index and HeadObject returns NotFound for any small object PUT before
// the restart, even though its bytes are still in blob_*.bin.
func TestPackedBackend_NewPackedBackend_LoadsExistingIndex(t *testing.T) {
	dir := t.TempDir()
	blobDir := dir + "/blobs"

	inner1 := cluster.NewSingletonBackendForTest(t)
	pb1, err := NewPackedBackend(inner1, blobDir, 64*1024)
	require.NoError(t, err)
	require.NoError(t, pb1.CreateBucket(context.Background(), "b"))

	_, err = pb1.PutObject(context.Background(), "b", "small.txt", strings.NewReader("hello"), "text/plain")
	require.NoError(t, err)
	require.NoError(t, pb1.Close())
	require.NoError(t, inner1.Close())

	// Re-open over the same on-disk state.
	inner2 := cluster.NewSingletonBackendForTest(t)
	pb2, err := NewPackedBackend(inner2, blobDir, 64*1024)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pb2.Close() })

	head, err := pb2.HeadObject(context.Background(), "b", "small.txt")
	require.NoError(t, err)
	require.Equal(t, int64(5), head.Size)
}
