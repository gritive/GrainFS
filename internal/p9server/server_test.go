package p9server

import (
	"context"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

func newTestBackend(t *testing.T) storage.Backend {
	t.Helper()
	b, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	return b
}

// Task 2: rootFile tests

func TestRootFile_Walk_EmptyReturnsClone(t *testing.T) {
	root := &rootFile{backend: newTestBackend(t)}
	qids, file, err := root.Walk(nil)
	require.NoError(t, err)
	require.Empty(t, qids)
	require.NotNil(t, file)
	_, ok := file.(*rootFile)
	require.True(t, ok)
}

func TestRootFile_Walk_UnknownBucket_ENOENT(t *testing.T) {
	root := &rootFile{backend: newTestBackend(t)}
	_, _, err := root.Walk([]string{"no-such-bucket"})
	require.ErrorIs(t, err, syscall.ENOENT)
}

func TestRootFile_Walk_ExistingBucket_ReturnsBucketFile(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "my-bucket"))

	root := &rootFile{backend: backend}
	qids, file, err := root.Walk([]string{"my-bucket"})
	require.NoError(t, err)
	require.Len(t, qids, 1)
	_, ok := file.(*bucketFile)
	require.True(t, ok)
}

func TestRootFile_Readdir_ListsBuckets(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "alpha"))
	require.NoError(t, backend.CreateBucket(ctx, "beta"))

	root := &rootFile{backend: backend}
	dirents, err := root.Readdir(0, 100)
	require.NoError(t, err)
	names := make([]string, len(dirents))
	for i, d := range dirents {
		names[i] = d.Name
	}
	require.Contains(t, names, "alpha")
	require.Contains(t, names, "beta")
}
