package nfs4server

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/storage/pullthrough"
)

type noPullthroughResolver struct{}

func (noPullthroughResolver) Resolve(string) (pullthrough.Upstream, bool) {
	return nil, false
}

func TestPartialIOBackendUnwrapsPullThroughDecorator(t *testing.T) {
	local, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, local.CreateBucket(context.Background(), nfs4Bucket))

	wrapped := pullthrough.NewBackend(local, noPullthroughResolver{})
	partial, ok := partialIOBackend(wrapped)
	require.True(t, ok, "NFS must see PartialIO through pullthrough to avoid full-object RMW")

	_, err = partial.WriteAt(context.Background(), nfs4Bucket, "file.bin", 0, []byte("abc"))
	require.NoError(t, err)
	rc, _, err := wrapped.GetObject(context.Background(), nfs4Bucket, "file.bin")
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, []byte("abc"), got)
}

func TestTruncatableBackendUnwrapsPullThroughDecorator(t *testing.T) {
	local, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, local.CreateBucket(context.Background(), nfs4Bucket))
	_, err = local.PutObject(context.Background(), nfs4Bucket, "file.bin", strings.NewReader("abc"), "application/octet-stream")
	require.NoError(t, err)

	wrapped := pullthrough.NewBackend(local, noPullthroughResolver{})
	truncatable, ok := truncatableBackend(wrapped)
	require.True(t, ok, "NFS must see Truncatable through pullthrough to avoid zero-filled RMW")

	require.NoError(t, truncatable.Truncate(context.Background(), nfs4Bucket, "file.bin", 4096))
	obj, err := wrapped.HeadObject(context.Background(), nfs4Bucket, "file.bin")
	require.NoError(t, err)
	assert.EqualValues(t, 4096, obj.Size)
}
