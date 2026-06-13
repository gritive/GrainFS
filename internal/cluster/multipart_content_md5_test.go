package cluster

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

func TestDistributedBackend_UploadPart_ContentMD5Mismatch(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "bucket"))
	up, err := b.CreateMultipartUpload(ctx, "bucket", "k", "text/plain")
	require.NoError(t, err)

	_, err = b.UploadPart(ctx, "bucket", "k", up.UploadID, 1, bytes.NewReader([]byte("hello")), "deadbeefdeadbeefdeadbeefdeadbeef")
	require.ErrorIs(t, err, storage.ErrContentMD5Mismatch)

	// The staged part must be gone so it cannot be completed.
	parts, err := b.ListParts(ctx, "bucket", "k", up.UploadID, 1000)
	require.NoError(t, err)
	require.Empty(t, parts)
}

func TestDistributedBackend_UploadPart_ContentMD5Match(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "bucket"))
	up, err := b.CreateMultipartUpload(ctx, "bucket", "k2", "text/plain")
	require.NoError(t, err)

	// md5("hello") = 5d41402abc4b2a76b9719d911017c592
	part, err := b.UploadPart(ctx, "bucket", "k2", up.UploadID, 1, bytes.NewReader([]byte("hello")), "5d41402abc4b2a76b9719d911017c592")
	require.NoError(t, err)
	require.Equal(t, "5d41402abc4b2a76b9719d911017c592", part.ETag)
}
