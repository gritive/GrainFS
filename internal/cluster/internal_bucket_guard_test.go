package cluster

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestInternalBucketGuard_ObjectOpsRejected verifies that all object data-plane
// operations return ErrInternalBucketNotObjectStore for internal (__grainfs_)
// buckets.
func TestInternalBucketGuard_ObjectOpsRejected(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()

	const internalBkt = "__grainfs_receipts"
	require.NoError(t, b.CreateBucket(ctx, internalBkt))

	t.Run("PutObjectWithRequest", func(t *testing.T) {
		_, err := b.PutObjectWithRequest(ctx, storage.PutObjectRequest{
			Bucket:      internalBkt,
			Key:         "k",
			Body:        strings.NewReader("data"),
			ContentType: "application/octet-stream",
		})
		require.ErrorIs(t, err, ErrInternalBucketNotObjectStore)
	})

	t.Run("GetObject", func(t *testing.T) {
		_, _, err := b.GetObject(ctx, internalBkt, "k")
		require.ErrorIs(t, err, ErrInternalBucketNotObjectStore)
	})

	t.Run("HeadObject", func(t *testing.T) {
		_, err := b.HeadObject(ctx, internalBkt, "k")
		require.ErrorIs(t, err, ErrInternalBucketNotObjectStore)
	})

	t.Run("ReadAt", func(t *testing.T) {
		buf := make([]byte, 4)
		_, err := b.ReadAt(ctx, internalBkt, "k", 0, buf)
		require.ErrorIs(t, err, ErrInternalBucketNotObjectStore)
	})

	t.Run("DeleteObject", func(t *testing.T) {
		err := b.DeleteObject(ctx, internalBkt, "k")
		require.ErrorIs(t, err, ErrInternalBucketNotObjectStore)
	})

	t.Run("AppendObject", func(t *testing.T) {
		_, err := b.AppendObject(ctx, internalBkt, "k", 0, bytes.NewReader([]byte("x")))
		require.ErrorIs(t, err, ErrInternalBucketNotObjectStore)
	})

	t.Run("SetObjectTagsPropose", func(t *testing.T) {
		err := b.SetObjectTagsPropose(internalBkt, "k", "", nil)
		require.ErrorIs(t, err, ErrInternalBucketNotObjectStore)
	})

	t.Run("GetObjectTags", func(t *testing.T) {
		_, err := b.GetObjectTags(internalBkt, "k", "")
		require.ErrorIs(t, err, ErrInternalBucketNotObjectStore)
	})

	t.Run("ListObjects", func(t *testing.T) {
		_, err := b.ListObjects(ctx, internalBkt, "", 100)
		require.ErrorIs(t, err, ErrInternalBucketNotObjectStore)
	})
}

// TestInternalBucketGuard_MultipartOpsRejected verifies that all multipart
// upload operations reject internal buckets with ErrInternalBucketNotObjectStore.
func TestInternalBucketGuard_MultipartOpsRejected(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()

	const internalBkt = "__grainfs_x"
	require.NoError(t, b.CreateBucket(ctx, internalBkt))

	t.Run("CreateMultipartUpload", func(t *testing.T) {
		_, err := b.CreateMultipartUpload(ctx, internalBkt, "k", "application/octet-stream")
		require.ErrorIs(t, err, ErrInternalBucketNotObjectStore)
	})

	t.Run("CreateMultipartUploadWithTags", func(t *testing.T) {
		_, err := b.CreateMultipartUploadWithTags(ctx, internalBkt, "k", "application/octet-stream", nil)
		require.ErrorIs(t, err, ErrInternalBucketNotObjectStore)
	})

	t.Run("UploadPart", func(t *testing.T) {
		_, err := b.UploadPart(ctx, internalBkt, "k", "upload-id-1", 1, strings.NewReader("data"), "")
		require.ErrorIs(t, err, ErrInternalBucketNotObjectStore)
	})

	t.Run("CompleteMultipartUpload", func(t *testing.T) {
		_, err := b.CompleteMultipartUpload(ctx, internalBkt, "k", "upload-id-1", nil)
		require.ErrorIs(t, err, ErrInternalBucketNotObjectStore)
	})

	t.Run("ListMultipartUploads", func(t *testing.T) {
		_, err := b.ListMultipartUploads(ctx, internalBkt, "", 100)
		require.ErrorIs(t, err, ErrInternalBucketNotObjectStore)
	})

	t.Run("ListParts", func(t *testing.T) {
		_, err := b.ListParts(ctx, internalBkt, "k", "upload-id-1", 100)
		require.ErrorIs(t, err, ErrInternalBucketNotObjectStore)
	})

	t.Run("AbortMultipartUpload", func(t *testing.T) {
		err := b.AbortMultipartUpload(ctx, internalBkt, "k", "upload-id-1")
		require.ErrorIs(t, err, ErrInternalBucketNotObjectStore)
	})
}

// TestInternalBucketGuard_VersionedOpsRejected verifies that versioned and
// async-put operations reject internal buckets with ErrInternalBucketNotObjectStore.
func TestInternalBucketGuard_VersionedOpsRejected(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()

	const internalBkt = "__grainfs_x"
	require.NoError(t, b.CreateBucket(ctx, internalBkt))

	t.Run("HeadObjectVersion", func(t *testing.T) {
		_, err := b.HeadObjectVersion(ctx, internalBkt, "k", "vid-1")
		require.ErrorIs(t, err, ErrInternalBucketNotObjectStore)
	})

	t.Run("GetObjectVersion", func(t *testing.T) {
		_, _, err := b.GetObjectVersion(ctx, internalBkt, "k", "vid-1")
		require.ErrorIs(t, err, ErrInternalBucketNotObjectStore)
	})

	t.Run("DeleteObjectVersion", func(t *testing.T) {
		err := b.DeleteObjectVersion(internalBkt, "k", "vid-1")
		require.ErrorIs(t, err, ErrInternalBucketNotObjectStore)
	})

	t.Run("ListObjectVersions", func(t *testing.T) {
		_, err := b.ListObjectVersions(ctx, internalBkt, "", 100)
		require.ErrorIs(t, err, ErrInternalBucketNotObjectStore)
	})

	t.Run("PutObjectAsync", func(t *testing.T) {
		_, _, err := b.PutObjectAsync(ctx, internalBkt, "k", strings.NewReader("data"), "application/octet-stream")
		require.ErrorIs(t, err, ErrInternalBucketNotObjectStore)
	})

	// Verify that the core functions called directly by the forward receiver
	// also reject internal buckets — this ensures coordinator-forwarded paths
	// cannot bypass the guard.
	t.Run("headObjectVersionCtx_core", func(t *testing.T) {
		_, err := b.headObjectVersionCtx(ctx, internalBkt, "k", "vid-1")
		require.ErrorIs(t, err, ErrInternalBucketNotObjectStore)
	})

	t.Run("getObjectVersionCtx_core", func(t *testing.T) {
		_, _, err := b.getObjectVersionCtx(ctx, internalBkt, "k", "vid-1")
		require.ErrorIs(t, err, ErrInternalBucketNotObjectStore)
	})
}

// TestInternalBucketGuard_ExternalBucketUnaffected confirms that external
// (user) buckets are not affected by the internal-bucket guard.
func TestInternalBucketGuard_ExternalBucketUnaffected(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()

	const userBkt = "b1"
	require.NoError(t, b.CreateBucket(ctx, userBkt))

	obj, err := b.PutObjectWithRequest(ctx, storage.PutObjectRequest{
		Bucket:      userBkt,
		Key:         "k",
		Body:        strings.NewReader("hello"),
		ContentType: "application/octet-stream",
	})
	require.NoError(t, err)
	require.NotNil(t, obj)

	head, err := b.HeadObject(ctx, userBkt, "k")
	require.NoError(t, err)
	require.Equal(t, int64(5), head.Size)
}
