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
