package storage_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

func TestLocalBackend_ScanLocalMultipartUploads_EmitsActiveUpload(t *testing.T) {
	b := newBackend(t)
	require.NoError(t, b.CreateBucket(ctx(), "b"))

	mpu, err := b.CreateMultipartUpload(ctx(), "b", "k", "text/plain")
	require.NoError(t, err)

	ch, err := b.ScanLocalMultipartUploads("b")
	require.NoError(t, err)
	var got []storage.MultipartUploadRecord
	for u := range ch {
		got = append(got, u)
	}
	require.Len(t, got, 1)
	require.Equal(t, "b", got[0].Bucket)
	require.Equal(t, "k", got[0].Key)
	require.Equal(t, mpu.UploadID, got[0].UploadID)
	require.Positive(t, got[0].InitiatedAt)
}

func TestLocalBackend_ScanLocalMultipartUploads_EmptyBucket(t *testing.T) {
	b := newBackend(t)
	require.NoError(t, b.CreateBucket(ctx(), "b"))
	ch, err := b.ScanLocalMultipartUploads("b")
	require.NoError(t, err)
	var got []storage.MultipartUploadRecord
	for u := range ch {
		got = append(got, u)
	}
	require.Empty(t, got)
}

func TestLocalBackend_ScanLocalMultipartUploads_OmitsCompleted(t *testing.T) {
	b := newBackend(t)
	require.NoError(t, b.CreateBucket(ctx(), "b"))

	// One stays in-progress.
	pending, err := b.CreateMultipartUpload(ctx(), "b", "pending", "text/plain")
	require.NoError(t, err)

	// One completes (Complete should remove the multipart entry).
	done, err := b.CreateMultipartUpload(ctx(), "b", "done", "text/plain")
	require.NoError(t, err)
	part, err := b.UploadPart(ctx(), "b", "done", done.UploadID, 1, strings.NewReader("hello"))
	require.NoError(t, err)
	_, err = b.CompleteMultipartUpload(ctx(), "b", "done", done.UploadID, []storage.Part{{PartNumber: 1, ETag: part.ETag, Size: 5}})
	require.NoError(t, err)

	ch, err := b.ScanLocalMultipartUploads("b")
	require.NoError(t, err)
	var got []storage.MultipartUploadRecord
	for u := range ch {
		got = append(got, u)
	}
	require.Len(t, got, 1)
	require.Equal(t, "pending", got[0].Key)
	require.Equal(t, pending.UploadID, got[0].UploadID)
}
