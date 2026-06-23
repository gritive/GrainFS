package cluster

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestMultipartDone_RoundTrip, TestFSM_CompleteMultipart_MarkerExistsAfterComplete,
// TestFSM_CompleteMultipart_MismatchErrors, TestSweepStaleMultipartDoneMarkers
// removed in M4: multipartDone, marshalMultipartDone, unmarshalMultipartDone and
// SweepStaleMultipartDoneMarkers are deleted (done-marker machinery fully removed).

// TestCompleteMultipartUpload_IdempotentRetryAfterSuccess verifies that a second
// call to CompleteMultipartUpload with the same uploadID returns the already-committed
// object (same VersionID/ETag/Size) instead of ErrUploadNotFound.
func TestCompleteMultipartUpload_IdempotentRetryAfterSuccess(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(ctx, "bucket"))

	up, err := b.CreateMultipartUpload(ctx, "bucket", "obj", "text/plain")
	require.NoError(t, err)

	part, err := b.UploadPart(ctx, "bucket", "obj", up.UploadID, 1, bytes.NewReader(bytes.Repeat([]byte("a"), 16)), "")
	require.NoError(t, err)

	obj1, err := b.CompleteMultipartUpload(ctx, "bucket", "obj", up.UploadID, []storage.Part{*part})
	require.NoError(t, err)

	// Retry: manifest is gone; det-vid latest-only blob must be found; return committed object.
	obj2, err := b.CompleteMultipartUpload(ctx, "bucket", "obj", up.UploadID, []storage.Part{*part})
	require.NoError(t, err, "retry after success must be idempotent, not ErrUploadNotFound")
	require.Equal(t, obj1.VersionID, obj2.VersionID)
	require.Equal(t, obj1.ETag, obj2.ETag)
	require.Equal(t, obj1.Size, obj2.Size)
}

// TestCompleteMultipartUpload_MismatchedKeyRetryNotFound verifies the M3 model: a
// same-uploadID retry against a DIFFERENT key returns ErrUploadNotFound (S3
// NoSuchUpload). The idempotency anchor is the per-(bucket,key) det-vid object, not
// a per-uploadID done-marker — so a wrong-key retry has no completed object under
// that key and (after the original complete deleted the manifest) no session, which
// is exactly NoSuchUpload.
func TestCompleteMultipartUpload_MismatchedKeyRetryNotFound(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(ctx, "b"))

	up, err := b.CreateMultipartUpload(ctx, "b", "obj", "text/plain")
	require.NoError(t, err)

	part, err := b.UploadPart(ctx, "b", "obj", up.UploadID, 1, bytes.NewReader(bytes.Repeat([]byte("a"), 16)), "")
	require.NoError(t, err)

	_, err = b.CompleteMultipartUpload(ctx, "b", "obj", up.UploadID, []storage.Part{*part})
	require.NoError(t, err)

	// Retry with the SAME uploadID but a DIFFERENT key: the manifest is gone and no
	// det-vid object exists under "other" → ErrUploadNotFound.
	_, err = b.CompleteMultipartUpload(ctx, "b", "other", up.UploadID, []storage.Part{*part})
	require.ErrorIs(t, err, storage.ErrUploadNotFound)
}
