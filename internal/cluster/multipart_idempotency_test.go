package cluster

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

func TestMultipartDone_RoundTrip(t *testing.T) {
	m := multipartDone{UploadID: "u1", Bucket: "b", Key: "k", VersionID: "v7", ModTime: 123}
	raw, err := marshalMultipartDone(m)
	require.NoError(t, err)
	got, err := unmarshalMultipartDone(raw)
	require.NoError(t, err)
	require.Equal(t, m, got)
}

// TestFSM_CompleteMultipart_MarkerExistsAfterComplete verifies that
// applyCompleteMultipart writes a multipartDone marker in the same txn as
// the manifest delete.
func TestFSM_CompleteMultipart_MarkerExistsAfterComplete(t *testing.T) {
	fsm := NewFSM(newTestStore(t), newStateKeyspaceEmpty())

	data, err := EncodeCommand(CmdCreateMultipartUpload, CreateMultipartUploadCmd{
		UploadID: "u-marker", Bucket: "bkt", Key: "obj.bin",
		ContentType: "application/octet-stream", CreatedAt: 100,
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(data))

	complete, err := EncodeCommand(CmdCompleteMultipart, CompleteMultipartCmd{
		Bucket: "bkt", Key: "obj.bin", UploadID: "u-marker",
		Size: 512, ContentType: "application/octet-stream",
		ETag: "etag1", ModTime: 200, VersionID: "v1",
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(complete))

	// Marker must exist after completion.
	require.NoError(t, fsm.db.View(func(txn MetadataTxn) error {
		item, err := txn.Get(fsm.keys.MultipartDoneKey("u-marker"))
		require.NoError(t, err)
		raw, err := fsm.itemValueCopy(item)
		require.NoError(t, err)
		marker, err := unmarshalMultipartDone(raw)
		require.NoError(t, err)
		require.Equal(t, "u-marker", marker.UploadID)
		require.Equal(t, "bkt", marker.Bucket)
		require.Equal(t, "obj.bin", marker.Key)
		require.Equal(t, "v1", marker.VersionID)
		require.Equal(t, int64(200), marker.ModTime)
		return nil
	}))
}

// TestFSM_CompleteMultipart_MismatchErrors verifies that a second
// applyCompleteMultipart with the SAME uploadID but a DIFFERENT bucket/key
// returns an error (genuine conflict, not idempotent retry).
func TestFSM_CompleteMultipart_MismatchErrors(t *testing.T) {
	fsm := NewFSM(newTestStore(t), newStateKeyspaceEmpty())

	data, err := EncodeCommand(CmdCreateMultipartUpload, CreateMultipartUploadCmd{
		UploadID: "u-mismatch", Bucket: "bkt", Key: "original.bin",
		ContentType: "application/octet-stream", CreatedAt: 100,
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(data))

	// First complete: manifest present, bucket/key = original.bin
	first, err := EncodeCommand(CmdCompleteMultipart, CompleteMultipartCmd{
		Bucket: "bkt", Key: "original.bin", UploadID: "u-mismatch",
		Size: 512, ContentType: "application/octet-stream",
		ETag: "etag1", ModTime: 200, VersionID: "v1",
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(first))

	// Second complete: manifest gone (marker present), but different key → must error.
	conflict, err := EncodeCommand(CmdCompleteMultipart, CompleteMultipartCmd{
		Bucket: "bkt", Key: "other.bin", UploadID: "u-mismatch",
		Size: 1024, ContentType: "application/octet-stream",
		ETag: "etag2", ModTime: 300, VersionID: "v2",
	})
	require.NoError(t, err)
	err = fsm.Apply(conflict)
	require.Error(t, err)
	require.NotErrorIs(t, err, storage.ErrUploadNotFound)
}

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

// TestSweepStaleMultipartDoneMarkers verifies that SweepStaleMultipartDoneMarkers
// deletes only markers older than minAge while leaving fresh markers intact.
func TestSweepStaleMultipartDoneMarkers(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(ctx, "bkt"))

	// Write a STALE marker: ModTime 48h in the past.
	staleModTime := time.Now().Add(-48 * time.Hour).Unix()
	completeViaFSM := func(uploadID string, modTime int64) {
		t.Helper()
		create, err := EncodeCommand(CmdCreateMultipartUpload, CreateMultipartUploadCmd{
			UploadID: uploadID, Bucket: "bkt", Key: uploadID + ".bin",
			ContentType: "application/octet-stream", CreatedAt: 100,
		})
		require.NoError(t, err)
		require.NoError(t, b.fsm.Apply(create))

		complete, err := EncodeCommand(CmdCompleteMultipart, CompleteMultipartCmd{
			Bucket: "bkt", Key: uploadID + ".bin", UploadID: uploadID,
			Size: 64, ContentType: "application/octet-stream",
			ETag: "etag-" + uploadID, ModTime: modTime, VersionID: "v1",
		})
		require.NoError(t, err)
		require.NoError(t, b.fsm.Apply(complete))
	}

	completeViaFSM("u-stale", staleModTime)
	// Write a FRESH marker: ModTime = now.
	completeViaFSM("u-fresh", time.Now().Unix())

	// Sweep with 24h minAge: only the stale marker qualifies.
	n, err := b.SweepStaleMultipartDoneMarkers(ctx, 256, 24*time.Hour)
	require.NoError(t, err)
	require.Equal(t, 1, n, "sweep must delete exactly 1 stale marker")

	// Stale marker must be gone.
	require.NoError(t, b.store.View(func(txn MetadataTxn) error {
		_, err := txn.Get(b.ks().MultipartDoneKey("u-stale"))
		require.ErrorIs(t, err, ErrMetaKeyNotFound, "stale marker must be deleted")
		return nil
	}))

	// Fresh marker must survive.
	require.NoError(t, b.store.View(func(txn MetadataTxn) error {
		_, err := txn.Get(b.ks().MultipartDoneKey("u-fresh"))
		require.NoError(t, err, "fresh marker must survive")
		return nil
	}))
}
