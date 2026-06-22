package cluster

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestCompleteMultipart_BlobAuthorityNoFSM proves the blob-primary multipart
// complete: for a versioning-enabled bucket, CompleteMultipartUpload's per-version
// quorum-meta blob is the SOLE AUTHORITY — no FSM obj:/lat: record is written, the
// object reads back via the blob, and the done-marker carries the encoded meta_blob
// so a retry/loser can re-write the winner's blob. Mirrors
// TestDeleteObjectMarker_BlobDurableNoPropose for the multipart path.
func TestCompleteMultipart_BlobAuthorityNoFSM(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	const bkt, key = "vbkt", "mp.bin"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))

	up, err := b.CreateMultipartUpload(ctx, bkt, key, "application/octet-stream")
	require.NoError(t, err)
	payload := []byte("multipart-blob-authority-payload")
	part, err := b.UploadPart(ctx, bkt, key, up.UploadID, 1, bytes.NewReader(payload), "")
	require.NoError(t, err)
	obj, err := b.CompleteMultipartUpload(ctx, bkt, key, up.UploadID, []storage.Part{*part})
	require.NoError(t, err)
	require.NotEmpty(t, obj.VersionID, "completed multipart object must carry a version id")
	vid := obj.VersionID

	// The completed object's per-version blob is durable.
	local, err := b.shardSvc.readQuorumMetaVersionsLocal(bkt, key)
	require.NoError(t, err)
	var completed *PutObjectMetaCmd
	for i := range local {
		if local[i].VersionID == vid {
			completed = &local[i]
		}
	}
	require.NotNil(t, completed, "completed multipart per-version blob must be durable")
	require.False(t, completed.IsDeleteMarker)
	require.False(t, completed.IsHardDeleted)
	require.NotEmpty(t, completed.Parts, "multipart object blob must carry its parts")

	// Raft-free: no FSM obj:/lat: records for the completed versioned object.
	require.NoError(t, b.store.View(func(txn MetadataTxn) error {
		_, gerr := txn.Get(b.ks().ObjectMetaKeyV(bkt, key, vid))
		require.ErrorIs(t, gerr, ErrMetaKeyNotFound, "multipart complete must not write an FSM per-version obj: record")
		_, gerr = txn.Get(b.ks().ObjectMetaKey(bkt, key))
		require.ErrorIs(t, gerr, ErrMetaKeyNotFound, "multipart complete must not write an FSM latest obj: record")
		_, gerr = txn.Get(b.ks().LatestKey(bkt, key))
		require.ErrorIs(t, gerr, ErrMetaKeyNotFound, "multipart complete must not write an FSM lat: pointer")
		return nil
	}))

	// The done-marker carries the encoded meta_blob so a retry re-writes the blob.
	marker, err := b.readDoneMarker(up.UploadID)
	require.NoError(t, err)
	require.NotNil(t, marker, "multipart complete must leave a done-marker")
	require.Equal(t, vid, marker.VersionID)
	require.NotEmpty(t, marker.MetaBlob, "done-marker must carry the winning meta_blob")

	// HEAD/GET read back via the per-version blob.
	head, err := b.HeadObject(ctx, bkt, key)
	require.NoError(t, err)
	require.Equal(t, int64(len(payload)), head.Size)

	rc, _, err := b.GetObject(ctx, bkt, key)
	require.NoError(t, err)
	defer rc.Close()
	got := new(bytes.Buffer)
	_, err = got.ReadFrom(rc)
	require.NoError(t, err)
	require.Equal(t, payload, got.Bytes(), "multipart object body must read back from the blob")
}

// TestCompleteMultipart_RetryRewritesBlobFromMarker proves the lost-object fix:
// if the per-version blob is missing after a complete (a crash/failure after the
// propose but before the blob landed), a retried CompleteMultipartUpload hits the
// done-marker idempotent branch and re-writes the WINNER's blob from the marker's
// meta_blob — so the object is never permanently 404.
func TestCompleteMultipart_RetryRewritesBlobFromMarker(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	const bkt, key = "vbkt", "mp.bin"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))

	up, err := b.CreateMultipartUpload(ctx, bkt, key, "application/octet-stream")
	require.NoError(t, err)
	payload := []byte("retry-rewrites-blob")
	part, err := b.UploadPart(ctx, bkt, key, up.UploadID, 1, bytes.NewReader(payload), "")
	require.NoError(t, err)
	obj, err := b.CompleteMultipartUpload(ctx, bkt, key, up.UploadID, []storage.Part{*part})
	require.NoError(t, err)
	vid := obj.VersionID

	// Simulate the crash window: the object is committed (done-marker + segments)
	// but the per-version blob never landed.
	require.NoError(t, b.shardSvc.deleteQuorumMetaVersionLocal(bkt, key, vid))
	_, err = b.HeadObject(ctx, bkt, key)
	require.ErrorIs(t, err, storage.ErrObjectNotFound, "blob gone → object reads 404 before the retry")

	// Retry: manifest is gone, so this hits the done-marker idempotent branch,
	// which re-writes the winner's blob from the marker's meta_blob.
	retried, err := b.CompleteMultipartUpload(ctx, bkt, key, up.UploadID, []storage.Part{*part})
	require.NoError(t, err)
	require.Equal(t, vid, retried.VersionID, "retry returns the original winning version")

	// The blob is durable again; HEAD/GET succeed.
	head, err := b.HeadObject(ctx, bkt, key)
	require.NoError(t, err)
	require.Equal(t, int64(len(payload)), head.Size)
}

// TestCompleteMultipart_NonVersionedKeepsFSM proves the carve-out: a
// non-versioned (Suspended/unset) bucket's multipart complete KEEPS the legacy
// FSM obj: write (no meta_blob) — non-versioned blob authority is a separate task.
func TestCompleteMultipart_NonVersionedKeepsFSM(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	const bkt, key = "plainbkt", "mp.bin"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	// No SetBucketVersioning → versioning disabled.

	up, err := b.CreateMultipartUpload(ctx, bkt, key, "application/octet-stream")
	require.NoError(t, err)
	payload := []byte("non-versioned-multipart")
	part, err := b.UploadPart(ctx, bkt, key, up.UploadID, 1, bytes.NewReader(payload), "")
	require.NoError(t, err)
	obj, err := b.CompleteMultipartUpload(ctx, bkt, key, up.UploadID, []storage.Part{*part})
	require.NoError(t, err)

	// Legacy: the FSM latest obj: record is written (non-versioned authority).
	require.NoError(t, b.store.View(func(txn MetadataTxn) error {
		_, gerr := txn.Get(b.ks().ObjectMetaKey(bkt, key))
		require.NoError(t, gerr, "non-versioned multipart must write the legacy FSM obj: record")
		return nil
	}))

	// The done-marker carries NO meta_blob (legacy mode).
	marker, err := b.readDoneMarker(up.UploadID)
	require.NoError(t, err)
	require.NotNil(t, marker)
	require.Empty(t, marker.MetaBlob, "non-versioned done-marker must not carry a meta_blob")

	// Read-back still works via the legacy FSM/latest-only path.
	head, err := b.HeadObject(ctx, bkt, key)
	require.NoError(t, err)
	require.Equal(t, int64(len(payload)), head.Size)
	_ = obj
}
