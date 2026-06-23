package cluster

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestCompleteMultipart_BlobAuthorityNoFSM proves the blob-primary multipart
// complete (M3): for a versioning-enabled bucket, CompleteMultipartUpload's
// per-version quorum-meta blob is the SOLE AUTHORITY — no FSM obj:/lat: record is
// written, no CmdCompleteMultipart propose, and the object reads back via the blob.
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

// TestCompleteMultipart_RetryShortCircuitsOnBlob proves the M3 idempotency anchor:
// the completed object's per-version blob — not a done-marker — IS the idempotency
// record. A retried CompleteMultipartUpload (manifest already deleted by the first)
// short-circuits on the existing det-vid per-version blob and returns the committed
// object without re-assembling.
func TestCompleteMultipart_RetryShortCircuitsOnBlob(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	const bkt, key = "vbkt", "mp.bin"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))

	up, err := b.CreateMultipartUpload(ctx, bkt, key, "application/octet-stream")
	require.NoError(t, err)
	payload := []byte("retry-short-circuits-on-blob")
	part, err := b.UploadPart(ctx, bkt, key, up.UploadID, 1, bytes.NewReader(payload), "")
	require.NoError(t, err)
	obj, err := b.CompleteMultipartUpload(ctx, bkt, key, up.UploadID, []storage.Part{*part})
	require.NoError(t, err)
	vid := obj.VersionID

	// The per-version blob is the durable idempotency record.
	_, ok, err := b.readQuorumMetaVersion(bkt, key, vid)
	require.NoError(t, err)
	require.True(t, ok, "completed object's per-version blob must be durable")

	// Retry: manifest is gone; the det-vid existence short-circuit returns the same
	// committed object.
	retried, err := b.CompleteMultipartUpload(ctx, bkt, key, up.UploadID, []storage.Part{*part})
	require.NoError(t, err, "retry must be idempotent via the per-version blob short-circuit")
	require.Equal(t, vid, retried.VersionID, "retry returns the original winning version")
	require.Equal(t, obj.ETag, retried.ETag)
	require.Equal(t, obj.Size, retried.Size)

	// HEAD/GET still succeed from the blob.
	head, err := b.HeadObject(ctx, bkt, key)
	require.NoError(t, err)
	require.Equal(t, int64(len(payload)), head.Size)
}

// TestCompleteMultipart_NonVersionedBlobAuthority proves M3's non-versioned shift:
// a non-versioned (Suspended/unset) bucket's multipart complete writes NO FSM
// obj:/lat: record — the latest-only quorum-meta blob (VersionID == det-vid) is the
// sole authority, read back via HEAD/GET. No CmdCompleteMultipart propose.
func TestCompleteMultipart_NonVersionedBlobAuthority(t *testing.T) {
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

	detVID, err := deriveMultipartVID(up.UploadID)
	require.NoError(t, err)
	require.Equal(t, detVID, obj.VersionID, "non-versioned completed object carries the det-vid")

	// No FSM obj:/lat: records — the latest-only blob is the sole authority.
	require.NoError(t, b.store.View(func(txn MetadataTxn) error {
		_, gerr := txn.Get(b.ks().ObjectMetaKey(bkt, key))
		require.ErrorIs(t, gerr, ErrMetaKeyNotFound, "non-versioned multipart must not write an FSM obj: record")
		_, gerr = txn.Get(b.ks().LatestKey(bkt, key))
		require.ErrorIs(t, gerr, ErrMetaKeyNotFound, "non-versioned multipart must not write an FSM lat: pointer")
		return nil
	}))

	// The latest-only quorum-meta blob is durable and carries the det-vid.
	cmd, err := b.readQuorumMetaCmd(bkt, key)
	require.NoError(t, err)
	require.Equal(t, detVID, cmd.VersionID, "latest-only blob VersionID must be the det-vid")

	// Read-back works via the latest-only blob.
	head, err := b.HeadObject(ctx, bkt, key)
	require.NoError(t, err)
	require.Equal(t, int64(len(payload)), head.Size)
}
