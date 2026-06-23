package cluster

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestListMultipartUploads_StrictScanAndLeakedManifestReconcile proves the M2b
// ListMultipartUploads contract on the off-FSM .qmeta_mpu blob: two in-progress
// uploads are both listed, and a manifest left behind after a successful
// complete (a leak — the best-effort post-propose deleteManifestBlob missed) is
// reconciled out because its deterministic det-vid object now exists. The leaked
// manifest is best-effort re-deleted, so a second list is clean.
func TestListMultipartUploads_StrictScanAndLeakedManifestReconcile(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	const bkt = "vbkt"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))

	// Two in-progress uploads — both must list.
	upA, err := b.CreateMultipartUpload(ctx, bkt, "a.bin", "application/octet-stream")
	require.NoError(t, err)
	upB, err := b.CreateMultipartUpload(ctx, bkt, "b.bin", "application/octet-stream")
	require.NoError(t, err)

	listed, err := b.ListMultipartUploads(ctx, bkt, "", 0)
	require.NoError(t, err)
	require.Equal(t, []string{upA.UploadID, upB.UploadID}, multipartUploadIDs(listed),
		"both in-progress uploads must be listed")

	// Complete upload A; its per-version det-vid blob is now the authority.
	payload := []byte("reconcile-payload")
	partA, err := b.UploadPart(ctx, bkt, "a.bin", upA.UploadID, 1, bytes.NewReader(payload), "")
	require.NoError(t, err)
	objA, err := b.CompleteMultipartUpload(ctx, bkt, "a.bin", upA.UploadID, []storage.Part{*partA})
	require.NoError(t, err)

	detVID, err := deriveMultipartVID(upA.UploadID)
	require.NoError(t, err)
	require.Equal(t, detVID, objA.VersionID, "completed object's version is the det-vid")

	// Simulate a LEAK: the manifest blob for the completed upload is left behind
	// (re-write it as if the post-propose best-effort delete had missed).
	leakMeta := clusterMultipartMeta{Bucket: bkt, Key: "a.bin", ContentType: "application/octet-stream", CreatedAt: upA.CreatedAt}
	raw, err := marshalClusterMultipartMeta(leakMeta)
	require.NoError(t, err)
	require.NoError(t, b.shardSvc.writeManifestBlobLocal(bkt, upA.UploadID, raw))

	// ListMultipartUploads reconciles the leaked manifest out (its det-vid object
	// exists) and keeps only the still-in-progress upload B.
	listed, err = b.ListMultipartUploads(ctx, bkt, "", 0)
	require.NoError(t, err)
	require.Equal(t, []string{upB.UploadID}, multipartUploadIDs(listed),
		"leaked manifest for a completed upload must be filtered out")

	// The leaked manifest was best-effort re-deleted, so a second list is clean.
	_, ok, err := b.readManifestBlob(bkt, upA.UploadID)
	require.NoError(t, err)
	require.False(t, ok, "reconcile must best-effort re-delete the leaked manifest blob")
}

// TestMultipartCompletedObjectExists_VersionedNoVersioningContext proves that
// multipartCompletedObjectExists detects a completed versioned upload even when
// no versioning context is stamped on the context (as on the ListMultipartUploads
// path). Without the version-agnostic fix this would fall to the non-versioned leg
// and return false (ErrObjectNotFound from readQuorumMetaCmd on a versioned bucket).
func TestMultipartCompletedObjectExists_VersionedNoVersioningContext(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	const bkt = "vbkt2"
	require.NoError(t, b.CreateBucket(context.Background(), bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))

	up, err := b.CreateMultipartUpload(context.Background(), bkt, "obj.bin", "application/octet-stream")
	require.NoError(t, err)
	part, err := b.UploadPart(context.Background(), bkt, "obj.bin", up.UploadID, 1, bytes.NewReader([]byte("body")), "")
	require.NoError(t, err)
	_, err = b.CompleteMultipartUpload(context.Background(), bkt, "obj.bin", up.UploadID, []storage.Part{*part})
	require.NoError(t, err)

	// Plain context — no versioning stamp, simulating the ListMultipartUploads path.
	_, exists, err := b.multipartCompletedObjectExists(context.Background(), bkt, "obj.bin", up.UploadID)
	require.NoError(t, err)
	require.True(t, exists, "version-agnostic check must find the completed versioned upload even without versioning context")
}

// TestListMultipartUploads_NonVersionedReconcileMatchesDetVID proves the
// non-versioned reconcile leg: a leaked manifest is filtered only when the
// latest-only blob's VersionID equals the upload's det-vid. An UNRELATED later
// PUT to the same key (a different VersionID) must NOT mask an in-flight upload.
func TestListMultipartUploads_NonVersionedReconcileMatchesDetVID(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	const bkt = "nbkt"
	require.NoError(t, b.CreateBucket(ctx, bkt)) // versioning disabled

	up, err := b.CreateMultipartUpload(ctx, bkt, "obj.bin", "application/octet-stream")
	require.NoError(t, err)
	part, err := b.UploadPart(ctx, bkt, "obj.bin", up.UploadID, 1, bytes.NewReader([]byte("body")), "")
	require.NoError(t, err)

	// In-progress: an unrelated PUT to a DIFFERENT key must not reconcile it away.
	_, err = b.PutObject(ctx, bkt, "other.bin", bytes.NewReader([]byte("x")), "application/octet-stream")
	require.NoError(t, err)
	listed, err := b.ListMultipartUploads(ctx, bkt, "", 0)
	require.NoError(t, err)
	require.Equal(t, []string{up.UploadID}, multipartUploadIDs(listed),
		"an in-progress upload must remain listed while no det-vid object exists")

	// Complete it: the latest-only blob now carries VersionID == det-vid.
	_, err = b.CompleteMultipartUpload(ctx, bkt, "obj.bin", up.UploadID, []storage.Part{*part})
	require.NoError(t, err)

	// Re-leak the manifest; the reconcile filters it out (latest blob VID == det-vid).
	raw, err := marshalClusterMultipartMeta(clusterMultipartMeta{Bucket: bkt, Key: "obj.bin", ContentType: "application/octet-stream", CreatedAt: up.CreatedAt})
	require.NoError(t, err)
	require.NoError(t, b.shardSvc.writeManifestBlobLocal(bkt, up.UploadID, raw))

	listed, err = b.ListMultipartUploads(ctx, bkt, "", 0)
	require.NoError(t, err)
	require.Empty(t, listed, "completed non-versioned upload's leaked manifest must reconcile out")
}
