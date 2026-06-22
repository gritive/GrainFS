package cluster

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestSweepDoneMarkers_KeepsMarkerUntilBlobDurable proves the GC durability gate:
// a meta_blob-bearing done-marker whose per-version blob is NOT yet cluster-wide
// durable must NOT be swept — it is the only copy of the winning object metadata
// until the blob lands, so deleting it would lose the object. Once the blob is
// durable, the marker is sweep-eligible.
func TestSweepDoneMarkers_KeepsMarkerUntilBlobDurable(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	const bkt, key = "vbkt", "mp.bin"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))

	up, err := b.CreateMultipartUpload(ctx, bkt, key, "application/octet-stream")
	require.NoError(t, err)
	part, err := b.UploadPart(ctx, bkt, key, up.UploadID, 1, bytes.NewReader([]byte("payload")), "")
	require.NoError(t, err)
	obj, err := b.CompleteMultipartUpload(ctx, bkt, key, up.UploadID, []storage.Part{*part})
	require.NoError(t, err)
	vid := obj.VersionID

	// minAge negative so the just-written marker qualifies by age deterministically.
	const minAge = -time.Second

	// Blob missing → the marker MUST be kept (the only metadata copy left).
	require.NoError(t, b.shardSvc.deleteQuorumMetaVersionLocal(bkt, key, vid))
	n, err := b.SweepStaleMultipartDoneMarkers(ctx, 100, minAge)
	require.NoError(t, err)
	require.Equal(t, 0, n, "a meta_blob marker whose per-version blob is absent must not be swept")
	marker, err := b.readDoneMarker(up.UploadID)
	require.NoError(t, err)
	require.NotNil(t, marker, "marker must still exist")

	// Re-write the blob (the retry path), then the marker becomes sweep-eligible.
	_, err = b.CompleteMultipartUpload(ctx, bkt, key, up.UploadID, []storage.Part{*part})
	require.NoError(t, err)
	n, err = b.SweepStaleMultipartDoneMarkers(ctx, 100, minAge)
	require.NoError(t, err)
	require.Equal(t, 1, n, "once the per-version blob is durable the marker is sweep-eligible")
}

// TestSweepDoneMarkers_NonVersionedSweptByAge proves the legacy path is unchanged:
// a non-versioned done-marker (no meta_blob) is swept purely on the age gate, with
// no per-version blob probe.
func TestSweepDoneMarkers_NonVersionedSweptByAge(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	const bkt, key = "plainbkt", "mp.bin"
	require.NoError(t, b.CreateBucket(ctx, bkt))

	up, err := b.CreateMultipartUpload(ctx, bkt, key, "application/octet-stream")
	require.NoError(t, err)
	part, err := b.UploadPart(ctx, bkt, key, up.UploadID, 1, bytes.NewReader([]byte("payload")), "")
	require.NoError(t, err)
	_, err = b.CompleteMultipartUpload(ctx, bkt, key, up.UploadID, []storage.Part{*part})
	require.NoError(t, err)

	n, err := b.SweepStaleMultipartDoneMarkers(ctx, 100, -time.Second)
	require.NoError(t, err)
	require.Equal(t, 1, n, "non-versioned marker is swept by age (no blob probe)")
}
