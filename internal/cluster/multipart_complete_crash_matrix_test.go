package cluster

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"

	"github.com/gritive/GrainFS/internal/lifecycle"
	"github.com/gritive/GrainFS/internal/storage"
)

// TestCompleteMultipart_CrashMatrix_LeakedManifestReconcileAndReclaim exercises the
// M3 crash matrix: a multipart completes (its per-version blob is durable), then the
// manifest blob is re-leaked (as if the best-effort post-complete delete had crashed
// before running). Assertions:
//
//  1. The completed object stays readable (per-version blob is the authority).
//  2. ListMultipartUploads reconciles the leaked manifest OUT (its det-vid object
//     exists) — the list is clean.
//  3. An UploadPart against the still-present leaked manifest writes an orphan part.
//  4. The leader-gated lifecycle AbortIncompleteMultipartUpload age path FIRES and
//     reclaims the orphan (manifest blob deleted + staged parts removed) — proven by
//     driving the MPUWorker directly and asserting the abort mechanism ran.
func TestCompleteMultipart_CrashMatrix_LeakedManifestReconcileAndReclaim(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	const bkt, key = "vbkt", "mp.bin"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))

	up, err := b.CreateMultipartUpload(ctx, bkt, key, "application/octet-stream")
	require.NoError(t, err)
	payload := []byte("crash-matrix-payload")
	part, err := b.UploadPart(ctx, bkt, key, up.UploadID, 1, bytes.NewReader(payload), "")
	require.NoError(t, err)
	obj, err := b.CompleteMultipartUpload(ctx, bkt, key, up.UploadID, []storage.Part{*part})
	require.NoError(t, err)

	// (1) The object is durable and readable after the complete.
	rc, _, err := b.GetObject(ctx, bkt, key)
	require.NoError(t, err)
	got, rerr := io.ReadAll(rc)
	require.NoError(t, rerr)
	require.NoError(t, rc.Close())
	require.Equal(t, payload, got)

	// Re-leak the manifest blob for the completed upload (simulate the crash window
	// where the best-effort post-complete deleteManifestBlob never ran).
	leakMeta := clusterMultipartMeta{Bucket: bkt, Key: key, ContentType: "application/octet-stream", CreatedAt: up.CreatedAt}
	leakRaw, err := marshalClusterMultipartMeta(leakMeta)
	require.NoError(t, err)
	require.NoError(t, b.shardSvc.writeManifestBlobLocal(bkt, up.UploadID, leakRaw))

	// (2) ListMultipartUploads reconciles the leaked manifest out (det-vid object exists).
	listed, err := b.ListMultipartUploads(ctx, bkt, "", 0)
	require.NoError(t, err)
	require.Empty(t, listed, "leaked manifest for a completed upload must reconcile out of LIST")

	// (3) UploadPart against the leaked manifest writes an orphan part (the manifest
	// blob still validates the session). Re-leak afterwards because the completed
	// object still exists, so this part is unreachable — a true orphan.
	require.NoError(t, b.shardSvc.writeManifestBlobLocal(bkt, up.UploadID, leakRaw))
	_, err = b.UploadPart(ctx, bkt, key, up.UploadID, 2, bytes.NewReader([]byte("orphan-part")), "")
	require.NoError(t, err, "UploadPart against the leaked manifest writes an orphan part")
	require.DirExists(t, b.partDir(up.UploadID), "orphan part dir must exist before the lifecycle reclaim")
	require.NoError(t, b.shardSvc.writeManifestBlobLocal(bkt, up.UploadID, leakRaw)) // ensure the manifest survived for the worker to find

	// (4) Drive the leader-gated lifecycle AbortIncompleteMultipartUpload age path. The
	// MPUWorker enumerates the leaked manifest via ScanLocalMultipartUploads (which does
	// NOT reconcile) and aborts it once the age threshold is crossed — reclaiming the
	// manifest blob + the orphan part dir.
	store := lifecycle.NewStore(newTestLifecycleDB(t))
	require.NoError(t, store.PutRaw(bkt, []byte(`<LifecycleConfiguration><Rule><ID>r</ID><Status>Enabled</Status><AbortIncompleteMultipartUpload><DaysAfterInitiation>3</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>`)))

	// now = manifest CreatedAt + 4 days, past the 3-day abort threshold.
	now := time.Unix(up.CreatedAt, 0).Add(4 * 24 * time.Hour)
	limiter := rate.NewLimiter(rate.Limit(100), 10)
	w := lifecycle.NewMPUWorker(store, b, storage.NewOperations(b), time.Minute, limiter,
		lifecycle.WithMPUNowForTest(func() time.Time { return now }))
	w.RunCycleForTest(ctx)

	// The abort mechanism FIRED.
	require.Equal(t, int64(1), w.AbortedTotal(), "the lifecycle age path must abort the leaked/orphan upload")

	// The orphan part + manifest blob are reclaimed.
	_, statErr := os.Stat(b.partDir(up.UploadID))
	require.True(t, os.IsNotExist(statErr), "abort must remove the orphan part dir")
	_, ok, err := b.readManifestBlob(bkt, up.UploadID)
	require.NoError(t, err)
	require.False(t, ok, "abort must delete the leaked manifest blob")

	// The completed object is STILL readable — the reclaim only removed the orphan.
	_, herr := b.HeadObject(ctx, bkt, key)
	require.NoError(t, herr, "the completed object must survive the orphan reclaim")
	require.Equal(t, obj.VersionID, mustHeadVID(t, b, bkt, key))
}

func mustHeadVID(t *testing.T, b *DistributedBackend, bucket, key string) string {
	t.Helper()
	o, err := b.HeadObject(context.Background(), bucket, key)
	require.NoError(t, err)
	return o.VersionID
}
