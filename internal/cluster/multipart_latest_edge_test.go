package cluster

// TestCompleteMultipart_VersionedLatestEdge is the regression-lock for the
// ModTime-primary latest-version rule (replaces the former max-VID create-time
// rule).
//
// Scenario (spec §deriveLatestVersion):
//
//	(1) CreateMultipartUpload(k)   → uploadID minted at ms-timestamp T1
//	(2) PutObject(k)               → vid minted at ms-timestamp T2 (T2 ≥ T1+1ms),
//	                                  object ModTime = floor(T2 seconds)
//	(3) sleep ≥1s                  → guarantees the next ModTime second differs
//	(4) CompleteMultipartUpload(k) → det-vid derived from the T1-epoch uploadID,
//	                                  object ModTime = floor(T3 seconds) > floor(T2)
//
// Ordering invariants:
//
//	mpuVID  = deriveMultipartVID(raw-uploadID)   ← UUIDv7 bytes[0:6] = T1 ms
//	putVID  = newVersionID()                      ← UUIDv7 bytes[0:6] = T2 ms
//	mpuVID < putVID                               (create-time order: upload first)
//	ModTime(mpu) > ModTime(put)                   (multipart COMPLETED later, +1s)
//
// Consequence (ModTime-primary IS the latest rule):
//
//	deriveLatestVersion → mpuVID is LATEST even though its VID is smaller, because
//	the multipart was the LAST COMPLETED write. HEAD and LIST must AGREE on this.
//
// This is the migration gate: under the old max-VID rule HEAD returned putVID;
// under ModTime-primary it returns mpuVID. The ≥1s sleep is required because
// ModTime is second-granular (time.Now().Unix()) — without it the PUT and the
// complete share a second and the VID tiebreak (putVID larger) would win, which
// is the documented same-second residual (see TODOS / CHANGELOG).

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

func TestCompleteMultipart_VersionedLatestEdge(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	const bkt, key = "vbkt", "edge-key.bin"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))

	// Step 1: Create the multipart upload (mints uploadID at T1).
	up, err := b.CreateMultipartUpload(ctx, bkt, key, "application/octet-stream")
	require.NoError(t, err)

	// Recover the raw backend uploadID (single-node returns it un-prefixed).
	rawUploadID := up.UploadID
	if _, parsed, ok := parseMultipartUploadID(up.UploadID); ok {
		rawUploadID = parsed
	}

	// Derive the multipart VID that Complete will assign (create-time deterministic).
	mpuVID, err := deriveMultipartVID(rawUploadID)
	require.NoError(t, err)

	// Upload a part so Complete has something to commit.
	part, err := b.UploadPart(ctx, bkt, key, up.UploadID, 1, bytes.NewReader([]byte("mpu-body")), "")
	require.NoError(t, err)

	// Step 2: PutObject on the SAME key while the multipart is still in flight.
	putObj, err := b.PutObject(ctx, bkt, key, bytes.NewReader([]byte("put-body")), "text/plain")
	require.NoError(t, err)
	putVID := putObj.VersionID
	require.NotEmpty(t, putVID)

	// The multipart was CREATED before the PUT, so its VID is the smaller one —
	// the precondition that makes this test meaningful (a lower VID winning latest
	// can ONLY happen under ModTime-primary, never under max-VID).
	require.Less(t, mpuVID, putVID,
		"multipart uploadID predates the PUT, so mpuVID must be the smaller VersionID")

	// Step 3: Advance the wall clock past the next whole second so the multipart's
	// completion ModTime is strictly greater than the PUT's (second granularity).
	time.Sleep(1100 * time.Millisecond)

	// Step 4: Complete the multipart upload — it commits LAST.
	mpObj, err := b.CompleteMultipartUpload(ctx, bkt, key, up.UploadID, []storage.Part{*part})
	require.NoError(t, err)
	require.Equal(t, mpuVID, mpObj.VersionID,
		"CompleteMultipartUpload must tag the object with the deterministic uploadID-derived vid")
	require.Greater(t, mpObj.LastModified, putObj.LastModified,
		"the later-completed multipart must carry a strictly greater ModTime than the PUT")

	// ModTime-primary: HeadObject MUST return the multipart's vid (latest because it
	// COMPLETED last), NOT the PUT's larger vid.
	head, herr := b.HeadObject(ctx, bkt, key)
	require.NoError(t, herr)
	require.Equal(t, mpuVID, head.VersionID,
		"HeadObject must return the later-completed multipart's vid (ModTime-primary latest), "+
			"not the PUT's larger vid")

	// LIST IsLatest MUST AGREE with HEAD — the same ModTime-primary winner is flagged
	// IsLatest, and it sorts first within the key.
	versions, lerr := b.ListObjectVersions(ctx, bkt, "", 0)
	require.NoError(t, lerr)
	var latestVID string
	var latestSeen bool
	for _, v := range versions {
		if v.Key == key && v.IsLatest {
			require.False(t, latestSeen, "exactly one version may be IsLatest per key")
			latestSeen = true
			latestVID = v.VersionID
		}
	}
	require.True(t, latestSeen, "LIST must flag a latest version")
	require.Equal(t, mpuVID, latestVID,
		"LIST IsLatest must agree with HeadObject (both ModTime-primary)")
	require.Equal(t, mpuVID, versions[0].VersionID,
		"the IsLatest (ModTime-primary winner) version must sort first within the key")

	// Both versions must remain individually addressable by vid.
	hvMPU, _, err := b.headObjectMetaV(ctx, bkt, key, mpuVID)
	require.NoError(t, err, "multipart version must be addressable by its det-vid")
	require.Equal(t, mpuVID, hvMPU.VersionID)

	hvPUT, _, err := b.headObjectMetaV(ctx, bkt, key, putVID)
	require.NoError(t, err, "PUT version must be addressable by its vid")
	require.Equal(t, putVID, hvPUT.VersionID)
}
