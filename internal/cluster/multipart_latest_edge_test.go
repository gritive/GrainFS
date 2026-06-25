package cluster

// TestCompleteMultipart_VersionedLatestEdge is the regression-lock for the
// ModTime-primary latest-version rule (the multipart-off-raft M1-M5 edge).
//
// Scenario (spec §deriveLatestVersion):
//
//	(1) CreateMultipartUpload(k)   → uploadID minted at T1 (mpuVID derived from it)
//	(2) PutObject(k)               → vid minted at T2 (T2 > T1)
//	(3) CompleteMultipartUpload(k) → finalised with the T1-derived det-vid, but its
//	                                 ModTime is stamped at completion time T3 (> T2)
//
// VersionID ordering (UUIDv7 = creation-time order):
//
//	mpuVID = deriveMultipartVID(raw-uploadID)   ← bytes[0:6] = T1
//	putVID = newVersionID()                      ← bytes[0:6] = T2
//	⇒ mpuVID < putVID   (the upload was CREATED before the PUT)
//
// ModTime ordering (the LATEST rule — ModTime-primary):
//
//	The multipart COMPLETES last (T3 > T2), so its ModTime is the highest. Under the
//	ModTime-primary rule (quorumMetaCmdWins = ModTime → VID → MetaSeq) the multipart
//	is the latest even though its VersionID is the smaller one. The old max-VID rule
//	wrongly kept the PUT as latest.
//
// Limitation: ModTime is second-granularity (time.Now().Unix()), so this test sleeps
// >= 1s between the PUT and the Complete to guarantee a strictly higher completion
// second. Sub-second races still fall back to the VID tiebreak (the PUT would win),
// which is the documented Pareto/partial-fix boundary.
//
// GET (deriveLatestVersion) and LIST (listObjectVersionsBlobAuth) must agree on the
// same latest rule — this test asserts the GET/Head side; the e2e asserts both.

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

// TestCompleteMultipart_VersionedLatestEdge locks the ModTime-primary rule: when a
// multipart is CREATED before a PutObject but COMPLETED after, the multipart is the
// latest (it completed last) even though its UUIDv7 vid is the smaller one, and BOTH
// versions remain addressable.
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
	// The PUT mints a fresh UUIDv7 at T2 ≥ T1+1ms, so putVID > mpuVID.
	putObj, err := b.PutObject(ctx, bkt, key, bytes.NewReader([]byte("put-body")), "text/plain")
	require.NoError(t, err)
	putVID := putObj.VersionID
	require.NotEmpty(t, putVID)

	// Sanity: the upload's vid really IS smaller than the PUT's, so a max-VID rule
	// would (wrongly) pick the PUT. This proves the test exercises the real edge.
	require.Less(t, mpuVID, putVID,
		"the multipart vid (minted at create) must be smaller than the later PUT's vid")

	// Step 3: ModTime is second-granularity — sleep so the multipart's completion
	// ModTime is a strictly higher second than the PUT's. This >= 1s gap IS the
	// documented limitation of the ModTime-primary fix.
	time.Sleep(1100 * time.Millisecond)

	// Complete the multipart upload (stamps ModTime at completion time T3 > T2).
	mpObj, err := b.CompleteMultipartUpload(ctx, bkt, key, up.UploadID, []storage.Part{*part})
	require.NoError(t, err)
	require.Equal(t, mpuVID, mpObj.VersionID,
		"CompleteMultipartUpload must tag the object with the deterministic uploadID-derived vid")

	// ModTime-primary latest: HeadObject MUST return the MULTIPART's vid (it completed
	// last, highest ModTime) — NOT the PUT's larger vid. This is the core of the fix.
	head, herr := b.HeadObject(ctx, bkt, key)
	require.NoError(t, herr)
	require.Equal(t, mpuVID, head.VersionID,
		"HeadObject must return the multipart's vid (completed last = latest by ModTime), "+
			"not the PUT's larger vid — ModTime-primary is the latest rule")
	require.NotEqual(t, putVID, head.VersionID,
		"HeadObject must NOT return the earlier-completed PUT's vid")

	// Both versions must be individually addressable by vid — this holds regardless
	// of which one is latest.
	hvMPU, _, err := b.headObjectMetaV(ctx, bkt, key, mpuVID)
	require.NoError(t, err, "multipart version must be addressable by its det-vid after a later PUT")
	require.Equal(t, mpuVID, hvMPU.VersionID)

	hvPUT, _, err := b.headObjectMetaV(ctx, bkt, key, putVID)
	require.NoError(t, err, "PUT version must be addressable by its vid")
	require.Equal(t, putVID, hvPUT.VersionID)
}
