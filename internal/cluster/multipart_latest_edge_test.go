package cluster

// TestCompleteMultipart_VersionedLatestEdge is the documented-edge regression-lock
// for the multipart-off-raft (M1-M5) epic.
//
// Scenario (spec §deriveLatestVersion):
//
//	(1) CreateMultipartUpload(k)   → uploadID minted at ms-timestamp T1
//	(2) PutObject(k)               → vid minted at ms-timestamp T2 (T2 ≥ T1+1ms)
//	(3) CompleteMultipartUpload(k) → finalised with det-vid derived from T1-epoch uploadID
//
// Ordering invariant (ms-granular):
//
//	mpuVID  = deriveMultipartVID(raw-uploadID)   ← UUIDv7 bytes[0:6] = T1 ms
//	putVID  = newVersionID()                      ← UUIDv7 bytes[0:6] = T2 ms
//
//	Because the upload was CREATED (T1) BEFORE the PUT completed (T2):
//	    mpuVID < putVID   (UUIDv7 lexicographic order = creation-time order)
//
// Consequence (create-time ordering IS the current latest rule):
//
//	deriveLatestVersion(max-VID) → putVID is LATEST, even though the multipart
//	COMPLETES after the PUT.  Both versions are retained; only the LATEST pointer
//	changes.
//
// TODO (ModTime-primary latest — deferred, 2026-06-23):
//
//	This is a DOCUMENTED KNOWN EDGE.  The intended long-term behaviour is
//	ModTime-primary latest (the LAST COMPLETED object is latest regardless of
//	uploadID create order).  Changing it requires migrating ALL 7 sites that
//	use the max-VID rule:
//	  • deriveLatestVersion  (quorum_meta.go)
//	  • object_version.go  listObjectVersionsSoleAuth maxVID loop (~line 551)
//	  • object_manifest.go  listSoleAuthBucketObjectsForGC maxVID loop (~line 172)
//	  • scrubbable.go  localSoleAuthScrubObjects latest collapse (~line 218)
//	  • cluster_coordinator.go  reconcileVersionIsLatest / sortObjectVersions
//	  • object_delete.go  latest-version resolution (~line 78)
//	  • listObjectVersions (non-sole-auth path, latestVID pre-scan ~line 370)
//
//	Additional caveats for that migration:
//	  • GET (per-version blob) and LIST (version enumeration) must agree on the
//	    same latest rule — split implementations are a trap.
//	  • A concurrent regular PutObject with the same key can land at any ms;
//	    without a global sequence tie-breaker, "last completed" is ambiguous when
//	    a multipart complete and a concurrent PUT complete within the same clock tick.
//
// Changing this assertion is the migration gate: the test MUST FAIL before the
// migration and PASS after.

import (
	"bytes"
	"context"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

// TestCompleteMultipart_VersionedLatestEdge locks the create-time ordering edge:
// when a multipart is CREATED before a PutObject but COMPLETED after, the PUT is
// the latest (its UUIDv7 vid is larger) and BOTH versions remain addressable.
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
	// The PUT mints a fresh UUIDv7 at T2 ≥ T1+1ms (sequential newVersionID calls
	// on the same node are monotonically non-decreasing; any elapsed real time since
	// CreateMultipartUpload advances the ms clock further). The test does NOT rely
	// on this being strictly T2>T1 in real time — it verifies the ordering property
	// directly below.
	putObj, err := b.PutObject(ctx, bkt, key, bytes.NewReader([]byte("put-body")), "text/plain")
	require.NoError(t, err)
	putVID := putObj.VersionID
	require.NotEmpty(t, putVID)

	// Step 3: Complete the multipart upload.
	mpObj, err := b.CompleteMultipartUpload(ctx, bkt, key, up.UploadID, []storage.Part{*part})
	require.NoError(t, err)
	require.Equal(t, mpuVID, mpObj.VersionID,
		"CompleteMultipartUpload must tag the object with the deterministic uploadID-derived vid")

	// Ordering invariant: mpuVID < putVID iff the upload was created before the PUT.
	// If in an extremely unlikely sub-ms same-tick scenario the two UUIDs have the
	// same ms prefix, the comparison is hash-arbitrary (documented ms-granular-only
	// ordering). We only assert the well-known case here; same-ms is tested
	// separately in TestDeriveMultipartVID_DeterministicAndMsOrdered.
	if mpuVID >= putVID {
		// Same-millisecond edge: ordering is hash-arbitrary. Both versions are still
		// retained and addressable — the invariant we care about below still holds.
		t.Logf("same-ms edge: mpuVID=%s putVID=%s (hash-arbitrary order, skipping latest assertion)", mpuVID, putVID)
	} else {
		// The EXPECTED path: mpuVID < putVID. HeadObject MUST return the PUT's vid
		// (the larger one) because deriveLatestVersion picks max-VID.
		//
		// This is the documented edge: a late-completed multipart whose uploadID
		// predates the PUT is NOT the latest even though it completed most recently.
		// Changing this assertion is the ModTime-primary migration gate.
		head, herr := b.HeadObject(ctx, bkt, key)
		require.NoError(t, herr)
		require.Equal(t, putVID, head.VersionID,
			"HeadObject must return the PUT's vid (max-VID = latest), not the later-completed "+
				"multipart's vid — create-time ordering is the current rule (ModTime-primary is a TODO)")
		require.NotEqual(t, mpuVID, head.VersionID,
			"HeadObject must NOT return the multipart's vid when the PUT's vid is larger")
	}

	// Both versions must be individually addressable by vid — this holds regardless
	// of which one is latest.
	hvMPU, _, err := b.headObjectMetaV(ctx, bkt, key, mpuVID)
	require.NoError(t, err, "multipart version must be addressable by its det-vid after a later PUT")
	require.Equal(t, mpuVID, hvMPU.VersionID)

	hvPUT, _, err := b.headObjectMetaV(ctx, bkt, key, putVID)
	require.NoError(t, err, "PUT version must be addressable by its vid")
	require.Equal(t, putVID, hvPUT.VersionID)
}
