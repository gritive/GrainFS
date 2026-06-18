package cluster

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestVerifyPerVersionCutover_CompleteWhenAllBlobsPresent verifies that a
// versioning-enabled bucket where every version has a per-version blob returns
// readiness.Complete == number of versions and all gap/stuck/unknown counts zero.
func TestVerifyPerVersionCutover_CompleteWhenAllBlobsPresent(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	const bkt, key = "cvbkt", "obj"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))
	putVersioned(t, b, ctx, bkt, key, "v1-body")
	putVersioned(t, b, ctx, bkt, key, "v2-body")

	r, err := b.verifyPerVersionCutover(bkt)
	require.NoError(t, err)
	require.Equal(t, 2, r.Complete, "both versions must report Complete when blobs are present")
	require.Zero(t, r.Gaps, "no gaps expected")
	require.Zero(t, r.Stuck, "no stuck expected")
	require.Zero(t, r.Unknown, "no unknown expected")
}

// TestVerifyPerVersionCutover_NoopOnUnversionedBucket verifies that a bucket
// without versioning enabled returns zero readiness (nothing to verify).
func TestVerifyPerVersionCutover_NoopOnUnversionedBucket(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	const bkt = "plain-cvbkt"
	require.NoError(t, b.CreateBucket(ctx, bkt))

	r, err := b.verifyPerVersionCutover(bkt)
	require.NoError(t, err)
	require.Zero(t, r.Complete)
	require.Zero(t, r.Gaps)
	require.Zero(t, r.Stuck)
	require.Zero(t, r.Unknown)
	require.Zero(t, r.Excluded)
}

// TestVerifyPerVersionCutover_GapWhenBlobMissingWithPlacement verifies that a
// version whose per-version blob is absent (but has placement) is counted as Gap.
// Gap means: quorum-meta coverage missing AND placement is resolvable (backfill
// can fix it). Since the single-node test backend has resolvable placement, a
// blob removed from disk produces a Gap.
func TestVerifyPerVersionCutover_GapWhenBlobMissingWithPlacement(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	const bkt, key = "cvbkt-gap", "obj"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))
	v1 := putVersioned(t, b, ctx, bkt, key, "v1-body")
	putVersioned(t, b, ctx, bkt, key, "v2-body") // stays complete

	// Remove v1's blob to create a gap.
	removePerVersionBlob(t, b, bkt, key, v1)

	r, err := b.verifyPerVersionCutover(bkt)
	require.NoError(t, err)
	// v1 missing → Gap (placement resolvable in single-node backend).
	// v2 present → Complete.
	require.Equal(t, 1, r.Complete, "v2 must be Complete")
	require.Equal(t, 1, r.Gaps+r.Stuck, "v1 must be Gap or Stuck (missing from quorum-meta)")
	require.Zero(t, r.Unknown, "no unknown expected")
	// GapRefs or StuckRefs must contain v1.
	allMissingRefs := append(r.GapRefs, r.StuckRefs...)
	var foundV1 bool
	for _, ref := range allMissingRefs {
		if ref.VersionID == v1 {
			foundV1 = true
		}
	}
	require.True(t, foundV1, "v1 must appear in GapRefs or StuckRefs")
}

// TestVerifyPerVersionCutover_ExcludesAppendableAndCoalesced verifies that
// appendable and coalesced versioned records are counted as Excluded.
func TestVerifyPerVersionCutover_ExcludesAppendableAndCoalesced(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	const bkt = "cvbkt-appendable"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))

	const (
		keyAppendable = "append-obj"
		vidAppendable = "019756c0-17f8-7000-b7e4-a5ef7c2f0001"
		keyCoalesced  = "coalesced-obj"
		vidCoalesced  = "019756c0-17f8-7000-b7e4-a5ef7c2f0002"
	)
	rawAppendable, err := marshalObjectMeta(objectMeta{
		Key: keyAppendable, ETag: "etag-app", ECData: 4, NodeIDs: []string{"n1"}, IsAppendable: true,
	})
	require.NoError(t, err)
	require.NoError(t, b.store.Update(func(txn MetadataTxn) error {
		return txn.Set(b.ks().ObjectMetaKeyV(bkt, keyAppendable, vidAppendable), rawAppendable)
	}))
	rawCoalesced, err := marshalObjectMeta(objectMeta{
		Key:       keyCoalesced,
		ETag:      "etag-coal",
		ECData:    4,
		NodeIDs:   []string{"n1"},
		Coalesced: []CoalescedShardRef{{CoalescedID: "cid1", Size: 100, ETag: "ce1"}},
	})
	require.NoError(t, err)
	require.NoError(t, b.store.Update(func(txn MetadataTxn) error {
		return txn.Set(b.ks().ObjectMetaKeyV(bkt, keyCoalesced, vidCoalesced), rawCoalesced)
	}))

	r, err := b.verifyPerVersionCutover(bkt)
	require.NoError(t, err)
	require.Equal(t, 2, r.Excluded, "appendable and coalesced must be Excluded")
	require.Zero(t, r.Complete+r.Gaps+r.Stuck+r.Unknown)
}

// TestVerifyPerVersionCutover_InternalBucketExcludesAll verifies that internal
// buckets count all FSM obj: records as Excluded without error.
func TestVerifyPerVersionCutover_InternalBucketExcludesAll(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)

	// Use a known internal bucket name (prefixed with __grainfs_).
	// Must bypass reserved-name check as internal buckets cannot be created via public API.
	internalBkt := "__grainfs_audit"
	require.True(t, storage.IsInternalBucket(internalBkt), "must be internal")
	require.NoError(t, b.CreateBucketBypassReserved(ctx, internalBkt))

	// Write two fake versioned FSM records for the internal bucket.
	for _, vid := range []string{"019756c0-17f8-7000-b7e4-a5ef7c2f0010", "019756c0-17f8-7000-b7e4-a5ef7c2f0011"} {
		raw, err := marshalObjectMeta(objectMeta{Key: "k", ETag: "e1", ECData: 4, NodeIDs: []string{"n1"}})
		require.NoError(t, err)
		require.NoError(t, b.store.Update(func(txn MetadataTxn) error {
			return txn.Set(b.ks().ObjectMetaKeyV(internalBkt, "k", vid), raw)
		}))
	}

	r, err := b.verifyPerVersionCutover(internalBkt)
	require.NoError(t, err)
	require.Equal(t, 2, r.Excluded, "internal bucket records must all be Excluded")
	require.Zero(t, r.Complete+r.Gaps+r.Stuck+r.Unknown)
}

// TestVerifyPerVersionCutover_UnknownOnDecodeError verifies that a record with
// a corrupt FSM value (undecodable raw) is counted as Unknown (fail-closed).
func TestVerifyPerVersionCutover_UnknownOnDecodeError(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	const bkt = "cvbkt-corrupt"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))

	// Write an undecodable versioned FSM record. An empty byte slice reliably
	// triggers fbSafe's "empty data" error path without risking a FlatBuffers
	// field-access panic on garbage heap data.
	const vid = "019756c0-17f8-7000-b7e4-a5ef7c2f0099"
	require.NoError(t, b.store.Update(func(txn MetadataTxn) error {
		return txn.Set(b.ks().ObjectMetaKeyV(bkt, "corrupt-key", vid), []byte{})
	}))

	r, err := b.verifyPerVersionCutover(bkt)
	require.NoError(t, err)
	require.Equal(t, 1, r.Unknown, "corrupt record must be counted as Unknown")
	require.Zero(t, r.Complete+r.Gaps+r.Stuck)
	require.Len(t, r.UnknownRefs, 1)
	require.Equal(t, vid, r.UnknownRefs[0].VersionID)
}

// TestVerifyPerVersionCutover_CompleteIncludesDeleteMarker verifies that a
// delete marker version with a present per-version blob is counted as Complete.
func TestVerifyPerVersionCutover_CompleteIncludesDeleteMarker(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	const bkt, key = "cvbkt-marker", "obj"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))
	putVersioned(t, b, ctx, bkt, key, "live-data")
	deleteVersioned(t, b, bkt, key)

	r, err := b.verifyPerVersionCutover(bkt)
	require.NoError(t, err)
	// Both the live version and the delete marker have blobs → 2 Complete.
	require.Equal(t, 2, r.Complete, "live version + delete marker must both be Complete")
	require.Zero(t, r.Gaps+r.Stuck+r.Unknown)
}

// TestVerifyPerVersionCutover_MaxVerifyRefsCapAtMaxVerifyRefs verifies that
// GapRefs/StuckRefs/UnknownRefs are capped at maxVerifyRefs.
func TestVerifyPerVersionCutover_MaxVerifyRefsCapAtMaxVerifyRefs(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	const bkt = "cvbkt-captest"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))

	// Write maxVerifyRefs+5 corrupt records to exceed the cap.
	total := maxVerifyRefs + 5
	for i := 0; i < total; i++ {
		vid := "019756c0-17f8-7000-b7e4-a5ef7c2f" + intToHexSuffix(i)
		require.NoError(t, b.store.Update(func(txn MetadataTxn) error {
			return txn.Set(b.ks().ObjectMetaKeyV(bkt, "k", vid), []byte("bad"))
		}))
	}

	r, err := b.verifyPerVersionCutover(bkt)
	require.NoError(t, err)
	require.Equal(t, total, r.Unknown, "all corrupt records must be counted in Unknown")
	require.LessOrEqual(t, len(r.UnknownRefs), maxVerifyRefs, "UnknownRefs must be capped at maxVerifyRefs")
}

// writeRawNonAppendableNoPlacementObjVersion writes a RAW versioned FSM obj:
// record that is non-appendable, non-marker, with EMPTY NodeIDs (no resolvable
// placement). Modeled on the _SkipsEmptyNodeIDs raw-store write in
// per_version_backfill_walker_test.go.
func writeRawNonAppendableNoPlacementObjVersion(t *testing.T, b *DistributedBackend, bucket, key, vid string) {
	t.Helper()
	raw, err := marshalObjectMeta(objectMeta{Key: key, ETag: "e-stuck", ECData: 0, NodeIDs: nil})
	require.NoError(t, err)
	require.NoError(t, b.store.Update(func(txn MetadataTxn) error {
		return txn.Set(b.ks().ObjectMetaKeyV(bucket, key, vid), raw)
	}))
}

// refKeys maps a slice of objVersionRef to their object keys.
func refKeys(refs []objVersionRef) []string {
	out := make([]string, len(refs))
	for i, r := range refs {
		out[i] = r.Key
	}
	return out
}

// TestVerifyPerVersionCutover_StuckOnNoPlacement verifies the dedicated Stuck
// path: a raw non-appendable, non-marker versioned record with empty NodeIDs and
// no per-version blob. The strict readback misses the VID (no blob), and the
// record is neither a marker nor has placement → STUCK (not Gap).
func TestVerifyPerVersionCutover_StuckOnNoPlacement(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	const bkt, key = "cvbkt-stuck", "stuck"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))

	const vid = "019756c0-17f8-7000-b7e4-a5ef7c2fb001"
	writeRawNonAppendableNoPlacementObjVersion(t, b, bkt, key, vid)

	r, err := b.verifyPerVersionCutover(bkt)
	require.NoError(t, err)
	require.Equal(t, 1, r.Stuck, "empty-NodeIDs non-marker record must be Stuck")
	require.Zero(t, r.Gaps, "must not be classified as Gap")
	require.Zero(t, r.Complete+r.Unknown+r.Excluded)
	require.Contains(t, refKeys(r.StuckRefs), key, "StuckRefs must name the stuck key")
}

// TestVerifyPerVersionCutover_UnknownOnPanicCorruptRecord proves the panic guard
// in forEachHostedObjVersion: a raw obj: value crafted to PANIC inside a
// FlatBuffers FIELD accessor (NOT just the root parse) becomes a decodeErr →
// UNKNOWN, and NO panic escapes verifyPerVersionCutover.
func TestVerifyPerVersionCutover_UnknownOnPanicCorruptRecord(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	const bkt, key = "cvbkt-panic", "panic-key"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))

	// Craft a buffer that passes GetRootAsObjectMeta (which only reads the root
	// table offset at bytes[0:4]) but panics at a FIELD accessor. A 4-byte buffer
	// whose root-offset points at byte 0 makes the vtable lookup read a soffset
	// that drives the table position out of bounds → panic when NodeIdsLength()
	// (or any accessor) dereferences the vtable. fbSafe only recovers the root
	// parse, so without the field-accessor recover this would crash the sweep.
	corrupt := []byte{0x04, 0x00, 0x00, 0x00}
	const vid = "019756c0-17f8-7000-b7e4-a5ef7c2fc001"
	require.NoError(t, b.store.Update(func(txn MetadataTxn) error {
		return txn.Set(b.ks().ObjectMetaKeyV(bkt, key, vid), corrupt)
	}))

	// Must NOT panic — the recover guard converts the field-accessor panic to an
	// error that classifies as Unknown.
	r, err := b.verifyPerVersionCutover(bkt)
	require.NoError(t, err)
	require.GreaterOrEqual(t, r.Unknown, 1, "panic-corrupt record must classify as Unknown")
	require.Zero(t, r.Complete+r.Gaps+r.Stuck)
}

// TestVerifyPerVersionCutover_UnknownWhenBlobPresentButLayoutUnreadable verifies
// Finding 1: a per-version blob that is present and decodable in the strict
// readback union but carries an unresolvable layout (no segments, no NodeIDs,
// ECData=0, not a delete marker) is classified UNKNOWN — fail-closed — because
// getObjectVersionCtx would return "no readable layout" (404) for this version
// after cutover. It must NOT be classified COMPLETE.
//
// Construction: we bypass PutObject and write a per-version quorum-meta blob
// directly (using EncodeCommand + writeQuorumMetaVersionLocal) with a
// PutObjectMetaCmd that has no segments, no NodeIDs, and ECData=0. The FSM
// ObjectMetaKeyV record is also written so forEachHostedObjVersion yields it.
// The strict readback finds the blob (the per-version file exists); the layout
// check then determines it is unreadable → UNKNOWN.
func TestVerifyPerVersionCutover_UnknownWhenBlobPresentButLayoutUnreadable(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	const bkt, key = "cvbkt-broken-layout", "broken"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))

	const vid = "019756c0-17f8-7000-b7e4-a5ef7c2fd001"

	// Write an FSM ObjectMetaKeyV record so forEachHostedObjVersion yields the VID.
	rawFSM, err := marshalObjectMeta(objectMeta{
		Key:     key,
		ETag:    "etag-broken",
		ECData:  0,   // ECData=0 → unresolvable
		NodeIDs: nil, // no placement
	})
	require.NoError(t, err)
	require.NoError(t, b.store.Update(func(txn MetadataTxn) error {
		return txn.Set(b.ks().ObjectMetaKeyV(bkt, key, vid), rawFSM)
	}))

	// Write a per-version quorum-meta blob with a broken layout: no segments,
	// no NodeIDs, ECData=0, not a delete marker. readQuorumMetaVersionsStrict
	// will find this blob, decode it, and return the cmd. The layout check must
	// then classify it UNKNOWN because none of the readable-layout predicates hold.
	cmd := PutObjectMetaCmd{
		Bucket:    bkt,
		Key:       key,
		VersionID: vid,
		ETag:      "etag-broken",
		ECData:    0,   // ECData=0 → placementConfig returns error → unresolvable
		NodeIDs:   nil, // no NodeIDs → ResolvePlacement returns ErrNotEC
		Segments:  nil, // no segments → SegmentReader path unavailable
		// IsDeleteMarker: false (default) → delete-marker fast-path skipped
	}
	blob, err := EncodeCommand(CmdPutObjectMeta, cmd)
	require.NoError(t, err)
	require.NotNil(t, b.shardSvc, "shardSvc must be available")
	require.NoError(t, b.shardSvc.writeQuorumMetaVersionLocal(bkt, key+"/"+vid, blob))

	r, err := b.verifyPerVersionCutover(bkt)
	require.NoError(t, err)
	require.Equal(t, 1, r.Unknown, "present blob with unreadable layout must be UNKNOWN, not COMPLETE")
	require.Zero(t, r.Complete, "must not classify as Complete — would 404 after cutover")
	require.Zero(t, r.Gaps+r.Stuck)
	require.Len(t, r.UnknownRefs, 1)
	require.Equal(t, vid, r.UnknownRefs[0].VersionID, "UnknownRefs must name the broken-layout VID")
}

// TestVerifyPerVersionCutover_FailClosedOnVersioningReadError verifies Finding 1:
// when GetBucketVersioning returns a store error (not ErrMetaKeyNotFound), the
// verifier must return that error — not silently treat the bucket as non-versioned
// and return zero readiness. A false-READY signal on a failed versioning read
// could allow a cutover that corrupts or loses all versioned objects in the bucket.
//
// The test injects a BadgerDB View error via errInjectStore (defined in
// per_version_backfill_walker_test.go). With the store erroring, any View call —
// including the GetBucketVersioning View — returns the injected error. The bucket
// is not an internal bucket, so the internal-bucket fast-path does not trigger,
// and the code reaches the versioning-state read.
func TestVerifyPerVersionCutover_FailClosedOnVersioningReadError(t *testing.T) {
	b := newTestDistributedBackend(t)

	// Inject a View error AFTER the backend is fully set up. Because the store
	// errors on ALL View calls, GetBucketVersioning will see the error. The old
	// code (using bucketVersioningEnabled) would silently return false, leading to
	// a zero-readiness non-error return. The fix must propagate the error.
	storeErr := errors.New("simulated BadgerDB View failure for versioning read")
	b.store = &errInjectStore{MetadataStore: b.store, active: true, viewErr: storeErr}

	// The bucket name must NOT be an internal bucket (IsInternalBucket must be false)
	// so the code reaches the versioning-state read rather than the fast-path.
	_, err := b.verifyPerVersionCutover("versioned-bucket-read-error")

	require.Error(t, err, "verifyPerVersionCutover must return an error when GetBucketVersioning fails")
	require.ErrorIs(t, err, storeErr, "returned error must wrap the original store error (fail-closed)")
}

// TestVerifyPerVersionCutover_VerifiesSuspendedBucket verifies that a bucket
// whose versioning was Enabled and then Suspended is still verified — NOT skipped.
//
// Suspended buckets retain all versioned objects written while Enabled. The S4a
// cutover removes the FSM ObjectMetaKeyV fallback that those versions rely on.
// Suspending versioning does NOT delete that history, so Suspended buckets are
// IN SCOPE for the cutover gate. If we skip them we emit a false-READY signal.
func TestVerifyPerVersionCutover_VerifiesSuspendedBucket(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	const bkt, key = "cvbkt-suspended", "obj"
	require.NoError(t, b.CreateBucket(ctx, bkt))

	// Enable versioning and write a versioned object (gets a per-version blob).
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))
	v1 := putVersioned(t, b, ctx, bkt, key, "v1-body")

	// Remove the per-version blob to create a gap.
	removePerVersionBlob(t, b, bkt, key, v1)

	// Suspend versioning — this does NOT delete the existing versioned history.
	require.NoError(t, b.SetBucketVersioning(bkt, "Suspended"))

	// The verifier must still classify the bucket (Gaps==1), NOT skip it (zero).
	r, err := b.verifyPerVersionCutover(bkt)
	require.NoError(t, err)
	require.Equal(t, 1, r.Gaps+r.Stuck, "Suspended bucket with missing blob must not be skipped — Gaps or Stuck must be 1")
	require.Zero(t, r.Complete, "no complete blobs expected (blob was removed)")
	require.Zero(t, r.Unknown, "no unknown expected")
}

// intToHexSuffix formats an int as a 4-char hex string for use in test UUIDs.
func intToHexSuffix(i int) string {
	return "0000"[len(intHex(i)):] + intHex(i)
}

func intHex(i int) string {
	const hextable = "0123456789abcdef"
	if i == 0 {
		return "0000"
	}
	var buf [4]byte
	pos := 3
	for i > 0 && pos >= 0 {
		buf[pos] = hextable[i&0xf]
		i >>= 4
		pos--
	}
	for pos >= 0 {
		buf[pos] = '0'
		pos--
	}
	return string(buf[:])
}
