package cluster

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// removePerVersionBlob removes the on-disk per-version quorum-meta blob file,
// simulating a pre-S1 / best-effort-failure gap.
func removePerVersionBlob(t *testing.T, b *DistributedBackend, bucket, key, versionID string) {
	t.Helper()
	require.NotNil(t, b.shardSvc, "shardSvc must be set")
	dirs := b.shardSvc.DataDirs()
	require.NotEmpty(t, dirs, "shardSvc must have at least one dataDir")
	p := filepath.Join(dirs[0], quorumMetaVersionsSubDir, bucket, key, versionID)
	err := os.Remove(p)
	require.NoError(t, err, "per-version blob must exist before removal: %s", p)
}

// nowUnixSec returns the current Unix timestamp in seconds.
func nowUnixSec() int64 { return time.Now().Unix() }

// TestWalkPerVersionBackfillCandidates_YieldsOnlyMissing verifies that the walker
// yields only the version whose per-version blob is absent from disk (simulating a
// pre-S1 gap), and skips the version whose blob is already present.
func TestWalkPerVersionBackfillCandidates_YieldsOnlyMissing(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	const bkt, key = "vbkt", "obj"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))
	v1 := putVersioned(t, b, ctx, bkt, key, "one")
	v2 := putVersioned(t, b, ctx, bkt, key, "two")
	// Simulate a pre-S1 gap: remove v1's per-version blob from disk.
	removePerVersionBlob(t, b, bkt, key, v1)

	var got []string
	require.NoError(t, b.walkPerVersionBackfillCandidates(bkt, nowUnixSec(), 0, func(c perVersionBackfillCandidate) error {
		got = append(got, c.VersionID)
		require.NotEmpty(t, c.Meta.NodeIDs, "candidate must carry placement")
		return nil
	}))
	require.Equal(t, []string{v1}, got, "only the missing-blob version is a candidate; v2 already has its blob")
	_ = v2
}

// TestWalkPerVersionBackfillCandidates_SkipsEmptyNodeIDs verifies that a version
// with no placement (empty NodeIDs) is skipped — no fan-out target.
func TestWalkPerVersionBackfillCandidates_SkipsEmptyNodeIDs(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	const bkt, key, vid = "vbkt2", "k", "fake-vid-1"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))

	// Write a versioned FSM record with empty NodeIDs directly (bypassing PutObject).
	raw, err := marshalObjectMeta(objectMeta{Key: key, ETag: "e1", ECData: 0, NodeIDs: nil})
	require.NoError(t, err)
	require.NoError(t, b.store.Update(func(txn MetadataTxn) error {
		return txn.Set(b.ks().ObjectMetaKeyV(bkt, key, vid), raw)
	}))

	var got []string
	require.NoError(t, b.walkPerVersionBackfillCandidates(bkt, nowUnixSec(), 0, func(c perVersionBackfillCandidate) error {
		got = append(got, c.VersionID)
		return nil
	}))
	require.Empty(t, got, "version with empty NodeIDs must be skipped")
}

// TestWalkPerVersionBackfillCandidates_SkipsYoungerThanMinAge verifies that a
// freshly-written version is not yielded when minAge is set to a large value.
func TestWalkPerVersionBackfillCandidates_SkipsYoungerThanMinAge(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	const bkt, key = "vbkt3", "obj"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))
	v1 := putVersioned(t, b, ctx, bkt, key, "body")
	removePerVersionBlob(t, b, bkt, key, v1)

	// minAge = 3600 seconds → the just-minted VID is younger, so it must be skipped.
	const minAge = int64(3600)
	var got []string
	require.NoError(t, b.walkPerVersionBackfillCandidates(bkt, nowUnixSec(), minAge, func(c perVersionBackfillCandidate) error {
		got = append(got, c.VersionID)
		return nil
	}))
	require.Empty(t, got, "version younger than minAge must be skipped")
}

// TestWalkPerVersionBackfillCandidates_SkipsUnversionedSlashKey verifies that a
// pre-versioning (unversioned) FSM record whose object key contains a slash is
// NOT yielded as a backfill candidate. Without the UUID guard, the walker
// mis-parses such a record (e.g. obj:bkt/a/b → key="a", vid="b") and would
// fan out a garbage-keyed per-version blob.
func TestWalkPerVersionBackfillCandidates_SkipsUnversionedSlashKey(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	const bkt = "vbkt-slash"
	const slashKey = "a/b" // unversioned key containing a slash
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))

	// Write a raw unversioned FSM record (ObjectMetaKey, not ObjectMetaKeyV) for
	// a key that contains a slash, simulating a pre-versioning object that was
	// written before the bucket had versioning enabled.
	raw, err := marshalObjectMeta(objectMeta{Key: slashKey, ETag: "etag1", ECData: 4, NodeIDs: []string{"n1"}})
	require.NoError(t, err)
	require.NoError(t, b.store.Update(func(txn MetadataTxn) error {
		return txn.Set(b.ks().ObjectMetaKey(bkt, slashKey), raw)
	}))

	var got []string
	require.NoError(t, b.walkPerVersionBackfillCandidates(bkt, nowUnixSec(), 0, func(c perVersionBackfillCandidate) error {
		got = append(got, c.VersionID)
		return nil
	}))
	require.Empty(t, got, "unversioned record with slash in key must not be yielded as a backfill candidate")
}

// TestWalkPerVersionBackfillCandidates_NoopOnUnversionedBucket verifies that the
// walker is a no-op when the bucket does not have versioning enabled.
func TestWalkPerVersionBackfillCandidates_NoopOnUnversionedBucket(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	const bkt = "plain-bkt"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	// No SetBucketVersioning call — bucket is unversioned.

	var called bool
	require.NoError(t, b.walkPerVersionBackfillCandidates(bkt, nowUnixSec(), 0, func(_ perVersionBackfillCandidate) error {
		called = true
		return nil
	}))
	require.False(t, called, "walker must be a no-op for an unversioned bucket")
}

// deleteVersioned performs a soft-delete on a versioned bucket and returns the
// delete-marker VersionID.
func deleteVersioned(t *testing.T, b *DistributedBackend, bkt, key string) string {
	t.Helper()
	vid, err := b.deleteObjectWithMarker(context.Background(), bkt, key)
	require.NoError(t, err)
	require.NotEmpty(t, vid)
	return vid
}

// decodePerVersionBlob reads the raw per-version quorum-meta blob from disk for
// (bucket, key, versionID) and decodes it to a PutObjectMetaCmd using the same
// path S2 uses (decodeQuorumMetaCmdBlob). This is the authoritative decoder for
// backfill tests.
func decodePerVersionBlob(t *testing.T, b *DistributedBackend, bucket, key, versionID string) PutObjectMetaCmd {
	t.Helper()
	require.NotNil(t, b.shardSvc)
	dirs := b.shardSvc.DataDirs()
	require.NotEmpty(t, dirs)
	p := filepath.Join(dirs[0], quorumMetaVersionsSubDir, bucket, key, versionID)
	data, err := os.ReadFile(p)
	require.NoError(t, err, "per-version blob must exist at %s", p)
	cmd, err := b.shardSvc.decodeQuorumMetaCmdBlob(data)
	require.NoError(t, err)
	return cmd
}

// TestBackfillPerVersionBlob_DecodesToReadRelevantFields verifies that after
// removing S1's per-version blob and running backfillPerVersionBlob, the
// re-written blob decodes to a PutObjectMetaCmd whose read-relevant fields
// (those consumed by objectAndPlacementFromCmd and deriveLatestVersion) equal
// the original S1-written blob's fields. Write-time-only fields (ExpectedETag,
// PreserveLatest) are NOT asserted.
func TestBackfillPerVersionBlob_DecodesToReadRelevantFields(t *testing.T) {
	b := newTestDistributedBackend(t)
	const bkt, key = "vbkt", "obj"
	require.NoError(t, b.CreateBucket(context.Background(), bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))
	v1 := putVersioned(t, b, context.Background(), bkt, key, "one")

	// Capture S1's blob decoded to a cmd, then delete the blob to force a backfill.
	wantCmd := decodePerVersionBlob(t, b, bkt, key, v1)
	removePerVersionBlob(t, b, bkt, key, v1)

	var cand perVersionBackfillCandidate
	require.NoError(t, b.walkPerVersionBackfillCandidates(bkt, nowUnixSec(), 0, func(c perVersionBackfillCandidate) error {
		cand = c
		return nil
	}))
	require.NotEmpty(t, cand.VersionID, "walker must yield the missing-blob version")
	require.NoError(t, b.backfillPerVersionBlob(context.Background(), cand))

	got := decodePerVersionBlob(t, b, bkt, key, v1)
	// Read-relevant fields: everything consumed by objectAndPlacementFromCmd
	// (quorum_meta.go:718) and deriveLatestVersion (:645).
	// ExpectedETag / PreserveLatest are write-time-only and absent from objectMeta,
	// so they are NOT asserted.
	require.Equal(t, wantCmd.VersionID, got.VersionID)
	require.Equal(t, wantCmd.Key, got.Key)
	require.Equal(t, wantCmd.Bucket, got.Bucket)
	require.Equal(t, wantCmd.ETag, got.ETag)
	require.Equal(t, wantCmd.Size, got.Size)
	require.Equal(t, wantCmd.ContentType, got.ContentType)
	require.Equal(t, wantCmd.ModTime, got.ModTime)
	require.Equal(t, wantCmd.ECData, got.ECData)
	require.Equal(t, wantCmd.ECParity, got.ECParity)
	require.Equal(t, wantCmd.StripeBytes, got.StripeBytes)
	require.Equal(t, wantCmd.NodeIDs, got.NodeIDs)
	require.Equal(t, wantCmd.PlacementGroupID, got.PlacementGroupID)
	require.Equal(t, wantCmd.IsDeleteMarker, got.IsDeleteMarker)
	require.Equal(t, wantCmd.MetaSeq, got.MetaSeq)
	require.Equal(t, wantCmd.SSEAlgorithm, got.SSEAlgorithm)
	require.Equal(t, wantCmd.ACL, got.ACL)
	require.Equal(t, wantCmd.UserMetadata, got.UserMetadata)
	require.Equal(t, wantCmd.Tags, got.Tags)
	require.Equal(t, wantCmd.Parts, got.Parts)
	require.Equal(t, wantCmd.Segments, got.Segments)
}

// TestBackfillPerVersionBlob_ReconstructsDeleteMarker verifies the
// load-bearing correctness case: a delete-marker backfilled blob must decode
// with IsDeleteMarker=true so deriveLatestVersion does not treat it as a live
// object.
func TestBackfillPerVersionBlob_ReconstructsDeleteMarker(t *testing.T) {
	b := newTestDistributedBackend(t)
	const bkt, key = "vbkt", "obj"
	require.NoError(t, b.CreateBucket(context.Background(), bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))
	_ = putVersioned(t, b, context.Background(), bkt, key, "one")
	mk := deleteVersioned(t, b, bkt, key) // soft-delete → marker version

	removePerVersionBlob(t, b, bkt, key, mk)

	var cand perVersionBackfillCandidate
	require.NoError(t, b.walkPerVersionBackfillCandidates(bkt, nowUnixSec(), 0, func(c perVersionBackfillCandidate) error {
		if c.VersionID == mk {
			cand = c
		}
		return nil
	}))
	require.NotEmpty(t, cand.VersionID, "walker must yield the missing marker blob")
	require.NoError(t, b.backfillPerVersionBlob(context.Background(), cand))

	got := decodePerVersionBlob(t, b, bkt, key, mk)
	require.True(t, got.IsDeleteMarker, "backfilled marker must decode as a delete marker")
}

// TestBackfillPerVersionBlob_IsIdempotent verifies that after a successful
// backfill, walkPerVersionBackfillCandidates no longer yields the version (the
// blob is present on disk — skip-if-exists). This is the no-overwrite
// guarantee: S3 never re-writes an existing blob, so it cannot clobber S1's
// fuller blob.
func TestBackfillPerVersionBlob_IsIdempotent(t *testing.T) {
	b := newTestDistributedBackend(t)
	const bkt, key = "vbkt", "obj"
	require.NoError(t, b.CreateBucket(context.Background(), bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))
	v1 := putVersioned(t, b, context.Background(), bkt, key, "one")
	removePerVersionBlob(t, b, bkt, key, v1)

	// First pass: walker yields, backfill writes the blob.
	var cand perVersionBackfillCandidate
	require.NoError(t, b.walkPerVersionBackfillCandidates(bkt, nowUnixSec(), 0, func(c perVersionBackfillCandidate) error {
		cand = c
		return nil
	}))
	require.NotEmpty(t, cand.VersionID)
	require.NoError(t, b.backfillPerVersionBlob(context.Background(), cand))

	// Second pass: walker must NOT yield the same version again (blob present).
	var got []string
	require.NoError(t, b.walkPerVersionBackfillCandidates(bkt, nowUnixSec(), 0, func(c perVersionBackfillCandidate) error {
		got = append(got, c.VersionID)
		return nil
	}))
	require.Empty(t, got, "walker must not re-yield a version whose blob was backfilled")
}

// TestWalkPerVersionBackfillCandidates_SkipsUnversionedUUIDSuffixKey verifies
// that an UNVERSIONED FSM record whose object key's last segment is a valid UUID
// (e.g. key = "a/<uuidv7>") is NOT yielded as a backfill candidate.
//
// Without the meta.Key == parsedKey guard, uuid.Parse accepts the UUID suffix,
// the walker mis-parses key="a" / vid="<uuid>", and yields a phantom candidate.
func TestWalkPerVersionBackfillCandidates_SkipsUnversionedUUIDSuffixKey(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	const bkt = "vbkt-uuid-suffix"
	// key whose last path segment is a valid UUIDv7 — stored as an UNVERSIONED record.
	const uuidSuffix = "019756c0-17f8-7000-b7e4-a5ef7c2f1234"
	uuidSuffixKey := "a/" + uuidSuffix
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))

	// Write a raw UNVERSIONED FSM record (ObjectMetaKey, not ObjectMetaKeyV).
	// The meta.Key == full key "a/<uuid>", not just "a".
	raw, err := marshalObjectMeta(objectMeta{
		Key:     uuidSuffixKey,
		ETag:    "etag-uuid-suffix",
		ECData:  4,
		NodeIDs: []string{"n1"},
	})
	require.NoError(t, err)
	require.NoError(t, b.store.Update(func(txn MetadataTxn) error {
		return txn.Set(b.ks().ObjectMetaKey(bkt, uuidSuffixKey), raw)
	}))

	var got []string
	require.NoError(t, b.walkPerVersionBackfillCandidates(bkt, nowUnixSec(), 0, func(c perVersionBackfillCandidate) error {
		got = append(got, c.VersionID)
		return nil
	}))
	require.Empty(t, got, "unversioned record with UUID-suffix key must not be yielded as a backfill candidate")
}

// TestWalkPerVersionBackfillCandidates_SkipsAppendableAndCoalesced verifies
// that versioned FSM records with IsAppendable=true or non-empty Coalesced are
// NOT yielded as backfill candidates (foundation spec appendable/coalesced
// carve-out: these stay FSM-authoritative and must not get a per-version blob).
func TestWalkPerVersionBackfillCandidates_SkipsAppendableAndCoalesced(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	const bkt = "vbkt-appendable"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))

	const (
		keyAppendable = "append-obj"
		vidAppendable = "019756c0-17f8-7000-b7e4-a5ef7c2f0001"
		keyCoalesced  = "coalesced-obj"
		vidCoalesced  = "019756c0-17f8-7000-b7e4-a5ef7c2f0002"
	)

	// Write a versioned record with IsAppendable=true.
	rawAppendable, err := marshalObjectMeta(objectMeta{
		Key:          keyAppendable,
		ETag:         "etag-appendable",
		ECData:       4,
		NodeIDs:      []string{"n1"},
		IsAppendable: true,
	})
	require.NoError(t, err)
	require.NoError(t, b.store.Update(func(txn MetadataTxn) error {
		return txn.Set(b.ks().ObjectMetaKeyV(bkt, keyAppendable, vidAppendable), rawAppendable)
	}))

	// Write a versioned record with non-empty Coalesced.
	rawCoalesced, err := marshalObjectMeta(objectMeta{
		Key:       keyCoalesced,
		ETag:      "etag-coalesced",
		ECData:    4,
		NodeIDs:   []string{"n1"},
		Coalesced: []CoalescedShardRef{{CoalescedID: "cid1", Size: 100, ETag: "ce1"}},
	})
	require.NoError(t, err)
	require.NoError(t, b.store.Update(func(txn MetadataTxn) error {
		return txn.Set(b.ks().ObjectMetaKeyV(bkt, keyCoalesced, vidCoalesced), rawCoalesced)
	}))

	var got []string
	require.NoError(t, b.walkPerVersionBackfillCandidates(bkt, nowUnixSec(), 0, func(c perVersionBackfillCandidate) error {
		got = append(got, c.VersionID)
		return nil
	}))
	require.Empty(t, got, "appendable and coalesced versioned records must not be yielded as backfill candidates")
}

// TestWalkPerVersionBackfillCandidates_MarkerEmptyNodeIDsDerivesSiblingPlacement
// verifies that a degraded delete-marker (ETag==deleteMarkerETag, NodeIDs==nil
// — the result of a deleteObjectWithMarker call when no quorum-meta existed for
// the prior version) is still yielded by the walker with NodeIDs populated from
// a sibling version of the same key.
//
// Setup: a regular versioned PUT (has NodeIDs), then a RAW delete-marker FSM
// record with empty NodeIDs injected directly (bypassing the normal
// deleteObjectWithMarker which would call writeQuorumMeta and produce the
// secondary CmdPutObjectMeta record that fills in NodeIDs). The marker's
// per-version blob is removed so the walker sees it as missing.
func TestWalkPerVersionBackfillCandidates_MarkerEmptyNodeIDsDerivesSiblingPlacement(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	const bkt, key = "vbkt-marker-sibling", "obj"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))

	// Write a regular versioned PUT — this produces an FSM record with NodeIDs.
	liveVID := putVersioned(t, b, ctx, bkt, key, "live-data")
	// Capture the live version's NodeIDs from its FSM record.
	var liveNodeIDs []string
	require.NoError(t, b.store.View(func(txn MetadataTxn) error {
		item, err := txn.Get(b.ks().ObjectMetaKeyV(bkt, key, liveVID))
		if err != nil {
			return err
		}
		raw, err := b.itemValueCopy(item)
		if err != nil {
			return err
		}
		m, err := unmarshalObjectMeta(raw)
		if err != nil {
			return err
		}
		liveNodeIDs = m.NodeIDs
		return nil
	}))
	require.NotEmpty(t, liveNodeIDs, "live version must have NodeIDs")

	// Write a RAW delete-marker FSM record with empty NodeIDs, simulating the
	// degraded case where deleteObjectWithMarker skipped writeQuorumMeta because
	// readQuorumMetaCmd returned no placement for the prior version.
	const markerVID = "019756c0-17f8-7100-b7e4-a5ef7c2f9999"
	markerRaw, err := marshalObjectMeta(objectMeta{Key: key, ETag: deleteMarkerETag})
	require.NoError(t, err)
	require.NoError(t, b.store.Update(func(txn MetadataTxn) error {
		return txn.Set(b.ks().ObjectMetaKeyV(bkt, key, markerVID), markerRaw)
	}))
	// Simulate the missing per-version blob by never having written one — the
	// marker VID blob path simply does not exist on disk.

	var got []perVersionBackfillCandidate
	require.NoError(t, b.walkPerVersionBackfillCandidates(bkt, nowUnixSec(), 0, func(c perVersionBackfillCandidate) error {
		got = append(got, c)
		return nil
	}))

	var found *perVersionBackfillCandidate
	for i := range got {
		if got[i].VersionID == markerVID {
			found = &got[i]
			break
		}
	}
	require.NotNil(t, found, "walker must yield the degraded delete-marker candidate")
	require.Equal(t, deleteMarkerETag, found.Meta.ETag, "candidate must carry deleteMarkerETag")
	require.Equal(t, liveNodeIDs, found.Meta.NodeIDs, "walker must derive NodeIDs from the sibling live version")
}

// TestWalkPerVersionBackfillCandidates_MarkerNoSiblingPlacementSkipped verifies
// that a degraded delete-marker (ETag==deleteMarkerETag, NodeIDs==nil) with NO
// sibling version that has non-empty NodeIDs is NOT yielded by the walker.
func TestWalkPerVersionBackfillCandidates_MarkerNoSiblingPlacementSkipped(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	const bkt, key = "vbkt-marker-no-sibling", "obj"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))

	// Write ONLY a RAW delete-marker FSM record with empty NodeIDs and no sibling.
	const markerVID = "019756c0-17f8-7100-b7e4-a5ef7c2f8888"
	markerRaw, err := marshalObjectMeta(objectMeta{Key: key, ETag: deleteMarkerETag})
	require.NoError(t, err)
	require.NoError(t, b.store.Update(func(txn MetadataTxn) error {
		return txn.Set(b.ks().ObjectMetaKeyV(bkt, key, markerVID), markerRaw)
	}))

	var got []perVersionBackfillCandidate
	require.NoError(t, b.walkPerVersionBackfillCandidates(bkt, nowUnixSec(), 0, func(c perVersionBackfillCandidate) error {
		got = append(got, c)
		return nil
	}))
	require.Empty(t, got, "degraded delete-marker with no sibling placement must not be yielded")
}

// TestWalkPerVersionBackfillCandidates_IncludesDeleteMarker verifies that a
// delete-marker version (ETag == deleteMarkerETag) is enumerated as a candidate
// when its per-version blob is absent.
func TestWalkPerVersionBackfillCandidates_IncludesDeleteMarker(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	const bkt, key = "vbkt4", "obj"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))

	// Write a regular version first, then create a delete-marker via DeleteObject.
	_ = putVersioned(t, b, ctx, bkt, key, "data")
	markerVID, err := b.deleteObjectWithMarker(ctx, bkt, key)
	require.NoError(t, err)
	require.NotEmpty(t, markerVID)

	// The delete-marker version should have a per-version blob written by S1.
	// Remove it to simulate a gap.
	removePerVersionBlob(t, b, bkt, key, markerVID)

	var got []perVersionBackfillCandidate
	require.NoError(t, b.walkPerVersionBackfillCandidates(bkt, nowUnixSec(), 0, func(c perVersionBackfillCandidate) error {
		got = append(got, c)
		return nil
	}))

	var found bool
	for _, c := range got {
		if c.VersionID == markerVID {
			found = true
			require.Equal(t, deleteMarkerETag, c.Meta.ETag, "delete-marker candidate must carry deleteMarkerETag")
		}
	}
	require.True(t, found, "delete-marker version must be enumerated as a backfill candidate")
}
