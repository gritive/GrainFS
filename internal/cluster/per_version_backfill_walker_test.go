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
