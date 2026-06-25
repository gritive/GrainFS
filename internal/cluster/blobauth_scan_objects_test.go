package cluster

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/scrubber"
)

// collectScanObjectRecs drains ScanObjects into a key→record map (latest-per-key).
func collectScanObjectRecs(t *testing.T, b *DistributedBackend, bucket string) map[string]scrubber.ObjectRecord {
	t.Helper()
	ch, err := b.ScanObjects(bucket)
	require.NoError(t, err)
	got := map[string]scrubber.ObjectRecord{}
	for rec := range ch {
		got[rec.Key] = rec
	}
	return got
}

// TestScanObjectsBlobAuthOn covers the blob-authoritative EC scrub enumeration: the
// live latest-per-key EC objects come from the per-version blob authority (NOT
// the stale FSM lat: walk), each reported with its OWN EC profile.
func TestScanObjectsBlobAuthOn(t *testing.T) {
	ctx := context.Background()

	t.Run("latest-per-key EC objects from blobs, own EC profile", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.True(t, b.ECActive())
		require.NoError(t, b.CreateBucket(ctx, "b"))
		setVersioningForTest(t, b, "b", "Enabled")
		// k1: two versions, latest = vidA2; genesis 1+0 profile.
		seedVersionBlob(t, b, "b", "k1", vidA1, PutObjectMetaCmd{ETag: "k1v1", ECData: 1, ECParity: 0})
		seedVersionBlob(t, b, "b", "k1", vidA2, PutObjectMetaCmd{ETag: "k1v2", ECData: 1, ECParity: 0})
		// k2: single version; 4+2 profile.
		seedVersionBlob(t, b, "b", "k2", vidB1, PutObjectMetaCmd{ETag: "k2v1", ECData: 4, ECParity: 2})

		got := collectScanObjectRecs(t, b, "b")
		require.Len(t, got, 2, "one record per key (latest only)")
		require.Equal(t, vidA2, got["k1"].VersionID, "k1 reports its latest version")
		require.Equal(t, 1, got["k1"].DataShards)
		require.Equal(t, 0, got["k1"].ParityShards)
		require.Equal(t, 4, got["k2"].DataShards, "k2 reports its OWN 4+2 profile")
		require.Equal(t, 2, got["k2"].ParityShards)
	})

	t.Run("delete-marker-latest key → no record", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "b"))
		setVersioningForTest(t, b, "b", "Enabled")
		seedVersionBlob(t, b, "b", "k", vidA1, PutObjectMetaCmd{ETag: "v1", ECData: 1, ECParity: 0})
		seedVersionBlob(t, b, "b", "k", vidA2, PutObjectMetaCmd{ETag: deleteMarkerETag, IsDeleteMarker: true})

		got := collectScanObjectRecs(t, b, "b")
		require.NotContains(t, got, "k", "a delete-marker latest has no shards to scrub")
	})

	t.Run("stale vid-bearing FSM record with no blob → not enumerated", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "b"))
		setVersioningForTest(t, b, "b", "Enabled")
		seedVersionBlob(t, b, "b", "live", vidA1, PutObjectMetaCmd{ETag: "live", ECData: 1, ECParity: 0})
		// Stale FSM record with a lat: pointer but NO blob and NOT a carve-out class.
		seedFSMObject(t, b, "b", "ghost", vidB1, objectMeta{Key: "ghost", ETag: "stale", ECData: 1, ECParity: 0}, true)

		got := collectScanObjectRecs(t, b, "b")
		require.Contains(t, got, "live")
		require.NotContains(t, got, "ghost", "stale non-carve-out FSM record is non-authoritative under on")
	})

	t.Run("non-EC object → no record", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "b"))
		setVersioningForTest(t, b, "b", "Enabled")
		seedVersionBlob(t, b, "b", "k", vidA1, PutObjectMetaCmd{ETag: "v1", ECData: 0, ECParity: 0})

		got := collectScanObjectRecs(t, b, "b")
		require.NotContains(t, got, "k", "a non-EC object has no EC shards to scrub")
	})

	t.Run("EC per-version blob → record", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "b"))
		setVersioningForTest(t, b, "b", "Enabled")
		// EC per-version blob (4+2) is the blob authority and must be scrubbed.
		seedVersionBlob(t, b, "b", "lk", vidA1, PutObjectMetaCmd{
			ETag: "bare", ECData: 4, ECParity: 2, NodeIDs: []string{b.currentSelfAddr()},
		})

		got := collectScanObjectRecs(t, b, "b")
		require.Contains(t, got, "lk", "EC per-version blob is scrubbed")
		require.Equal(t, 4, got["lk"].DataShards)
		require.Equal(t, 2, got["lk"].ParityShards)
	})

	t.Run("corrupt local blob → synchronous error", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "b"))
		setVersioningForTest(t, b, "b", "Enabled")
		// Write an undecodable blob into the per-version tree.
		require.NoError(t, b.shardSvc.writeQuorumMetaVersionLocal("b", filepath.Join("k", vidA1), []byte("not-a-valid-blob")))

		ch, err := b.ScanObjects("b")
		require.Error(t, err, "strict scan surfaces a corrupt blob synchronously")
		require.Nil(t, ch)
	})
}

// TestScanObjectsBlobAuthOffUnchanged confirms the off path still uses the FSM
// lat: walk + quorum-meta merge.
func TestScanObjectsBlobAuthOffUnchanged(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	require.True(t, b.ECActive())
	require.NoError(t, b.CreateBucket(ctx, "b"))
	// FSM lat:-indexed EC object (off-path source).
	seedPlacementMeta(t, b, "b", "fsm.bin", "v1", []string{b.selfAddr}, 1, 0)
	// blob-authority off (default)

	got := collectScanObjectRecs(t, b, "b")
	require.Contains(t, got, "fsm.bin", "off path still reads the FSM lat: walk")
}
