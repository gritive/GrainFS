package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestHasLiveShardRecord_VersioningEnabled proves that for a versioning-enabled
// bucket the per-version blob is the SOLE shard-liveness authority
// (live/tombstone/marker). A per-version MISS is orphan-eligible — there is no FSM
// read fallback, so a stale FSM record (which greenfield never produces) can never
// keep shards alive.
func TestHasLiveShardRecord_VersioningEnabled(t *testing.T) {
	ctx := context.Background()
	b := orphanWalkerBackend(t)
	require.NoError(t, b.CreateBucket(ctx, "vb"))
	setVersioningForTest(t, b, "vb", "Enabled")
	self := b.currentSelfAddr()

	seedVersionBlob(t, b, "vb", "k", "vLive", PutObjectMetaCmd{ETag: "e", NodeIDs: []string{self}})
	seedVersionBlob(t, b, "vb", "k", "vTomb", PutObjectMetaCmd{ETag: "e", NodeIDs: []string{self}, IsHardDeleted: true})
	seedVersionBlob(t, b, "vb", "k", "vMark", PutObjectMetaCmd{ETag: deleteMarkerETag, IsDeleteMarker: true, NodeIDs: []string{self}})
	// Appendable object: blob-resident (greenfield writes the blob, not an FSM record).
	seedVersionBlob(t, b, "vb", "ak", "vA", PutObjectMetaCmd{ETag: "app", IsAppendable: true, NodeIDs: []string{self}})
	// A stale plain-versioned FSM record with NO blob: it must NOT keep shards alive
	// (the FSM record is never read).
	seedFSMObject(t, b, "vb", "k", "vGhost", objectMeta{Key: "k", ETag: "stale"}, true)

	cases := []struct {
		key, vid    string
		wantLive    bool
		wantCertain bool
	}{
		{"k", "vLive", true, true},
		{"k", "vTomb", false, true},
		{"k", "vMark", false, true},
		{"ak", "vA", true, true},     // appendable blob → live
		{"k", "vGhost", false, true}, // stale FSM record, no blob → orphan-eligible
		{"k", "vMissing", false, true},
	}
	for _, tc := range cases {
		live, certain := b.hasLiveShardRecord("vb", tc.key, tc.vid)
		require.Equal(t, tc.wantLive, live, "live %s/%s", tc.key, tc.vid)
		require.Equal(t, tc.wantCertain, certain, "certain %s/%s", tc.key, tc.vid)
	}
}

// TestWalkOrphanShards_VersioningEnabled_BlobAuthority proves the shard sweep, for a
// versioning-enabled bucket: protects a live version (per-version blob), protects an
// appendable object (blob-resident), and ORPHANS a hard-deleted version's shards
// even when a stale plain-versioned FSM record lingers (the blob tombstone wins; the
// lingering FSM record is never read and must NOT keep the shards alive).
func TestWalkOrphanShards_VersioningEnabled_BlobAuthority(t *testing.T) {
	ctx := context.Background()
	b := orphanWalkerBackend(t)
	require.NoError(t, b.CreateBucket(ctx, "vb"))
	setVersioningForTest(t, b, "vb", "Enabled")
	root := b.shardSvc.DataDirs()[0]
	self := b.currentSelfAddr()

	// live versioned object: blob + shard → protected.
	seedVersionBlob(t, b, "vb", "k", "vLive", PutObjectMetaCmd{ETag: "e", ECData: 1, NodeIDs: []string{self}})
	live := writeShardLeaf(t, root, "vb/k/vLive", []int{0}, oldEnough)
	// hard-deleted version: tombstone blob + LINGERING plain FSM record + shard → orphan.
	seedVersionBlob(t, b, "vb", "k", "vDead", PutObjectMetaCmd{ETag: "e", ECData: 1, NodeIDs: []string{self}, IsHardDeleted: true})
	seedFSMObject(t, b, "vb", "k", "vDead", objectMeta{Key: "k", ETag: "e", ECData: 1}, true)
	dead := writeShardLeaf(t, root, "vb/k/vDead", []int{0}, oldEnough)
	// appendable object: blob-resident (greenfield) + shard → protected.
	seedVersionBlob(t, b, "vb", "ak", "vA", PutObjectMetaCmd{ETag: "app", IsAppendable: true, ECData: 1, NodeIDs: []string{self}})
	carve := writeShardLeaf(t, root, "vb/ak/vA", []int{0}, oldEnough)

	got := collectOrphans(t, b, nil)
	require.Equal(t, []string{dead}, got, "only the hard-deleted version's shards are orphaned")
	_, _ = live, carve
}
