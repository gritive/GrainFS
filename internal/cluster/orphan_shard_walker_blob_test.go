package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestHasLiveShardRecord_VersioningEnabled proves that for a versioning-enabled
// bucket the per-version blob is the shard-liveness authority (live/tombstone/marker),
// with carve-out (appendable/coalesced) FSM records still protected, and a stale
// plain-versioned FSM record is NON-authoritative (orphan-eligible).
func TestHasLiveShardRecord_VersioningEnabled(t *testing.T) {
	ctx := context.Background()
	b := orphanWalkerBackend(t)
	require.NoError(t, b.CreateBucket(ctx, "vb"))
	setVersioningForTest(t, b, "vb", "Enabled")
	self := b.currentSelfAddr()

	seedVersionBlob(t, b, "vb", "k", "vLive", PutObjectMetaCmd{ETag: "e", NodeIDs: []string{self}})
	seedVersionBlob(t, b, "vb", "k", "vTomb", PutObjectMetaCmd{ETag: "e", NodeIDs: []string{self}, IsHardDeleted: true})
	seedVersionBlob(t, b, "vb", "k", "vMark", PutObjectMetaCmd{ETag: deleteMarkerETag, IsDeleteMarker: true, NodeIDs: []string{self}})
	// Appendable carve-out (FSM-authoritative, no blob).
	seedFSMObject(t, b, "vb", "ak", "vA", objectMeta{Key: "ak", ETag: "app", IsAppendable: true}, true)
	// A stale plain-versioned FSM record with no blob (e.g. C4 left it on hard delete).
	seedFSMObject(t, b, "vb", "k", "vGhost", objectMeta{Key: "k", ETag: "stale"}, true)

	cases := []struct {
		key, vid    string
		wantLive    bool
		wantCertain bool
	}{
		{"k", "vLive", true, true},
		{"k", "vTomb", false, true},
		{"k", "vMark", false, true},
		{"ak", "vA", true, true},     // carve-out FSM
		{"k", "vGhost", false, true}, // stale plain-versioned FSM record → orphan-eligible
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
// appendable carve-out (FSM forward-map), and ORPHANS a hard-deleted version's shards
// even when a stale plain-versioned FSM record lingers (blob tombstone wins; the
// lingering FSM record must NOT keep the shards alive).
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
	// appendable carve-out: FSM record (no blob) + shard → protected via forward-map.
	seedFSMObject(t, b, "vb", "ak", "vA", objectMeta{Key: "ak", ETag: "app", IsAppendable: true, ECData: 1}, true)
	carve := writeShardLeaf(t, root, "vb/ak/vA", []int{0}, oldEnough)

	got := collectOrphans(t, b, nil)
	require.Equal(t, []string{dead}, got, "only the hard-deleted version's shards are orphaned")
	_, _ = live, carve
}
