package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// versionByKeyVID indexes a []*storage.ObjectVersion by (Key, VersionID) for
// assertions.
func versionByKeyVID(vs []*storage.ObjectVersion) map[[2]string]*storage.ObjectVersion {
	out := map[[2]string]*storage.ObjectVersion{}
	for _, v := range vs {
		out[[2]string{v.Key, v.VersionID}] = v
	}
	return out
}

// k-sortable UUIDv7-shaped strings used across the soleauth list tests. They are
// lexicographically ordered so the *second* string is the per-key max VID.
const (
	vidA1 = "019ed400-0000-7000-8000-000000000001"
	vidA2 = "019ed400-0000-7000-8000-000000000002"
	vidB1 = "019ed400-0000-7000-8000-00000000000a"
	vidB2 = "019ed400-0000-7000-8000-00000000000b"
)

// TestListObjectVersionsSoleAuthOn covers the soleauth=on early-return branch at
// the top of DistributedBackend.ListObjectVersions: the per-version blob tree is
// the SOLE AUTHORITY for versioned objects, merged with FSM carve-out classes.
func TestListObjectVersionsSoleAuthOn(t *testing.T) {
	ctx := context.Background()

	t.Run("multiple versions incl delete-marker-latest → all listed, IsLatest on max VID even if marker", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "bon"))
		setVersioningForTest(t, b, "bon", "Enabled")
		// k1: v1 live, v2 live (max → latest)
		seedVersionBlob(t, b, "bon", "k1", vidA1, PutObjectMetaCmd{ETag: "k1-v1", Size: 10, ModTime: 100})
		seedVersionBlob(t, b, "bon", "k1", vidA2, PutObjectMetaCmd{ETag: "k1-v2", Size: 20, ModTime: 200})
		// k2: v1 live, v2 DELETE MARKER (max → latest, still IsLatest)
		seedVersionBlob(t, b, "bon", "k2", vidB1, PutObjectMetaCmd{ETag: "k2-v1", Size: 5, ModTime: 300})
		seedVersionBlob(t, b, "bon", "k2", vidB2, PutObjectMetaCmd{ETag: deleteMarkerETag, IsDeleteMarker: true, ModTime: 400})

		vs, err := b.ListObjectVersions(ctx, "bon", "", 0)
		require.NoError(t, err)
		require.Len(t, vs, 4, "every version including the delete marker is listed")

		idx := versionByKeyVID(vs)
		require.Equal(t, "k1-v1", idx[[2]string{"k1", vidA1}].ETag)
		require.False(t, idx[[2]string{"k1", vidA1}].IsLatest)
		require.True(t, idx[[2]string{"k1", vidA2}].IsLatest, "max VID is latest")

		require.False(t, idx[[2]string{"k2", vidB1}].IsLatest)
		dm := idx[[2]string{"k2", vidB2}]
		require.NotNil(t, dm)
		require.True(t, dm.IsDeleteMarker, "delete marker is present in the listing")
		require.True(t, dm.IsLatest, "a delete-marker-latest is still flagged IsLatest")
	})

	t.Run("key whose blobs are gone but a stale vid-bearing FSM record lingers → key ABSENT", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "bstale"))
		setVersioningForTest(t, b, "bstale", "Enabled")
		// A live blob-backed key so the listing is non-empty.
		seedVersionBlob(t, b, "bstale", "live", vidA1, PutObjectMetaCmd{ETag: "live"})
		// A stale, non-carve-out vid-bearing FSM record with NO backing blob.
		seedFSMObject(t, b, "bstale", "ghost", vidB1, objectMeta{Key: "ghost", ETag: "stale"}, true)

		vs, err := b.ListObjectVersions(ctx, "bstale", "", 0)
		require.NoError(t, err)
		idx := versionByKeyVID(vs)
		require.NotNil(t, idx[[2]string{"live", vidA1}])
		for _, v := range vs {
			require.NotEqual(t, "ghost", v.Key, "stale versioned FSM record must NOT resurrect under soleauth=on")
		}
	})

	t.Run("appendable carve-out → appears", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "bapp"))
		setVersioningForTest(t, b, "bapp", "Enabled")
		seedFSMObject(t, b, "bapp", "ak", vidA1, objectMeta{Key: "ak", ETag: "app", IsAppendable: true}, true)

		vs, err := b.ListObjectVersions(ctx, "bapp", "", 0)
		require.NoError(t, err)
		idx := versionByKeyVID(vs)
		require.NotNil(t, idx[[2]string{"ak", vidA1}], "appendable carve-out appears")
		require.Equal(t, "app", idx[[2]string{"ak", vidA1}].ETag)
	})

	t.Run("versioned appendable slashless mirror → no bogus empty-VID duplicate", func(t *testing.T) {
		// codex code-gate [P2]: a versioned appendable object can persist BOTH the
		// versioned key obj:b/ak/v1 (lat:b/ak→v1) AND a slashless mirror obj:b/ak.
		// The bucket-wide carve-out scan must follow lat: (like read1's per-key
		// fsmCarveoutObject) and emit ONLY the versioned (ak,v1) entry — NOT a
		// spurious (ak,"") row (which would duplicate the object and yield two
		// IsLatest=true rows for one key).
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "bmir"))
		setVersioningForTest(t, b, "bmir", "Enabled")
		// versioned appendable record + lat:bmir/ak→vidA1
		seedFSMObject(t, b, "bmir", "ak", vidA1, objectMeta{Key: "ak", ETag: "app", IsAppendable: true}, true)
		// slashless mirror obj:bmir/ak (appendable), WITHOUT touching lat:
		seedFSMObject(t, b, "bmir", "ak", "", objectMeta{Key: "ak", ETag: "app", IsAppendable: true}, false)

		vs, err := b.ListObjectVersions(ctx, "bmir", "", 0)
		require.NoError(t, err)
		idx := versionByKeyVID(vs)
		require.NotNil(t, idx[[2]string{"ak", vidA1}], "versioned appendable entry appears")
		require.Nil(t, idx[[2]string{"ak", ""}], "slashless mirror must NOT emit a bogus empty-VID carve-out")
		// Exactly one row for key "ak", and exactly one IsLatest.
		var akRows, akLatest int
		for _, v := range vs {
			if v.Key == "ak" {
				akRows++
				if v.IsLatest {
					akLatest++
				}
			}
		}
		require.Equal(t, 1, akRows, "exactly one version row for the appendable key")
		require.Equal(t, 1, akLatest, "exactly one IsLatest=true row for the key")
	})

	t.Run("coalesced carve-out → appears", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "bco"))
		setVersioningForTest(t, b, "bco", "Enabled")
		m := objectMeta{Key: "ck", ETag: "co", Coalesced: []CoalescedShardRef{{CoalescedID: "c1", Size: 10}}}
		seedFSMObject(t, b, "bco", "ck", vidA1, m, true)

		vs, err := b.ListObjectVersions(ctx, "bco", "", 0)
		require.NoError(t, err)
		idx := versionByKeyVID(vs)
		require.NotNil(t, idx[[2]string{"ck", vidA1}], "coalesced carve-out appears")
	})

	t.Run("legacy bare record → appears with VersionID empty, IsLatest true", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "bbare"))
		// bare obj: record, NO lat: pointer, NOT appendable/coalesced.
		seedFSMObject(t, b, "bbare", "lk", "", objectMeta{Key: "lk", ETag: "bare"}, false)

		vs, err := b.ListObjectVersions(ctx, "bbare", "", 0)
		require.NoError(t, err)
		idx := versionByKeyVID(vs)
		v := idx[[2]string{"lk", ""}]
		require.NotNil(t, v, "legacy bare record appears")
		require.Equal(t, "", v.VersionID)
		require.True(t, v.IsLatest)
		require.Equal(t, "bare", v.ETag)
	})

	t.Run("(Key,VID) collision blob-vs-carveout → blob wins", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "bcol"))
		setVersioningForTest(t, b, "bcol", "Enabled")
		// Same (key, vid) exists as both a blob AND an appendable FSM carve-out.
		seedVersionBlob(t, b, "bcol", "k", vidA1, PutObjectMetaCmd{ETag: "blob-wins", Size: 99})
		seedFSMObject(t, b, "bcol", "k", vidA1, objectMeta{Key: "k", ETag: "carveout-loses", IsAppendable: true}, true)

		vs, err := b.ListObjectVersions(ctx, "bcol", "", 0)
		require.NoError(t, err)
		idx := versionByKeyVID(vs)
		v := idx[[2]string{"k", vidA1}]
		require.NotNil(t, v)
		require.Equal(t, "blob-wins", v.ETag, "blob wins a (Key,VID) collision")
		require.Equal(t, int64(99), v.Size)
		// Only one entry for this (Key,VID).
		count := 0
		for _, x := range vs {
			if x.Key == "k" && x.VersionID == vidA1 {
				count++
			}
		}
		require.Equal(t, 1, count, "no duplicate for the colliding (Key,VID)")
	})

	t.Run("maxKeys truncation at the leaf", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "btrunc"))
		setVersioningForTest(t, b, "btrunc", "Enabled")
		seedVersionBlob(t, b, "btrunc", "k1", vidA1, PutObjectMetaCmd{ETag: "1"})
		seedVersionBlob(t, b, "btrunc", "k2", vidB1, PutObjectMetaCmd{ETag: "2"})
		seedVersionBlob(t, b, "btrunc", "k3", vidB2, PutObjectMetaCmd{ETag: "3"})

		vs, err := b.ListObjectVersions(ctx, "btrunc", "", 2)
		require.NoError(t, err)
		require.Len(t, vs, 2, "leaf truncates to maxKeys as passed")
	})

	t.Run("soleauth read error → propagated (fail closed)", func(t *testing.T) {
		b, db := newTestDistributedBackendWithDB(t)
		require.NoError(t, b.CreateBucket(ctx, "berr"))
		require.NoError(t, db.Close())

		vs, err := b.ListObjectVersions(ctx, "berr", "", 0)
		require.Error(t, err)
		require.Nil(t, vs)
	})
}

// TestListObjectVersionsSoleAuthOffUnchanged confirms the off (default) path is
// byte-identical to today's FSM-backed listing for BOTH a versioned multi-key
// listing AND a legacy-bare listing.
func TestListObjectVersionsSoleAuthOffUnchanged(t *testing.T) {
	ctx := context.Background()

	t.Run("versioned multi-key blob listing (blob-primary)", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "voff"))
		setVersioningForTest(t, b, "voff", "Enabled")
		// Blob-primary: ListObjectVersions derives from the per-version blob tree;
		// IsLatest is the max-VersionID per key.
		seedVersionBlob(t, b, "voff", "k1", vidA1, PutObjectMetaCmd{ETag: "k1-v1"})
		seedVersionBlob(t, b, "voff", "k1", vidA2, PutObjectMetaCmd{ETag: "k1-v2"})
		seedVersionBlob(t, b, "voff", "k2", vidB1, PutObjectMetaCmd{ETag: "k2-v1"})

		vs, err := b.ListObjectVersions(ctx, "voff", "", 0)
		require.NoError(t, err)
		require.Len(t, vs, 3)
		idx := versionByKeyVID(vs)
		require.True(t, idx[[2]string{"k1", vidA2}].IsLatest)
		require.False(t, idx[[2]string{"k1", vidA1}].IsLatest)
		require.True(t, idx[[2]string{"k2", vidB1}].IsLatest)
	})

	t.Run("off: legacy bare listing", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "boff"))
		seedFSMObject(t, b, "boff", "lk", "", objectMeta{Key: "lk", ETag: "bare"}, false)

		vs, err := b.ListObjectVersions(ctx, "boff", "", 0)
		require.NoError(t, err)
		require.Len(t, vs, 1)
		require.Equal(t, "lk", vs[0].Key)
		require.Equal(t, "", vs[0].VersionID)
		require.True(t, vs[0].IsLatest)
		require.Equal(t, "bare", vs[0].ETag)
	})
}
