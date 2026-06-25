package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestGetObjectTags_BlobAuthOn_BlobDerive proves that under blob-authoritative the
// per-version blob is the BLOB AUTHORITY for tags: latest (versionID=="") and a
// specific-version read both return the blob's Tags, a stale vid-bearing FSM
// record is NEVER resurrected (blob-absent versioned object → 404), carve-out
// classes (appendable/coalesced/legacy bare-unversioned) fall back to FSM Tags,
// and a blob-authority read error propagates. The off path is byte-identical FSM.
func TestGetObjectTags_BlobAuthOn_BlobDerive(t *testing.T) {
	ctx := context.Background()

	t.Run("latest via versionID== blob Tags", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "vb"))
		seedVersionBlob(t, b, "vb", "k", "v1", PutObjectMetaCmd{
			Tags: []storage.Tag{{Key: "a", Value: "1"}},
		})
		seedVersionBlob(t, b, "vb", "k", "v2", PutObjectMetaCmd{
			Tags:    []storage.Tag{{Key: "b", Value: "2"}},
			MetaSeq: 1,
		})
		setVersioningForTest(t, b, "vb", "Enabled")

		tags, err := b.GetObjectTags("vb", "k", "")
		require.NoError(t, err)
		require.Equal(t, []storage.Tag{{Key: "b", Value: "2"}}, tags, "latest derive picks max-VID blob's tags")
	})

	t.Run("specific-version blob Tags", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "vb"))
		seedVersionBlob(t, b, "vb", "k", "v1", PutObjectMetaCmd{
			Tags: []storage.Tag{{Key: "a", Value: "1"}},
		})
		seedVersionBlob(t, b, "vb", "k", "v2", PutObjectMetaCmd{
			Tags:    []storage.Tag{{Key: "b", Value: "2"}},
			MetaSeq: 1,
		})
		setVersioningForTest(t, b, "vb", "Enabled")

		tags, err := b.GetObjectTags("vb", "k", "v1")
		require.NoError(t, err)
		require.Equal(t, []storage.Tag{{Key: "a", Value: "1"}}, tags, "specific-version returns that blob's tags")
	})

	t.Run("blob absent but stale vid-bearing FSM record with Tags 404", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "vb"))
		// stale FSM versioned record carrying tags, NO per-version blob.
		seedFSMObject(t, b, "vb", "k", "v1", objectMeta{
			Key: "k", ETag: "e", Tags: []storage.Tag{{Key: "stale", Value: "yes"}},
		}, true)
		setVersioningForTest(t, b, "vb", "Enabled")

		tags, err := b.GetObjectTags("vb", "k", "")
		require.ErrorIs(t, err, storage.ErrObjectNotFound, "blob-absent versioned object is 404, never stale FSM tags")
		require.Nil(t, tags)
	})

	t.Run("appendable carve-out blob Tags", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "ba"))
		seedVersionBlob(t, b, "ba", "k", "v1", PutObjectMetaCmd{
			ETag: "e", IsAppendable: true,
			Tags:    []storage.Tag{{Key: "ap", Value: "1"}},
			NodeIDs: []string{b.currentSelfAddr()},
		})
		setVersioningForTest(t, b, "ba", "Enabled")

		tags, err := b.GetObjectTags("ba", "k", "")
		require.NoError(t, err)
		require.Equal(t, []storage.Tag{{Key: "ap", Value: "1"}}, tags)
	})

	t.Run("coalesced carve-out blob Tags", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "bc"))
		seedVersionBlob(t, b, "bc", "k", "v1", PutObjectMetaCmd{
			ETag: "e", IsAppendable: true,
			Coalesced: []CoalescedShardRef{{CoalescedID: "c1", Size: 10}},
			Tags:      []storage.Tag{{Key: "co", Value: "1"}},
			NodeIDs:   []string{b.currentSelfAddr()},
		})
		setVersioningForTest(t, b, "bc", "Enabled")

		tags, err := b.GetObjectTags("bc", "k", "")
		require.NoError(t, err)
		require.Equal(t, []storage.Tag{{Key: "co", Value: "1"}}, tags)
	})

	t.Run("legacy bare-unversioned carve-out latest-only blob Tags", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "bl"))
		// Non-versioned object: tags live on the latest-only quorum-meta blob.
		seedLatestBlob(t, b, "bl", "k", PutObjectMetaCmd{
			ETag: "e", Tags: []storage.Tag{{Key: "lg", Value: "1"}},
			NodeIDs: []string{b.currentSelfAddr()},
		})

		tags, err := b.GetObjectTags("bl", "k", "")
		require.NoError(t, err)
		require.Equal(t, []storage.Tag{{Key: "lg", Value: "1"}}, tags)
	})

	t.Run("latest delete-marker with blobs present → 404, NOT stale carve-out Tags", func(t *testing.T) {
		// codex code-gate [P1]: per-version blobs exist and the max-VID is a
		// delete marker (latest not-live). The versioned object is GONE → 404. A
		// coexisting appendable FSM carve-out record must NOT be served instead.
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "bdm0"))
		seedVersionBlob(t, b, "bdm0", "k", "v1", PutObjectMetaCmd{
			Tags: []storage.Tag{{Key: "old", Value: "1"}},
		})
		seedVersionBlob(t, b, "bdm0", "k", "v2", PutObjectMetaCmd{
			ETag: deleteMarkerETag, IsDeleteMarker: true, MetaSeq: 1,
		})
		// Stale appendable FSM carve-out at the same key (would be wrongly served
		// by the buggy "only-not-live → carve-out" fallthrough).
		seedFSMObject(t, b, "bdm0", "k", "vapp", objectMeta{
			Key: "k", ETag: "app", IsAppendable: true,
			Tags: []storage.Tag{{Key: "stale", Value: "x"}},
		}, true)
		setVersioningForTest(t, b, "bdm0", "Enabled")

		tags, err := b.GetObjectTags("bdm0", "k", "")
		require.ErrorIs(t, err, storage.ErrObjectNotFound, "latest delete-marker → 404, no carve-out fallthrough")
		require.Nil(t, tags)
	})

	t.Run("specific delete-marker version → ErrMethodNotAllowed", func(t *testing.T) {
		// codex code-gate [P2]: a specific-version tag read targeting a
		// delete-marker blob must fold like T2's object read, not return Tags.
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "bdm1"))
		seedVersionBlob(t, b, "bdm1", "k", "v1", PutObjectMetaCmd{
			ETag: deleteMarkerETag, IsDeleteMarker: true,
		})
		setVersioningForTest(t, b, "bdm1", "Enabled")

		tags, err := b.GetObjectTags("bdm1", "k", "v1")
		require.ErrorIs(t, err, storage.ErrMethodNotAllowed)
		require.Nil(t, tags)
	})

	t.Run("blob-authority read error propagates", func(t *testing.T) {
		b, db := newTestDistributedBackendWithDB(t)
		require.NoError(t, b.CreateBucket(ctx, "berr"))
		require.NoError(t, db.Close())
		tags, err := b.GetObjectTags("berr", "k", "")
		require.Error(t, err)
		require.Nil(t, tags)
	})
}

// TestGetObjectTags_BlobAuthOff_LatestOnlyBlob proves the off (default,
// non-versioned) path reads tags from the latest-only quorum-meta blob (the
// non-versioned authority), and a missing record is ErrObjectNotFound.
func TestGetObjectTags_BlobAuthOff_LatestOnlyBlob(t *testing.T) {
	ctx := context.Background()

	t.Run("latest-only blob Tags returned (off)", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "off"))
		seedLatestBlob(t, b, "off", "k", PutObjectMetaCmd{
			ETag: "e", Tags: []storage.Tag{{Key: "x", Value: "1"}},
			NodeIDs: []string{b.currentSelfAddr()},
		})
		tags, err := b.GetObjectTags("off", "k", "")
		require.NoError(t, err)
		require.Equal(t, []storage.Tag{{Key: "x", Value: "1"}}, tags)
	})

	t.Run("missing record 404 (off)", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "off2"))
		tags, err := b.GetObjectTags("off2", "missing", "")
		require.ErrorIs(t, err, storage.ErrObjectNotFound)
		require.Nil(t, tags)
	})
}
