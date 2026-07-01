package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestDeriveLatestVersion_SkipsHardDeleted proves the latest derive treats a
// hard-delete tombstone as a non-present version: a tombstone at the max VID must
// fall through to the live predecessor (not 404 the whole key), and a key whose
// only/max live version is tombstoned is gone.
func TestDeriveLatestVersion_SkipsHardDeleted(t *testing.T) {
	t.Run("tombstone at max VID falls to live predecessor", func(t *testing.T) {
		cmds := []PutObjectMetaCmd{
			{VersionID: "v1", ETag: "e1"},
			{VersionID: "v2", ETag: "e2", IsHardDeleted: true},
		}
		latest, live := deriveLatestVersion(cmds)
		require.True(t, live)
		require.Equal(t, "v1", latest.VersionID, "hard-deleted v2 must not be the latest")
		require.Equal(t, "e1", latest.ETag)
	})

	t.Run("only version tombstoned → not live", func(t *testing.T) {
		_, live := deriveLatestVersion([]PutObjectMetaCmd{{VersionID: "v1", IsHardDeleted: true}})
		require.False(t, live)
	})
}

// TestHeadObjectMetaV_HardDeletedTombstone_Returns404 proves a specific-version
// read of a hard-deleted version is 404 (NoSuchVersion), distinct from a
// delete-marker version which is 405 (MethodNotAllowed).
func TestHeadObjectMetaV_HardDeletedTombstone_Returns404(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(ctx, "b"))
	setVersioningForTest(t, b, "b", "Enabled")

	seedVersionBlob(t, b, "b", "k", "vhard", PutObjectMetaCmd{ETag: "e", IsHardDeleted: true})
	seedVersionBlob(t, b, "b", "k", "vmark", PutObjectMetaCmd{ETag: deleteMarkerETag, IsDeleteMarker: true})

	_, _, err := b.headObjectMetaV(ctx, "b", "k", "vhard")
	require.ErrorIs(t, err, storage.ErrObjectNotFound, "hard-deleted version must be 404, not resurrected")

	_, _, err = b.headObjectMetaV(ctx, "b", "k", "vmark")
	require.ErrorIs(t, err, storage.ErrMethodNotAllowed, "delete-marker version stays 405")
}

// TestHeadObjectMeta_LatestHardDeleted_FallsToPredecessor proves a latest read
// after the current latest version was hard-deleted surfaces the live predecessor.
func TestHeadObjectMeta_LatestHardDeleted_FallsToPredecessor(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(ctx, "b"))
	setVersioningForTest(t, b, "b", "Enabled")

	seedVersionBlob(t, b, "b", "k", "v1", PutObjectMetaCmd{ETag: "e1", Size: 10})
	seedVersionBlob(t, b, "b", "k", "v2", PutObjectMetaCmd{ETag: "e2", Size: 20, IsHardDeleted: true})

	obj, _, err := b.headObjectMeta(ctx, "b", "k")
	require.NoError(t, err)
	require.NotNil(t, obj)
	require.Equal(t, "v1", obj.VersionID, "latest must fall to the live predecessor of the hard-deleted v2")
	require.Equal(t, "e1", obj.ETag)
}

// TestGetObjectTags_HardDeletedVersion_Returns404 proves a specific-version tag
// read of a hard-deleted version is 404, not a stale/empty tag set.
func TestGetObjectTags_HardDeletedVersion_Returns404(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(ctx, "b"))
	setVersioningForTest(t, b, "b", "Enabled")

	seedVersionBlob(t, b, "b", "k", "vhard", PutObjectMetaCmd{ETag: "e", IsHardDeleted: true})

	_, err := b.GetObjectTags("b", "k", "vhard")
	require.ErrorIs(t, err, storage.ErrObjectNotFound)
}

// TestListObjectVersionsBlobAuth_ExcludesHardDeleted proves ListObjectVersions
// omits a hard-deleted version entirely while keeping live and delete-marker
// versions (a delete marker IS a version in S3 ListObjectVersions; a hard delete
// is not).
func TestListObjectVersionsBlobAuth_ExcludesHardDeleted(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(ctx, "b"))
	setVersioningForTest(t, b, "b", "Enabled")

	seedVersionBlob(t, b, "b", "k", "v1", PutObjectMetaCmd{ETag: "e1"})
	seedVersionBlob(t, b, "b", "k", "v2", PutObjectMetaCmd{ETag: "e2", IsHardDeleted: true})
	seedVersionBlob(t, b, "b", "k", "v3", PutObjectMetaCmd{ETag: deleteMarkerETag, IsDeleteMarker: true})

	vers, err := b.listObjectVersionsBlobAuth("b", "", 0)
	require.NoError(t, err)
	vids := map[string]bool{}
	for _, v := range vers {
		vids[v.VersionID] = true
	}
	require.True(t, vids["v1"], "live version listed")
	require.False(t, vids["v2"], "hard-deleted version must be excluded from ListObjectVersions")
	require.True(t, vids["v3"], "delete-marker version remains a listed version")
}

// TestListObjectsPerVersion_LatestHardDeleted_ShowsPredecessor proves LIST-latest
// surfaces the live predecessor when the current latest version was hard-deleted,
// and omits a key whose only version is a tombstone.
func TestListObjectsPerVersion_LatestHardDeleted_ShowsPredecessor(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(ctx, "b"))
	setVersioningForTest(t, b, "b", "Enabled")

	seedVersionBlob(t, b, "b", "k", "v1", PutObjectMetaCmd{ETag: "e1"})
	seedVersionBlob(t, b, "b", "k", "v2", PutObjectMetaCmd{ETag: "e2", IsHardDeleted: true})
	seedVersionBlob(t, b, "b", "gone", "v1", PutObjectMetaCmd{ETag: "x", IsHardDeleted: true})

	out, err := b.listObjectsPerVersion(ctx, "b", "")
	require.NoError(t, err)
	require.Len(t, out, 1, "only key k (predecessor) remains; key gone is fully tombstoned")
	require.Equal(t, "k", out[0].Key)
	require.Equal(t, "v1", out[0].VersionID, "latest falls to the live predecessor")
	require.Equal(t, "e1", out[0].ETag)
}

// TestListBlobAuthBucketObjectsForGC_ExcludesHardDeleted proves the segment-GC
// known-set omits a hard-deleted version (so its now-dead segments become
// orphan-eligible) while keeping live versions.
func TestListBlobAuthBucketObjectsForGC_ExcludesHardDeleted(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(ctx, "b"))
	setVersioningForTest(t, b, "b", "Enabled")

	seedVersionBlob(t, b, "b", "k", "v1", PutObjectMetaCmd{ETag: "e1", Size: 10})
	seedVersionBlob(t, b, "b", "k", "v2", PutObjectMetaCmd{ETag: "e2", Size: 20, IsHardDeleted: true})

	objs, err := b.listBlobAuthBucketObjectsForGC("b")
	require.NoError(t, err)
	require.Len(t, objs, 1, "hard-deleted version excluded from the GC known-set")
	require.Equal(t, "v1", objs[0].VersionID)
}

// TestScanObjectsBlobAuth_ExcludesHardDeleted proves the EC scrub set omits a
// hard-deleted version and collapses to the live predecessor.
func TestScanObjectsBlobAuth_ExcludesHardDeleted(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(ctx, "b"))
	setVersioningForTest(t, b, "b", "Enabled")

	seedVersionBlob(t, b, "b", "k", "v1", PutObjectMetaCmd{ETag: "e1", ECData: 4, ECParity: 2})
	seedVersionBlob(t, b, "b", "k", "v2", PutObjectMetaCmd{ETag: "e2", ECData: 4, ECParity: 2, IsHardDeleted: true})

	recs, err := b.scanObjectsBlobAuth("b")
	require.NoError(t, err)
	require.Len(t, recs, 1, "hard-deleted latest excluded; scrub set collapses to live predecessor")
	require.Equal(t, "v1", recs[0].VersionID)
}
