package cluster

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

func TestListAllObjects_EmptyBucket(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "empty"))

	objs, err := b.ListAllObjects()
	require.NoError(t, err)
	assert.Empty(t, objs)
}

func TestListAllObjects_PreservesVersionHistoryAndDeleteMarkers(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "tomb"))

	v1, err := b.PutObject(context.Background(), "tomb", "doc.txt", strings.NewReader("v1"), "text/plain")
	require.NoError(t, err)
	v2, err := b.PutObject(context.Background(), "tomb", "doc.txt", strings.NewReader("v2"), "text/plain")
	require.NoError(t, err)
	markerID, err := b.DeleteObjectReturningMarker("tomb", "doc.txt")
	require.NoError(t, err)

	objs, err := b.ListAllObjects()
	require.NoError(t, err)

	byVersion := make(map[string]storage.SnapshotObject)
	for _, o := range objs {
		byVersion[o.VersionID] = o
	}
	require.Contains(t, byVersion, v1.VersionID)
	require.Contains(t, byVersion, v2.VersionID)
	require.Contains(t, byVersion, markerID)
	assert.False(t, byVersion[v1.VersionID].IsLatest)
	assert.False(t, byVersion[v2.VersionID].IsLatest)
	assert.True(t, byVersion[markerID].IsLatest)
	assert.True(t, byVersion[markerID].IsDeleteMarker)
	assert.Equal(t, deleteMarkerETag, byVersion[markerID].ETag)
}

func TestListAllObjects_NoBuckets(t *testing.T) {
	b := newTestDistributedBackend(t)

	objs, err := b.ListAllObjects()
	require.NoError(t, err)
	assert.Empty(t, objs)
}

func TestRestoreObjects_StaleBlob(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "stale"))

	// Snapshot references an object whose blob does NOT exist on disk.
	snap := []storage.SnapshotObject{{
		Bucket:    "stale",
		Key:       "missing.bin",
		ETag:      "etag-xyz",
		Size:      100,
		Modified:  time.Now().UnixMilli(),
		VersionID: "v-missing",
		IsLatest:  true,
	}}

	count, stale, err := b.RestoreObjects(snap)
	require.NoError(t, err)
	assert.Equal(t, 0, count, "no objects restored because blob is absent")
	require.Len(t, stale, 1)
	assert.Equal(t, "stale", stale[0].Bucket)
	assert.Equal(t, "missing.bin", stale[0].Key)
}

func TestRestoreObjects_ResolvesEmptyVersionIDBeforeDeletingCurrentMetadata(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "legacy-wal"))
	obj, err := b.PutObject(context.Background(), "legacy-wal", "inc.txt", strings.NewReader("included"), "text/plain")
	require.NoError(t, err)
	require.NotEmpty(t, obj.VersionID)

	snap := []storage.SnapshotObject{{
		Bucket:      "legacy-wal",
		Key:         "inc.txt",
		ETag:        obj.ETag,
		Size:        obj.Size,
		ContentType: obj.ContentType,
		Modified:    obj.LastModified,
		IsLatest:    true,
	}}

	count, stale, err := b.RestoreObjects(snap)
	require.NoError(t, err)
	require.Empty(t, stale)
	require.Equal(t, 1, count)

	versions, err := b.ListObjectVersions("legacy-wal", "inc.txt", 10)
	require.NoError(t, err)
	require.Len(t, versions, 1)
	require.Equal(t, obj.VersionID, versions[0].VersionID)
}

func TestRestoreObjects_ResolvesMismatchedVersionIDWhenBucketRowIsMissing(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "wal-replay"))
	obj, err := b.PutObject(context.Background(), "wal-replay", "inc.txt", strings.NewReader("included"), "text/plain")
	require.NoError(t, err)
	require.NotEmpty(t, obj.VersionID)
	require.NoError(t, b.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(b.ks().BucketKey("wal-replay"))
	}))

	snap := []storage.SnapshotObject{{
		Bucket:      "wal-replay",
		Key:         "inc.txt",
		ETag:        obj.ETag,
		Size:        obj.Size,
		ContentType: obj.ContentType,
		Modified:    obj.LastModified,
		VersionID:   "wal-version-id",
		IsLatest:    true,
	}}

	count, stale, err := b.RestoreObjects(snap)
	require.NoError(t, err)
	require.Empty(t, stale)
	require.Equal(t, 1, count)

	var latestVersionID string
	require.NoError(t, b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(b.ks().LatestKey("wal-replay", "inc.txt"))
		if err != nil {
			return err
		}
		if err := item.Value(func(v []byte) error {
			latestVersionID = string(v)
			return nil
		}); err != nil {
			return err
		}
		item, err = txn.Get(b.ks().ObjectMetaKeyV("wal-replay", "inc.txt", latestVersionID))
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			meta, err := unmarshalObjectMeta(v)
			require.NoError(t, err)
			require.Equal(t, obj.ETag, meta.ETag)
			require.Equal(t, obj.Size, meta.Size)
			return nil
		})
	}))
	require.Equal(t, obj.VersionID, latestVersionID)
}

func TestRestoreObjects_PreservesCurrentECPlacementMetadata(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "ec"))
	createBlob(t, b, "ec", "k", "v1")
	require.NoError(t, b.propose(context.Background(), CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket:           "ec",
		Key:              "k",
		Size:             3,
		ContentType:      "text/plain",
		ETag:             "etag",
		ModTime:          123,
		VersionID:        "v1",
		RingVersion:      42,
		ECData:           1,
		ECParity:         0,
		NodeIDs:          []string{"node-a"},
		PlacementGroupID: "group-2",
	}))

	count, stale, err := b.RestoreObjects([]storage.SnapshotObject{{
		Bucket:      "ec",
		Key:         "k",
		ETag:        "etag",
		Size:        3,
		ContentType: "text/plain",
		Modified:    123,
		VersionID:   "v1",
		IsLatest:    true,
	}})
	require.NoError(t, err)
	require.Empty(t, stale)
	require.Equal(t, 1, count)

	require.NoError(t, b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(b.ks().ObjectMetaKeyV("ec", "k", "v1"))
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			meta, err := unmarshalObjectMeta(v)
			require.NoError(t, err)
			require.Equal(t, uint64(42), meta.RingVersion)
			require.Equal(t, uint8(1), meta.ECData)
			require.Equal(t, uint8(0), meta.ECParity)
			require.Equal(t, []string{"node-a"}, meta.NodeIDs)
			require.Equal(t, "group-2", meta.PlacementGroupID)
			return nil
		})
	}))
}

// TestListAllObjects_PreservesTags asserts the snapshot read path copies
// meta.Tags into SnapshotObject.Tags. Pre-fix, ListAllObjects's literal at
// snapshotable.go:60 omitted Tags, which made the RestoreObjects Tags-forward
// fix dead code: a full snapshot+restore round-trip would lose tags because
// snap.Tags was always nil on the read side. End-to-end round-trip guard.
func TestListAllObjects_PreservesTags(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "tagrt"))

	obj, err := b.PutObject(context.Background(), "tagrt", "doc.txt", strings.NewReader("v1"), "text/plain")
	require.NoError(t, err)

	tags := []storage.Tag{
		{Key: "env", Value: "prod"},
		{Key: "team", Value: "storage"},
	}
	require.NoError(t, b.SetObjectTags("tagrt", "doc.txt", obj.VersionID, tags))

	snap, err := b.ListAllObjects()
	require.NoError(t, err)
	require.NotEmpty(t, snap, "expected at least one snapshot object")

	var found *storage.SnapshotObject
	for i := range snap {
		if snap[i].VersionID == obj.VersionID {
			found = &snap[i]
			break
		}
	}
	require.NotNil(t, found, "snapshot must include the tagged version")
	require.Equal(t, tags, found.Tags, "ListAllObjects must populate SnapshotObject.Tags from meta.Tags")
}

// TestRestoreObjects_PreservesTags asserts the snapshot restore propose path
// forwards SnapshotObject.Tags into PutObjectMetaCmd. Pre-fix the literal at
// snapshotable.go:199 omitted Tags, which applyPutObjectMeta then wrote as
// empty, clobbering the snapshot's tag history.
func TestRestoreObjects_PreservesTags(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "tagsnap"))
	createBlob(t, b, "tagsnap", "doc.bin", "v1")

	snapTags := []storage.Tag{
		{Key: "env", Value: "prod"},
		{Key: "team", Value: "storage"},
	}

	count, stale, err := b.RestoreObjects([]storage.SnapshotObject{{
		Bucket:      "tagsnap",
		Key:         "doc.bin",
		ETag:        "etag-tag",
		Size:        4,
		ContentType: "application/octet-stream",
		Modified:    time.Now().UnixMilli(),
		VersionID:   "v1",
		IsLatest:    true,
		Tags:        snapTags,
	}})
	require.NoError(t, err)
	require.Empty(t, stale)
	require.Equal(t, 1, count)

	got, err := b.GetObjectTags("tagsnap", "doc.bin", "v1")
	require.NoError(t, err)
	require.Equal(t, snapTags, got, "snapshot Tags must round-trip through restore propose")
}

func TestRestoreObjects_RemovesExtraObjects(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "extra"))

	// Put two objects that are NOT in the snapshot.
	for i, k := range []string{"extra1.txt", "extra2.txt"} {
		vid := strings.Repeat("x", i+1)
		createBlob(t, b, "extra", k, vid)
		require.NoError(t, b.putMeta("extra", k, vid, "etag", 3, "text/plain"))
	}

	// Restore with empty snapshot → both objects should be deleted.
	count, stale, err := b.RestoreObjects(nil)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
	assert.Empty(t, stale)

	objs, err := b.ListAllObjects()
	require.NoError(t, err)
	assert.Empty(t, objs, "all objects outside snapshot should be gone")
}

func TestRestoreObjects_PreservesVersionHistoryAndDeleteMarkers(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "hist"))

	v1, err := b.PutObject(context.Background(), "hist", "doc.txt", strings.NewReader("v1"), "text/plain")
	require.NoError(t, err)
	v2, err := b.PutObject(context.Background(), "hist", "doc.txt", strings.NewReader("v2"), "text/plain")
	require.NoError(t, err)
	markerID, err := b.DeleteObjectReturningMarker("hist", "doc.txt")
	require.NoError(t, err)

	snap, err := b.ListAllObjects()
	require.NoError(t, err)

	v3, err := b.PutObject(context.Background(), "hist", "doc.txt", strings.NewReader("v3"), "text/plain")
	require.NoError(t, err)
	require.NoError(t, b.RestoreBuckets([]storage.SnapshotBucket{{Name: "hist", VersioningState: "Enabled"}}))
	count, stale, err := b.RestoreObjects(snap)
	require.NoError(t, err)
	require.Empty(t, stale)
	require.Equal(t, len(snap), count)

	versions, err := b.ListObjectVersions("hist", "doc.txt", 10)
	require.NoError(t, err)
	ids := make(map[string]*storage.ObjectVersion)
	for _, v := range versions {
		ids[v.VersionID] = v
	}
	require.Contains(t, ids, v1.VersionID)
	require.Contains(t, ids, v2.VersionID)
	require.Contains(t, ids, markerID)
	require.NotContains(t, ids, v3.VersionID)
	require.True(t, ids[markerID].IsLatest)
	require.True(t, ids[markerID].IsDeleteMarker)
}

func TestBlobExists_VersionedPath(t *testing.T) {
	b := newTestDistributedBackend(t)

	vid := "v-abc123"
	createBlob(t, b, "bkt", "file.dat", vid)

	assert.True(t, b.blobExists("bkt", "file.dat", vid))
	assert.False(t, b.blobExists("bkt", "file.dat", "v-nonexistent"))
}

func TestBlobExists_ResolvesEmptyVersionID(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "res"))

	vid := "v-resolved"
	createBlob(t, b, "res", "obj.bin", vid)
	require.NoError(t, b.putMeta("res", "obj.bin", vid, "etag", 7, "application/octet-stream"))

	// blobExists with empty versionID should resolve from the lat: pointer.
	assert.True(t, b.blobExists("res", "obj.bin", ""))
}

func TestBlobExists_LegacyUnversionedPath(t *testing.T) {
	b := newTestDistributedBackend(t)

	// Create a legacy file at {root}/data/{bucket}/{key}.
	legacyPath := filepath.Join(b.root, "data", "bkt2", "legacy.dat")
	require.NoError(t, os.MkdirAll(filepath.Dir(legacyPath), 0o755))
	require.NoError(t, os.WriteFile(legacyPath, []byte("legacy"), 0o644))

	// versionID="" with no lat: pointer → legacy unversioned path should match.
	assert.True(t, b.blobExists("bkt2", "legacy.dat", ""))
}

// --- helpers ---

func createBlob(t *testing.T, b *DistributedBackend, bucket, key, versionID string) {
	t.Helper()
	p := b.objectPathV(bucket, key, versionID)
	require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
	require.NoError(t, os.WriteFile(p, []byte("blob"), 0o644))
}

// putMeta proposes a CmdPutObjectMeta via the FSM.
func (b *DistributedBackend) putMeta(bucket, key, versionID, etag string, size int64, ct string) error {
	return b.propose(context.Background(), CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket:      bucket,
		Key:         key,
		VersionID:   versionID,
		ETag:        etag,
		Size:        size,
		ContentType: ct,
		ModTime:     time.Now().UnixMilli(),
	})
}
