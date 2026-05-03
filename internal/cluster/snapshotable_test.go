package cluster

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

func TestListAllObjects_EmptyBucket(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket("empty"))

	objs, err := b.ListAllObjects()
	require.NoError(t, err)
	assert.Empty(t, objs)
}

func TestListAllObjects_PreservesVersionHistoryAndDeleteMarkers(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket("tomb"))

	v1, err := b.PutObject("tomb", "doc.txt", strings.NewReader("v1"), "text/plain")
	require.NoError(t, err)
	v2, err := b.PutObject("tomb", "doc.txt", strings.NewReader("v2"), "text/plain")
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
	require.NoError(t, b.CreateBucket("stale"))

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

func TestRestoreObjects_RemovesExtraObjects(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket("extra"))

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
	require.NoError(t, b.CreateBucket("hist"))

	v1, err := b.PutObject("hist", "doc.txt", strings.NewReader("v1"), "text/plain")
	require.NoError(t, err)
	v2, err := b.PutObject("hist", "doc.txt", strings.NewReader("v2"), "text/plain")
	require.NoError(t, err)
	markerID, err := b.DeleteObjectReturningMarker("hist", "doc.txt")
	require.NoError(t, err)

	snap, err := b.ListAllObjects()
	require.NoError(t, err)

	v3, err := b.PutObject("hist", "doc.txt", strings.NewReader("v3"), "text/plain")
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
	require.NoError(t, b.CreateBucket("res"))

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
