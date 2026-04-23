package snapshot_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/snapshot"
	"github.com/gritive/GrainFS/internal/storage"
)

// mockBucketSnapshotable extends mockSnapshotable with BucketSnapshotable.
type mockBucketSnapshotable struct {
	objects []storage.SnapshotObject
	buckets []storage.SnapshotBucket
}

func (m *mockBucketSnapshotable) ListAllObjects() ([]storage.SnapshotObject, error) {
	return m.objects, nil
}

func (m *mockBucketSnapshotable) RestoreObjects(objects []storage.SnapshotObject) (int, []storage.StaleBlob, error) {
	m.objects = objects
	return len(objects), nil, nil
}

func (m *mockBucketSnapshotable) ListAllBuckets() ([]storage.SnapshotBucket, error) {
	return m.buckets, nil
}

func (m *mockBucketSnapshotable) RestoreBuckets(buckets []storage.SnapshotBucket) error {
	m.buckets = buckets
	return nil
}

// TestSnapshot_PreservesBucketMeta verifies that BucketSnapshotable backends
// have their bucket state (versioning) captured at Create and replayed at
// Restore.
func TestSnapshot_PreservesBucketMeta(t *testing.T) {
	dir := t.TempDir()
	backend := &mockBucketSnapshotable{
		buckets: []storage.SnapshotBucket{
			{Name: "b1", VersioningState: "Enabled"},
			{Name: "b2", VersioningState: "Suspended"},
		},
	}
	mgr, err := snapshot.NewManager(dir, backend, "")
	require.NoError(t, err)

	snap, err := mgr.Create("test")
	require.NoError(t, err)
	require.Len(t, snap.BucketMeta, 2, "Snapshot must capture bucket metadata")

	// Simulate a wipe: clear backend's bucket state.
	backend.buckets = nil

	_, _, err = mgr.Restore(snap.Seq)
	require.NoError(t, err)
	require.Len(t, backend.buckets, 2, "Restore must replay bucket metadata")

	assert.Equal(t, "b1", backend.buckets[0].Name)
	assert.Equal(t, "Enabled", backend.buckets[0].VersioningState)
	assert.Equal(t, "Suspended", backend.buckets[1].VersioningState)
}

// TestSnapshot_OldFormat_BackwardCompat verifies that snapshots without
// BucketMeta (older format) restore cleanly without touching bucket state.
func TestSnapshot_OldFormat_BackwardCompat(t *testing.T) {
	dir := t.TempDir()
	backend := &mockBucketSnapshotable{
		buckets: []storage.SnapshotBucket{
			{Name: "pre-existing", VersioningState: "Enabled"},
		},
	}
	mgr, err := snapshot.NewManager(dir, backend, "")
	require.NoError(t, err)

	snap, err := mgr.Create("test")
	require.NoError(t, err)

	// Simulate an old-format snapshot by nil-ing BucketMeta on disk.
	snap.BucketMeta = nil
	require.NoError(t, snapshot.WriteSnapshotForTest(mgr, snap))

	// Change backend state to confirm Restore is a no-op for bucket meta.
	backend.buckets = []storage.SnapshotBucket{
		{Name: "other", VersioningState: "Suspended"},
	}

	_, _, err = mgr.Restore(snap.Seq)
	require.NoError(t, err)

	// Backend bucket state must be untouched (old format has no BucketMeta).
	require.Len(t, backend.buckets, 1)
	assert.Equal(t, "other", backend.buckets[0].Name)
	assert.Equal(t, "Suspended", backend.buckets[0].VersioningState)
}
