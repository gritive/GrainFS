package snapshot_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/snapshot"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/storage/wal"
)

// newPITRSealer builds a DEK sealer for the encrypted PITR WAL. NewDEKKeeper
// randomizes the DEK, so the SAME sealer instance must seal (OpenEncrypted) and
// replay (PITRRestore) — the random-DEK wall. Production restores the keeper from
// the FSM-persisted KEK-sealed DEK (LoadFromFSM).
func newPITRSealer(t *testing.T) wal.RecordSealer {
	t.Helper()
	clusterID := bytes.Repeat([]byte("c"), 16)
	keeper, err := encrypt.NewDEKKeeper(make([]byte, encrypt.KEKSize), clusterID)
	require.NoError(t, err)
	return storage.NewDEKKeeperAdapter(keeper, clusterID)
}

type pitrMockBackend struct {
	objects []storage.SnapshotObject
	buckets []storage.SnapshotBucket
}

func (m *pitrMockBackend) ListAllObjects() ([]storage.SnapshotObject, error) {
	return m.objects, nil
}

func (m *pitrMockBackend) ListAllBuckets() ([]storage.SnapshotBucket, error) {
	return m.buckets, nil
}

func (m *pitrMockBackend) RestoreBuckets(buckets []storage.SnapshotBucket) error {
	m.buckets = buckets
	return nil
}

func (m *pitrMockBackend) RestoreObjects(objects []storage.SnapshotObject) (int, []storage.StaleBlob, error) {
	m.objects = objects
	return len(objects), nil, nil
}

func TestPITRRestore_ReplaysVersionHistoryAndDeleteMarker(t *testing.T) {
	snapDir := t.TempDir()
	walDir := t.TempDir()
	backend := &pitrMockBackend{objects: []storage.SnapshotObject{{
		Bucket:    "b",
		Key:       "k",
		ETag:      "etag-v1",
		Size:      2,
		VersionID: "v1",
		IsLatest:  true,
	}}}
	mgr := snapshot.NewTestManager(t, snapDir, backend, walDir)
	_, err := mgr.Create("base")
	require.NoError(t, err)

	w, err := wal.Open(walDir)
	require.NoError(t, err)
	w.AppendAsync(wal.Entry{Op: wal.OpPut, Bucket: "b", Key: "k", ETag: "etag-v2", Size: 2, VersionID: "v2"})
	w.AppendAsync(wal.Entry{Op: wal.OpDelete, Bucket: "b", Key: "k", VersionID: "del-v3"})
	require.NoError(t, w.Close())

	_, err = mgr.PITRRestore(time.Now().Add(time.Second))
	require.NoError(t, err)

	byVersion := map[string]storage.SnapshotObject{}
	for _, o := range backend.objects {
		byVersion[o.VersionID] = o
	}
	require.Len(t, byVersion, 3)
	require.False(t, byVersion["v1"].IsLatest)
	require.False(t, byVersion["v2"].IsLatest)
	require.True(t, byVersion["del-v3"].IsLatest)
	require.True(t, byVersion["del-v3"].IsDeleteMarker)
}

// TestPITRRestore_ReplaysEncryptedWAL is the regression for the P1 silent-loss
// bug: prod seals the PITR WAL with a DEK sealer, but PITRRestore replayed it as
// plaintext and silently dropped every post-snapshot mutation. With a sealer the
// mutation is replayed; without one the restore fails closed (never silent-drops).
func TestPITRRestore_ReplaysEncryptedWAL(t *testing.T) {
	snapDir := t.TempDir()
	walDir := t.TempDir()
	backend := &pitrMockBackend{objects: []storage.SnapshotObject{{
		Bucket: "b", Key: "k", ETag: "etag-v1", Size: 2, VersionID: "v1", IsLatest: true,
	}}}
	mgr := snapshot.NewTestManager(t, snapDir, backend, walDir)
	sealer := newPITRSealer(t)

	_, err := mgr.Create("base")
	require.NoError(t, err)

	// Post-snapshot mutation written to an ENCRYPTED WAL (as production does).
	w, err := wal.OpenEncrypted(walDir, sealer, wal.PITRWALNamespace)
	require.NoError(t, err)
	w.AppendAsync(wal.Entry{Op: wal.OpPut, Bucket: "b", Key: "k", ETag: "etag-v2", Size: 2, VersionID: "v2"})
	require.NoError(t, w.Close())

	// No sealer: must fail CLOSED (not silently return only the base snapshot).
	_, err = mgr.PITRRestore(time.Now().Add(time.Second))
	require.ErrorIs(t, err, wal.ErrEncryptedWALNeedsSealer)

	// With the sealer: the post-snapshot mutation is replayed.
	mgr.SetPITRWALSealer(sealer)
	_, err = mgr.PITRRestore(time.Now().Add(time.Second))
	require.NoError(t, err)
	byVersion := map[string]storage.SnapshotObject{}
	for _, o := range backend.objects {
		byVersion[o.VersionID] = o
	}
	require.Contains(t, byVersion, "v2", "post-snapshot mutation must survive restore")
	require.Equal(t, "etag-v2", byVersion["v2"].ETag)
}

func TestPITRRestore_DropsDeletedHistoryForUnversionedBuckets(t *testing.T) {
	snapDir := t.TempDir()
	backend := &pitrMockBackend{
		buckets: []storage.SnapshotBucket{{Name: "b", VersioningState: ""}},
		objects: []storage.SnapshotObject{
			{
				Bucket:    "b",
				Key:       "ready",
				ETag:      "etag-v1",
				Size:      5,
				VersionID: "v1",
				IsLatest:  false,
			},
			{
				Bucket:         "b",
				Key:            "ready",
				ETag:           "DEL",
				VersionID:      "del-v2",
				IsLatest:       true,
				IsDeleteMarker: true,
			},
		},
	}
	mgr := snapshot.NewTestManager(t, snapDir, backend, "")
	_, err := mgr.Create("base")
	require.NoError(t, err)

	_, err = mgr.PITRRestore(time.Now().Add(time.Second))
	require.NoError(t, err)
	require.Empty(t, backend.objects, "unversioned deleted objects must not restore stale non-latest history")
}
