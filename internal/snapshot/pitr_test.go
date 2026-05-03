package snapshot_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/snapshot"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/storage/wal"
)

type pitrMockBackend struct {
	objects []storage.SnapshotObject
}

func (m *pitrMockBackend) ListAllObjects() ([]storage.SnapshotObject, error) {
	return m.objects, nil
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
	mgr, err := snapshot.NewManager(snapDir, backend, walDir)
	require.NoError(t, err)
	_, err = mgr.Create("base")
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
