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

func TestPITRRestore_ReplaysEncryptedWAL(t *testing.T) {
	snapDir := t.TempDir()
	walDir := t.TempDir()
	backend := &pitrMockBackend{}
	enc, err := encrypt.NewEncryptor(bytes.Repeat([]byte{0x77}, 32))
	require.NoError(t, err)

	mgr, err := snapshot.NewManagerWithEncryptor(snapDir, backend, walDir, enc)
	require.NoError(t, err)
	_, err = mgr.Create("base")
	require.NoError(t, err)

	w, err := wal.OpenEncrypted(walDir, storage.NewEncryptorAdapter(enc, make([]byte, 16)), "pitr-wal")
	require.NoError(t, err)
	w.AppendAsync(wal.Entry{Op: wal.OpPut, Bucket: "b", Key: "secret-key", ETag: "secret-etag", Size: 2, VersionID: "v1"})
	require.NoError(t, w.Close())

	_, err = mgr.PITRRestore(time.Now().Add(time.Second))
	require.NoError(t, err)
	require.Len(t, backend.objects, 1)
	require.Equal(t, "secret-key", backend.objects[0].Key)
	require.Equal(t, "secret-etag", backend.objects[0].ETag)
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
	mgr, err := snapshot.NewManager(snapDir, backend, "")
	require.NoError(t, err)
	_, err = mgr.Create("base")
	require.NoError(t, err)

	_, err = mgr.PITRRestore(time.Now().Add(time.Second))
	require.NoError(t, err)
	require.Empty(t, backend.objects, "unversioned deleted objects must not restore stale non-latest history")
}
