package cluster

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage/wal"
)

// TestWAL_WrapDistributedBackend_PutRecordsVersionID verifies that wrapping a
// DistributedBackend with the WAL backend records Put entries whose VersionID
// matches the version assigned by the FSM.
func TestWAL_WrapDistributedBackend_PutRecordsVersionID(t *testing.T) {
	dist := newTestDistributedBackend(t)
	require.NoError(t, dist.CreateBucket(context.Background(), "vbucket"))

	walDir := t.TempDir()
	w, err := wal.Open(walDir)
	require.NoError(t, err)
	backend := wal.NewBackend(dist, w)

	obj1, err := backend.PutObject(context.Background(), "vbucket", "k", strings.NewReader("v1"), "text/plain")
	require.NoError(t, err)
	require.NotEmpty(t, obj1.VersionID, "DistributedBackend must assign a VersionID")

	obj2, err := backend.PutObject(context.Background(), "vbucket", "k", strings.NewReader("v2-longer"), "text/plain")
	require.NoError(t, err)
	require.NotEmpty(t, obj2.VersionID)
	require.NotEqual(t, obj1.VersionID, obj2.VersionID)

	require.NoError(t, w.Flush())
	require.NoError(t, w.Close())

	var entries []wal.Entry
	n, err := wal.Replay(walDir, 0, time.Now().Add(time.Second), func(e wal.Entry) {
		entries = append(entries, e)
	})
	require.NoError(t, err)
	require.Equal(t, 2, n, "both PUTs must produce WAL entries")
	assert.Equal(t, wal.OpPut, entries[0].Op)
	assert.Equal(t, obj1.VersionID, entries[0].VersionID, "WAL entry 1 carries FSM-assigned VersionID")
	assert.Equal(t, wal.OpPut, entries[1].Op)
	assert.Equal(t, obj2.VersionID, entries[1].VersionID, "WAL entry 2 carries FSM-assigned VersionID")
}

// TestWAL_WrapDistributedBackend_DeleteObjectVersion verifies that hard-version
// delete flows through the WAL wrapper and appears as OpDeleteVersion.
func TestWAL_WrapDistributedBackend_DeleteObjectVersion(t *testing.T) {
	dist := newTestDistributedBackend(t)
	require.NoError(t, dist.CreateBucket(context.Background(), "vbucket"))

	walDir := t.TempDir()
	w, err := wal.Open(walDir)
	require.NoError(t, err)
	backend := wal.NewBackend(dist, w)

	obj, err := backend.PutObject(context.Background(), "vbucket", "k", strings.NewReader("v1"), "text/plain")
	require.NoError(t, err)

	// Hard-delete this specific version via the WAL wrapper.
	require.NoError(t, backend.DeleteObjectVersion("vbucket", "k", obj.VersionID))

	require.NoError(t, w.Flush())
	require.NoError(t, w.Close())

	var entries []wal.Entry
	_, err = wal.Replay(walDir, 0, time.Now().Add(time.Second), func(e wal.Entry) {
		entries = append(entries, e)
	})
	require.NoError(t, err)
	require.Len(t, entries, 2, "Put + DeleteVersion")
	assert.Equal(t, wal.OpPut, entries[0].Op)
	assert.Equal(t, wal.OpDeleteVersion, entries[1].Op)
	assert.Equal(t, obj.VersionID, entries[1].VersionID)
}

func TestWAL_WrapDistributedBackend_DeleteObjectRecordsMarkerVersionID(t *testing.T) {
	dist := newTestDistributedBackend(t)
	require.NoError(t, dist.CreateBucket(context.Background(), "vbucket"))

	walDir := t.TempDir()
	w, err := wal.Open(walDir)
	require.NoError(t, err)
	backend := wal.NewBackend(dist, w)

	_, err = backend.PutObject(context.Background(), "vbucket", "k", strings.NewReader("v1"), "text/plain")
	require.NoError(t, err)
	markerID, err := backend.DeleteObjectReturningMarker("vbucket", "k")
	require.NoError(t, err)
	require.NotEmpty(t, markerID)

	require.NoError(t, w.Flush())
	require.NoError(t, w.Close())

	var entries []wal.Entry
	_, err = wal.Replay(walDir, 0, time.Now().Add(time.Second), func(e wal.Entry) {
		entries = append(entries, e)
	})
	require.NoError(t, err)
	require.Len(t, entries, 2, "Put + delete marker")
	assert.Equal(t, wal.OpDelete, entries[1].Op)
	assert.Equal(t, markerID, entries[1].VersionID)
}

// TestWAL_WrapDistributedBackend_ReplayProducesSameState verifies end-to-end
// that the WAL captures enough to rebuild the logical object state: PUT two
// versions, replay the WAL, and confirm the final "latest" state matches what
// DistributedBackend holds.
func TestWAL_WrapDistributedBackend_ReplayProducesSameState(t *testing.T) {
	dist := newTestDistributedBackend(t)
	require.NoError(t, dist.CreateBucket(context.Background(), "b"))

	walDir := t.TempDir()
	w, err := wal.Open(walDir)
	require.NoError(t, err)
	backend := wal.NewBackend(dist, w)

	_, err = backend.PutObject(context.Background(), "b", "a", strings.NewReader("A"), "text/plain")
	require.NoError(t, err)
	_, err = backend.PutObject(context.Background(), "b", "b", strings.NewReader("BB"), "text/plain")
	require.NoError(t, err)
	_, err = backend.PutObject(context.Background(), "b", "a", strings.NewReader("A-v2"), "text/plain")
	require.NoError(t, err)

	require.NoError(t, w.Flush())
	require.NoError(t, w.Close())

	// Replay WAL to reconstruct latest-state map.
	latest := map[string]wal.Entry{}
	_, err = wal.Replay(walDir, 0, time.Now().Add(time.Second), func(e wal.Entry) {
		switch e.Op {
		case wal.OpPut:
			latest[e.Bucket+"/"+e.Key] = e
		case wal.OpDelete, wal.OpDeleteVersion:
			delete(latest, e.Bucket+"/"+e.Key)
		}
	})
	require.NoError(t, err)

	// Sanity: we wrote 3 entries, resolved state has 2 keys.
	require.Len(t, latest, 2)

	// Each replayed key has the newest PUT's VersionID, ETag, and size.
	rcA, objA, err := dist.GetObject(context.Background(), "b", "a")
	require.NoError(t, err)
	defer rcA.Close()
	_, _ = io.ReadAll(rcA)
	replayed := latest["b/a"]
	assert.Equal(t, objA.VersionID, replayed.VersionID)
	assert.Equal(t, objA.Size, replayed.Size)
}
