package server

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/snapshot"
	"github.com/gritive/GrainFS/internal/storage"
)

func TestWithSnapshotManager_InjectedInstanceIsUsedAndNotReplaced(t *testing.T) {
	dir := t.TempDir()
	store := encrypt.NewKEKStore()
	require.NoError(t, store.Add(1, make([]byte, encrypt.KEKSize)))
	var cid [16]byte
	cid[0] = 0x5A

	backend, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })

	snap := storageSnapshotable(backend)
	require.NotNil(t, snap, "LocalBackend must implement Snapshotable for this test to be meaningful")

	mgr, err := snapshot.NewManager(filepath.Join(dir, "snapshots"), snap, store, cid)
	require.NoError(t, err)

	// Construct a Server with all conditions for self-construction present:
	// dataDir + Snapshotable + WithSnapshotKEK. Add WithSnapshotManager on top —
	// the injected instance must win; a second Manager over the same dir would
	// re-introduce the nextSeq two-writer collision.
	s := &Server{}
	WithDataDir(dir)(s)
	WithSnapshotKEK(store, cid)(s)
	WithSnapshotManager(mgr)(s)
	s.initSnapshotManager(ServerStorage{Snapshotable: snap})

	require.Same(t, mgr, s.SnapMgrForTest(),
		"injected Manager must be the exact instance the server uses; "+
			"a self-constructed second Manager would re-introduce the two-writer seq-collision bug")
}
