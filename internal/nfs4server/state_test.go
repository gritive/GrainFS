package nfs4server

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStateManager_RootFH(t *testing.T) {
	sm := NewStateManager()
	rootFH := sm.RootFH()

	path, ok := sm.ResolveFH(rootFH)
	require.True(t, ok)
	assert.Equal(t, "/", path)
}

func TestStateManager_GetOrCreateFH(t *testing.T) {
	sm := NewStateManager()

	fh1 := sm.GetOrCreateFH("/dir/file.txt")
	fh2 := sm.GetOrCreateFH("/dir/file.txt")

	// Same path should return same FH
	assert.Equal(t, fh1, fh2)

	// Different path should return different FH
	fh3 := sm.GetOrCreateFH("/other.txt")
	assert.NotEqual(t, fh1, fh3)

	// Resolve back
	path, ok := sm.ResolveFH(fh1)
	require.True(t, ok)
	assert.Equal(t, "/dir/file.txt", path)
}

func TestStateManager_InvalidateForBucket(t *testing.T) {
	sm := NewStateManager()
	fh1 := sm.GetOrCreateFH("/b1/a.txt")
	fh2 := sm.GetOrCreateFH("/b1/b.txt")
	fh3 := sm.GetOrCreateFH("/b2/c.txt")
	sm.BindFHGeneration(fh1, "b1", 1)
	sm.BindFHGeneration(fh2, "b1", 1)
	sm.BindFHGeneration(fh3, "b2", 1)

	require.Equal(t, 2, sm.InvalidateForBucket("b1"))
	_, ok := sm.FHBinding(fh1)
	require.False(t, ok)
	_, ok = sm.FHBinding(fh2)
	require.False(t, ok)
	_, ok = sm.FHBinding(fh3)
	require.True(t, ok)
}

func TestStateManager_InvalidateFH(t *testing.T) {
	sm := NewStateManager()

	fh := sm.GetOrCreateFH("/delete-me.txt")
	sm.InvalidateFH("/delete-me.txt")

	_, ok := sm.ResolveFH(fh)
	assert.False(t, ok, "invalidated FH should not resolve")

	// New FH for recreated path should be different
	fh2 := sm.GetOrCreateFH("/delete-me.txt")
	assert.NotEqual(t, fh, fh2)
}

func TestStateManager_InvalidateKey_DropsFileMeta(t *testing.T) {
	sm := NewStateManager()
	sm.fileMeta.Store("dir/file.txt", nfsFileMeta{Mode: 0644})
	sm.InvalidateKey("dir/file.txt")
	_, ok := sm.fileMeta.Load("dir/file.txt")
	assert.False(t, ok, "fileMeta entry must be removed after InvalidateKey")
}

func TestStateManager_InvalidateKey_RefreshesParentDirMtime(t *testing.T) {
	sm := NewStateManager()
	sm.MarkDir("/dir")
	originalMtime := sm.DirMtime("/dir")
	// Sleep a microsecond so the new mtime can differ.
	time.Sleep(time.Microsecond)
	sm.InvalidateKey("dir/file.txt")
	updatedMtime := sm.DirMtime("/dir")
	assert.Greater(t, updatedMtime, originalMtime, "parent dir mtime must advance so cached READDIR is seen as stale")
}

func TestStateManager_InvalidateKey_RootParent(t *testing.T) {
	sm := NewStateManager()
	originalMtime := sm.DirMtime("/")
	time.Sleep(time.Microsecond)
	sm.InvalidateKey("file.txt")
	updatedMtime := sm.DirMtime("/")
	assert.Greater(t, updatedMtime, originalMtime, "root mtime must advance for top-level key")
}

func TestStateManager_InvalidateKey_PreservesFilehandles(t *testing.T) {
	sm := NewStateManager()
	fh := sm.GetOrCreateFH("/dir/file.txt")
	sm.InvalidateKey("dir/file.txt")
	resolved, ok := sm.ResolveFH(fh)
	require.True(t, ok, "fh mapping must survive an out-of-band invalidate (clients keep open handles)")
	assert.Equal(t, "/dir/file.txt", resolved)
}

func TestStateManager_InvalidateKey_UntrackedParentSkipped(t *testing.T) {
	sm := NewStateManager()
	// /dir is NOT marked. Without explicit MarkDir, InvalidateKey leaves it
	// unstored — we don't synthesize tracking for unseen directories.
	sm.InvalidateKey("dir/file.txt")
	assert.False(t, sm.IsDir("/dir"), "InvalidateKey must not synthesize untracked parent dirs")
}

func TestServer_Invalidate_OnlyInvalidatesRegisteredExport(t *testing.T) {
	// Server.Invalidate is the duck-typed cluster.CacheInvalidator entry.
	sm := NewStateManager()
	sm.fileMeta.Store(fileMetaCacheKey("exported", "file.txt"), nfsFileMeta{Mode: 0644})
	sm.fileMeta.Store(fileMetaCacheKey("other-bucket", "file.txt"), nfsFileMeta{Mode: 0644})
	srv := &Server{state: sm}
	srv.exports.Store(buildSnap(map[string]exportConfig{"exported": {generation: 1}}))

	srv.Invalidate("other-bucket", "file.txt")
	_, ok := sm.fileMeta.Load(fileMetaCacheKey("other-bucket", "file.txt"))
	assert.True(t, ok, "unregistered export invalidation must be ignored")

	srv.Invalidate("exported", "file.txt")
	_, ok = sm.fileMeta.Load(fileMetaCacheKey("exported", "file.txt"))
	assert.False(t, ok, "registered export invalidation must drop fileMeta")
}

func TestStateManager_SetClientID(t *testing.T) {
	sm := NewStateManager()

	var verifier [8]byte
	copy(verifier[:], "test1234")

	id := sm.SetClientID(verifier)
	assert.Greater(t, id, uint64(0))

	// Not confirmed yet
	v, ok2 := sm.clients.Load(id)
	assert.True(t, ok2)
	cs := v.(*ClientState)
	assert.False(t, cs.Confirmed.Load())

	// Confirm
	ok := sm.ConfirmClientID(id)
	assert.True(t, ok)

	v, _ = sm.clients.Load(id)
	cs = v.(*ClientState)
	assert.True(t, cs.Confirmed.Load())
}

func TestStateManager_ConfirmInvalidClientID(t *testing.T) {
	sm := NewStateManager()
	ok := sm.ConfirmClientID(99999)
	assert.False(t, ok)
}

func TestCompoundDispatcher_MultiOp(t *testing.T) {
	d := NewDispatcher(nil)

	req := &CompoundRequest{
		Ops: []Op{
			{OpCode: OpPutRootFH},
			{OpCode: OpGetFH},
		},
	}
	resp := &CompoundResponse{}

	d.Dispatch(req, resp)
	require.Len(t, resp.Results, 2)
	assert.Equal(t, NFS4_OK, resp.Results[0].Status)
	assert.Equal(t, NFS4_OK, resp.Results[1].Status)
}

func TestCompoundDispatcher_StopsOnError(t *testing.T) {
	d := NewDispatcher(nil)

	req := &CompoundRequest{
		Ops: []Op{
			{OpCode: OpGetFH}, // no current FH set → error
			{OpCode: OpPutRootFH},
		},
	}
	resp := &CompoundResponse{}

	d.Dispatch(req, resp)
	// Should stop after first op fails
	require.Len(t, resp.Results, 1)
	assert.Equal(t, NFS4ERR_BADHANDLE, resp.Results[0].Status)
}
