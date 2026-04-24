package nfs4server

import (
	"testing"

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

func TestStateManager_SetClientID(t *testing.T) {
	sm := NewStateManager()

	var verifier [8]byte
	copy(verifier[:], "test1234")

	id := sm.SetClientID(verifier)
	assert.Greater(t, id, uint64(0))

	// Not confirmed yet
	sm.clientMu.Lock()
	cs := sm.clients[id]
	sm.clientMu.Unlock()
	assert.False(t, cs.Confirmed)

	// Confirm
	ok := sm.ConfirmClientID(id)
	assert.True(t, ok)

	sm.clientMu.Lock()
	cs = sm.clients[id]
	sm.clientMu.Unlock()
	assert.True(t, cs.Confirmed)
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
