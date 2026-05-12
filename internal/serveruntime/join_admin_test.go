package serveruntime

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJoinHandler_isSelf(t *testing.T) {
	h := &JoinHandler{raftAddr: "127.0.0.1:8301"}
	assert.True(t, h.isSelf("127.0.0.1:8301"))
	assert.False(t, h.isSelf("127.0.0.1:8302"))
	assert.False(t, h.isSelf("192.168.1.1:8301"))
}

func TestWipeSoloRaftState(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "meta_raft", "data"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "raft", "raft-v2"), 0o755))

	require.NoError(t, wipeSoloRaftState(dir))

	_, err := os.Stat(filepath.Join(dir, "meta_raft"))
	assert.True(t, os.IsNotExist(err))
	_, err = os.Stat(filepath.Join(dir, "meta_raft.pre-join-backup"))
	assert.NoError(t, err)
}

func TestWipeSoloRaftState_Idempotent(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "meta_raft"), 0o755))

	require.NoError(t, wipeSoloRaftState(dir))
	require.NoError(t, wipeSoloRaftState(dir))
}
