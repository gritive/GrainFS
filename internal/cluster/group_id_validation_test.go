package cluster

import (
	"context"
	"errors"
	"testing"

	"github.com/gritive/GrainFS/internal/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMetaFSM_RejectsReservedGroupID: codex P0 #2 — applyPutShardGroup must
// not let a reserved name slip into shardGroups state. Apply runs on log
// replay too, so we WARN+SKIP rather than error (an old log entry containing
// a name that became reserved later must not crash startup). The state
// table stays clean either way.
func TestMetaFSM_RejectsReservedGroupID(t *testing.T) {
	cases := []string{"__meta__", "__sys", "__internal"}
	for _, id := range cases {
		t.Run("rejects "+id, func(t *testing.T) {
			f := NewMetaFSM()
			// applyCmd returns nil (warn-and-skip), but the group must not
			// appear in ShardGroups state.
			err := f.applyCmd(makePutShardGroupCmd(t, id, []string{"n0"}))
			require.NoError(t, err, "apply must not error on reserved ID (warn+skip)")

			for _, sg := range f.ShardGroups() {
				assert.NotEqual(t, id, sg.ID, "reserved group must not enter state")
			}
		})
	}
}

// TestMetaFSM_AllowsValidGroupID is a sanity check that the validation does
// not regress normal IDs.
func TestMetaFSM_AllowsValidGroupID(t *testing.T) {
	f := NewMetaFSM()
	require.NoError(t, f.applyCmd(makePutShardGroupCmd(t, "group-0", []string{"n0"})))
	require.Len(t, f.ShardGroups(), 1)
	assert.Equal(t, "group-0", f.ShardGroups()[0].ID)
}

// TestMetaRaft_ProposeShardGroup_RejectsReserved: proposers see a hard
// error so the user (or higher-level admin tool) gets immediate feedback,
// rather than a silent log-replay skip.
func TestMetaRaft_ProposeShardGroup_RejectsReserved(t *testing.T) {
	dir := t.TempDir()
	m, err := NewMetaRaft(MetaRaftConfig{NodeID: "n0", DataDir: dir})
	require.NoError(t, err)
	defer m.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // never reach raft propose; validation must short-circuit first.

	cases := []string{"", "__meta__", "__internal-x"}
	for _, id := range cases {
		t.Run("rejects "+id, func(t *testing.T) {
			err := m.ProposeShardGroup(ctx, ShardGroupEntry{ID: id, PeerIDs: []string{"n0"}})
			require.Error(t, err)
			if id != "" {
				// __-prefixed → ErrReservedGroupID. Empty → "groupID empty".
				assert.True(t, errors.Is(err, raft.ErrReservedGroupID),
					"expected ErrReservedGroupID for %q, got %v", id, err)
			} else {
				assert.Contains(t, err.Error(), "empty")
			}
		})
	}
}

// TestInstantiateLocalGroup_RejectsReservedID: lifecycle is the last line
// of defense before raft state is opened on disk. A reserved name here is
// fatal — the caller (serve.go) terminates the process on error.
func TestInstantiateLocalGroup_RejectsReservedID(t *testing.T) {
	cfg := GroupLifecycleConfig{
		NodeID:  "n0",
		DataDir: t.TempDir(),
	}
	cases := []string{"__meta__", "__sys"}
	for _, id := range cases {
		t.Run("rejects "+id, func(t *testing.T) {
			_, err := instantiateLocalGroup(cfg, ShardGroupEntry{ID: id, PeerIDs: []string{"n0"}})
			require.Error(t, err)
			assert.True(t, errors.Is(err, raft.ErrReservedGroupID),
				"expected ErrReservedGroupID, got %v", err)
		})
	}
}
