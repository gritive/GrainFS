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

// TestMetaFSM_Restore_DropsReservedShardGroups: codex P0 — Restore must
// mirror applyPutShardGroup's warn-and-skip semantic, otherwise nodes that
// catch up via snapshot install would carry reserved IDs while peers that
// replay from log would not. That is silent quorum divergence.
//
// We bypass apply (which would itself filter) and inject the reserved ID
// directly into the FSM state, then take a snapshot, then restore into a
// fresh FSM and verify the reserved entry is NOT present in the restored
// state. This simulates the rolling-upgrade path where a pre-v0.0.19
// snapshot contains a reserved ID.
func TestMetaFSM_Restore_DropsReservedShardGroups(t *testing.T) {
	f := NewMetaFSM()
	// Direct injection — same-package test bypasses apply validation. This
	// is exactly the situation a pre-v0.0.19 snapshot encodes.
	f.shardGroups["__meta__"] = ShardGroupEntry{ID: "__meta__", PeerIDs: []string{"n0"}}
	f.shardGroups["__legacy"] = ShardGroupEntry{ID: "__legacy", PeerIDs: []string{"n0"}}
	f.shardGroups["group-0"] = ShardGroupEntry{ID: "group-0", PeerIDs: []string{"n0"}}

	snap, err := f.Snapshot()
	require.NoError(t, err)

	f2 := NewMetaFSM()
	require.NoError(t, f2.Restore(snap))

	// Reserved IDs dropped.
	got := f2.ShardGroups()
	gotIDs := make(map[string]bool, len(got))
	for _, sg := range got {
		gotIDs[sg.ID] = true
	}
	assert.False(t, gotIDs["__meta__"], "Restore must drop __meta__ from pre-v0.0.19 snapshot")
	assert.False(t, gotIDs["__legacy"], "Restore must drop __-prefixed IDs")
	assert.True(t, gotIDs["group-0"], "Restore must keep valid IDs")
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
