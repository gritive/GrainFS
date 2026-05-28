package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
)

// TestRestore_ClusterKeyDropped_FiresCallback proves the persist+restore+fire
// path for the zero-CA cutover drop bit (spec §8 H3). PR-1 has NO command that
// sets the FSM bit; this test forces the field directly to exercise the
// snapshot encode → restore decode → SetOnClusterKeyDropped fire path.
func TestRestore_ClusterKeyDropped_FiresCallback(t *testing.T) {
	src := NewMetaFSM()
	wireSnapshotKEK(t, src)
	src.clusterKeyDropped = true // forced state (PR-1 has no command that sets it)

	snap, err := src.Snapshot()
	require.NoError(t, err)

	dst := NewMetaFSM()
	wireSnapshotKEK(t, dst)
	dropped := false
	dst.SetOnClusterKeyDropped(func() { dropped = true })
	require.NoError(t, dst.Restore(raft.SnapshotMeta{}, snap))
	require.True(t, dropped, "Restore of dropped=true snapshot fires SetOnClusterKeyDropped")
	require.True(t, dst.clusterKeyDropped, "field restored")
}

// TestRestore_NotDropped_DoesNotFire locks the PR-1 default: a snapshot whose
// cluster_key_dropped is false must NOT fire the boot callback.
func TestRestore_NotDropped_DoesNotFire(t *testing.T) {
	src := NewMetaFSM()
	wireSnapshotKEK(t, src)
	snap, err := src.Snapshot()
	require.NoError(t, err)

	dst := NewMetaFSM()
	wireSnapshotKEK(t, dst)
	dropped := false
	dst.SetOnClusterKeyDropped(func() { dropped = true })
	require.NoError(t, dst.Restore(raft.SnapshotMeta{}, snap))
	require.False(t, dropped, "PR-1 default false must not fire")
}
