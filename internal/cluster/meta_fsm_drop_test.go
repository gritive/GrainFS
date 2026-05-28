package cluster

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func buildDropCmd(t *testing.T, voters []string, configIdx uint64) []byte {
	t.Helper()
	payload, err := encodeDropClusterKeyAcceptCmd(DropClusterKeyStamp{Voters: voters, ConfigIndex: configIdx})
	require.NoError(t, err)
	return payload
}

func TestApplyDropClusterKeyAccept_SetsDropped(t *testing.T) {
	fsm := NewMetaFSM()
	var callCount atomic.Int32
	fsm.SetOnClusterKeyDropped(func() { callCount.Add(1) })

	require.NoError(t, applyMetaCmdForTest(fsm, MetaCmdTypeDropClusterKeyAccept, buildDropCmd(t, []string{"node-A", "node-B"}, 5)))

	require.True(t, fsm.ClusterKeyDropped())
	require.Equal(t, int32(1), callCount.Load())
}

func TestApplyDropClusterKeyAccept_Idempotent(t *testing.T) {
	fsm := NewMetaFSM()
	var callCount atomic.Int32
	fsm.SetOnClusterKeyDropped(func() { callCount.Add(1) })

	cmd := buildDropCmd(t, []string{"node-A", "node-B"}, 5)
	require.NoError(t, applyMetaCmdForTest(fsm, MetaCmdTypeDropClusterKeyAccept, cmd))
	require.NoError(t, applyMetaCmdForTest(fsm, MetaCmdTypeDropClusterKeyAccept, cmd))

	require.True(t, fsm.ClusterKeyDropped())
	require.Equal(t, int32(1), callCount.Load())
}

func TestApplyDropClusterKeyAccept_DoesNotConsultLiveConfig(t *testing.T) {
	fsm := NewMetaFSM()
	var called atomic.Bool
	fsm.SetOnClusterKeyDropped(func() { called.Store(true) })

	require.NoError(t, applyMetaCmdForTest(fsm, MetaCmdTypeDropClusterKeyAccept, buildDropCmd(t, []string{"node-A", "node-B"}, 5)))

	require.True(t, fsm.ClusterKeyDropped())
	require.True(t, called.Load())
}
