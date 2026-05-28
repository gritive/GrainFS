package cluster

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type staticDropConfigReader struct {
	voters []string
}

func (s staticDropConfigReader) EffectiveConfiguration() ([]string, uint64) {
	return append([]string(nil), s.voters...), 0
}

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
	fsm.SetRaftConfigReader(staticDropConfigReader{voters: []string{"node-A", "node-B"}})

	require.NoError(t, applyMetaCmdForTest(fsm, MetaCmdTypeDropClusterKeyAccept, buildDropCmd(t, []string{"node-A", "node-B"}, 5)))

	require.True(t, fsm.ClusterKeyDropped())
	require.Equal(t, int32(1), callCount.Load())
}

func TestApplyDropClusterKeyAccept_Idempotent(t *testing.T) {
	fsm := NewMetaFSM()
	var callCount atomic.Int32
	fsm.SetOnClusterKeyDropped(func() { callCount.Add(1) })
	fsm.SetRaftConfigReader(staticDropConfigReader{voters: []string{"node-A", "node-B"}})

	cmd := buildDropCmd(t, []string{"node-A", "node-B"}, 5)
	require.NoError(t, applyMetaCmdForTest(fsm, MetaCmdTypeDropClusterKeyAccept, cmd))
	require.NoError(t, applyMetaCmdForTest(fsm, MetaCmdTypeDropClusterKeyAccept, cmd))

	require.True(t, fsm.ClusterKeyDropped())
	require.Equal(t, int32(1), callCount.Load())
}

func TestApplyDropClusterKeyAccept_ConfigStampMismatchRejected(t *testing.T) {
	fsm := NewMetaFSM()
	var called atomic.Bool
	fsm.SetOnClusterKeyDropped(func() { called.Store(true) })
	fsm.SetRaftConfigReader(staticDropConfigReader{voters: []string{"node-A", "node-B", "node-C"}})

	require.NoError(t, applyMetaCmdForTest(fsm, MetaCmdTypeDropClusterKeyAccept, buildDropCmd(t, []string{"node-A", "node-B"}, 5)))

	require.False(t, fsm.ClusterKeyDropped())
	require.False(t, called.Load())
}

func TestApplyDropClusterKeyAccept_NoConfigReaderAccepts(t *testing.T) {
	fsm := NewMetaFSM()
	var called atomic.Bool
	fsm.SetOnClusterKeyDropped(func() { called.Store(true) })

	require.NoError(t, applyMetaCmdForTest(fsm, MetaCmdTypeDropClusterKeyAccept, buildDropCmd(t, []string{"node-A"}, 5)))

	require.True(t, fsm.ClusterKeyDropped())
	require.True(t, called.Load())
}
