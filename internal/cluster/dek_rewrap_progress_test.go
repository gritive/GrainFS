package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func mustProgress(t *testing.T, nodeID string, gen uint32) []byte {
	t.Helper()
	data, err := encodeMetaDEKRewrapProgressCmd(nodeID, gen)
	require.NoError(t, err)
	return data
}

func TestDEKRewrapProgressCmd_RoundTrip(t *testing.T) {
	data, err := encodeMetaDEKRewrapProgressCmd("node-7", 42)
	require.NoError(t, err)
	nodeID, gen, err := decodeMetaDEKRewrapProgressCmd(data)
	require.NoError(t, err)
	require.Equal(t, "node-7", nodeID)
	require.Equal(t, uint32(42), gen)
}

func TestDEKRewrapProgress_Decode_RejectsEmpty(t *testing.T) {
	_, _, err := decodeMetaDEKRewrapProgressCmd(nil)
	require.Error(t, err)
}

func TestDEKRewrapProgress_ApplyAccumulates_AndPredicate(t *testing.T) {
	f := &MetaFSM{}
	require.NoError(t, f.applyDEKRewrapProgress(mustProgress(t, "A", 5)))
	require.NoError(t, f.applyDEKRewrapProgress(mustProgress(t, "B", 5)))

	require.True(t, f.IsGenFullyRewrapped(5, []string{"A", "B"}))
	require.False(t, f.IsGenFullyRewrapped(5, []string{"A", "B", "C"}))
	require.False(t, f.IsGenFullyRewrapped(7, []string{"A"}))
	require.False(t, f.IsGenFullyRewrapped(5, nil))
}

func TestDEKRewrapProgress_VoterSetShrink_ReEvaluates(t *testing.T) {
	f := &MetaFSM{}
	require.NoError(t, f.applyDEKRewrapProgress(mustProgress(t, "A", 3)))
	require.NoError(t, f.applyDEKRewrapProgress(mustProgress(t, "B", 3)))
	require.True(t, f.IsGenFullyRewrapped(3, []string{"A", "B"}))
	require.False(t, f.IsGenFullyRewrapped(3, []string{"A", "B", "C"}))
	require.True(t, f.IsGenFullyRewrapped(3, []string{"A"}))
}

func TestDEKRewrapProgress_Apply_RejectsEmptyNodeID(t *testing.T) {
	f := &MetaFSM{}
	require.Error(t, f.applyDEKRewrapProgress(mustProgress(t, "", 1)))
}

func TestDEKRewrapDone_ConcurrentApplyAndRead(t *testing.T) {
	f, _ := newTestMetaFSMWithKEKAndDEK(t)
	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			data, _ := encodeMetaDEKRewrapProgressCmd("node-A", 1)
			_ = f.applyDEKRewrapProgress(data)
		}
		close(done)
	}()
	for i := 0; i < 1000; i++ {
		_ = f.IsGenFullyRewrapped(1, []string{"node-A"})
	}
	<-done
}
