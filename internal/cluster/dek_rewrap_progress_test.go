package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func mustProgress(t *testing.T, nodeID string, gen uint32) []byte {
	t.Helper()
	data, err := encodeMetaDEKRewrapProgressCmd(nodeID, gen, 0)
	require.NoError(t, err)
	return data
}

func TestDEKRewrapProgressCmd_RoundTrip(t *testing.T) {
	data, err := encodeMetaDEKRewrapProgressCmd("node-7", 42, 0)
	require.NoError(t, err)
	nodeID, gen, epoch, err := decodeMetaDEKRewrapProgressCmd(data)
	require.NoError(t, err)
	require.Equal(t, "node-7", nodeID)
	require.Equal(t, uint32(42), gen)
	require.Equal(t, uint32(0), epoch)
}

func TestDEKRewrapProgress_Decode_RejectsEmpty(t *testing.T) {
	_, _, _, err := decodeMetaDEKRewrapProgressCmd(nil)
	require.Error(t, err)
}

func TestDEKRewrapProgress_ApplyAccumulates_AndPredicate(t *testing.T) {
	f := &MetaFSM{}
	require.NoError(t, f.applyDEKRewrapProgress(mustProgress(t, "A", 5)))
	require.NoError(t, f.applyDEKRewrapProgress(mustProgress(t, "B", 5)))

	require.True(t, f.IsGenFullyRewrapped(5, []string{"A", "B"}, 0))
	require.False(t, f.IsGenFullyRewrapped(5, []string{"A", "B", "C"}, 0))
	require.False(t, f.IsGenFullyRewrapped(7, []string{"A"}, 0))
	require.False(t, f.IsGenFullyRewrapped(5, nil, 0))
}

func TestDEKRewrapProgress_VoterSetShrink_ReEvaluates(t *testing.T) {
	f := &MetaFSM{}
	require.NoError(t, f.applyDEKRewrapProgress(mustProgress(t, "A", 3)))
	require.NoError(t, f.applyDEKRewrapProgress(mustProgress(t, "B", 3)))
	require.True(t, f.IsGenFullyRewrapped(3, []string{"A", "B"}, 0))
	require.False(t, f.IsGenFullyRewrapped(3, []string{"A", "B", "C"}, 0))
	require.True(t, f.IsGenFullyRewrapped(3, []string{"A"}, 0))
}

func TestDEKRewrapProgress_Apply_RejectsEmptyNodeID(t *testing.T) {
	f := &MetaFSM{}
	require.Error(t, f.applyDEKRewrapProgress(mustProgress(t, "", 1)))
}

// mustProgressEpoch encodes a progress cmd with a non-zero epoch for Task 3 tests.
func mustProgressEpoch(t *testing.T, nodeID string, gen, epoch uint32) []byte {
	t.Helper()
	data, err := encodeMetaDEKRewrapProgressCmd(nodeID, gen, epoch)
	require.NoError(t, err)
	return data
}

// TestDEKRewrapProgress_MaxMonotonic verifies that a stale re-report (epoch lower
// than what was already recorded) does not decrease the stored epoch.
func TestDEKRewrapProgress_MaxMonotonic(t *testing.T) {
	f := &MetaFSM{}
	require.NoError(t, f.applyDEKRewrapProgress(mustProgressEpoch(t, "node-1", 2, 1)))
	require.NoError(t, f.applyDEKRewrapProgress(mustProgressEpoch(t, "node-1", 2, 0))) // stale re-report
	// epoch must remain 1 (max-monotonic; stale report must not lower it)
	f.mu.RLock()
	got := f.dekRewrapDone[2]["node-1"]
	f.mu.RUnlock()
	require.Equal(t, uint32(1), got, "epoch must remain at max (1), not lowered to 0 by stale re-report")
}

// TestDEKRewrapProgress_EpochAwareQuery verifies that IsGenFullyRewrapped checks
// the per-node epoch against requiredEpoch.
func TestDEKRewrapProgress_EpochAwareQuery(t *testing.T) {
	f := &MetaFSM{}
	// node-1 reported epoch 0; node-2 reported epoch 1.
	require.NoError(t, f.applyDEKRewrapProgress(mustProgressEpoch(t, "node-1", 3, 0)))
	require.NoError(t, f.applyDEKRewrapProgress(mustProgressEpoch(t, "node-2", 3, 1)))

	// Both nodes satisfy requiredEpoch 0.
	require.True(t, f.IsGenFullyRewrapped(3, []string{"node-1", "node-2"}, 0))
	// node-1 (epoch 0) does NOT satisfy requiredEpoch 1.
	require.False(t, f.IsGenFullyRewrapped(3, []string{"node-1", "node-2"}, 1))
	// node-2 alone satisfies requiredEpoch 1.
	require.True(t, f.IsGenFullyRewrapped(3, []string{"node-2"}, 1))
}

func TestDEKRewrapDone_ConcurrentApplyAndRead(t *testing.T) {
	f, _ := newTestMetaFSMWithKEKAndDEK(t)
	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			data, _ := encodeMetaDEKRewrapProgressCmd("node-A", 1, 0)
			_ = f.applyDEKRewrapProgress(data)
		}
		close(done)
	}()
	for i := 0; i < 1000; i++ {
		_ = f.IsGenFullyRewrapped(1, []string{"node-A"}, 0)
	}
	<-done
}
