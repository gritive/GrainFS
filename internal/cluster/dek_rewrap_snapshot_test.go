package cluster

import (
	"testing"

	"github.com/gritive/GrainFS/internal/raft"
	"github.com/stretchr/testify/require"
)

// TestDKVSTrailer_RewrapDone tests that dekRewrapDone is preserved
// through Snapshot/Restore round-trip.
//
// RED (before implementation): Restore doesn't repopulate f.dekRewrapDone,
// so IsGenFullyRewrapped returns false after round-trip.
func TestDKVSTrailer_RewrapDone(t *testing.T) {
	t.Run("round-trip", func(t *testing.T) {
		// newTestMetaFSMWithKEKAndDEK seeds gen 1 — without DEK versions the
		// DKVS trailer is skipped entirely and rewrap_done would have nothing
		// to attach to.
		fsm, _ := newTestMetaFSMWithKEKAndDEK(t)

		// Record node-A and node-B as done for gen 1.
		require.NoError(t, fsm.applyDEKRewrapProgress(mustProgress(t, "node-A", 1)))
		require.NoError(t, fsm.applyDEKRewrapProgress(mustProgress(t, "node-B", 1)))

		// Sanity: predicate true before snapshot.
		require.True(t, fsm.IsGenFullyRewrapped(1, []string{"node-A", "node-B"}))

		// Snapshot the FSM.
		snapBytes, err := fsm.Snapshot()
		require.NoError(t, err)

		// Restore into a fresh FSM with the same KEK wiring so the envelope opens.
		fsm2 := NewMetaFSM()
		wireTestKEK(t, fsm2)
		require.NoError(t, fsm2.Restore(raft.SnapshotMeta{}, snapBytes))

		// Both nodes must be present → true.
		require.True(t, fsm2.IsGenFullyRewrapped(1, []string{"node-A", "node-B"}),
			"expected both nodes restored for gen 1")
		// Only A is not enough for {A,C}.
		require.False(t, fsm2.IsGenFullyRewrapped(1, []string{"node-A", "node-C"}),
			"node-C was never recorded, must return false")
		// Gen 2 was never recorded.
		require.False(t, fsm2.IsGenFullyRewrapped(2, []string{"node-A"}),
			"gen 2 was never recorded, must return false")
	})
}
