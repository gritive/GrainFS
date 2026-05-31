package cluster

import (
	"bytes"
	"testing"

	"github.com/gritive/GrainFS/internal/raft"
	"github.com/stretchr/testify/require"
)

// TestDKVSTrailer_RewrapDone tests that dekRewrapDone is preserved
// through Snapshot/Restore round-trip, and that pre-S6d snapshots without the
// rewrap_done field decode cleanly to an empty set (backward compat).
func TestDKVSTrailer_RewrapDone(t *testing.T) {
	t.Run("compat-absent-field", func(t *testing.T) {
		// Encode a DKVS payload with nil rewrapDone (wire-identical to a pre-S6d
		// snapshot where the field is absent: Offset(12)==0 → RewrapDoneLength()==0).
		// Decoding must return empty (nil) without panic — the backward-compat
		// contract for all additive trailer fields.
		versions := map[uint32][]byte{1: bytes.Repeat([]byte{0x01}, 60)}
		payload, err := encodeMetaDEKVersionSnapshot(versions, 1, nil, 0, nil)
		require.NoError(t, err)
		_, _, _, _, done, err := decodeMetaDEKVersionSnapshot(payload)
		require.NoError(t, err)
		require.Empty(t, done, "absent rewrap_done field must decode to empty set")
	})

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
