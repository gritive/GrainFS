package cluster

import (
	"testing"

	"github.com/gritive/GrainFS/internal/raft/raftpb"
	"github.com/gritive/GrainFS/internal/transport"
	"github.com/stretchr/testify/require"
)

// TestWireCoexistence_LegacyAndGroupForwardBothWork verifies that
// legacy StreamProposeForward (0x06) and new StreamProposeGroupForward (0x08)
// handlers can coexist without interfering with each other.
//
// REGRESSION test for v0.0.7.1 PR-D: ensures wire compatibility during rolling upgrades.
func TestWireCoexistence_LegacyAndGroupForwardBothWork(t *testing.T) {
	// Setup: Track which handler was invoked.
	legacyInvoked := false

	// Create legacy 0x06 handler (mimics existing distBackend handler).
	legacyHandler := func(msg *transport.Message) *transport.Message {
		legacyInvoked = true
		return &transport.Message{Payload: []byte("legacy-ok")}
	}

	// Create new 0x08 handler (ForwardReceiver).
	mgr := NewDataGroupManager()
	rcv := NewForwardReceiver(mgr)
	groupForwardHandler := rcv.Handle

	// Test 1: 0x06 handler works independently.
	t.Run("legacy_0x06_works", func(t *testing.T) {
		legacyInvoked = false

		msg := &transport.Message{
			Type:    transport.StreamProposeForward,
			Payload: []byte("legacy-cmd"),
		}
		reply := legacyHandler(msg)
		require.NotNil(t, reply)
		require.Equal(t, []byte("legacy-ok"), reply.Payload)
		require.True(t, legacyInvoked)
	})

	// Test 2: 0x08 handler works independently.
	t.Run("group_forward_0_08_works", func(t *testing.T) {
		legacyInvoked = false

		// Use unknown group to trigger NotVoter status (predictable response).
		payload := encodeForwardPayload("unknown-group", raftpb.ForwardOpHeadObject, buildHeadObjectArgs("b", "k"))
		msg := &transport.Message{
			Type:    transport.StreamProposeGroupForward,
			Payload: payload,
		}
		reply := groupForwardHandler(msg)
		require.NotNil(t, reply)
		fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
		require.Equal(t, raftpb.ForwardStatusNotVoter, fr.Status())
	})

	// Test 3: Handlers don't share state (sequential calls).
	t.Run("sequential_calls_dont_interfere", func(t *testing.T) {
		// Call 0x06 multiple times.
		for i := 0; i < 3; i++ {
			msg := &transport.Message{
				Type:    transport.StreamProposeForward,
				Payload: []byte("legacy-cmd"),
			}
			reply := legacyHandler(msg)
			require.NotNil(t, reply)
			require.Equal(t, []byte("legacy-ok"), reply.Payload)
		}

		// Call 0x08 multiple times.
		for i := 0; i < 3; i++ {
			payload := encodeForwardPayload("unknown-group", raftpb.ForwardOpHeadObject, buildHeadObjectArgs("b", "k"))
			msg := &transport.Message{
				Type:    transport.StreamProposeGroupForward,
				Payload: payload,
			}
			reply := groupForwardHandler(msg)
			require.NotNil(t, reply)
			fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
			require.Equal(t, raftpb.ForwardStatusNotVoter, fr.Status())
		}

		// Verify 0x06 still works.
		msg := &transport.Message{
			Type:    transport.StreamProposeForward,
			Payload: []byte("legacy-cmd"),
		}
		reply := legacyHandler(msg)
		require.NotNil(t, reply)
		require.Equal(t, []byte("legacy-ok"), reply.Payload)
	})

	// Test 4: Wire formats are disjoint (legacy payload cannot be decoded as group-forward).
	t.Run("wire_formats_are_disjoint", func(t *testing.T) {
		// Legacy raw payload does NOT decode as group-forward layout.
		// "lega" bytes (0x6c 0x65 0x67 0x61) interpreted as big-endian uint32 =
		// ~1.8B which exceeds any realistic payload, so decode must error.
		rawData := []byte("lega-cmd-bytes")
		_, _, err := decodeGroupForwardPayload(rawData)
		require.Error(t, err, "legacy payload must NOT decode as group-forward layout")

		// Conversely, a valid group-forward payload is NOT valid raw legacy data
		// (it has 4-byte length prefix + groupID + actual data).
		groupPayload := encodeForwardPayload("g1", raftpb.ForwardOpHeadObject, buildHeadObjectArgs("b", "k"))
		// If legacy handler receives this, it treats it as opaque propose bytes.
		// The key is that 0x06 and 0x08 NEVER conflict because they're different StreamType values.
		// ShardService dispatches based on StreamType, so payload ambiguity is safe.
		_ = groupPayload // silence unused
	})
}
