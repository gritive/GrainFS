package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/raft"
	"github.com/stretchr/testify/require"
)

// TestProposeRegisterPendingLearner_SurfacesRebindError is a real-MetaRaft
// regression test for the bug where ProposeRegisterPendingLearner used
// waitApplied (which discards FSM apply errors) instead of waitAppliedResult
// (which surfaces them). The peer_registry_test.go tests bypass the Raft
// propose+apply path by calling registry.registerPendingLearner directly, so
// they never caught the swallowed error.
//
// This test exercises the full propose → apply → error-surfaced round-trip
// using a real single-node MetaRaft with a live runApplyLoop. A regression to
// waitApplied causes the second ProposeRegisterPendingLearner to return nil
// instead of errNodeIDRebind, and this test fails.
func TestProposeRegisterPendingLearner_SurfacesRebindError(t *testing.T) {
	m := newSingleMetaRaft(t)
	t.Cleanup(func() { _ = m.Close() })

	require.NoError(t, m.Bootstrap())
	require.NoError(t, m.Start(context.Background(), nil))
	require.Eventually(t, func() bool {
		return m.node.State() == raft.Leader
	}, 2*time.Second, 20*time.Millisecond, "single node must become leader")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var spki1, spki2 [32]byte
	spki1[0], spki2[0] = 1, 2

	// First registration of node-a with spki1 must succeed.
	require.NoError(t, m.ProposeRegisterPendingLearner(ctx, "node-a", spki1, "10.0.0.2:9000"),
		"first register must succeed")

	// Rebinding node-a to a DIFFERENT SPKI must be rejected AND the error must be
	// surfaced to the caller (regression: waitApplied swallowed it, waitAppliedResult surfaces it).
	err := m.ProposeRegisterPendingLearner(ctx, "node-a", spki2, "10.0.0.9:9000")
	require.Error(t, err, "rebind to a different SPKI must surface an error from Propose (not be swallowed)")
	require.ErrorIs(t, err, errNodeIDRebind,
		"error must be errNodeIDRebind (the invite-hijack guard), got: %v", err)
}
