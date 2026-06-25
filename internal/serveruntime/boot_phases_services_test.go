package serveruntime

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// servicesPhasePrereqs runs every prior boot phase (config, storage open,
// transport, raft, storage runtime) so the services phase can run end-to-end
// against real components.
func servicesPhasePrereqs(t *testing.T) *bootState {
	t.Helper()
	ctx, state := storagePhasePrereqs(t)
	require.NoError(t, bootShardService(ctx, state))
	require.NoError(t, bootShardRoutes(state))
	require.NoError(t, bootOwnedGroupsAndEC(ctx, state))
	return state
}

// TestBootSnapshotAndApplyLoop_PopulatesState — happy path: phase wires the
// FSM onto distBackend and fires the apply-loop goroutine. The cleanup
// stack (state.stopApply close) is exercised via t.Cleanup -> state.Cleanup
// which closes stopApply via the bootOwnedGroupsAndEC ownership.
//
// As of M5 PR 29 the v1 SnapshotManager is no longer wired; raftv2 owns
// snapshot lifecycle internally.
func TestBootSnapshotAndApplyLoop_PopulatesState(t *testing.T) {
	state := servicesPhasePrereqs(t)

	// Before the phase: fsm is nil.
	assert.Nil(t, state.fsm, "fsm nil before phase")

	require.NoError(t, bootSnapshotAndApplyLoop(state))

	// After: fsm populated.
	assert.NotNil(t, state.fsm, "fsm populated")
}
