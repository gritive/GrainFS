package serveruntime

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/badgerrole"
)

// servicesPhasePrereqs runs every prior boot phase (config, storage open,
// transport, raft, storage runtime) so the services phase can run end-to-end
// against real components.
func servicesPhasePrereqs(t *testing.T) *bootState {
	t.Helper()
	ctx, state := storagePhasePrereqs(t)
	require.NoError(t, bootShardService(ctx, state))
	require.NoError(t, bootStreamRouter(state))
	require.NoError(t, bootOwnedGroupsAndEC(ctx, state, func(badgerrole.Decision) {}))
	return state
}

// TestBootSnapshotAndApplyLoop_PopulatesState — happy path: phase wires the
// FSM + SnapshotManager onto distBackend, builds the cachedBackend wrap chain,
// and registers the s3-cache invalidator. The apply-loop goroutine is fired;
// the cleanup stack (state.stopApply close) is exercised via t.Cleanup ->
// state.Cleanup which closes stopApply via the bootOwnedGroupsAndEC ownership.
func TestBootSnapshotAndApplyLoop_PopulatesState(t *testing.T) {
	state := servicesPhasePrereqs(t)

	// Before the phase: services-owned fields nil; effectiveEC is set by
	// storage phase but fsm/snapMgr/cachedBackend belong to services.
	assert.Nil(t, state.fsm, "fsm nil before phase")
	assert.Nil(t, state.snapMgr, "snapMgr nil before phase")
	assert.Nil(t, state.cachedBackend, "cachedBackend nil before phase")

	require.NoError(t, bootSnapshotAndApplyLoop(state))

	// After: every services field populated.
	assert.NotNil(t, state.fsm, "fsm populated")
	assert.NotNil(t, state.snapMgr, "SnapshotManager populated")
	assert.NotNil(t, state.cachedBackend, "cachedBackend populated")
}
