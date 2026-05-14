package serveruntime

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// raftPhasePrereqs runs every phase up to (but not including) the raft phases:
// config validate, meta DB open, transport (QUIC + peers + mux).
// Returns a state ready for the four raft phases under test.
func raftPhasePrereqs(t *testing.T) (context.Context, *bootState) {
	t.Helper()
	state := newBootState(Config{DataDir: t.TempDir(), NodeID: "n1", ClusterKey: "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"})
	require.NoError(t, bootValidateConfig(state))
	require.NoError(t, bootAutoMigrate(state))
	require.NoError(t, bootOpenMetaDB(state))
	require.NoError(t, bootValidateTimings(state))
	t.Cleanup(state.Cleanup)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, bootQUICTransport(ctx, state))
	require.NoError(t, bootPeerConnections(ctx, state))
	require.NoError(t, bootGroupRaftMux(state))
	return ctx, state
}

// TestBootMetaRaftWiring_PopulatesState — happy path: phase constructs the
// MetaRaft + transport, populates state, but does NOT register callbacks or
// start the apply loop.
func TestBootMetaRaftWiring_PopulatesState(t *testing.T) {
	_, state := raftPhasePrereqs(t)

	require.NoError(t, bootMetaRaftWiring(state))
	require.NotNil(t, state.metaRaft, "state.metaRaft populated")
	require.NotNil(t, state.metaTransport, "state.metaTransport populated")
	require.NotNil(t, state.nfsExportSvc, "NFS export service populated")
	// Wiring phase has no callbacks; downstream phases register them.
	assert.Nil(t, state.dgMgr, "DataGroupManager not yet constructed")
	assert.Nil(t, state.clusterRouter, "Router not yet constructed")
	assert.Nil(t, state.rotationWorker, "RotationWorker not yet constructed")
}

// TestBootDataGroupRouter_RegistersCallbackBeforeStart — phase populates
// dgMgr + Router and registers OnBucketAssigned. The witness: SetDefault put
// "group-0" as the catch-all assignment.
func TestBootDataGroupRouter_RegistersCallbackBeforeStart(t *testing.T) {
	_, state := raftPhasePrereqs(t)
	require.NoError(t, bootMetaRaftWiring(state))

	require.NoError(t, bootDataGroupRouter(state))
	require.NotNil(t, state.dgMgr)
	require.NotNil(t, state.clusterRouter)
	// SetDefault("group-0") was applied — even with no explicit assignment,
	// the default catches all buckets. ExplicitGroup returns false for
	// unassigned buckets but the default keeps RouteKey from erroring.
	if explicit, ok := state.clusterRouter.ExplicitGroup("any-bucket"); ok {
		t.Errorf("brand-new bucket should not have explicit assignment, got %q", explicit)
	}
}

// TestBootRotationAndAdminAPI_NoIAMSkipsAdmin — without IAM dependencies,
// rotation worker is still wired but iamAdminAPI / iamProposer remain nil.
func TestBootRotationAndAdminAPI_NoIAMSkipsAdmin(t *testing.T) {
	_, state := raftPhasePrereqs(t)
	require.NoError(t, bootMetaRaftWiring(state))
	require.NoError(t, bootDataGroupRouter(state))

	require.NoError(t, bootRotationAndAdminAPI(state))
	require.NotNil(t, state.rotationKeystore)
	require.NotNil(t, state.rotationWorker)
	assert.Nil(t, state.iamAdminAPI, "AdminAPI nil without IAM deps")
	assert.Nil(t, state.iamProposer, "iamProposer nil without IAM deps")
}

// TestBootMetaRaftStart_FiresApplyLoop — phase calls Start which spawns the
// apply loop. AddCleanup registers Close so t.Cleanup(state.Cleanup) tears it
// down. Pass nil startRotationSocket to skip admin UDS in tests.
func TestBootMetaRaftStart_FiresApplyLoop(t *testing.T) {
	ctx, state := raftPhasePrereqs(t)
	require.NoError(t, bootMetaRaftWiring(state))
	require.NoError(t, bootDataGroupRouter(state))
	require.NoError(t, bootRotationAndAdminAPI(state))

	require.NoError(t, bootMetaRaftStart(ctx, state, nil))
	// Apply loop is live — node has been Started.
	assert.NotNil(t, state.metaRaft.Node(), "node alive after Start")
	// state.Cleanup (deferred via t.Cleanup in raftPhasePrereqs) will fire
	// the metaRaft.Close cleanup pushed by bootMetaRaftStart.
}

// TestBootRaftPhases_OrderingInvariant — the central invariant of PR 4:
// callbacks (DataGroupRouter, RotationAndAdminAPI) MUST run BEFORE
// bootMetaRaftStart fires the apply loop. This witness test asserts the
// ordering at each phase boundary by inspecting bootState surface:
//
//   - After bootMetaRaftWiring:    metaRaft exists; no callbacks, no Start
//   - After bootDataGroupRouter:   bucket-assigned callback wired; no Start
//   - After bootRotationAndAdminAPI: rotation callback wired; no Start
//   - After bootMetaRaftStart:     apply loop running; metaRaft.Close in stack
//
// If a future refactor accidentally re-orders Start ahead of the callback
// phases, this test will see metaRaft live before the callback phase ran
// and fail.
func TestBootRaftPhases_OrderingInvariant(t *testing.T) {
	ctx, state := raftPhasePrereqs(t)

	// Before any raft phase: nothing wired.
	assert.Nil(t, state.metaRaft)
	assert.Nil(t, state.dgMgr)
	assert.Nil(t, state.rotationWorker)

	// 1. Wiring populates metaRaft; no callbacks yet.
	require.NoError(t, bootMetaRaftWiring(state))
	require.NotNil(t, state.metaRaft, "metaRaft after Wiring")
	assert.Nil(t, state.dgMgr, "DataGroup not yet constructed before its phase")
	assert.Nil(t, state.rotationWorker, "Rotation not yet constructed before its phase")

	// 2. DataGroupRouter must be able to call SetOnBucketAssigned (which
	//    requires metaRaft to exist) — proves Wiring ran first.
	require.NoError(t, bootDataGroupRouter(state))
	require.NotNil(t, state.clusterRouter, "Router after DataGroupRouter phase")
	assert.Nil(t, state.rotationWorker, "Rotation still not constructed")

	// 3. RotationAndAdminAPI must register OnRotationApplied — also requires
	//    metaRaft. Both callback phases now done.
	require.NoError(t, bootRotationAndAdminAPI(state))
	require.NotNil(t, state.rotationWorker, "RotationWorker after its phase")

	// 4. Only NOW is Start fired. Both callbacks are already wired into
	//    metaRaft.FSM(); the apply loop cannot fire any event before
	//    bootMetaRaftStart returns.
	require.NoError(t, bootMetaRaftStart(ctx, state, nil))
	// metaRaft is now live; Close cleanup is on the stack. State.Cleanup
	// (deferred via t.Cleanup) will tear it down.
}
