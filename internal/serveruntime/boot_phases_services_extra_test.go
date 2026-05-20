package serveruntime

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
)

// disableBalancerForTest patches the live ClusterConfig so BalancerEnabled is
// false. Needed because ClusterConfig defaults BalancerEnabled=true (matches
// the former --balancer-enabled flag default); without this patch the
// services-extra tests would start a real balancer goroutine.
func disableBalancerForTest(t *testing.T, state *bootState) {
	t.Helper()
	disabled := false
	require.NoError(t, state.metaRaft.FSM().ApplyClusterConfigPatchForTest(cluster.ClusterConfigPatch{BalancerEnabled: &disabled}))
}

// servicesExtraPrereqs runs every prior boot phase, including
// bootSnapshotAndApplyLoop. The returned state is ready for the PR-final
// services-extra phases under test.
//
// Heavy-weight HTTP/admin/scrubber phases are NOT exercised here because they
// require IAMStore, dashboard token, working admin UDS, and a fully resolved
// data-plane raft leader — that level of integration belongs to E2E. The
// witness ordering test below covers state-field nil/non-nil transitions for
// the lightweight phases that can run with a minimal Config.
func servicesExtraPrereqs(t *testing.T) (context.Context, *bootState) {
	t.Helper()
	state := servicesPhasePrereqs(t)
	require.NoError(t, bootSnapshotAndApplyLoop(state))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx, state
}

// TestBootServicesExtraPhases_OrderingInvariant — witness test that mirrors
// the PR 4 ordering test pattern. Walks the lightweight services-extra phases
// (balancer, WAL+forwarders) and asserts state fields are nil before each
// phase and populated after. If a refactor accidentally re-orders the phases,
// this test catches the regression. It also preserves the individual phase
// population checks without paying for separate full boot prerequisites.
//
// Heavier phases (bootBackendWrap onwards) are not in this loop because they
// require IAMStore + recovery-marker + admin UDS plumbing not present in the
// minimal test Config. Those are covered by the smoke E2E test.
func TestBootServicesExtraPhases_OrderingInvariant(t *testing.T) {
	ctx, state := servicesExtraPrereqs(t)
	disableBalancerForTest(t, state)

	// Before any phase: every services-extra field nil.
	assert.Nil(t, state.balancerProposer)
	assert.Nil(t, state.gossipReceiver)
	assert.Nil(t, state.wal)
	assert.Nil(t, state.forwardSender)
	assert.Nil(t, state.forwardReceiver)
	assert.Nil(t, state.metaForwardSender)
	assert.Nil(t, state.metaReadSender)
	assert.Nil(t, state.clusterCoord)
	assert.Equal(t, 0, state.seedGroups, "seedGroups zero before phase")

	// 1. Balancer + gossip — skips balancer but starts capability gossip.
	require.NoError(t, bootBalancerAndGossip(ctx, state))
	assert.Nil(t, state.balancerProposer, "balancerProposer skipped (no flag)")
	assert.NotNil(t, state.gossipReceiver, "gossipReceiver started for capability evidence")
	// WAL still not opened — proves WALAndForwarders has not yet run.
	assert.Nil(t, state.wal, "WAL not opened before its phase")

	// 2. WAL + forwarders — populates everything.
	require.NoError(t, bootWALAndForwarders(ctx, state))
	assert.NotNil(t, state.wal, "WAL after phase")
	assert.NotEmpty(t, state.walDir, "walDir set")
	assert.NotNil(t, state.forwardSender, "ForwardSender after phase")
	assert.NotNil(t, state.forwardReceiver, "ForwardReceiver after phase")
	assert.NotNil(t, state.metaForwardSender, "MetaForwardSender after phase")
	assert.NotNil(t, state.metaReadSender, "MetaReadSender after phase")
	assert.NotNil(t, state.clusterCoord, "ClusterCoordinator after phase")
	// seedGroups is max(clusterSize*4, 8); single-node cluster -> 8.
	assert.GreaterOrEqual(t, state.seedGroups, 8, "seedGroups computed")
}
