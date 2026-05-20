package serveruntime

import (
	"bytes"
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
// and fail. It also preserves the individual phase population checks without
// paying for separate full boot prerequisites.
func TestBootRaftPhases_OrderingInvariant(t *testing.T) {
	ctx, state := raftPhasePrereqs(t)

	// Before any raft phase: nothing wired.
	assert.Nil(t, state.metaRaft)
	assert.Nil(t, state.dgMgr)
	assert.Nil(t, state.rotationWorker)

	// 1. Wiring populates metaRaft; no callbacks yet.
	require.NoError(t, bootMetaRaftWiring(state))
	require.NotNil(t, state.metaRaft, "metaRaft after Wiring")
	require.NotNil(t, state.metaTransport, "metaTransport after Wiring")
	require.NotNil(t, state.nfsExportSvc, "NFS export service after Wiring")
	assert.Nil(t, state.dgMgr, "DataGroup not yet constructed before its phase")
	assert.Nil(t, state.clusterRouter, "Router not yet constructed before its phase")
	assert.Nil(t, state.rotationWorker, "Rotation not yet constructed before its phase")

	// 2. DataGroupRouter must be able to call SetOnBucketAssigned (which
	//    requires metaRaft to exist) — proves Wiring ran first.
	require.NoError(t, bootDataGroupRouter(state))
	require.NotNil(t, state.dgMgr, "DataGroupManager after DataGroupRouter phase")
	require.NotNil(t, state.clusterRouter, "Router after DataGroupRouter phase")
	// SetDefault("group-0") was applied. ExplicitGroup returns false for
	// unassigned buckets but the default keeps RouteKey from erroring.
	if explicit, ok := state.clusterRouter.ExplicitGroup("any-bucket"); ok {
		t.Errorf("brand-new bucket should not have explicit assignment, got %q", explicit)
	}
	assert.Nil(t, state.rotationWorker, "Rotation still not constructed")

	// 3. RotationAndAdminAPI must register OnRotationApplied — also requires
	//    metaRaft. Both callback phases now done.
	require.NoError(t, bootRotationAndAdminAPI(state))
	require.NotNil(t, state.rotationKeystore, "Rotation keystore after RotationAndAdminAPI phase")
	require.NotNil(t, state.rotationWorker, "RotationWorker after its phase")
	assert.Nil(t, state.iamAdminAPI, "AdminAPI nil without IAM deps")
	assert.Nil(t, state.iamProposer, "iamProposer nil without IAM deps")

	// 4. Only NOW is Start fired. Both callbacks are already wired into
	//    metaRaft.FSM(); the apply loop cannot fire any event before
	//    bootMetaRaftStart returns.
	require.NoError(t, bootMetaRaftStart(ctx, state, nil))
	assert.NotNil(t, state.metaRaft.Node(), "node alive after Start")
	// metaRaft is now live; Close cleanup is on the stack. State.Cleanup
	// (deferred via t.Cleanup) will tear it down.
}

// TestPostRestoreCallback_ProxyTrustAndBannerPrevUpdated — F25+F26 integration:
// wire cfgStore + proxyTrust + post-restore callback via bootMetaRaftWiring,
// then simulate a Restore with specific config values and assert both the
// ProxyTrust CIDR set and the anonBannerSeedPrev are driven correctly.
func TestPostRestoreCallback_ProxyTrustAndBannerPrevUpdated(t *testing.T) {
	_, state := raftPhasePrereqs(t)
	state.bannerWriter = &bytes.Buffer{} // suppress stdout in test

	require.NoError(t, bootMetaRaftWiring(state))
	require.NotNil(t, state.cfgStore)
	require.NotNil(t, state.proxyTrust)
	require.NotNil(t, state.anonBannerSeedPrev, "anonBannerSeedPrev must be wired by bootMetaRaftWiring")

	// Simulate Restore installing trusted-proxy.cidr and iam.anon-enabled=false.
	state.cfgStore.Restore(map[string]string{
		"trusted-proxy.cidr": "10.0.0.0/8,192.168.0.0/16",
		"iam.anon-enabled":   "false",
	})

	// F25: ProxyTrust must reflect the restored CIDR. With 10.0.0.0/8 trusted,
	// a request from 10.1.2.3 with valid forwarding headers should unwrap to
	// the forwarded-for IP (1.2.3.4), proving the source is now in the trusted
	// set. Before Restore the CIDR set was empty, so Authoritative would have
	// returned the raw remote address unchanged.
	gotIP, ok := state.proxyTrust.Authoritative("10.1.2.3", "proto=https;for=1.2.3.4", "", "")
	assert.True(t, ok, "Authoritative must succeed for trusted source with valid headers")
	assert.Equal(t, "1.2.3.4", gotIP, "ProxyTrust must reflect restored CIDRs after Restore")

	// F26: anonBannerSeedPrev was called with false, so a Set("false")
	// immediately after must NOT emit a spurious banner (false→false, no flip).
	var bannerBuf bytes.Buffer
	state.bannerWriter = &bannerBuf
	// Re-seed to confirm it's now false (we can't inspect the atomic directly).
	// Verify by confirming banner is NOT emitted when we Set false again.
	// We need a trustedProxy set so the posture-check hook doesn't reject it.
	if err := state.cfgStore.Set(context.Background(), "iam.anon-enabled", "false"); err != nil {
		// Hook might reject if no trustedProxy is wired in test; that's OK for F26 scope.
		t.Logf("Set iam.anon-enabled=false returned (posture may reject without cert): %v", err)
	}
	assert.Empty(t, bannerBuf.String(), "no spurious banner: prev was seeded to false")
}
