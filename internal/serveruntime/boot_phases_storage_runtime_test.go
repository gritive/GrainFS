package serveruntime

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/badgerrole"
	"github.com/gritive/GrainFS/internal/raft"
)

// storagePhasePrereqs runs every prior boot phase (config, storage open,
// transport, raft) and seeds state.node + state.rpcTransport, leaving the
// state ready for the three storage runtime phases under test.
//
// --cluster-key is required in all modes; a fixed test key is supplied.
// In solo mode bootShardService still runs the !JoinMode branch — meta-raft
// has a single voter so the leader wait completes near-instantly.
// The data-plane raft node is constructed but not Bootstrap'd — bootStreamRouter
// fires node.Start() in production; tests rely on the cleanup stack to Stop it.
func storagePhasePrereqs(t *testing.T) (context.Context, *bootState) {
	t.Helper()
	state := newBootState(Config{
		DataDir:    t.TempDir(),
		NodeID:     "n1",
		ClusterKey: "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899",
	})
	require.NoError(t, bootValidateConfig(state))
	require.NoError(t, bootAutoMigrate(state))
	require.NoError(t, bootOpenMetaDB(state))
	require.NoError(t, bootValidateTimings(state))
	require.NoError(t, bootOpenRaftLogStore(state))
	require.NoError(t, bootOpenSharedRaftLogDB(state))
	require.NoError(t, bootOpenSharedFSMDB(state))
	t.Cleanup(state.Cleanup)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, bootQUICTransport(ctx, state))
	require.NoError(t, bootPeerConnections(ctx, state))
	require.NoError(t, bootGroupRaftMux(state))

	// Mirror the run.go raft-node construction: needed by the storage phases.
	raftCfg := raft.DefaultConfig(state.nodeID, state.peers)
	raftCfg.ManagedMode = state.cfg.BadgerManagedMode
	raftCfg.LogGCInterval = state.cfg.RaftLogGCInterval
	node := raft.NewNode(raftCfg, state.logStore)
	state.node = node
	state.rpcTransport = raft.NewQUICRPCTransport(state.quicTransport, node)
	state.rpcTransport.SetTransport()

	require.NoError(t, bootMetaRaftWiring(state))
	require.NoError(t, bootDataGroupRouter(state))
	require.NoError(t, bootRotationAndAdminAPI(state))
	require.NoError(t, bootMetaRaftStart(ctx, state, nil))
	return ctx, state
}

// TestBootShardService_ComputesEffectiveEC — bootShardService resolves the EC
// profile for the local cluster size and constructs a ShardService.
func TestBootShardService_ComputesEffectiveEC(t *testing.T) {
	ctx, state := storagePhasePrereqs(t)

	require.NoError(t, bootShardService(ctx, state))
	assert.Greater(t, state.effectiveEC.NumShards(), 0, "effective EC profile resolved")
	assert.NotNil(t, state.shardSvc, "ShardService constructed")
	// Single-node cluster → 1+0 auto profile.
	assert.Equal(t, 1, state.effectiveEC.DataShards)
	assert.Equal(t, 0, state.effectiveEC.ParityShards)
}

// TestBootStreamRouter_RegistersHandlersAndStartsNode — bootStreamRouter wires
// the QUIC stream multiplexer and fires the data-plane raft apply loop. The
// cleanup stack must grow by one (node.Stop registered for shutdown).
func TestBootStreamRouter_RegistersHandlersAndStartsNode(t *testing.T) {
	ctx, state := storagePhasePrereqs(t)
	require.NoError(t, bootShardService(ctx, state))

	cleanupsBefore := len(state.cleanups)
	require.NoError(t, bootStreamRouter(state))
	assert.NotNil(t, state.streamRouter, "StreamRouter constructed")
	assert.Equal(t, cleanupsBefore+1, len(state.cleanups), "node.Stop cleanup pushed")
}

// TestBootOwnedGroupsAndEC_WiresDistBackend — runs the heavy phase end-to-end
// with full prereqs, asserts distBackend is wired and the LoadReporter store
// is constructed. The shutdown hook is exercised by the t.Cleanup stack.
func TestBootOwnedGroupsAndEC_WiresDistBackend(t *testing.T) {
	ctx, state := storagePhasePrereqs(t)
	require.NoError(t, bootShardService(ctx, state))
	require.NoError(t, bootStreamRouter(state))

	require.NoError(t, bootOwnedGroupsAndEC(ctx, state, func(badgerrole.Decision) {}))
	assert.NotNil(t, state.distBackend, "DistributedBackend constructed")
	assert.NotNil(t, state.shardCache, "shard cache constructed")
	assert.NotNil(t, state.rebalancer, "rebalancer constructed")
	assert.NotNil(t, state.loadReporter, "LoadReporter constructed")
	assert.NotNil(t, state.loadReporterStor, "LoadReporter store constructed")
	assert.NotNil(t, state.stopApply, "stopApply channel constructed")
}

// TestBootStoragePhases_OrderingInvariant — witness test. Asserts each phase
// boundary by checking which state fields are nil before vs populated after.
// Mirrors the PR 4 ordering test pattern: if a refactor accidentally re-orders
// the storage phases, the test catches it.
func TestBootStoragePhases_OrderingInvariant(t *testing.T) {
	ctx, state := storagePhasePrereqs(t)

	// Before any storage phase: nothing wired.
	assert.Nil(t, state.shardSvc)
	assert.Nil(t, state.streamRouter)
	assert.Nil(t, state.distBackend)
	assert.Nil(t, state.shardCache)
	assert.Nil(t, state.rebalancer)
	assert.Equal(t, 0, state.effectiveEC.NumShards(), "effectiveEC zero-value before phases")

	// 1. ShardService — populates shardSvc + effectiveEC; no router yet.
	require.NoError(t, bootShardService(ctx, state))
	require.NotNil(t, state.shardSvc, "shardSvc after bootShardService")
	require.Greater(t, state.effectiveEC.NumShards(), 0, "effectiveEC after bootShardService")
	assert.Nil(t, state.streamRouter, "streamRouter not yet constructed")
	assert.Nil(t, state.distBackend, "distBackend not yet constructed")

	// 2. StreamRouter — populates streamRouter; distBackend still nil.
	require.NoError(t, bootStreamRouter(state))
	require.NotNil(t, state.streamRouter, "streamRouter after bootStreamRouter")
	assert.Nil(t, state.distBackend, "distBackend not yet constructed")

	// 3. OwnedGroupsAndEC — populates distBackend + shardCache + rebalancer +
	//    loadReporter; shutdown hook registered.
	require.NoError(t, bootOwnedGroupsAndEC(ctx, state, func(badgerrole.Decision) {}))
	require.NotNil(t, state.distBackend, "distBackend after bootOwnedGroupsAndEC")
	require.NotNil(t, state.shardCache, "shardCache after bootOwnedGroupsAndEC")
	require.NotNil(t, state.rebalancer, "rebalancer after bootOwnedGroupsAndEC")
	require.NotNil(t, state.loadReporter, "loadReporter after bootOwnedGroupsAndEC")
}
