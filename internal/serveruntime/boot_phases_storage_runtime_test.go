package serveruntime

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/raft"
)

// storagePhasePrereqs runs every prior boot phase (config, storage open,
// transport, raft) and seeds state.node + state.rpcTransport, leaving the
// state ready for the three storage runtime phases under test.
//
// cluster-key is required in all modes; a fixed test key is supplied.
// In solo mode bootShardService still runs the !JoinMode branch — meta-raft
// has a single voter so the leader wait completes near-instantly.
// The data-plane raft node is constructed but not Bootstrap'd — bootShardRoutes
// fires node.Start() in production; tests rely on the cleanup stack to Stop it.
func storagePhasePrereqs(t *testing.T) (context.Context, *bootState) {
	t.Helper()
	dataDir := t.TempDir()
	state := newBootState(Config{
		DataDir:    dataDir,
		DataDirs:   []string{dataDir},
		NodeID:     "n1",
		ClusterKey: "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899",
	})
	require.NoError(t, bootValidateConfig(state))
	require.NoError(t, bootAutoMigrate(state))
	require.NoError(t, bootOpenMetaDB(state))
	require.NoError(t, bootValidateTimings(state))
	require.NoError(t, bootOpenSharedFSMDB(state))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, bootClusterTransport(ctx, state))
	require.NoError(t, bootGroupRaftMux(state))

	// Mirror the run.go raft-node construction: needed by the storage phases.
	raftCfg := raft.DefaultConfig(state.nodeID, state.peers)
	raftCfg.ManagedMode = true
	raftCfg.LogGCInterval = state.cfg.RaftLogGCInterval
	node, closeNode, err := cluster.NewRaftV2NodeForServeruntime(raftCfg, state.raftDir)
	require.NoError(t, err)
	t.Cleanup(func() {
		if closeNode != nil {
			_ = closeNode()
		}
	})
	t.Cleanup(state.Cleanup)
	state.node = node
	state.rpcTransport = cluster.NewRaftRPCTransport(state.clusterTransport, node)
	state.rpcTransport.SetTransport()

	require.NoError(t, bootMetaRaftWiring(state))
	require.NoError(t, bootDataGroupRouter(state))
	require.NoError(t, bootRotationAndAdminAPI(state))
	require.NoError(t, bootMetaRaftStart(ctx, state))
	return ctx, state
}

// TestBootShardService_NoLegacyWALDirCreated proves S4: boot no longer opens a
// shard WAL, so no legacy {dataDir}/datawal directory is created. Durability is
// write-time fsync / EC; there is no WAL to replay.
func TestBootShardService_NoLegacyWALDirCreated(t *testing.T) {
	ctx, state := storagePhasePrereqs(t)

	require.NoError(t, bootShardService(ctx, state))

	_, err := os.Stat(filepath.Join(state.cfg.DataDir, "datawal"))
	require.True(t, os.IsNotExist(err), "S4: boot must not create a legacy datawal directory")
}

func TestRuntimeTopologyNodesPrefersJoinedMetaNodes(t *testing.T) {
	nodes := []cluster.MetaNodeEntry{
		{ID: "n0", Address: "127.0.0.1:7000"},
		{ID: "n1", Address: "127.0.0.1:7001"},
		{ID: "n2", Address: "127.0.0.1:7002"},
		{ID: "n3", Address: "127.0.0.1:7003"},
		{ID: "n4", Address: "127.0.0.1:7004"},
	}

	got := runtimeTopologyNodes("n0", "127.0.0.1:7000", []string{"127.0.0.1:7001"}, nodes)

	require.Equal(t, []string{
		"127.0.0.1:7000",
		"127.0.0.1:7001",
		"127.0.0.1:7002",
		"127.0.0.1:7003",
		"127.0.0.1:7004",
	}, got)
}

func TestECConfigForShardGroupUsesJoinedGroupWidth(t *testing.T) {
	group := cluster.ShardGroupEntry{
		ID:      "group-12",
		PeerIDs: []string{"n0", "n1", "n2", "n3", "n4"},
	}
	staleBootConfig := cluster.ECConfig{DataShards: 1, ParityShards: 1}

	got := ecConfigForShardGroup(group, staleBootConfig)

	require.Equal(t, cluster.ECConfig{DataShards: 3, ParityShards: 2}, got)
}

func TestBootShardService_DoesNotOverwriteReplayedShardGroups(t *testing.T) {
	ctx, state := storagePhasePrereqs(t)
	require.NoError(t, WaitForMetaRaftLeader(ctx, state.metaRaft, 5*time.Second))
	require.NoError(t, state.metaRaft.ProposeShardGroup(ctx, cluster.ShardGroupEntry{
		ID:      "group-1",
		PeerIDs: []string{"n1", "n2", "n3"},
	}))
	require.Eventually(t, func() bool {
		group, ok := state.metaRaft.FSM().ShardGroup("group-1")
		return ok && assert.ObjectsAreEqual([]string{"n1", "n2", "n3"}, group.PeerIDs)
	}, 5*time.Second, 10*time.Millisecond)

	require.NoError(t, bootShardService(ctx, state))

	group, ok := state.metaRaft.FSM().ShardGroup("group-1")
	require.True(t, ok)
	assert.Equal(t, []string{"n1", "n2", "n3"}, group.PeerIDs)
}

// TestBootShardService_ShardPackThresholdIsHardError proves S3: requesting
// shard-packing via the GRAINFS_SHARD_PACK_THRESHOLD env gate is refused at boot
// with a clear error (packing is disabled — a durable pack index was never built).
func TestBootShardService_ShardPackThresholdIsHardError(t *testing.T) {
	ctx, state := storagePhasePrereqs(t)
	t.Setenv("GRAINFS_SHARD_PACK_THRESHOLD", "1024")

	err := bootShardService(ctx, state)
	require.Error(t, err)
	require.Contains(t, err.Error(), "shard-pack")
}

// TestBootStoragePhases_OrderingInvariant — witness test. Asserts each phase
// boundary by checking which state fields are nil before vs populated after.
// Mirrors the PR 4 ordering test pattern: if a refactor accidentally re-orders
// the storage phases, the test catches it. It also preserves the individual
// phase population checks without paying for separate full boot prerequisites.
func TestBootStoragePhases_OrderingInvariant(t *testing.T) {
	ctx, state := storagePhasePrereqs(t)

	// Before any storage phase: nothing wired.
	assert.Nil(t, state.shardSvc)
	assert.Nil(t, state.distBackend)
	assert.Nil(t, state.shardCache)
	assert.Equal(t, 0, state.effectiveEC.NumShards(), "effectiveEC zero-value before phases")

	// 1. ShardService - populates shardSvc + effectiveEC; no router yet. The shard
	//    WAL was removed in S4: boot wires/opens/replays no WAL.
	require.NoError(t, bootShardService(ctx, state))
	require.NotNil(t, state.shardSvc, "shardSvc after bootShardService")
	require.Greater(t, state.effectiveEC.NumShards(), 0, "effectiveEC after bootShardService")
	// Single-node cluster -> 1+0 auto profile.
	assert.Equal(t, 1, state.effectiveEC.DataShards)
	assert.Equal(t, 0, state.effectiveEC.ParityShards)
	assert.Nil(t, state.distBackend, "distBackend not yet constructed")

	// 2. ShardRoutes — registers the native shard routes; distBackend still nil.
	// node.Start (and its Stop cleanup) moved to run.go, BEFORE invite-join
	// Phase-2 (the §6 join-deadlock fix) — bootShardRoutes pushes no cleanup.
	cleanupsBefore := len(state.cleanups)
	require.NoError(t, bootShardRoutes(state))
	assert.Equal(t, cleanupsBefore, len(state.cleanups), "bootShardRoutes pushes no cleanup (node.Start lives in run.go)")
	assert.Nil(t, state.distBackend, "distBackend not yet constructed")
	// Mirror run.go's early Start so the later phases see a running actor,
	// exactly as production boot does.
	state.node.Start()
	state.AddCleanup(func() { state.node.Close() })

	// 3. OwnedGroupsAndEC — populates distBackend + shardCache + loadReporter;
	//    shutdown hook registered.
	require.NoError(t, bootOwnedGroupsAndEC(ctx, state))
	require.NotNil(t, state.distBackend, "distBackend after bootOwnedGroupsAndEC")
	require.NotNil(t, state.shardCache, "shardCache after bootOwnedGroupsAndEC")
	require.NotNil(t, state.loadReporter, "loadReporter after bootOwnedGroupsAndEC")
	require.NotNil(t, state.loadReporterStor, "loadReporter store after bootOwnedGroupsAndEC")
	require.NotNil(t, state.stopApply, "stopApply channel after bootOwnedGroupsAndEC")
}
