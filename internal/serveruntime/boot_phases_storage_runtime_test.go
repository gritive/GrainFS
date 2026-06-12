package serveruntime

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/badgerrole"
	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/storage"

	"go.uber.org/goleak"
)

// storagePhasePrereqs runs every prior boot phase (config, storage open,
// transport, raft) and seeds state.node + state.rpcTransport, leaving the
// state ready for the three storage runtime phases under test.
//
// cluster-key is required in all modes; a fixed test key is supplied.
// In solo mode bootShardService still runs the !JoinMode branch — meta-raft
// has a single voter so the leader wait completes near-instantly.
// The data-plane raft node is constructed but not Bootstrap'd — bootStreamRouter
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
	require.NoError(t, bootMetaRaftStart(ctx, state, nil))
	return ctx, state
}

func TestBootShardServiceDataWALPrefersDEKKeeper(t *testing.T) {
	state := newBootState(Config{DataDir: t.TempDir()})
	kek := bytes.Repeat([]byte{0x61}, encrypt.KEKSize)
	clusterID := bytes.Repeat([]byte{0x62}, 16)
	keeper, err := encrypt.NewDEKKeeper(kek, clusterID)
	require.NoError(t, err)
	state.dekKeeper = keeper
	state.clusterID = clusterID

	sealer, err := dataWALSealerForState(state)
	require.NoError(t, err)
	require.IsType(t, &storage.DEKKeeperAdapter{}, sealer)
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

// TestBootStoragePhases_OrderingInvariant — witness test. Asserts each phase
// boundary by checking which state fields are nil before vs populated after.
// Mirrors the PR 4 ordering test pattern: if a refactor accidentally re-orders
// the storage phases, the test catches it. It also preserves the individual
// phase population checks without paying for separate full boot prerequisites.
// TestBootShardService_ClosesShardPackActorOnShutdown asserts the boot path
// registers a cleanup that closes the shard service on shutdown. With a shard
// pack store wired (WAL present), the shard-pack actor goroutine spawns; if no
// cleanup closes it, it leaks past shutdown.
func TestBootShardService_ClosesShardPackActorOnShutdown(t *testing.T) {
	ctx, state := storagePhasePrereqs(t)
	// Enable the shard-pack store so the WAL-wired shard-pack actor goroutine spawns.
	state.cfg.ShardPackThreshold = 1024
	baseline := goleak.IgnoreCurrent()

	require.NoError(t, bootShardService(ctx, state))
	require.NotNil(t, state.shardSvc)

	// Production shutdown runs the cleanup stack — it must close the shard
	// service so the shard-pack actor goroutine is stopped.
	state.Cleanup()

	goleak.VerifyNone(t, baseline)
}

func TestBootShardServiceWiresDataWALRepairCollector(t *testing.T) {
	ctx, state := storagePhasePrereqs(t)

	require.NoError(t, bootShardService(ctx, state))

	require.NotNil(t, state.dataWALRepairCollector)
	require.NotNil(t, state.shardSvc)
	require.True(t, state.shardSvc.HasDataWALRepairSink(),
		"shard service must be constructed with the repair-candidate sink")
}

func TestBootStoragePhases_OrderingInvariant(t *testing.T) {
	ctx, state := storagePhasePrereqs(t)

	// Before any storage phase: nothing wired.
	assert.Nil(t, state.shardSvc)
	assert.Nil(t, state.distBackend)
	assert.Nil(t, state.shardCache)
	assert.Nil(t, state.rebalancer)
	assert.Nil(t, state.dataWAL, "data WAL not opened before shard service phase")
	assert.Empty(t, state.dataWALDir, "dataWALDir not set before shard service phase")
	assert.Equal(t, 0, state.effectiveEC.NumShards(), "effectiveEC zero-value before phases")

	// 1. ShardService — populates shardSvc + effectiveEC + data WAL; no router yet.
	require.NoError(t, bootShardService(ctx, state))
	require.NotNil(t, state.shardSvc, "shardSvc after bootShardService")
	require.NotNil(t, state.dataWAL, "data WAL after shard service phase")
	assert.NotEmpty(t, state.dataWALDir, "dataWALDir set after shard service phase")
	assert.True(t, state.shardSvc.HasDataWAL(), "shard service receives data WAL")
	require.Greater(t, state.effectiveEC.NumShards(), 0, "effectiveEC after bootShardService")
	// Single-node cluster -> 1+0 auto profile.
	assert.Equal(t, 1, state.effectiveEC.DataShards)
	assert.Equal(t, 0, state.effectiveEC.ParityShards)
	assert.Nil(t, state.distBackend, "distBackend not yet constructed")

	// 2. StreamRouter — registers the native shard routes; distBackend still nil.
	cleanupsBefore := len(state.cleanups)
	require.NoError(t, bootStreamRouter(state))
	assert.Equal(t, cleanupsBefore+1, len(state.cleanups), "node.Stop cleanup pushed")
	assert.Nil(t, state.distBackend, "distBackend not yet constructed")

	// 3. OwnedGroupsAndEC — populates distBackend + shardCache + rebalancer +
	//    loadReporter; shutdown hook registered.
	require.NoError(t, bootOwnedGroupsAndEC(ctx, state, func(badgerrole.Decision) {}))
	require.NotNil(t, state.distBackend, "distBackend after bootOwnedGroupsAndEC")
	require.NotNil(t, state.shardCache, "shardCache after bootOwnedGroupsAndEC")
	require.NotNil(t, state.rebalancer, "rebalancer after bootOwnedGroupsAndEC")
	require.NotNil(t, state.loadReporter, "loadReporter after bootOwnedGroupsAndEC")
	require.NotNil(t, state.loadReporterStor, "loadReporter store after bootOwnedGroupsAndEC")
	require.NotNil(t, state.stopApply, "stopApply channel after bootOwnedGroupsAndEC")
}

// TestPutMultiNodeStreamEnabled verifies multi-node streaming PUT is now the
// DEFAULT (unset/empty = ON) and opts OUT only on an explicit falsey value
// (0/false/no/off, case-insensitive, trimmed). A parse that keyed off == "1"
// (or != "0") would be a footgun here — e.g. "false" must disable, not enable.
func TestPutMultiNodeStreamEnabled(t *testing.T) {
	tests := []struct {
		name string
		set  bool
		val  string
		want bool
	}{
		{name: "unset defaults ON", set: false, want: true},
		{name: "empty defaults ON", set: true, val: "", want: true},
		{name: "explicit 1 stays ON", set: true, val: "1", want: true},
		{name: "true stays ON", set: true, val: "true", want: true},
		{name: "garbage stays ON", set: true, val: "anything", want: true},
		{name: "zero opts out", set: true, val: "0", want: false},
		{name: "false opts out", set: true, val: "false", want: false},
		{name: "f opts out", set: true, val: "f", want: false},
		{name: "no opts out", set: true, val: "no", want: false},
		{name: "n opts out", set: true, val: "n", want: false},
		{name: "off opts out", set: true, val: "off", want: false},
		{name: "disable opts out", set: true, val: "disable", want: false},
		{name: "disabled opts out", set: true, val: "disabled", want: false},
		{name: "FALSE opts out (case-insensitive)", set: true, val: "FALSE", want: false},
		{name: "Off opts out (case + spaces)", set: true, val: "  Off  ", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.set {
				t.Setenv("GRAINFS_PUT_MULTINODE_STREAM", tc.val)
			} else {
				t.Setenv("GRAINFS_PUT_MULTINODE_STREAM", "")
			}
			assert.Equal(t, tc.want, putMultiNodeStreamEnabled())
		})
	}
}

// TestBootWiring_PropagatesMultiNodeStreamToServingGroups is the regression
// test for the #717 wiring gap: SetPutPipelineMultiNode was wired on the
// group-0 distBackend only, but group-0 is excluded from object placement
// (candidateGroupsFor), so PUTs route to per-group backends created via
// instantiateGroupWithConfig — which propagated SetPutPipeline but not the
// multi-node flag. Result was a silent spool fallback on every multi-node PUT.
func TestBootWiring_PropagatesMultiNodeStreamToServingGroups(t *testing.T) {
	cases := []struct {
		name   string
		envVal string // "" means unset
		want   bool
	}{
		{name: "env enabled propagates to per-group backend", envVal: "1", want: true},
		{name: "env unset = default ON (streaming dispatched on every serving group)", envVal: "", want: true},
		{name: "explicit opt-out propagates OFF", envVal: "0", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("GRAINFS_PUT_MULTINODE_STREAM", tc.envVal)

			ctx, state := storagePhasePrereqs(t)
			require.NoError(t, bootShardService(ctx, state))
			require.NoError(t, bootOwnedGroupsAndEC(ctx, state, func(badgerrole.Decision) {}))
			require.NotNil(t, state.putPipeline, "boot must construct the PUT pipeline")

			// Assert on the REAL per-group backend that boot instantiated via
			// instantiateGroupWithConfig and registered with the DataGroupManager
			// — i.e. the exact object the router dispatches non-group-0 PUTs to.
			// candidateGroupsFor excludes group-0 from placement, so group-1 is a
			// representative serving group.
			dg := state.dgMgr.Get("group-1")
			require.NotNil(t, dg, "boot must instantiate group-1 as a serving group")
			require.NotNil(t, dg.Backend(), "group-1 must have a wired backend")

			assert.Equal(t, tc.want, dg.Backend().PutPipelineMultiNodeEnabled(),
				"per-group serving backend multi-node flag must track the env gate")
		})
	}
}
