package serveruntime

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/badgerrole"
	"github.com/gritive/GrainFS/internal/cache/shardcache"
	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/gossip"
	"github.com/gritive/GrainFS/internal/metrics/readamp"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/storage/datawal"
	"github.com/gritive/GrainFS/internal/storage/directio"
	"github.com/gritive/GrainFS/internal/transport"
)

// warnIfReducedDataFsync emits a loud startup warning when the data-plane fsync
// policy (set at directio init from GRAINFS_FSYNC_MODE; default SyncFull) has
// been weakened. The knob is env-only (measurement / dev), so this is the only
// operator-visible signal that durability is reduced. SyncFull (the default)
// logs nothing.
func warnIfReducedDataFsync() {
	switch directio.CurrentSyncMode() {
	case directio.SyncFast:
		log.Warn().Msg("GRAINFS_FSYNC_MODE=fast: data-plane fsync skips F_FULLFSYNC (no power-loss durability on macOS; no-op vs full on Linux)")
	case directio.SyncOff:
		log.Warn().Msg("GRAINFS_FSYNC_MODE=off: data-plane fsync DISABLED; durability relies on cross-node EC reconstruction — UNSAFE for single-node and correlated power loss")
	}
}

// bootShardService completes the cluster topology bootstrap and constructs
// the ShardService for distributed data replication.
//
// Cluster bootstrap (non-join mode only): wait for the meta-raft leader,
// propose this node's MetaNodeEntry into the FSM, seed the initial shard
// groups, and wait for them to commit. Sync the router with whatever bucket
// assignments the FSM has so far, then flip the router into "explicit-only"
// mode (post-seed: every bucket must have an explicit assignment).
//
// ShardService: applies cfg-driven options (DirectIO, MeasureReadAmp,
// AddressBook), then constructs the service. effectiveEC is captured here
// because seedGroups (cluster size * 4, min 8) is the durability headroom
// computation that downstream phases need to reuse for per-group EC.
func bootShardService(ctx context.Context, state *bootState) error {
	// Warn if the data-plane fsync policy (GRAINFS_FSYNC_MODE) has been weakened.
	warnIfReducedDataFsync()
	// Open the data WAL before any cluster shard service is constructed so that
	// (a) WithDataWAL receives a live appender, and (b) RecoverDataWAL runs
	// before bootShardRoutes registers transport handlers that would otherwise
	// surface partially-recovered state to peers.
	state.dataWALDir = filepath.Join(state.cfg.DataDir, "datawal")
	sealer, err := dataWALSealerForState(state)
	if err != nil {
		return err
	}
	dw, err := datawal.Open(state.dataWALDir, sealer, datawal.NamespaceShard)
	if err != nil {
		return fmt.Errorf("open data WAL: %w", err)
	}
	state.dataWAL = dw
	state.AddCleanup(func() { _ = dw.Close() })

	clusterSize := 1 + len(state.peers)
	// Option B (uniform genesis seeding): a solo genesis node that declares a
	// target size via --bootstrap-expect-nodes derives its EC width and seed
	// group count from the DECLARED target N, not the solo size of 1, so every
	// boot consumer (balancer/backend/DEK/router) latches the final uniform EC.
	// The actual seed is deferred to seed-on-quorum in the post-join hook.
	deferGenesisSeed := !state.inviteJoinMode &&
		len(state.metaRaft.FSM().ShardGroups()) == 0 &&
		state.cfg.BootstrapExpectNodes > 1 &&
		clusterSize == 1
	if deferGenesisSeed {
		clusterSize = state.cfg.BootstrapExpectNodes
	}
	seedGroups := seedGroupCountForClusterSize(clusterSize)

	var ecWidth int
	if clusterSize == 1 {
		ecWidth = len(state.cfg.DataDirs)
	} else {
		ecWidth = clusterSize
	}
	state.effectiveEC = cluster.AutoECConfigForClusterSize(ecWidth)

	var activeCheckSize int
	if clusterSize == 1 {
		activeCheckSize = len(state.cfg.DataDirs)
	} else {
		activeCheckSize = clusterSize
	}
	if !state.effectiveEC.IsActive(activeCheckSize) {
		return fmt.Errorf("no effective EC profile for dynamic EC width %d (cluster size %d, drive count %d)", ecWidth, clusterSize, len(state.cfg.DataDirs))
	}
	normalGroupVoters := state.effectiveEC.NumShards()

	if committed := state.metaRaft.Node().CommittedIndex(); committed > 0 {
		replayCtx, replayCancel := context.WithTimeout(ctx, 5*time.Second)
		if err := state.metaRaft.WaitApplied(replayCtx, committed); err != nil {
			replayCancel()
			return fmt.Errorf("wait for meta-raft replay before shard bootstrap: %w", err)
		}
		replayCancel()
	}

	// Genesis-only seed work. A zero-CA inviteJoinMode joiner must NOT seed shard
	// groups or wait to be its own meta-raft leader — it follows the existing
	// leader and receives shard groups via replication. inviteJoinMode skips
	// meta-raft bootstrap (boot_phases_raft.go), so its meta-raft has no local
	// leader; running this block would time out on WaitForMetaRaftLeader.
	if !state.inviteJoinMode && len(state.metaRaft.FSM().ShardGroups()) == 0 {
		if err := WaitForMetaRaftLeader(ctx, state.metaRaft, 15*time.Second); err != nil {
			return err
		}
		addNodeCtx, addNodeCancel := context.WithTimeout(ctx, 10*time.Second)
		if err := state.metaRaft.ProposeAddNode(addNodeCtx, cluster.MetaNodeEntry{ID: state.nodeID, Address: state.raftAddr, Role: 0}); err != nil {
			log.Debug().Err(err).Str("node_id", state.nodeID).Str("addr", state.raftAddr).Msg("seed node metadata propose failed (non-fatal)")
		}
		addNodeCancel()

		if deferGenesisSeed {
			// Defer the seed until --bootstrap-expect-nodes have joined. Leave the
			// router closed (no groups to route to) so writes are rejected until
			// seed-on-quorum fires in the leader-side post-join hook
			// (handleDeferredSeed). The "pending" condition is DERIVED there from
			// the flag + replicated group count + live nodes, so no in-process flag
			// is needed and the decision survives a leader change mid-bootstrap.
			log.Warn().
				Int("expect_nodes", state.cfg.BootstrapExpectNodes).
				Int("seed_groups", seedGroups).
				Int("k", state.effectiveEC.DataShards).
				Int("m", state.effectiveEC.ParityShards).
				Msg("Option B: deferring genesis shard-group seed until target node count joins (uniform EC)")
		} else {
			if err := SeedInitialShardGroups(ctx, state.metaRaft, state.nodeID, state.raftAddr, state.peers, seedGroups, normalGroupVoters); err != nil {
				return err
			}
			if err := WaitForShardGroupCount(ctx, state.metaRaft.FSM(), seedGroups, 30*time.Second); err != nil {
				return err
			}
			state.clusterRouter.Sync(state.metaRaft.FSM().BucketAssignments())
			state.clusterRouter.SetRequireExplicitAssignments(true)
		}
	}

	state.dataWALRepairCollector = cluster.NewDataWALRepairCollector()
	shardSvcOpts := []cluster.ShardServiceOption{
		cluster.WithDataWAL(state.dataWAL),
		// Single-node (ParityShards==0) has no EC redundancy, so a large
		// metadata-only shard write must fsync the shard file directly — read
		// live so a later EC reconfig is honored.
		cluster.WithNoRedundancy(func() bool { return state.effectiveEC.ParityShards == 0 }),
		cluster.WithDataWALRepairSink(state.dataWALRepairCollector),
	}
	if state.cfg.DirectIO {
		shardSvcOpts = append(shardSvcOpts, cluster.WithDirectIO())
		log.Info().Msg("direct I/O enabled for local shard writes (page cache bypass)")
	}
	// Shard-packing is disabled (S3): a durable pack index was never built, so
	// per-blob fsync cannot replace the WAL-replay-reconstructed index. Refuse to
	// boot when it is explicitly enabled, via the flag/config or the env var, so
	// no operator silently relies on packing.
	if state.cfg.ShardPackThreshold > 0 {
		return fmt.Errorf("shard-packing is disabled (--shard-pack-threshold=%d): a durable pack index is not implemented; set it to 0 to boot", state.cfg.ShardPackThreshold)
	}
	if env := os.Getenv("GRAINFS_SHARD_PACK_THRESHOLD"); env != "" {
		if n, perr := strconv.Atoi(env); perr == nil && n > 0 {
			return fmt.Errorf("shard-packing is disabled (GRAINFS_SHARD_PACK_THRESHOLD=%d): a durable pack index is not implemented; unset the env to boot", n)
		}
	}
	if state.cfg.MeasureReadAmp {
		readamp.Enable()
		log.Info().Msg("read-amplification simulator enabled — see grainfs_readamp_* counters at /metrics")
	}
	shardSvcOpts = append(shardSvcOpts, cluster.WithNodeAddressBook(state.metaRaft.FSM()))
	if state.dekKeeper != nil && len(state.clusterID) != 16 {
		return fmt.Errorf("bootShardService: DEK keeper wired but clusterID is %d bytes (want 16)", len(state.clusterID))
	}
	shardSvcOpts = append(shardSvcOpts, cluster.WithShardDEKKeeper(state.dekKeeper, state.clusterID))
	state.shardSvc = cluster.NewMultiRootShardService(state.cfg.DataDirs, state.clusterTransport, shardSvcOpts...)
	// Stop the shard-pack actor goroutine (spawned when a WAL is wired) on
	// shutdown. Registered after the data WAL cleanup so the LIFO cleanup stack
	// closes the shard service first — the actor must not write into a WAL that
	// has already been closed.
	state.AddCleanup(func() { _ = state.shardSvc.Close() })

	// Replay the data WAL into the shard service before bootShardRoutes
	// registers transport handlers; this keeps peers from observing partially-
	// recovered local shard state.
	if state.shardSvc != nil {
		if err := state.shardSvc.RecoverDataWAL(ctx); err != nil {
			return fmt.Errorf("recover shard data WAL: %w", err)
		}
	}
	return nil
}

func dataWALSealerForState(state *bootState) (datawal.RecordSealer, error) {
	switch {
	case state.dekKeeper != nil:
		if len(state.clusterID) != 16 {
			return nil, fmt.Errorf("data WAL DEK sealer requires 16-byte clusterID, got %d", len(state.clusterID))
		}
		return storage.NewDEKKeeperAdapter(state.dekKeeper, state.clusterID), nil
	default:
		return nil, nil
	}
}

// bootShardRoutes registers the shard data-plane handlers on the cluster
// transport's native routes. (raft.Node.Start moved to run.go, immediately
// after the Raft RPC bridge wiring: it must precede invite-join Phase-2 or the
// joiner deadlocks on the leader's AddVoter AppendEntries — see the comment at
// the Start site.)
func bootShardRoutes(state *bootState) error {
	// Native /shard/rpc buffered route — carries every buffered shard op
	// (Write/Read/ReadRange/Delete/quorum-meta/shadow-meta/Ping).
	state.clusterTransport.RegisterBufferedRoute(transport.RouteShardRPC, state.shardSvc.NativeRPCHandler())
	// Native /shard/write route (Phase 8 N6).
	state.clusterTransport.RegisterShardWriteHandler(state.shardSvc.NativeWriteHandler())
	// Native /shard/read route (Phase 8 N7-1).
	state.clusterTransport.RegisterShardReadHandler(state.shardSvc.NativeReadHandler())
	// Phase B1: node-level append-segment peer-fetch handler. Each node
	// hosts multiple group backends — the request payload carries groupID
	// so the handler resolves the right per-group root via DataGroupManager.
	// Lookups happen lazily at RPC time, so it's fine that groups are added
	// to the manager AFTER this registration (bootOwnedGroupsAndEC).
	cluster.RegisterAppendSegmentHandler(state.clusterTransport, dataGroupAppendSegmentLookup{m: state.dgMgr})

	return nil
}

// dataGroupAppendSegmentLookup adapts *cluster.DataGroupManager to
// cluster.RegisterAppendSegmentHandler's lookup interface, exposing the
// embedded *DistributedBackend for each known groupID.
type dataGroupAppendSegmentLookup struct{ m *cluster.DataGroupManager }

func (a dataGroupAppendSegmentLookup) Backend(groupID string) *cluster.DistributedBackend {
	if a.m == nil {
		return nil
	}
	dg := a.m.Get(groupID)
	if dg == nil {
		return nil
	}
	gb := dg.Backend()
	if gb == nil {
		return nil
	}
	return gb.DistributedBackend
}

func runtimeTopologyNodes(selfNodeID, selfAddr string, seedPeers []string, nodes []cluster.MetaNodeEntry) []string {
	if len(nodes) == 0 {
		return append([]string{selfAddr}, seedPeers...)
	}

	out := make([]string, 0, len(nodes)+1)
	seen := make(map[string]struct{}, len(nodes)+1)
	add := func(addr string) {
		if addr == "" {
			return
		}
		if _, ok := seen[addr]; ok {
			return
		}
		seen[addr] = struct{}{}
		out = append(out, addr)
	}
	add(selfAddr)
	for _, node := range nodes {
		if node.ID == selfNodeID {
			add(selfAddr)
			continue
		}
		add(node.Address)
	}
	if len(out) == 0 {
		return append([]string{selfAddr}, seedPeers...)
	}
	return out
}

func ecConfigForShardGroup(entry cluster.ShardGroupEntry, fallback cluster.ECConfig) cluster.ECConfig {
	cfg := cluster.DesiredECConfigForGroup(entry)
	if cfg.IsActive(len(entry.PeerIDs)) {
		return cfg
	}
	return fallback
}

func refreshRuntimeTopologyFromMetaNodes(state *bootState, nodes []cluster.MetaNodeEntry) {
	clusterSize := len(nodes)
	if clusterSize < 1 {
		clusterSize = 1
	}
	var ecWidth int
	if clusterSize == 1 {
		ecWidth = len(state.cfg.DataDirs)
	} else {
		ecWidth = clusterSize
	}
	cfg := cluster.AutoECConfigForClusterSize(ecWidth)

	var activeCheckSize int
	if clusterSize == 1 {
		activeCheckSize = len(state.cfg.DataDirs)
	} else {
		activeCheckSize = clusterSize
	}
	if cfg.IsActive(activeCheckSize) {
		state.effectiveEC = cfg
		if state.distBackend != nil {
			allNodes := runtimeTopologyNodes(state.nodeID, state.raftAddr, state.peers, nodes)
			state.distBackend.SetClusterTopology(allNodes, cfg)
		}
		if state.clusterCoord != nil {
			state.clusterCoord.WithECConfig(cfg)
		}
		log.Info().
			Int("cluster_size", clusterSize).
			Int("drive_count", len(state.cfg.DataDirs)).
			Int("effective_k", cfg.DataShards).
			Int("effective_m", cfg.ParityShards).
			Msg("cluster EC refreshed from meta topology")
	}
}

// ownedGroupsState is the per-group multi-raft tracking struct. Held outside
// bootOwnedGroupsAndEC because the shutdown hook (registered via AddCleanup
// inside the phase) needs to access it after the phase returns.
type ownedGroupsState struct {
	mu           sync.Mutex
	wg           sync.WaitGroup
	m            map[string]*cluster.GroupBackend
	inFlight     map[string]bool
	shuttingDown bool
}

// bootOwnedGroupsAndEC is the largest storage runtime phase. It wires the
// distributed backend, the legacy group-0 wrapper, the shard cache, the
// rebalancer, the per-group multi-raft instantiation loop, and the
// shutdown hook that drains every owned group on cleanup.
//
// Phase ordering is critical here:
//  1. NewDistributedBackend → SetShardService → SetShardCache → group-0
//     wrap → dgMgr.Add. group-0 has the legacy single-backend semantics.
//  2. distBackend.SetRouter + SetShardGroupSource. Routing wired before
//     any per-group instantiation can fire.
//  3. Rebalancer + SetOnRebalancePlan callback (race-free: meta-raft is
//     already Started by PR 4, so this MUST register before any rebalance
//     plan apply commits — the callback is set before any future plan
//     proposal.).
//  4. ownedGroups loop: synchronous cold-start instantiation for entries
//     already in FSM (records startup decisions on Badger role failures
//     before the server accepts traffic), then SetOnShardGroupAdded
//     callback for runtime entries replayed/applied later.
//  5. Shutdown hook: nil out the OnShardGroupAdded callback first to stop
//     accepting new instantiations, mark shuttingDown, wait for in-flight
//     goroutines, then close every owned group in parallel with 5s timeout.
//
// recordStartupDecision is plumbed via parameter (rather than method on
// bootState) because it's a closure over startupDecisions that the run.go
// body reads for the boot summary; passing it in keeps the phase signature
// honest about what's mutated.

// instantiateGroupWithConfig is the canonical entry point for booting a
// per-server GroupBackend in this runtime. It bundles InstantiateLocalGroup
// with the post-instantiation runtime configuration (currently
// SetCoalesceConfig for --append-size-cap-bytes propagation), preventing the
// wiring drift that commit 1895f2d2 fixed. Add additional per-group runtime
// config here so new flags reach every group, including dynamically-
// instantiated ones.
func (state *bootState) instantiateGroupWithConfig(glc cluster.GroupLifecycleConfig, entry cluster.ShardGroupEntry) (*cluster.GroupBackend, error) {
	gb, err := cluster.InstantiateLocalGroup(glc, entry)
	if err != nil {
		return nil, err
	}
	gb.SetCoalesceConfig(state.coalesceCfg)
	gb.SetShardGroupSource(state.metaRaft.FSM())
	return gb, nil
}

func bootOwnedGroupsAndEC(ctx context.Context, state *bootState, recordStartupDecision func(badgerrole.Decision)) error {
	// group-0 main backend: FSM state lives in the per-node shared FSM-state
	// DB under the "group-0" keyspace prefix (C2 P3). The shared DB is owned
	// by bootOpenSharedFSMDB; this backend opens in shared mode (Close no-ops
	// the DB close).
	distBackend, err := cluster.NewDistributedBackendForGroup(state.cfg.DataDir, state.sharedFSMStore, state.node, "group-0")
	if err != nil {
		return fmt.Errorf("failed to initialize distributed storage: %w", err)
	}
	state.distBackend = distBackend
	// Close stops the coalesce worker + backstop scanner goroutines; in
	// shared mode it never touches the store. Registered AFTER the shared
	// FSM DB's cleanup, so LIFO order stops these goroutines BEFORE the DB
	// they read closes (otherwise they outlive shutdown and can panic on a
	// closed BadgerDB — pre-existing gap surfaced by the Phase 6.5 S3 review).
	state.AddCleanup(func() { _ = distBackend.Close() })

	allNodes := runtimeTopologyNodes(state.nodeID, state.raftAddr, state.peers, state.metaRaft.FSM().Nodes())
	distBackend.SetShardService(state.shardSvc, allNodes)

	state.shardCache = shardcache.New(state.cfg.ShardCacheSize)
	distBackend.SetShardCache(state.shardCache)
	log.Info().Int64("bytes", state.cfg.ShardCacheSize).Msg("ec shard cache configured")

	group0Backend := cluster.WrapDistributedBackend("group-0", distBackend)
	group0 := cluster.NewDataGroupWithBackend(
		"group-0",
		SeedShardGroupVoters(state.nodeID, state.raftAddr, state.peers, liveNonRevokedNodes(state.metaRaft.FSM()), "group-0", 3),
		group0Backend,
	)
	state.dgMgr.Add(group0)
	distBackend.SetRouter(state.clusterRouter)
	distBackend.SetShardGroupSource(state.metaRaft.FSM())

	rebalancerCfg := cluster.DefaultRebalancerConfig()
	rebalancer := cluster.NewRebalancer(state.nodeID, state.metaRaft, state.dgMgr, rebalancerCfg)
	rebalancer.SetGroupRebalancer(
		cluster.NewDataGroupPlanExecutor(state.nodeID, state.dgMgr, state.metaRaft.FSM(), state.metaRaft),
	)
	state.metaRaft.FSM().SetOnRebalancePlan(func(plan *cluster.RebalancePlan) {
		if state.inviteJoinMode {
			return
		}
		execCtx, execCancel := context.WithTimeout(ctx, rebalancerCfg.PlanTimeout)
		go func() {
			defer execCancel()
			if err := rebalancer.ExecutePlan(execCtx, plan); err != nil {
				log.Error().Err(err).Str("plan_id", plan.PlanID).Msg("rebalancer: ExecutePlan failed")
			}
		}()
	})
	go rebalancer.Run(ctx)
	state.rebalancer = rebalancer

	evacExec := cluster.NewDataGroupPlanExecutor(state.nodeID, state.dgMgr, state.metaRaft.FSM(), state.metaRaft)
	loadPick := func(groupID string, exclude map[string]struct{}) (string, bool) {
		members := map[string]struct{}{}
		if dg := state.dgMgr.Get(groupID); dg != nil {
			for _, p := range dg.PeerIDs() {
				members[p] = struct{}{}
			}
		}
		var candidates []string
		for _, n := range state.metaRaft.FSM().Nodes() {
			if _, isMember := members[n.ID]; isMember {
				continue
			}
			candidates = append(candidates, n.ID)
		}
		return cluster.PickHealthyExcluding(candidates, func(id string) (float64, bool) {
			if ls, ok := state.metaRaft.FSM().LoadSnapshot()[id]; ok {
				return ls.DiskUsedPct, true
			}
			return 0, false
		}, exclude)
	}
	evacuator := cluster.NewDataGroupEvacuator(state.nodeID, state.metaRaft.FSM(), state.dgMgr, evacExec, loadPick, 30*time.Second)
	state.metaRaft.FSM().SetOnNodeRevoked(func(string) { evacuator.Wake() })
	// Run UNCONDITIONALLY on every node (mirror the ungated `go rebalancer.Run(ctx)`).
	// LOAD-BEARING: data groups are led by invite-JOINED nodes, and only that group's
	// leader can issue the data-raft ChangeMembership that evicts a revoked voter.
	// Gating on joinMode would make joiner-led groups never evict. The single-node /
	// leads-no-foreign-group case is a true no-op via empty led-targets, not a boot gate.
	go evacuator.Run(ctx)
	state.evacuator = evacuator

	owned := &ownedGroupsState{
		m:        make(map[string]*cluster.GroupBackend),
		inFlight: make(map[string]bool),
	}
	state.stopApply = make(chan struct{})

	instantiateOwnedIfNeeded := func(entry cluster.ShardGroupEntry) error {
		if entry.ID == "group-0" {
			return nil
		}
		EnsureShardGroupPlaceholder(state.dgMgr, entry)
		groupNodeID, isVoter := cluster.NewShardGroupPeerSet(entry).MatchLocal(state.nodeID, state.raftAddr)
		if !isVoter {
			return nil
		}
		owned.mu.Lock()
		if owned.shuttingDown {
			owned.mu.Unlock()
			return nil
		}
		if _, ok := owned.m[entry.ID]; ok {
			owned.mu.Unlock()
			return nil
		}
		if owned.inFlight[entry.ID] {
			owned.mu.Unlock()
			return nil
		}
		owned.inFlight[entry.ID] = true
		owned.mu.Unlock()
		defer func() {
			owned.mu.Lock()
			delete(owned.inFlight, entry.ID)
			owned.mu.Unlock()
		}()

		glc := cluster.GroupLifecycleConfig{
			NodeID:           groupNodeID,
			DataDir:          state.cfg.DataDir,
			ShardSvc:         state.shardSvc,
			Transport:        state.groupRaftMux.ForGroup(entry.ID),
			AddrBook:         state.metaRaft.FSM(),
			EC:               ecConfigForShardGroup(entry, state.effectiveEC),
			ElectionTimeout:  state.cfg.RaftElectionTimeout,
			HeartbeatTimeout: state.cfg.RaftHeartbeatInterval,
			FSMStore:         state.sharedFSMStore,
		}
		gb, err := state.instantiateGroupWithConfig(glc, entry)
		if err != nil {
			return fmt.Errorf("group %s: instantiate local group: %w", entry.ID, err)
		}
		gb.SetShardCache(state.shardCache)
		// Register the group's raft handler on the per-server mux. As of M5
		// PR 29 the v1 dispatch is gone — the group's raft node is always
		// the cluster-layer v2 adapter, satisfying raft.RaftV2Handler.
		state.groupRaftMux.Register(entry.ID, gb.Node())
		state.dgMgr.Add(cluster.NewDataGroupWithBackend(entry.ID, entry.PeerIDs, gb))
		go gb.RunApplyLoop(state.stopApply)
		owned.mu.Lock()
		owned.m[entry.ID] = gb
		owned.mu.Unlock()
		log.Info().Str("group_id", entry.ID).Strs("peers", entry.PeerIDs).Msg("instantiateLocalGroup ok")
		return nil
	}

	scheduleOwnedInstantiation := func(entry cluster.ShardGroupEntry) {
		owned.mu.Lock()
		if owned.shuttingDown {
			owned.mu.Unlock()
			return
		}
		owned.wg.Add(1)
		owned.mu.Unlock()
		go func() {
			defer owned.wg.Done()
			if err := instantiateOwnedIfNeeded(entry); err != nil {
				log.Error().Err(HandleRuntimeGroupInstantiationError(entry.ID, err)).Str("group_id", entry.ID).Msg("runtime data group instantiation failed")
			}
		}()
	}

	state.metaRaft.FSM().SetOnShardGroupAdded(func(entry cluster.ShardGroupEntry) {
		scheduleOwnedInstantiation(entry)
	})
	for _, entry := range state.metaRaft.FSM().ShardGroups() {
		if err := instantiateOwnedIfNeeded(entry); err != nil {
			recordStartupDecision(badgerrole.Decision{
				Role:    badgerrole.RoleGroupState,
				GroupID: entry.ID,
				Status:  badgerrole.DecisionOpenFailed,
				Action:  badgerrole.RecoveryActionStartReadOnly,
				Reason:  err.Error(),
				Err:     err,
			})
		}
	}

	state.AddCleanup(func() {
		state.metaRaft.FSM().SetOnShardGroupAdded(nil)
		owned.mu.Lock()
		owned.shuttingDown = true
		owned.mu.Unlock()
		owned.wg.Wait()

		owned.mu.Lock()
		toClose := make([]*cluster.GroupBackend, 0, len(owned.m))
		for _, gb := range owned.m {
			toClose = append(toClose, gb)
		}
		owned.mu.Unlock()
		var wg sync.WaitGroup
		for _, gb := range toClose {
			wg.Add(1)
			go func(gb *cluster.GroupBackend) {
				defer wg.Done()
				if err := cluster.ShutdownLocalGroup(context.Background(), gb, 5*time.Second); err != nil {
					log.Warn().Err(err).Str("group_id", gb.ID()).Msg("shutdownLocalGroup")
				}
			}(gb)
		}
		wg.Wait()
	})

	state.loadReporterStor = gossip.NewNodeStatsStore(cluster.DefaultLoadReportInterval * 3)
	state.loadReporter = cluster.NewLoadReporter(state.nodeID, state.loadReporterStor, state.metaRaft, cluster.DefaultLoadReportInterval)
	go state.loadReporter.Run(ctx)

	distBackend.SetECConfig(state.effectiveEC)
	log.Info().
		Str("mode", "auto").
		Int("effective_k", state.effectiveEC.DataShards).
		Int("effective_m", state.effectiveEC.ParityShards).
		Bool("active", state.effectiveEC.IsActive(len(allNodes))).
		Int("cluster_size", len(allNodes)).Msg("cluster EC configured")

	return nil
}
