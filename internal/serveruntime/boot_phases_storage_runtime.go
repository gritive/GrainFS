package serveruntime

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/badgerrole"
	"github.com/gritive/GrainFS/internal/cache/shardcache"
	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/metrics/readamp"
	"github.com/gritive/GrainFS/internal/transport"
)

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
	clusterSize := 1 + len(state.peers)
	seedGroups := seedGroupCountForClusterSize(clusterSize)
	state.effectiveEC = cluster.AutoECConfigForClusterSize(clusterSize)
	if !state.effectiveEC.IsActive(clusterSize) {
		return fmt.Errorf("no effective EC profile for cluster size %d", clusterSize)
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

	if !state.joinMode && len(state.metaRaft.FSM().ShardGroups()) == 0 {
		if err := WaitForMetaRaftLeader(ctx, state.metaRaft, 15*time.Second); err != nil {
			return err
		}
		addNodeCtx, addNodeCancel := context.WithTimeout(ctx, 10*time.Second)
		if err := state.metaRaft.ProposeAddNode(addNodeCtx, cluster.MetaNodeEntry{ID: state.nodeID, Address: state.raftAddr, Role: 0}); err != nil {
			log.Debug().Err(err).Str("node_id", state.nodeID).Str("addr", state.raftAddr).Msg("seed node metadata propose failed (non-fatal)")
		}
		addNodeCancel()

		if err := SeedInitialShardGroups(ctx, state.metaRaft, state.nodeID, state.raftAddr, state.peers, seedGroups, normalGroupVoters); err != nil {
			return err
		}
		if err := WaitForShardGroupCount(ctx, state.metaRaft.FSM(), seedGroups, 30*time.Second); err != nil {
			return err
		}
		state.clusterRouter.Sync(state.metaRaft.FSM().BucketAssignments())
		state.clusterRouter.SetRequireExplicitAssignments(true)
	}

	shardSvcOpts := []cluster.ShardServiceOption{cluster.WithEncryptor(state.cfg.Encryptor)}
	if state.cfg.DirectIO {
		shardSvcOpts = append(shardSvcOpts, cluster.WithDirectIO())
		log.Info().Msg("direct I/O enabled for local shard writes (page cache bypass)")
	}
	if state.cfg.ShardPackThreshold > 0 {
		shardSvcOpts = append(shardSvcOpts, cluster.WithShardPackThreshold(state.cfg.ShardPackThreshold))
		log.Info().Int("threshold", state.cfg.ShardPackThreshold).Msg("cluster shard pack requested")
	}
	if state.cfg.MeasureReadAmp {
		readamp.Enable()
		log.Info().Msg("read-amplification simulator enabled — see grainfs_readamp_* counters at /metrics")
	}
	shardSvcOpts = append(shardSvcOpts, cluster.WithNodeAddressBook(state.metaRaft.FSM()))
	state.shardSvc = cluster.NewShardService(state.cfg.DataDir, state.quicTransport, shardSvcOpts...)
	return nil
}

// bootStreamRouter sets up the QUIC stream multiplexer (Raft RPCs on the
// Control stream, Shard RPCs on the Data stream) and registers the body
// handlers that consume body streams directly off the QUIC transport. Then
// fires raft.Node.Start to begin the apply loop on the data-plane raft.
//
// The body handler registration is critical: without it, every
// StreamShardWriteBody falls through the catch-all router (router.Dispatch
// only sees per-message handlers, not body streams), the stream closes
// without a response, and the caller sees "decode response: read header:
// EOF". The pre-2024-fix bug here meant N×replication produced only the
// leader's local copy.
//
// node.Start fires the data-plane raft apply loop. After this returns,
// distBackend.RunApplyLoop (started in bootOwnedGroupsAndEC) will see
// applied entries flow.
func bootStreamRouter(state *bootState) error {
	state.streamRouter = transport.NewStreamRouter()
	state.streamRouter.Handle(transport.StreamData, state.shardSvc.HandleRPC())
	state.quicTransport.SetStreamHandler(state.streamRouter.Dispatch)
	state.quicTransport.HandleBody(transport.StreamShardWriteBody, state.shardSvc.HandleWriteBody())
	state.quicTransport.HandleRead(transport.StreamShardReadBody, state.shardSvc.HandleReadBody())
	// Phase B1: node-level append-segment peer-fetch handler. Each node
	// hosts multiple group backends — the request payload carries groupID
	// so the handler resolves the right per-group root via DataGroupManager.
	// Lookups happen lazily at RPC time, so it's fine that groups are added
	// to the manager AFTER this registration (bootOwnedGroupsAndEC).
	cluster.RegisterAppendSegmentHandler(state.quicTransport, dataGroupAppendSegmentLookup{m: state.dgMgr})

	state.node.Start()
	// state.node.Close() goes through the cluster.RaftNode interface.
	state.AddCleanup(func() { state.node.Close() })
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
	cfg := cluster.AutoECConfigForClusterSize(clusterSize)
	if cfg.IsActive(clusterSize) {
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
	return gb, nil
}

func bootOwnedGroupsAndEC(ctx context.Context, state *bootState, recordStartupDecision func(badgerrole.Decision)) error {
	// group-0 main backend: FSM state lives in the per-node shared FSM-state
	// DB under the "group-0" keyspace prefix (C2 P3). The shared DB is owned
	// by bootOpenSharedFSMDB; this backend opens in shared mode (Close no-ops
	// the DB close).
	distBackend, err := cluster.NewDistributedBackendForGroup(state.cfg.DataDir, state.sharedFSMDB, state.node, "group-0")
	if err != nil {
		return fmt.Errorf("failed to initialize distributed storage: %w", err)
	}
	state.distBackend = distBackend

	allNodes := runtimeTopologyNodes(state.nodeID, state.raftAddr, state.peers, state.metaRaft.FSM().Nodes())
	distBackend.SetShardService(state.shardSvc, allNodes)

	state.shardCache = shardcache.New(state.cfg.ShardCacheSize)
	distBackend.SetShardCache(state.shardCache)
	log.Info().Int64("bytes", state.cfg.ShardCacheSize).Msg("ec shard cache configured")

	group0Backend := cluster.WrapDistributedBackend("group-0", distBackend)
	group0 := cluster.NewDataGroupWithBackend(
		"group-0",
		SeedShardGroupVoters(state.nodeID, state.raftAddr, state.peers, state.metaRaft.FSM().Nodes(), "group-0", 3),
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
		if state.joinMode {
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
			FSMStore:         state.sharedFSMDB,
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

	state.loadReporterStor = cluster.NewNodeStatsStore(cluster.DefaultLoadReportInterval * 3)
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
