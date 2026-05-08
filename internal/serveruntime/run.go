package serveruntime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	hzserver "github.com/cloudwego/hertz/pkg/app/server"
	"github.com/google/uuid"
	"golang.org/x/time/rate"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/alerts"
	"github.com/gritive/GrainFS/internal/badgerrole"
	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/dashboard"
	"github.com/gritive/GrainFS/internal/eventstore"
	"github.com/gritive/GrainFS/internal/icebergcatalog"
	"github.com/gritive/GrainFS/internal/incident"
	"github.com/gritive/GrainFS/internal/incident/badgerstore"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/receipt"
	"github.com/gritive/GrainFS/internal/resourceguard"
	"github.com/gritive/GrainFS/internal/resourcewatch"
	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/server"
	"github.com/gritive/GrainFS/internal/server/admin"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/storage/pullthrough"
	"github.com/gritive/GrainFS/internal/storage/wal"
	"github.com/gritive/GrainFS/internal/transport"
	"github.com/gritive/GrainFS/internal/volume"
)

// Run is the cluster-mode server entry point. cmd/grainfs/runServe builds a
// Config from cobra flags and pre-resolves auth/encryptor inputs, then calls
// Run. Run owns lifecycle: starts every component, blocks on ctx.Done(), and
// orchestrates graceful shutdown.
func Run(ctx context.Context, cfg Config) error {
	addr := cfg.Addr
	dataDir := cfg.DataDir
	clusterKey := cfg.ClusterKey
	authOpts := cfg.AuthOpts
	raftAddrExplicit := cfg.RaftAddrExplicit

	// PR 1+2 (boot decomposition milestone, see docs/superpowers/specs/
	// 2026-05-08-serveruntime-boot-decomposition.md): bootState carries
	// per-phase artifacts; Cleanup runs LIFO at function exit; phase
	// functions populate state and register their teardown.
	state := newBootState(cfg)
	defer state.Cleanup()

	// PR 2 phases: config validation + storage open.
	if err := bootValidateConfig(state); err != nil {
		return err
	}
	if err := bootAutoMigrate(state); err != nil {
		return err
	}
	if err := bootOpenMetaDB(state); err != nil {
		return err
	}
	if err := bootValidateTimings(state); err != nil {
		return err
	}
	if err := bootOpenRaftLogStore(state); err != nil {
		return err
	}
	if err := bootOpenSharedRaftLogDB(state); err != nil {
		return err
	}

	// Locals mirror state fields so the rest of Run (PRs 3-6 will migrate
	// these too) keeps using the same names. After PR 6 these will be
	// gone and downstream phases will read state directly.
	nodeID := state.nodeID
	raftAddr := state.raftAddr
	peers := state.peers
	joinMode := cfg.JoinMode
	roleRegistry := state.roleRegistry
	db := state.db
	logStore := state.logStore
	startupDecisions := state.startupDecisions
	recordStartupDecision := func(decision badgerrole.Decision) {
		state.startupDecisions = append(state.startupDecisions, decision)
		startupDecisions = state.startupDecisions
	}

	// PR 3 transport phases.
	if err := bootQUICTransport(ctx, state); err != nil {
		return err
	}
	if err := bootPeerConnections(ctx, state); err != nil {
		return err
	}
	// groupRaftMux must exist BEFORE NewMetaTransportQUICMux (line ~190)
	// so the meta-raft transport auto-registers on construction. Moving
	// the mux phase here (vs after node creation, where it lived
	// pre-PR 3) preserves that invariant — nothing between here and
	// metaTransport reads the mux.
	if err := bootGroupRaftMux(state); err != nil {
		return err
	}
	quicTransport := state.quicTransport
	raftAddr = state.raftAddr // bootQUICTransport may have resolved 127.0.0.1:0 → bound port

	raftCfg := raft.DefaultConfig(nodeID, peers)
	raftCfg.ManagedMode = cfg.BadgerManagedMode
	raftCfg.LogGCInterval = cfg.RaftLogGCInterval
	node := raft.NewNode(raftCfg, logStore)
	if !joinMode {
		if err := node.Bootstrap(); err != nil && !errors.Is(err, raft.ErrAlreadyBootstrapped) {
			return fmt.Errorf("raft bootstrap: %w", err)
		}
	}
	state.node = node

	// Wire QUIC transport to Raft RPC layer
	rpcTransport := raft.NewQUICRPCTransport(quicTransport, node)
	rpcTransport.SetTransport()
	state.rpcTransport = rpcTransport

	// PR 4 raft phases. Order matters: callbacks (DataGroupRouter,
	// RotationAndAdminAPI) MUST register on metaRaft.FSM() BEFORE
	// bootMetaRaftStart fires the apply loop, otherwise the first apply
	// event races the SetOn* call. Phase ordering captures the invariant.
	if err := bootMetaRaftWiring(state); err != nil {
		return err
	}
	if err := bootDataGroupRouter(state); err != nil {
		return err
	}
	if err := bootRotationAndAdminAPI(state); err != nil {
		return err
	}
	if err := bootMetaRaftStart(ctx, state, StartRotationSocket); err != nil {
		return err
	}
	metaRaft := state.metaRaft
	dgMgr := state.dgMgr
	clusterRouter := state.clusterRouter
	iamAdminAPI := state.iamAdminAPI
	iamProposer := state.iamProposer

	// Seed data groups from cluster size only. Operators no longer choose this:
	// group count is placement headroom, not a durability policy. Computed
	// here (not in the storage phase) because the join-mode reconnect path
	// downstream re-uses seedGroups when waiting for shard group commits.
	clusterSize := 1 + len(peers)
	seedGroups := clusterSize * 4
	if seedGroups < 8 {
		seedGroups = 8
	}

	// PR 5 storage runtime phases. Order matters:
	//  1. bootShardService — completes cluster bootstrap (non-join), seeds
	//     groups, builds ShardService. Captures effectiveEC.
	//  2. bootStreamRouter — wires QUIC stream multiplexer (Control/Data),
	//     registers shard body handlers, starts the data-plane raft node.
	//  3. bootOwnedGroupsAndEC — wires distBackend, group-0 wrapper, shard
	//     cache, rebalancer, per-group multi-raft cold-start + runtime
	//     instantiation, LoadReporter, EC config; registers shutdown hook.
	if err := bootShardService(ctx, state); err != nil {
		return err
	}
	if err := bootStreamRouter(state); err != nil {
		return err
	}
	if err := bootOwnedGroupsAndEC(ctx, state, recordStartupDecision); err != nil {
		return err
	}
	shardSvc := state.shardSvc
	distBackend := state.distBackend
	shardCache := state.shardCache
	effectiveEC := state.effectiveEC
	stopApply := state.stopApply
	router := state.streamRouter

	// PR 6 services phase 1: snapshot manager + Restore + wrap chain (packblob
	// + cachedBackend) + s3-cache invalidator + go RunApplyLoop. Phase ordering
	// invariant: invalidator MUST register before the apply-loop goroutine
	// fires — otherwise FSM-replicated writes can land before invalidator
	// wiring and stale cache entries survive cross-node.
	if err := bootSnapshotAndApplyLoop(state); err != nil {
		return fmt.Errorf("failed to initialize distributed storage: %w", err)
	}
	fsm := state.fsm
	_ = state.cachedBackend
	_ = state.snapMgr

	// Start balancer if enabled (cluster mode only).
	var balancerProposer *cluster.BalancerProposer
	var gossipReceiver *cluster.GossipReceiver
	if cfg.BalancerEnabled {
		statsStore := cluster.NewNodeStatsStore(3 * cfg.BalancerGossipInterval)
		bopts := BalancerOptions{
			GossipInterval:      cfg.BalancerGossipInterval,
			WarmupTimeout:       cfg.BalancerWarmupTimeout,
			ImbalanceTriggerPct: cfg.BalancerImbalanceTriggerPct,
			ImbalanceStopPct:    cfg.BalancerImbalanceStopPct,
			MigrationRate:       cfg.BalancerMigrationRate,
			LeaderTenureMin:     cfg.BalancerLeaderTenureMin,
			CBThreshold:         cfg.BalancerCBThreshold,
			MigrationMaxRetries: cfg.BalancerMigrationMaxRetries,
			MigrationPendingTTL: cfg.BalancerMigrationPendingTTL,
		}
		var err error
		balancerProposer, gossipReceiver, err = StartBalancer(ctx, bopts, nodeID, dataDir, statsStore, node, peers, fsm, quicTransport, shardSvc, effectiveEC.NumShards())
		if err != nil {
			log.Warn().Err(err).Msg("balancer start failed")
		}
	}

	// Ensure a single GossipReceiver drains tr.Receive() whenever a feature
	// needs StreamReceipt gossip (heal-receipt's RoutingCache lives on this
	// path). Only one consumer is allowed because Receive() is a single
	// channel — competing readers would deliver each message to only one.
	// When balancer is off but heal-receipt is on, create a bare receiver;
	// its NodeStatsStore is unused in this path but required by the ctor.
	if gossipReceiver == nil && cfg.HealReceiptEnabled {
		standaloneStats := cluster.NewNodeStatsStore(3 * cfg.BalancerGossipInterval)
		gossipReceiver = cluster.NewGossipReceiver(quicTransport, standaloneStats)
		go gossipReceiver.Run(ctx)
		log.Info().Str("component", "gossip").Msg("gossip receiver started (receipt-only, balancer disabled)")
	}

	walDir := filepath.Join(dataDir, "wal")
	w, err := wal.Open(walDir)
	if err != nil {
		return fmt.Errorf("open WAL: %w", err)
	}
	state.AddCleanup(func() { w.Close() })

	// v0.0.7.1 PR-D: Live multi-raft routing — ClusterCoordinator + ForwardSender/Receiver.
	// ClusterCoordinator implements storage.Backend and routes bucket-scoped ops to the
	// correct group leader via ForwardSender. 0x08 handler (ForwardReceiver) receives
	// forwarded calls on voter nodes and dispatches to local GroupBackend.
	forwardDialer := func(callCtx context.Context, peer string, payload []byte) ([]byte, error) {
		msg := &transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload}
		reply, err := quicTransport.Call(callCtx, peer, msg)
		if err != nil {
			return nil, err
		}
		return reply.Payload, nil
	}
	forwardStreamDialer := func(callCtx context.Context, peer string, payload []byte, body io.Reader) ([]byte, error) {
		msg := &transport.Message{Type: transport.StreamGroupForwardBody, Payload: payload}
		reply, err := quicTransport.CallWithBody(callCtx, peer, msg, body)
		if err != nil {
			return nil, err
		}
		return reply.Payload, nil
	}
	forwardReadStreamDialer := func(callCtx context.Context, peer string, payload []byte) ([]byte, io.ReadCloser, error) {
		msg := &transport.Message{Type: transport.StreamGroupForwardRead, Payload: payload}
		reply, body, err := quicTransport.CallRead(callCtx, peer, msg)
		if err != nil {
			return nil, nil, err
		}
		return reply.Payload, body, nil
	}

	forwardSender := cluster.NewForwardSender(forwardDialer).
		WithStreamDialer(forwardStreamDialer).
		WithReadStreamDialer(forwardReadStreamDialer).
		WithLeaderHintResolver(func(hint string) string {
			if addr, ok := cluster.ResolveNodeAddress(metaRaft.FSM(), hint); ok {
				return addr
			}
			return hint
		})
	forwardReceiver := cluster.NewForwardReceiver(dgMgr)
	forwardReceiver.Register(shardSvc)

	metaForwardDialer := func(peer string, payload []byte) ([]byte, error) {
		msg := &transport.Message{Type: transport.StreamMetaProposeForward, Payload: payload}
		reply, err := quicTransport.Call(ctx, peer, msg)
		if err != nil {
			return nil, err
		}
		return reply.Payload, nil
	}
	metaForwardSender := cluster.NewMetaProposeForwardSender(metaForwardDialer)
	distBackend.SetBucketAssigner(cluster.NewForwardingBucketAssigner(metaRaft, func(ctx context.Context, command []byte) error {
		return metaForwardSender.Send(ctx, MetaProposalTargets(metaRaft.Node().LeaderID(), peers), command)
	}))
	metaForwardReceiver := cluster.NewMetaProposeForwardReceiver(metaRaft)
	router.Handle(transport.StreamMetaProposeForward, metaForwardReceiver.Handle)
	metaJoinReceiver := cluster.NewMetaJoinReceiver(metaRaft)
	router.Handle(transport.StreamMetaJoin, metaJoinReceiver.Handle)
	metaReadDialer := func(peer string, payload []byte) ([]byte, error) {
		msg := &transport.Message{Type: transport.StreamMetaCatalogRead, Payload: payload}
		reply, err := quicTransport.Call(ctx, peer, msg)
		if err != nil {
			return nil, err
		}
		return reply.Payload, nil
	}
	metaReadSender := cluster.NewMetaCatalogReadSender(metaReadDialer)

	clusterCoord := cluster.NewClusterCoordinator(
		distBackend,    // base for cluster-wide ops (CreateBucket, etc.)
		dgMgr,          // local owned groups (self-leader shortcut)
		clusterRouter,  // bucket → group lookup
		metaRaft.FSM(), // ShardGroupSource (PeerIDs, leader hints)
		nodeID,         // selfID for leader check
	).WithForwardSender(forwardSender).
		WithNodeAddressResolver(metaRaft.FSM()).
		WithSelfPeerAlias(raftAddr).
		WithECConfig(effectiveEC).
		WithObjectIndexProposer(cluster.NewForwardingObjectIndexProposer(metaRaft, func(ctx context.Context, command []byte) error {
			return metaForwardSender.Send(ctx, MetaProposalTargets(metaRaft.Node().LeaderID(), peers), command)
		}))
	metaReadReceiver := cluster.NewMetaCatalogReadReceiver(cluster.NewMetaCatalog(metaRaft, clusterCoord, "s3://grainfs-tables/warehouse"))
	router.Handle(transport.StreamMetaCatalogRead, metaReadReceiver.Handle)
	if joinMode {
		if err := PerformMetaJoin(ctx, quicTransport, peers, nodeID, raftAddr); err != nil {
			return err
		}
		if err := WaitForShardGroupCount(ctx, metaRaft.FSM(), seedGroups, 30*time.Second); err != nil {
			return err
		}
		clusterRouter.Sync(metaRaft.FSM().BucketAssignments())
		clusterRouter.SetRequireExplicitAssignments(true)
	}

	// Use ClusterCoordinator as the primary backend for S3, NFSv4, NBD, then
	// wrap it with WAL so routed object mutations are captured for PITR.
	var backend storage.Backend = wal.NewBackend(clusterCoord, w)
	log.Info().Msg("v0.0.7.1 PR-D: ClusterCoordinator wired — live multi-raft routing enabled")

	// Wrap with pull-through cache. Resolver is IAM-backed: at request time it
	// looks up the per-bucket BucketUpstream record. With zero records configured,
	// the resolver always returns (nil, false) and the backend behaves like the
	// bare local backend — no opt-in flag needed.
	//
	// Per /plan-eng-review override A10 — fail-fast at startup if cfg.IAMStore is
	// nil. This guards against future construction-order regressions: NewIAMResolver
	// requires a non-nil store and would panic on first request otherwise.
	if cfg.IAMStore == nil {
		return fmt.Errorf("pullthrough: IAMStore required (cfg.IAMStore is nil)")
	}
	backend = pullthrough.NewBackend(backend, pullthrough.NewIAMResolver(cfg.IAMStore))
	log.Info().Msg("pull-through cache enabled (IAM-backed resolver)")
	startupResult := badgerrole.ReduceStartupDecisions(roleRegistry, startupDecisions)
	for _, decision := range startupResult.Decisions {
		log.Info().
			Str("role", string(decision.Role)).
			Str("group_id", decision.GroupID).
			Str("status", string(decision.Status)).
			Dur("probe_duration", decision.ProbeDuration).
			Msg("badger role startup probe")
	}
	startupReadOnly := startupResult.Mode == badgerrole.StartupModeReadOnly
	if startupResult.Mode == badgerrole.StartupModeBlocked {
		return fmt.Errorf("badger startup recovery blocked server start: %v", startupResult.BlockedReasons)
	}
	recoveryReadOnly := startupReadOnly
	if startupReadOnly {
		backend = storage.NewRecoveryWriteGate(backend, storage.ErrRecoveryWriteDisabled)
		log.Warn().Strs("reasons", startupResult.ReadOnlyReasons).Msg("badger startup recovery read-only gate enabled")
	}
	if marker, err := cluster.LoadRecoverClusterMarker(dataDir); err != nil {
		return fmt.Errorf("load recovery marker: %w", err)
	} else if marker != nil && !marker.Writable {
		recoveryReadOnly = true
		if !startupReadOnly {
			backend = storage.NewRecoveryWriteGate(backend, storage.ErrRecoveryWriteDisabled)
		}
		log.Warn().Str("marker", filepath.Join(dataDir, cluster.RecoverClusterMarkerPath)).Msg("recovered cluster write gate enabled")
	}

	// DiskCollector exposes grainfs_disk_used_pct metric. In multi-node mode
	// the balancer owns its own collector; in singleton mode nothing else
	// would emit disk stats. Register unconditionally — duplicate registration
	// is guarded inside NewDiskCollector.
	diskCollector := cluster.NewDiskCollector(nodeID, dataDir, nil, 30*time.Second)
	// Wiring of OnThreshold + Run() happens after clusterAlerts is built (below)
	// so the callback can dispatch critical webhooks on transitions.

	// Auto-create "default" bucket only for singleton startup. In cluster mode,
	// bucket creation is a cluster-wide metadata operation and must be driven by
	// an explicit client/API action, not repeated independently by every node.
	if ShouldCreateDefaultBucketOnStartup(peers, recoveryReadOnly) {
		if err := CreateDefaultBucketWithRetry(ctx, backend, 30*time.Second); err != nil {
			return fmt.Errorf("create default bucket: %w", err)
		}
	}

	// Start auto-snapshotter for object-level PITR snapshots (separate from
	// Raft snapshots above). Uses the WAL-wrapped backend so replay is
	// anchored to the object mutation log. Start only after startup bucket
	// metadata exists and the routed snapshot enumeration path is usable; data
	// groups are instantiated asynchronously during boot.
	if err := StartAutoSnapshotterWhenReady(ctx, dataDir, walDir, backend, cfg.SnapInterval, cfg.SnapRetain, 30*time.Second); err != nil {
		log.Warn().Err(err).Msg("auto-snapshot init failed")
	}

	log.Info().Str("component", "server").Str("version", cfg.Version).
		Str("node_id", nodeID).Str("raft_addr", raftAddr).Strs("peers", peers).
		Str("addr", addr).Str("data", dataDir).Msg("server started")

	// Startup config snapshot — debug-level log of every flag-derived runtime
	// value. Useful for diffing against a known-good config when an operator
	// is comparing two installs or troubleshooting drift after a restart.
	LogStartupConfigSnapshot(cfg.FlagsSnapshot, addr, dataDir, nodeID, raftAddr, peers)

	clusterAlerts := server.NewAlertsState(cfg.AlertWebhook, alerts.Options{Secret: cfg.AlertSecret}, alerts.DegradedConfig{})

	// Wire predictive disk warnings into the collector now that clusterAlerts
	// exists. Thresholds are taken as fractions on the flag (more natural for
	// operators) but DiskCollector works in percent.
	diskCollector.SetThresholds(cfg.DiskWarnFrac*100, cfg.DiskCritFrac*100)
	diskCollector.SetOnThreshold(func(level cluster.DiskThresholdLevel, pct float64, availBytes uint64) {
		// Webhook send may block on retries — dispatch in a goroutine so the
		// collect loop is never delayed.
		switch level {
		case cluster.DiskLevelCritical:
			log.Warn().Float64("pct", pct).Uint64("avail_bytes", availBytes).Msg("disk usage CRITICAL")
			go func() {
				_ = clusterAlerts.Send(alerts.Alert{
					Type:     "disk_critical",
					Severity: alerts.SeverityCritical,
					Resource: nodeID,
					Message:  fmt.Sprintf("disk used %.1f%% (avail %d bytes) on %s", pct, availBytes, dataDir),
				})
			}()
		case cluster.DiskLevelWarn:
			log.Warn().Float64("pct", pct).Uint64("avail_bytes", availBytes).Msg("disk usage warning")
			go func() {
				_ = clusterAlerts.Send(alerts.Alert{
					Type:     "disk_warn",
					Severity: alerts.SeverityWarning,
					Resource: nodeID,
					Message:  fmt.Sprintf("disk used %.1f%% (avail %d bytes) on %s", pct, availBytes, dataDir),
				})
			}()
		case cluster.DiskLevelOK:
			log.Info().Float64("pct", pct).Msg("disk usage recovered to normal")
		}
	})
	go diskCollector.Run(ctx)
	srvOpts := []server.Option{
		// cluster status / remove-peer must reflect *meta-raft* membership —
		// that is the cluster-wide membership ledger that `serve --join`
		// updates via performMetaJoin. The node initialised at line 872 is a
		// legacy per-process raft instance whose Peers() never grows on join.
		server.WithClusterInfo(NewRaftClusterInfo(metaRaft.Node(), peers, distBackend, metaRaft.FSM())),
		server.WithClusterMembership(NewRaftMembership(metaRaft.Node(), metaRaft.FSM())),
		server.WithEventStore(eventstore.New(db)),
		server.WithAlerts(clusterAlerts),
		server.WithDataDir(dataDir),
	}
	if len(peers) == 0 && !raftAddrExplicit && !joinMode {
		legacyStore := icebergcatalog.NewStore(db, "s3://grainfs-tables/warehouse")
		metaCatalog := cluster.NewMetaCatalog(metaRaft, backend, "s3://grainfs-tables/warehouse")
		if err := MigrateLegacySingletonIcebergCatalog(ctx, legacyStore, metaCatalog, backend); err != nil {
			return fmt.Errorf("migrate singleton Iceberg catalog: %w", err)
		}
		srvOpts = append(srvOpts, server.WithIcebergCatalog(metaCatalog))
	} else {
		metaForward := func(ctx context.Context, command []byte) error {
			return metaForwardSender.Send(ctx, MetaProposalTargets(metaRaft.Node().LeaderID(), peers), command)
		}
		metaReadTargets := func() []string {
			return MetaProposalTargets(metaRaft.Node().LeaderID(), peers)
		}
		srvOpts = append(srvOpts, server.WithIcebergCatalog(cluster.NewMetaCatalogWithForwarders(metaRaft, backend, "s3://grainfs-tables/warehouse", metaForward, metaReadSender, metaReadTargets)))
	}
	// Propagate S3 auth (IAM verifier + secret lookup) into cluster mode.
	// Previously the legacy --access-key/--secret-key wiring was local-only
	// and cluster mode silently ran without auth; the IAM-only model
	// shares the same authOpts across both code paths.
	srvOpts = append(srvOpts, authOpts...)
	if balancerProposer != nil {
		srvOpts = append(srvOpts, server.WithBalancerInfo(NewBalancerInfoAdapter(balancerProposer)))
	}

	// Phase 16 Week 5 Slice 2 — HealReceipt API + gossip + broadcast fallback.
	rcptPSK := cfg.HealReceiptPSK
	if rcptPSK == "" {
		rcptPSK = clusterKey
	}
	rcptOpts := ReceiptOptions{
		Enabled:        cfg.HealReceiptEnabled,
		PSK:            rcptPSK,
		Retention:      cfg.HealReceiptRetention,
		GossipInterval: cfg.HealReceiptGossipInterval,
		WindowSize:     cfg.HealReceiptWindow,
	}
	newSrvOpts, receiptWiring, err := SetupClusterReceipt(
		ctx, rcptOpts, dataDir, nodeID, peers,
		quicTransport, router, gossipReceiver, srvOpts,
	)
	if err != nil {
		return fmt.Errorf("heal-receipt wiring: %w", err)
	}
	srvOpts = newSrvOpts
	state.AddCleanup(func() { receiptWiring.Close() })

	var incidentRecorder *incident.Recorder
	incidentDB, incidentDecision, err := badgerrole.OpenRole(roleRegistry, badgerrole.RoleIncidentState, badgerrole.PathContext{DataDir: dataDir})
	if err != nil {
		if feature, ok := OptionalRoleDisabled(roleRegistry, incidentDecision); ok {
			LogOptionalRoleDisabled(badgerrole.RoleIncidentState, feature, err)
		} else {
			return fmt.Errorf("open incident db: %w", err)
		}
	} else {
		defer incidentDB.Close()
		incidentVlogEntry := resourcewatch.RegisterDB(resourcewatch.DBCategoryIncident, incidentDB)
		defer resourcewatch.DeregisterDB(incidentVlogEntry)
		incidentStore := badgerstore.New(incidentDB)
		incidentRecorder = incident.NewRecorder(incidentStore, incident.NewReducer())
		distBackend.SetIncidentRecorder(incidentRecorder)
		srvOpts = append(srvOpts, server.WithIncidentStore(incidentStore))
		guardDeps := resourceguard.Deps{
			NodeID:   nodeID,
			Alerts:   clusterAlerts,
			Recorder: incidentRecorder,
		}
		if cfg.FDWatchEnabled {
			resourceguard.StartFD(ctx, cfg.FDOpts, guardDeps)
		}
		if cfg.GoroutineWatchEnabled {
			resourceguard.StartGoroutine(ctx, cfg.GoroutineOpts, guardDeps)
		}
		if cfg.VlogWatchEnabled {
			resourceguard.StartVlog(ctx, cfg.VlogResourceGuardOpts, guardDeps)
		}
	}
	clusterIncidentRecorder, scrubberIncidentRecorder := IncidentRecorderInterfaces(incidentRecorder)

	// Slice 4 of refactor/unify-storage-paths: cluster-mode lifecycle.
	// Construct the manager before srv.New so the S3 PutBucketLifecycle API
	// can reuse the same config store the worker scans. The worker itself
	// runs leader-only — see LifecycleManager.Run.
	var lifecycleMgr *cluster.LifecycleManager
	if cfg.LifecycleInterval > 0 {
		lifecycleMgr = cluster.NewLifecycleManager(distBackend, cfg.LifecycleInterval)
		srvOpts = append(srvOpts, server.WithLifecycleStore(lifecycleMgr.Store()))
	}

	volMgr, blockCache, dedupDB, err := BuildVolumeManager(VolumeManagerOptions{DedupEnabled: cfg.DedupEnabled, BlockCacheSize: cfg.BlockCacheSize}, dataDir, backend)
	if err != nil {
		return fmt.Errorf("volume manager: %w", err)
	}
	if dedupDB != nil {
		defer dedupDB.Close()
		dedupVlogEntry := resourcewatch.RegisterDB(resourcewatch.DBCategoryDedup, dedupDB)
		defer resourcewatch.DeregisterDB(dedupVlogEntry)
	}
	srvOpts = append(srvOpts, server.WithVolumeManager(volMgr), server.WithBlockCache(blockCache), server.WithShardCache(shardCache))
	if !joinMode {
		srvOpts = append(srvOpts, server.WithReadIndexer(distBackend))
	}
	srvOpts = append(srvOpts, server.WithRaftSnapshotter(distBackend))

	distBackend.RegisterReadIndexHandler()
	distBackend.RegisterProposeForwardHandler()

	mutationGate := server.NewMutationGate(nil)
	if recoveryReadOnly {
		mutationGate.SetBlocked(storage.ErrRecoveryWriteDisabled)
	}
	srvOpts = append(srvOpts, server.WithMutationGate(mutationGate))

	srv := server.New(addr, backend, srvOpts...)

	// P5: wire IAM proposer so CreateBucket auto-issues an Admin grant to
	// the creator SA. nil-safe when iamProposer was not built above.
	if iamProposer != nil {
		srv.SetIAMProposer(iamProposer)
	}

	// --- Admin / dashboard wiring (Volume CLI Phase B) ---
	// Open the dashboard auth token (creates <data>/dashboard.token mode 0600
	// on first run). Install the middleware on /ui/* and register /ui/api/*
	// admin routes BEFORE the data-plane Hertz starts serving.
	tokenStore, err := dashboard.Open(filepath.Join(dataDir, "dashboard.token"))
	if err != nil {
		return fmt.Errorf("dashboard token: %w", err)
	}
	adminDeps := &admin.Deps{
		Manager:    srv.VolumeManager(),
		Token:      tokenStore,
		PublicURL:  cfg.PublicURL,
		NodeID:     nodeID,
		PeerHealth: NewPeerHealthAdapter(distBackend),
		VlogBreakdown: NewVlogBreakdownAdapter(VlogBreakdownOptions{
			Enabled:       cfg.VlogWatchEnabled,
			DataDir:       dataDir,
			WarnRatio:     cfg.VlogWarnRatio,
			CriticalRatio: cfg.VlogCriticalRatio,
		}),
		VolumePlacement: NewVolumePlacementAdapter(metaRaft),
	}
	dataHertz := srv.HertzEngine()
	dataHertz.Use(server.DashboardTokenMiddleware(tokenStore))
	admin.RegisterUI(dataHertz, adminDeps)

	// Open the admin Unix socket. Operator commands (`grainfs volume *`, `grainfs
	// dashboard`) reach this socket; permissions are governed by the file mode
	// (0660) and optional --admin-group chown.
	adminSocket := cfg.AdminSocket
	if adminSocket == "" {
		adminSocket = filepath.Join(dataDir, "admin.sock")
	}
	adminSrv, err := admin.Start(admin.Config{
		SocketPath: adminSocket,
		Group:      cfg.AdminGroup,
		Deps:       adminDeps,
		ExtraRoutes: func(h *hzserver.Hertz) {
			srv.RegisterClusterAdminUDS(h)
			if iamAdminAPI != nil {
				RegisterIAMAdminRoutes(h, iamAdminAPI)
			}
		},
	})
	if err != nil {
		return fmt.Errorf("admin server: %w", err)
	}
	log.Info().Str("path", adminSocket).
		Str("hint", fmt.Sprintf("--endpoint %q", adminSocket)).
		Msg("admin endpoint")
	// Best-effort fallback: ensures the socket is unlinked even if the explicit
	// shutdown path is bypassed (e.g. early return). The explicit shutdown
	// sequence below stops admin BEFORE the data plane drains so operator
	// commands cannot land on a half-shutdown server (spec A5).
	state.AddCleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = adminSrv.Stop(stopCtx)
	})

	// receiptWiring.keyStore may be nil when heal-receipt is disabled — in that
	// case NewReceiptTrackingEmitter degrades to a pass-through (signing unhealthy).
	var activeEmitter scrubber.Emitter = srv.HealEmitter()
	if receiptWiring != nil && receiptWiring.Store() != nil {
		rte := server.NewReceiptTrackingEmitter(srv.HealEmitter(), receiptWiring.Store(), receiptWiring.KeyStore())
		defer rte.Close()
		activeEmitter = rte
	}

	// Phase 16 Week 3: cluster mode also needs startup recovery for the
	// node's local data dir (per-node multipart parts + .tmp leftovers).
	if rec, err := server.RunStartupRecovery(ctx, dataDir, srv.Operations(), activeEmitter); err != nil && !errors.Is(err, context.Canceled) {
		log.Warn().Err(err).Msg("startup recovery failed")
	} else if rec.OrphanTmpRemoved+rec.OrphanMultipartRemoved+len(rec.Errors) > 0 {
		log.Info().
			Int("orphan_tmp", rec.OrphanTmpRemoved).
			Int("orphan_multipart", rec.OrphanMultipartRemoved).
			Int("errors", len(rec.Errors)).Msg("startup recovery summary")
	}

	// Cluster-mode scrubber with ShardOwner filtering.
	// Each node only verifies shards assigned to it in the placement vector,
	// avoiding redundant cross-node I/O. RepairShard is idempotent so
	// concurrent repair from multiple nodes is safe.
	//
	// ShardPlacementMonitor detects locally-missing shards between full
	// scrub cycles; its onMissing callback calls RepairShardLocal which
	// resolves the latest version and pulls survivor shards from peers.
	// --- Object-layer replication scrub shared infrastructure.
	// Wired unconditionally so the admin-trigger Director works even when
	// periodic scrub is disabled (--scrub-interval=0). Periodic scrub +
	// placement monitor still require a positive interval.
	//
	// All three plumbings (walk, opener, repair) route through the local
	// data-group that owns the bucket — single-node serve still sits inside
	// a multi-raft group structure, so the volume bucket's files live under
	// {dataDir}/groups/<gid>/ rather than the bare distBackend root.
	groupBackendForBucket := func(bucket string) *cluster.DistributedBackend {
		dg, ok := dgMgr.GroupForBucket(bucket, clusterRouter)
		if !ok || dg == nil || dg.Backend() == nil {
			return nil
		}
		return dg.Backend().DistributedBackend
	}
	opener := scrubber.LocalOpener(func(bucket, key string) (io.ReadCloser, error) {
		gb := groupBackendForBucket(bucket)
		if gb == nil {
			return nil, fmt.Errorf("scrub opener: no local group for %s", bucket)
		}
		return gb.OpenLocalReplica(bucket, key)
	})
	repairer := scrubber.ReplicaRepairer(ReplicaRepairerFunc(func(rctx context.Context, bucket, key string) error {
		gb := groupBackendForBucket(bucket)
		if gb == nil {
			return fmt.Errorf("scrub repair: no local group for %s", bucket)
		}
		return gb.RepairReplica(rctx, bucket, key)
	}))
	replSource := scrubber.NewReplicationObjectSource("replication", volume.VolumeBucketName, volume.MetaPrefix, backend)
	replVerifier := scrubber.NewReplicationVerifier(opener, repairer)

	// Director owns CLI-triggered sessions + (later) cluster-broadcast
	// trigger. Independent of periodic scrub interval — operators must be
	// able to run `grainfs volume scrub <name>` even with --scrub-interval=0.
	director := scrubber.NewDirector(scrubber.DirectorOpts{
		Incident:  scrubberIncidentRecorder,
		QueueSize: 64,
		NodeID:    nodeID,
	})
	director.Register("replication", replSource, replVerifier)

	// PR4: EC scrub source via per-bucket group resolver. The resolver maps
	// bucket → DataGroup → GroupBackend (which embeds *DistributedBackend so
	// it implements scrubber.Scrubbable). When the bucket lives on a peer-
	// owned group, the resolver returns (nil, false) and Iter closes an
	// empty channel; the FSM-replicated trigger ensures the owning peer's
	// Director runs the actual scrub.
	ecResolver := func(bucket string) (scrubber.Scrubbable, bool) {
		dg, ok := dgMgr.GroupForBucket(bucket, clusterRouter)
		if !ok || dg == nil {
			return nil, false
		}
		gb := dg.Backend()
		if gb == nil {
			return nil, false
		}
		return gb, true
	}
	ecSource := scrubber.NewECScrubSource(ecResolver, nodeID)
	ecScrubVerifier := scrubber.NewShardVerifier(distBackend)
	ecScrubLimiter := rate.NewLimiter(rate.Limit(100), 100)
	ecVerifier := scrubber.NewECScrubVerifier(distBackend, ecScrubVerifier, ecScrubLimiter, activeEmitter, nodeID, ecSource)
	director.Register("ec", ecSource, ecVerifier)

	// PR4: cluster-wide scrub trigger via meta-raft. Each node's MetaFSM
	// fires onScrubTrigger when MetaScrubTriggerCmd applies; Director.ApplyFromFSM
	// creates a session for the same SessionID and runs the source with the
	// resolver above.
	if metaRaft != nil {
		metaRaft.FSM().SetOnScrubTrigger(func(entry scrubber.ScrubTriggerEntry) {
			director.ApplyFromFSM(entry)
		})
	}
	forwardReceiver.WithScrubSessionLookup(director)

	director.Start(ctx)
	adminDeps.Director = director
	adminDeps.ScrubProposer = NewScrubProposerAdapter(metaRaft, director, nodeID)
	adminDeps.ScrubAggregator = NewScrubAggregatorAdapter(clusterCoord)

	if cfg.ScrubInterval > 0 {
		sc := scrubber.New(distBackend, cfg.ScrubInterval)
		sc.SetEmitter(activeEmitter)
		sc.RegisterSource("replication", replSource, replVerifier)
		sc.Start(ctx)

		placementMonitors := NewPlacementMonitorRegistry()
		startPlacementMonitor := func(monitorCtx context.Context, dg *cluster.DataGroup) {
			gb := dg.Backend()
			if clusterIncidentRecorder != nil {
				gb.SetIncidentRecorder(clusterIncidentRecorder)
			}
			placementMonitor := cluster.NewShardPlacementMonitor(gb.FSMRef(), gb, shardSvc, gb.NodeID(), cfg.ScrubInterval)
			splitShardKey := func(shardKey string) (string, string) {
				objectKey, versionID := shardKey, ""
				if i := strings.LastIndexByte(shardKey, '/'); i >= 0 {
					objectKey, versionID = shardKey[:i], shardKey[i+1:]
				}
				return objectKey, versionID
			}
			placementMonitor.SetOnMissing(func(bucket, shardKey string, shardIdx int) {
				// shardKey from placement resolution is objectKey+"/"+versionID.
				// Split on the last "/" so RepairShard can skip LookupLatestVersion.
				objectKey, versionID := splitShardKey(shardKey)
				correlationID := uuid.Must(uuid.NewV7()).String()
				receiptID := "rcpt-" + correlationID
				repairReq := cluster.IncidentRepairRequest{
					Bucket:        bucket,
					Key:           objectKey,
					VersionID:     versionID,
					ShardIdx:      shardIdx,
					Recorder:      clusterIncidentRecorder,
					CorrelationID: correlationID,
				}
				if err := gb.RepairShardLocalWithIncident(monitorCtx, repairReq); err != nil {
					log.Warn().Str("group", dg.ID()).Str("bucket", bucket).Str("key", shardKey).Int("shard", shardIdx).Err(err).Msg("placement monitor repair failed")
				} else if receiptWiring != nil && receiptWiring.Store() != nil && receiptWiring.KeyStore() != nil {
					r := &receipt.HealReceipt{
						ReceiptID:     receiptID,
						Timestamp:     time.Now().UTC(),
						Object:        receipt.ObjectRef{Bucket: bucket, Key: objectKey, VersionID: versionID},
						ShardsLost:    []int32{int32(shardIdx)},
						ShardsRebuilt: []int32{int32(shardIdx)},
						EventIDs:      []string{correlationID},
						CorrelationID: correlationID,
					}
					if err := receipt.Sign(r, receiptWiring.KeyStore()); err != nil {
						log.Warn().Str("correlation_id", correlationID).Err(err).Msg("placement monitor receipt sign failed")
					} else if err := receiptWiring.Store().Put(r); err != nil {
						log.Warn().Str("correlation_id", correlationID).Str("receipt_id", receiptID).Err(err).Msg("placement monitor receipt store failed")
					} else if err := receiptWiring.Store().Flush(); err != nil {
						log.Warn().Str("correlation_id", correlationID).Str("receipt_id", receiptID).Err(err).Msg("placement monitor receipt flush failed")
					} else if err := gb.RecordRepairReceiptSigned(context.Background(), repairReq, receiptID); err != nil {
						log.Warn().Str("correlation_id", correlationID).Str("receipt_id", receiptID).Err(err).Msg("placement monitor incident proof update failed")
					}
				}
			})
			placementMonitor.SetOnCorrupt(func(bucket, shardKey string, shardIdx int, readErr error) {
				objectKey, versionID := splitShardKey(shardKey)
				if err := gb.QuarantineCorruptShardLocal(bucket, objectKey, versionID, shardIdx, readErr.Error()); err != nil {
					log.Warn().Str("group", dg.ID()).Str("bucket", bucket).Str("key", shardKey).Int("shard", shardIdx).Err(err).Msg("placement monitor quarantine failed")
				}
			})
			go placementMonitor.Start(monitorCtx)
		}
		refreshPlacementMonitors := func() {
			placementMonitors.Refresh(ctx, dgMgr.All(), startPlacementMonitor)
		}
		refreshPlacementMonitors()
		go func() {
			ticker := time.NewTicker(cfg.ScrubInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					refreshPlacementMonitors()
				}
			}
		}()
		log.Info().Dur("interval", cfg.ScrubInterval).Msg("cluster scrubber started")
	}

	// Background EC resharding is group-local: each locally-owned DataGroup has
	// its own Raft leader and object metadata DB. Followers skip each pass in
	// ReshardManager, but we still start one manager per local group so
	// leadership changes are picked up without serve-level rewiring.
	if cfg.ReshardInterval > 0 {
		reshardManagers := NewReshardManagerRegistry()
		startReshardManager := func(managerCtx context.Context, dg *cluster.DataGroup) {
			gb := dg.Backend()
			leader := gb.RaftNode()
			if leader == nil {
				log.Warn().Str("group", dg.ID()).Msg("reshard manager skipped: group has no raft node")
				return
			}
			go cluster.NewReshardManager(gb, leader, cfg.ReshardInterval).Start(managerCtx)
		}
		refreshReshardManagers := func() {
			reshardManagers.Refresh(ctx, dgMgr.All(), startReshardManager)
		}
		refreshReshardManagers()
		go func() {
			ticker := time.NewTicker(cfg.ReshardInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					refreshReshardManagers()
				}
			}
		}()
		log.Info().Dur("interval", cfg.ReshardInterval).Msg("cluster reshard manager started")
	}

	// Start the leader-aware worker loop. Only the Raft leader runs the
	// worker; followers skip the scan so we don't waste IO on proposals that
	// would be rejected anyway. LifecycleManager polls node.State() and
	// starts/stops the worker on leadership transitions.
	if lifecycleMgr != nil {
		go lifecycleMgr.Run(ctx)
		log.Info().Dur("interval", cfg.LifecycleInterval).Msg("cluster lifecycle manager started")
	}

	// Start the degraded mode monitor — checks live node count vs EC threshold
	// every 30 s. The first check fires immediately so the server knows its
	// state before serving any requests.
	degradedMon := cluster.NewDegradedMonitor(distBackend, clusterAlerts.Tracker(), cfg.DegradedInterval).
		WithQuorumCheck(node, clusterAlerts)
	go degradedMon.Run(ctx)

	go func() {
		if err := srv.Run(); err != nil {
			log.Error().Err(err).Str("addr", addr).
				Msg("http server error — confirm TCP port is free (lsof -i TCP:" + addr + "), or pass --port=0 to pick a free port")
		}
	}()

	// Post-Phase-18 local-path merge: universal node services (NFS/NFSv4/NBD)
	// are now wired in cluster mode too, not just local. Formerly local-only
	// because runCluster never called the NFS/NBD wiring. Scrubber/lifecycle
	// remain local-specific pending ECBackend→cluster integration (A.2).
	nodeSvc := StartNodeServices(ctx, backend, volMgr, cfg.NFS4Port, cfg.NBDPort, cfg.NBDVolumeSize, distBackend)
	state.AddCleanup(func() { nodeSvc.Close() })

	// Cross-protocol cache coherency: an S3 mutation replicated from another
	// cluster node lands here as an FSM apply that fans out via
	// distBackend.registry. Without this registration, NFS metadata caches
	// (fileMeta, parent-dir mtimes) stay stale until the next backend stat
	// re-fetches. Registers only when NFS4 actually started so non-NFS
	// deployments don't pay for an empty invalidator.
	if nfs := nodeSvc.NFS4(); nfs != nil {
		distBackend.RegisterCacheInvalidator("nfs4", cluster.CacheInvalidatorFunc(nfs.Invalidate))
	}

	<-ctx.Done()
	log.Info().Str("component", "server").Msg("graceful shutdown started")

	// 1. Stop admin Unix socket FIRST so operator commands fail fast with
	// "connection refused" instead of landing on a server that's draining.
	// Spec A5: admin-first, data-plane-second.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := adminSrv.Stop(shutdownCtx); err != nil {
		log.Warn().Err(err).Msg("admin server shutdown error")
	}

	// 2. Drain in-flight HTTP requests on the data plane.
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Warn().Err(err).Msg("http server shutdown error")
	}

	// 3. Transfer Raft leadership before stopping
	if err := node.TransferLeadership(); err != nil {
		log.Debug().Err(err).Msg("leadership transfer skipped")
	} else {
		log.Info().Str("component", "raft").Msg("leadership transferred")
	}

	// 4. Stop Raft apply loop
	close(stopApply)

	log.Info().Str("component", "server").Msg("server stopped")
	return nil
}
