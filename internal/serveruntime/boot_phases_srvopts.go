package serveruntime

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/alerts"
	"github.com/gritive/GrainFS/internal/badgerrole"
	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/compat"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/eventstore"
	"github.com/gritive/GrainFS/internal/iam/pdp"
	"github.com/gritive/GrainFS/internal/incident"
	"github.com/gritive/GrainFS/internal/incident/badgerstore"
	"github.com/gritive/GrainFS/internal/lifecycle"
	"github.com/gritive/GrainFS/internal/migration"
	"github.com/gritive/GrainFS/internal/resourceguard"
	"github.com/gritive/GrainFS/internal/resourcewatch"
	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/internal/server"
	"github.com/gritive/GrainFS/internal/server/alertssvc"
	"github.com/gritive/GrainFS/internal/storage"
)

// bootSrvOptsAndReceipt assembles the slice of server.Option that will be
// passed to server.New. It also wires the disk collector's threshold callback
// (now that clusterAlerts exists), the heal-receipt stack, the incident
// recorder + resource guards, and the lifecycle manager.
//
// Inputs:  state.cfg (many flags), state.metaRaft, state.peers, state.nodeID,
//
//	state.distBackend, state.db, state.backend, state.diskCollector,
//	state.balancerProposer, state.metaForwardSender,
//	state.clusterTransport, state.streamRouter, state.gossipReceiver,
//	state.roleRegistry, state.recoveryReadOnly, state.shardCache,
//	state.joinMode.
//
// Outputs: state.srvOpts, state.clusterAlerts, state.receiptWiring,
//
//	state.incidentRecorder, state.lifecycleMgr, state.mutationGate.
//
// Cleanup: receiptWiring.Close, incidentDB.Close + DeregisterDB are all
// registered via state.AddCleanup so behavior matches the original `defer`
// ordering at Run() exit.
//
// Phase ordering: must run AFTER bootBackendWrap (state.backend, state.diskCollector
// populated) and BEFORE bootHTTPServerAndAdmin (which calls server.New(state.srvOpts)).
func bootSrvOptsAndReceipt(ctx context.Context, state *bootState) error {
	cfg := state.cfg

	// Fail fast on missing deps that bootMetaRaftWiring must have populated.
	// A silent nil-skip here masks boot-phase ordering bugs: the S3 server
	// would start with no anon middleware wiring, violating the Phase 0 anon
	// contract. See F#45.
	if state.cfgStore == nil {
		return fmt.Errorf("boot phase srvopts: cfgStore nil; bootMetaRaftWiring must run first")
	}
	if state.iamPolicyStores == nil {
		return fmt.Errorf("boot phase srvopts: iamPolicyStores nil; bootMetaRaftWiring must run first")
	}

	// Route webhook URL + secret through ClusterConfig so a PATCH that
	// rotates either lands without a serve restart. The serveruntime.Config
	// flag-derived fields (AlertWebhook, AlertSecret) are bootstrap seeds for
	// ClusterConfig and are scheduled for removal in a follow-up task.
	//
	// Pass the DEK-backed data encryptor as a typed nil-safe interface —
	// assigning a typed nil directly to an interface parameter would produce a
	// non-nil interface holding a nil pointer, defeating the `enc == nil` guard
	// inside the dispatcher.
	var alertDecrypter alerts.SecretOpener
	if state.dekKeeper != nil {
		alertDecrypter = storage.NewDEKKeeperAdapter(state.dekKeeper, state.clusterID)
	}
	state.clusterAlerts = alertssvc.NewStateWithConfig(
		state.metaRaft.FSM().ClusterConfig(),
		alertDecrypter,
		[]encrypt.AADField{encrypt.FieldString(cluster.ClusterConfigAlertSecretField)},
		alerts.Options{},
		alerts.DegradedConfig{},
		"cluster",
	)

	// Wire predictive disk warnings into the collector now that clusterAlerts
	// exists. The warn/critical fractions themselves are read live from
	// ClusterConfig inside the collector at each tick (wired at
	// NewDiskCollector time in bootBackendWrap), so a `cluster config set
	// disk-warn-threshold ...` PATCH lands without a serve restart.
	clusterAlerts := state.clusterAlerts
	nodeID := state.nodeID
	dataDir := cfg.DataDir
	state.diskCollector.SetOnThreshold(func(level cluster.DiskThresholdLevel, pct float64, availBytes uint64) {
		// Dispatcher.Send is fire-and-forget — the controller goroutine owns
		// retry, so the collect loop is never blocked.
		switch level {
		case cluster.DiskLevelCritical:
			log.Warn().Float64("pct", pct).Uint64("avail_bytes", availBytes).Msg("disk usage CRITICAL")
			clusterAlerts.Send(alerts.Alert{
				Type:     "disk_critical",
				Severity: alerts.SeverityCritical,
				Resource: nodeID,
				Message:  fmt.Sprintf("disk used %.1f%% (avail %d bytes) on %s", pct, availBytes, dataDir),
			})
		case cluster.DiskLevelWarn:
			log.Warn().Float64("pct", pct).Uint64("avail_bytes", availBytes).Msg("disk usage warning")
			clusterAlerts.Send(alerts.Alert{
				Type:     "disk_warn",
				Severity: alerts.SeverityWarning,
				Resource: nodeID,
				Message:  fmt.Sprintf("disk used %.1f%% (avail %d bytes) on %s", pct, availBytes, dataDir),
			})
		case cluster.DiskLevelOK:
			log.Info().Float64("pct", pct).Msg("disk usage recovered to normal")
		}
	})
	go state.diskCollector.Run(ctx)

	metaRaft := state.metaRaft
	peers := state.peers
	srvOpts := []server.Option{
		// cluster status / remove-peer must reflect *meta-raft* membership —
		// that is the cluster-wide membership ledger that `serve --join`
		// updates via performMetaJoin.
		server.WithClusterInfo(NewRaftClusterInfo(metaRaft.Node(), peers, state.distBackend, metaRaft.FSM()).
			WithCapabilityGate(state.capabilityGate).
			WithDataGroups(state.dgMgr)),
		server.WithClusterMembership(NewRaftMembership(metaRaft.Node(), metaRaft.FSM())),
		server.WithExpandPlacement(makeExpandPlacementFunc(state.clusterCoord, metaRaft)),
		server.WithEventStore(eventstore.New(state.db)),
		server.WithAlerts(clusterAlerts),
		server.WithDataDir(dataDir),
	}
	srvOpts = append(srvOpts, cfg.AuthOpts...)
	if ensureProtocolCredentialStore(state) != nil {
		srvOpts = append(srvOpts, server.WithProtocolCredentialAuth(
			state.protocolCredentialStore,
			protocolCredentialEnvelopeFromState(state),
		))
	}
	// T33: wire the policy authorizer so Layer 1 (iamCheck) evaluates
	// policy.Evaluate. Both iamPolicyStores and cfgStore are guaranteed non-nil
	// by the fail-fast guards at the top of this function.
	policyAuthz := s3auth.NewAuthorizer(state.iamPolicyStores.Resolver, state.cfgStore)
	// Always install the PDP decorator on the S3 data plane; it is a pure
	// pass-through unless iam.pdp + data_plane.enabled are set (read per request
	// from the cfg store), so hot-enable works with no dependency rebuild.
	dataPlanePDP := pdp.NewDecorator(policyAuthz, state.cfgStore, ensurePDPTokenSource(state), "data_plane")
	srvOpts = append(srvOpts, server.WithPolicyAuthorizer(dataPlanePDP))
	if state.balancerProposer != nil {
		srvOpts = append(srvOpts, server.WithBalancerInfo(NewBalancerInfoAdapter(state.balancerProposer)))
	}

	// Phase 16 Week 5 Slice 2 — HealReceipt API + gossip + broadcast fallback.
	rcptPSK := cfg.HealReceiptPSK
	if rcptPSK == "" {
		rcptPSK = cfg.ClusterKey
	}
	rcptOpts := ReceiptOptions{
		Enabled:        cfg.HealReceiptEnabled,
		PSK:            rcptPSK,
		Retention:      cfg.HealReceiptRetention,
		GossipInterval: cfg.HealReceiptGossipInterval,
		WindowSize:     cfg.HealReceiptWindow,
	}
	receiptPeerProvider := func() []string {
		return receiptPeerAddresses(nodeID, state.raftAddr, peers, metaRaft.FSM().Nodes())
	}
	newSrvOpts, receiptWiring, err := SetupClusterReceiptWithPeerProvider(
		ctx, rcptOpts, dataDir, nodeID, receiptPeerProvider,
		state.clusterTransport, state.gossipReceiver, srvOpts,
	)
	if err != nil {
		return fmt.Errorf("heal-receipt wiring: %w", err)
	}
	srvOpts = newSrvOpts
	state.receiptWiring = receiptWiring
	state.AddCleanup(func() { receiptWiring.Close() })

	var incidentRecorder *incident.Recorder
	incidentDB, incidentDecision, err := badgerrole.OpenRole(state.roleRegistry, badgerrole.RoleIncidentState, badgerrole.PathContext{DataDir: dataDir})
	if err != nil {
		if feature, ok := OptionalRoleDisabled(state.roleRegistry, incidentDecision); ok {
			LogOptionalRoleDisabled(badgerrole.RoleIncidentState, feature, err)
		} else {
			return fmt.Errorf("open incident db: %w", err)
		}
	} else {
		// Convert the original `defer incidentDB.Close()` + DeregisterDB into
		// AddCleanup so it runs at Run() exit, not at phase-function exit.
		state.AddCleanup(func() { _ = incidentDB.Close() })
		incidentVlogEntry := resourcewatch.RegisterDB(resourcewatch.DBCategoryIncident, incidentDB)
		state.AddCleanup(func() { resourcewatch.DeregisterDB(incidentVlogEntry) })
		incidentStore := badgerstore.New(incidentDB)
		incidentRecorder = incident.NewRecorder(incidentStore, incident.NewReducer())
		if imported, err := importBadgerRecoveryJournal(ctx, incidentStore, dataDir); err != nil {
			log.Warn().Err(err).Msg("badger recovery journal import failed")
		} else if imported > 0 {
			log.Info().Int("imported", imported).Msg("badger recovery journal imported")
		}
		state.distBackend.SetIncidentRecorder(incidentRecorder)
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
	state.incidentRecorder = incidentRecorder

	// Bucket Lifecycle Policy (ADR 0011): replicate via meta-Raft FSM,
	// executor leader-only.
	if cfg.LifecycleInterval > 0 {
		if state.sharedFSMStore == nil {
			return fmt.Errorf("lifecycle: shared FSM store not opened (boot ordering)")
		}
		// Phase 6.5 S3: the lifecycle store shares the FSM-state DB, routed
		// through the MetadataStore contract (serveruntime owns the raw handle
		// lifecycle; the backend no longer exposes it).
		lstore := lifecycle.NewStore(state.sharedFSMStore)
		state.lifecycleStore = lstore
		prop := &cluster.LifecycleProposer{Propose: state.metaRaft.Propose}
		// Use Node() (interface) — not RaftNode() (v1 concrete) — so the
		// v2 adapter resolves under M5 PR 28 serveruntime=v2 default.
		// RaftLeadership.Subscribe polls State() (raftLeadershipPollInterval),
		// which works for both v1 and v2.
		lead := &cluster.RaftLeadership{Node: state.distBackend.Node()}
		state.metaRaft.FSM().SetLifecycle(lstore) // pattern from boot_phases_scrubber.go:127
		scrubbable, ok := state.backend.(lifecycle.Scrubbable)
		if !ok {
			return fmt.Errorf("lifecycle: state.backend does not implement Scrubbable (type %T)", state.backend)
		}
		state.lifecycleSvc = lifecycle.NewService(
			lstore, prop, lead,
			scrubbable,                           // Scrubbable — full wrapper stack (PackedBackend visible)
			storage.NewOperations(state.backend), // ObjectDeleter
			cfg.LifecycleInterval,
			lifecycle.WithNodeID(nodeID),
		)
		srvOpts = append(srvOpts, server.WithLifecycleService(state.lifecycleSvc))
	}

	if state.sharedFSMStore == nil {
		return fmt.Errorf("migration: shared FSM store not opened (boot ordering)")
	}
	mstore := migration.NewJobStore(state.sharedFSMStore)
	state.metaRaft.FSM().SetMigration(mstore)
	if state.capabilityGate == nil {
		state.capabilityGate = cluster.NewCapabilityGate(compat.DefaultRegistry, capabilityEvidenceTTL(state))
		state.metaRaft.SetCapabilityGate(state.capabilityGate)
	}
	refreshCapabilityGate(state)
	mprop := &cluster.MigrationProposer{
		Propose: state.metaRaft.Propose,
		ProposeWithGate: func(ctx context.Context, plan compat.GatePlan, cmdType clusterpb.MetaCmdType, payload []byte) error {
			_, err := state.metaRaft.ProposeWithGate(ctx, plan, cmdType, payload)
			return err
		},
		GatePlan: func(operation compat.Operation) (compat.GatePlan, error) {
			refreshCapabilityGate(state)
			return state.capabilityGate.RequireMetaRaftCapability(compat.CapabilityMigrationCutoverV1, operation, time.Now())
		},
	}
	if state.iamProposer != nil {
		state.iamProposer.Cutover = mprop.ProposeBucketUpstreamCutover
	}
	if cfg.MigrationInterval > 0 {
		mlead := &cluster.RaftLeadership{Node: state.distBackend.Node()}
		state.migrationSvc = migration.NewService(mstore, mprop, mlead, nil, nil, cfg.MigrationInterval)
	}

	srvOpts = append(srvOpts, server.WithShardCache(state.shardCache))
	// A joiner (legacy joinMode OR zero-CA inviteJoinMode) must NOT install the
	// group-0 DistributedBackend read-index fence: it is not the group-0 leader
	// and has no usable legacy peers, so ReadIndex returns ErrNotLeader and GETs
	// 500 before the ClusterCoordinator forward path can route them to the real
	// group leader (the same path PUTs already use). Skip the fence so reads reach
	// the coordinator and forward correctly.
	if !state.inviteJoinMode {
		srvOpts = append(srvOpts, server.WithReadIndexer(state.distBackend))
	}
	srvOpts = append(srvOpts, server.WithRaftSnapshotter(state.distBackend))

	state.distBackend.RegisterReadIndexHandler()
	state.distBackend.RegisterProposeForwardHandler()

	state.mutationGate = server.NewMutationGate(nil)
	if state.recoveryReadOnly {
		state.mutationGate.SetBlocked(storage.ErrRecoveryWriteDisabled)
	}
	srvOpts = append(srvOpts, server.WithMutationGate(state.mutationGate))

	state.srvOpts = srvOpts
	return nil
}
