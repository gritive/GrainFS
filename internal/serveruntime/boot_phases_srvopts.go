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
	"github.com/gritive/GrainFS/internal/eventstore"
	"github.com/gritive/GrainFS/internal/icebergcatalog"
	"github.com/gritive/GrainFS/internal/incident"
	"github.com/gritive/GrainFS/internal/incident/badgerstore"
	"github.com/gritive/GrainFS/internal/lifecycle"
	"github.com/gritive/GrainFS/internal/migration"
	"github.com/gritive/GrainFS/internal/resourceguard"
	"github.com/gritive/GrainFS/internal/resourcewatch"
	"github.com/gritive/GrainFS/internal/server"
	"github.com/gritive/GrainFS/internal/storage"
)

// bootSrvOptsAndReceipt assembles the slice of server.Option that will be
// passed to server.New. It also wires the disk collector's threshold callback
// (now that clusterAlerts exists), the heal-receipt stack, the incident
// recorder + resource guards, the lifecycle manager, and the volume manager.
//
// Inputs:  state.cfg (many flags), state.metaRaft, state.peers, state.nodeID,
//
//	state.distBackend, state.db, state.backend, state.diskCollector,
//	state.balancerProposer, state.metaForwardSender, state.metaReadSender,
//	state.quicTransport, state.streamRouter, state.gossipReceiver,
//	state.roleRegistry, state.recoveryReadOnly, state.shardCache,
//	state.joinMode.
//
// Outputs: state.srvOpts, state.clusterAlerts, state.receiptWiring,
//
//	state.incidentRecorder, state.lifecycleMgr, state.volMgr,
//	state.mutationGate.
//
// Cleanup: receiptWiring.Close, incidentDB.Close + DeregisterDB, dedupDB.Close +
// DeregisterDB are all registered via state.AddCleanup so behavior matches the
// original `defer` ordering at Run() exit.
//
// Phase ordering: must run AFTER bootBackendWrap (state.backend, state.diskCollector
// populated) and BEFORE bootHTTPServerAndAdmin (which calls server.New(state.srvOpts)).
func bootSrvOptsAndReceipt(ctx context.Context, state *bootState) error {
	cfg := state.cfg

	// Route webhook URL + secret through ClusterConfig so a PATCH that
	// rotates either lands without a serve restart. The serveruntime.Config
	// flag-derived fields (AlertWebhook, AlertSecret) are bootstrap seeds for
	// ClusterConfig and are scheduled for removal in a follow-up task.
	//
	// Pass the encryptor as a typed nil-safe interface — assigning a typed
	// (*encrypt.Encryptor)(nil) directly to an interface parameter would
	// produce a non-nil interface holding a nil pointer, defeating the
	// `enc == nil` guard inside the dispatcher.
	var alertDecrypter alerts.SecretDecrypter
	if cfg.Encryptor != nil {
		alertDecrypter = cfg.Encryptor
	}
	state.clusterAlerts = server.NewAlertsStateWithConfig(
		state.metaRaft.FSM().ClusterConfig(),
		alertDecrypter,
		cluster.ClusterConfigAlertSecretAAD,
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
	go state.diskCollector.Run(ctx)

	metaRaft := state.metaRaft
	peers := state.peers
	srvOpts := []server.Option{
		// cluster status / remove-peer must reflect *meta-raft* membership —
		// that is the cluster-wide membership ledger that `serve --join`
		// updates via performMetaJoin.
		server.WithClusterInfo(NewRaftClusterInfo(metaRaft.Node(), peers, state.distBackend, metaRaft.FSM())),
		server.WithClusterMembership(NewRaftMembership(metaRaft.Node(), metaRaft.FSM())),
		server.WithEventStore(eventstore.New(state.db)),
		server.WithAlerts(clusterAlerts),
		server.WithDataDir(dataDir),
	}
	if len(peers) == 0 && !cfg.RaftAddrExplicit && !state.joinMode {
		legacyStore := icebergcatalog.NewStore(state.db, "s3://grainfs-tables/warehouse")
		metaCatalog := cluster.NewMetaCatalog(metaRaft, state.backend, "s3://grainfs-tables/warehouse")
		if err := MigrateLegacySingletonIcebergCatalog(ctx, legacyStore, metaCatalog, state.backend); err != nil {
			return fmt.Errorf("migrate singleton Iceberg catalog: %w", err)
		}
		srvOpts = append(srvOpts, server.WithIcebergCatalog(metaCatalog))
	} else {
		metaForward := func(ctx context.Context, command []byte) error {
			return state.metaForwardSender.Send(ctx, MetaProposalTargets(metaRaft.Node().LeaderID(), peers), command)
		}
		metaReadTargets := func() []string {
			return MetaProposalTargets(metaRaft.Node().LeaderID(), peers)
		}
		srvOpts = append(srvOpts, server.WithIcebergCatalog(cluster.NewMetaCatalogWithForwarders(metaRaft, state.backend, "s3://grainfs-tables/warehouse", metaForward, state.metaReadSender, metaReadTargets)))
	}
	srvOpts = append(srvOpts, cfg.AuthOpts...)
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
		state.quicTransport, state.streamRouter, state.gossipReceiver, srvOpts,
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
		lstore := lifecycle.NewStore(state.distBackend.FSMDB())
		prop := &cluster.LifecycleProposer{Propose: state.metaRaft.Propose}
		// Use Node() (interface) — not RaftNode() (v1 concrete) — so the
		// v2 adapter resolves under M5 PR 28 serveruntime=v2 default.
		// RaftLeadership.Subscribe polls State() (raftLeadershipPollInterval),
		// which works for both v1 and v2.
		lead := &cluster.RaftLeadership{Node: state.distBackend.Node()}
		state.metaRaft.FSM().SetLifecycle(lstore) // pattern from boot_phases_scrubber.go:127
		state.lifecycleSvc = lifecycle.NewService(
			lstore, prop, lead,
			state.distBackend,                        // Scrubbable
			storage.NewOperations(state.distBackend), // ObjectDeleter
			cfg.LifecycleInterval,
		)
		srvOpts = append(srvOpts, server.WithLifecycleService(state.lifecycleSvc))
	}

	mstore := migration.NewJobStore(state.distBackend.FSMDB())
	state.metaRaft.FSM().SetMigration(mstore)
	gate := cluster.NewCapabilityGate(compat.DefaultRegistry, 15*time.Second)
	refreshGate := func() {
		gate.SetMetaRaftSnapshot(state.metaRaft.Node().CommittedIndex(), state.metaRaft.Node().Configuration())
		gate.ReportEvidence(state.metaRaft.FSM().CapabilityEvidence(state.metaRaft.Node().ID(), time.Now()))
	}
	refreshGate()
	state.metaRaft.SetCapabilityGate(gate)
	mprop := &cluster.MigrationProposer{
		Propose: state.metaRaft.Propose,
		ProposeWithGate: func(ctx context.Context, plan compat.GatePlan, cmdType clusterpb.MetaCmdType, payload []byte) error {
			_, err := state.metaRaft.ProposeWithGate(ctx, plan, cmdType, payload)
			return err
		},
		GatePlan: func(operation compat.Operation) (compat.GatePlan, error) {
			refreshGate()
			return gate.RequireMetaRaftCapability(compat.CapabilityMigrationCutoverV1, operation, time.Now())
		},
	}
	if state.iamProposer != nil {
		state.iamProposer.Cutover = mprop.ProposeBucketUpstreamCutover
	}
	if cfg.MigrationInterval > 0 {
		mlead := &cluster.RaftLeadership{Node: state.distBackend.Node()}
		state.migrationSvc = migration.NewService(mstore, mprop, mlead, nil, nil, cfg.MigrationInterval)
	}

	volMgr, blockCache, dedupDB, err := BuildVolumeManager(VolumeManagerOptions{DedupEnabled: cfg.DedupEnabled, BlockCacheSize: cfg.BlockCacheSize}, dataDir, state.backend)
	if err != nil {
		return fmt.Errorf("volume manager: %w", err)
	}
	state.volMgr = volMgr
	if dedupDB != nil {
		state.AddCleanup(func() { _ = dedupDB.Close() })
		dedupVlogEntry := resourcewatch.RegisterDB(resourcewatch.DBCategoryDedup, dedupDB)
		state.AddCleanup(func() { resourcewatch.DeregisterDB(dedupVlogEntry) })
	}
	srvOpts = append(srvOpts, server.WithVolumeManager(volMgr), server.WithBlockCache(blockCache), server.WithShardCache(state.shardCache))
	if !state.joinMode {
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
