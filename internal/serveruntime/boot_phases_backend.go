package serveruntime

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/badgerrole"
	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/storage/packblob"
	"github.com/gritive/GrainFS/internal/storage/pullthrough"
	"github.com/gritive/GrainFS/internal/storage/wal"
)

// bootBackendWrap composes the final storage.Backend chain consumed by the
// data plane:
//
//	ClusterCoord → wal.Backend → pullthrough.Backend → optional RecoveryWriteGate.
//
// Also reduces startup probe decisions, honours the recovery cluster marker,
// constructs the DiskCollector (alerts wired in a later phase), creates the
// default bucket on singleton startup, kicks off the auto-snapshotter, and
// emits the startup config snapshot.
//
// Inputs:  state.clusterCoord, state.wal, state.cfg.IAMStore,
//
//	state.roleRegistry, state.startupDecisions, state.cfg.DataDir,
//	state.peers, state.cfg.RaftAddrExplicit, state.joinMode,
//	state.walDir, state.nodeID,
//	state.raftAddr, state.addr from cfg, state.cfg.FlagsSnapshot.
//
// Outputs: state.backend, state.recoveryReadOnly, state.diskCollector.
//
// Phase ordering: must run AFTER bootWALAndForwarders and BEFORE any code that
// reads state.backend (HTTP server, scrubber sources, node services).
func bootBackendWrap(ctx context.Context, state *bootState) error {
	cfg := state.cfg

	// Use ClusterCoordinator as the primary backend for S3, NFSv4, NBD. In
	// single-node mode, opt-in packed blobs can sit on this routed path and
	// avoid per-object shard-file commits for small objects.
	var routed storage.Backend = state.clusterCoord
	if cfg.PackThreshold > 0 && !cfg.RaftAddrExplicit && cfg.NodeID == "" && !state.joinMode {
		blobDir := filepath.Join(cfg.DataDir, "blobs")
		pb, err := packblob.NewPackedBackendWithOptions(routed, blobDir, int64(cfg.PackThreshold), packblob.PackedBackendOptions{
			Compress:  false,
			Encryptor: cfg.Encryptor,
		})
		if err != nil {
			return err
		}
		routed = pb
		log.Info().Int("threshold", cfg.PackThreshold).Msg("single-node packed blob storage enabled on routed backend")
	} else if cfg.PackThreshold > 0 {
		log.Warn().Int("threshold", cfg.PackThreshold).Msg("packed blob fast path is currently single-node only; cluster routed backend keeps EC shard storage")
	}
	state.lifecycleBackend = routed

	// Wrap it with WAL so routed object mutations are captured for PITR.
	var backend storage.Backend = wal.NewBackend(routed, state.wal)

	// Wrap with pull-through cache. Per /plan-eng-review override A10 — fail-fast at
	// startup if cfg.IAMStore is nil. This guards against future construction-order
	// regressions: NewIAMResolver requires a non-nil store and would panic on first
	// request otherwise.
	if cfg.IAMStore == nil {
		return fmt.Errorf("pullthrough: IAMStore required (cfg.IAMStore is nil)")
	}
	backend = pullthrough.NewBackend(backend, pullthrough.NewIAMResolver(cfg.IAMStore))
	log.Info().Msg("pull-through cache enabled (IAM-backed resolver)")

	startupResult := badgerrole.ReduceStartupDecisions(state.roleRegistry, state.startupDecisions)
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
	if startupReadOnly {
		backend = storage.NewRecoveryWriteGate(backend, storage.ErrRecoveryWriteDisabled)
		log.Warn().Strs("reasons", startupResult.ReadOnlyReasons).Msg("badger startup recovery read-only gate enabled")
	}
	state.backend = backend
	state.recoveryReadOnly = startupReadOnly

	// DiskCollector exposes grainfs_disk_used_pct metric. In multi-node mode
	// the balancer owns its own collector; in singleton mode nothing else
	// would emit disk stats. Register unconditionally — duplicate registration
	// is guarded inside NewDiskCollector. Threshold + OnThreshold + Run are
	// wired in bootSrvOptsAndReceipt once clusterAlerts is built.
	state.diskCollector = cluster.NewMultiRootDiskCollector(state.nodeID, cfg.DataDirs, nil, 30*time.Second, state.metaRaft.FSM().ClusterConfig())

	// Auto-create "default" bucket only for singleton startup. In cluster mode,
	// bucket creation is a cluster-wide metadata operation and must be driven by
	// an explicit client/API action, not repeated independently by every node.
	if ShouldCreateDefaultBucketOnStartup(state.peers, startupReadOnly) {
		if err := CreateDefaultBucketWithRetry(ctx, backend, 30*time.Second); err != nil {
			return fmt.Errorf("create default bucket: %w", err)
		}
	}

	// Start auto-snapshotter for object-level PITR snapshots (separate from
	// Raft snapshots above). Uses the WAL-wrapped backend so replay is
	// anchored to the object mutation log.
	objSnapMgr, err := StartAutoSnapshotterWhenReady(ctx, cfg.DataDir, state.walDir, backend, state.metaRaft.FSM().ClusterConfig(), state.cfg.Encryptor, 30*time.Second)
	if err != nil {
		log.Warn().Err(err).Msg("auto-snapshot init failed")
	}
	state.objSnapMgr = objSnapMgr

	log.Info().Str("component", "server").Str("version", cfg.Version).
		Str("node_id", state.nodeID).Str("raft_addr", state.raftAddr).Strs("peers", state.peers).
		Str("addr", cfg.Addr).Str("data", cfg.DataDir).Msg("server started")
	LogStartupConfigSnapshot(cfg.FlagsSnapshot, cfg.Addr, cfg.DataDir, state.nodeID, state.raftAddr)
	logClusterConfigLoaded(state.metaRaft.FSM().ClusterConfig())
	return nil
}

// logClusterConfigLoaded emits a single structured event with the effective
// view of ClusterConfig after raft is up and any restored snapshot has been
// applied. Secret bytes are never logged — only a bool indicating presence.
func logClusterConfigLoaded(cfg *cluster.ClusterConfig) {
	log.Info().
		Str("event", "cluster_config_loaded").
		Uint64("rev", cfg.Rev()).
		Bool("balancer-enabled", cfg.BalancerEnabled()).
		Float64("balancer-imbalance-trigger-pct", cfg.BalancerImbalanceTriggerPct()).
		Float64("balancer-imbalance-stop-pct", cfg.BalancerImbalanceStopPct()).
		Int32("balancer-migration-rate", cfg.BalancerMigrationRate()).
		Dur("balancer-leader-tenure-min", cfg.BalancerLeaderTenureMin()).
		Dur("balancer-warmup-timeout", cfg.BalancerWarmupTimeout()).
		Float64("balancer-cb-threshold", cfg.BalancerCBThreshold()).
		Int32("balancer-migration-max-retries", cfg.BalancerMigrationMaxRetries()).
		Dur("balancer-migration-pending-ttl", cfg.BalancerMigrationPendingTTL()).
		Dur("balancer-gossip-interval", cfg.BalancerGossipInterval()).
		Str("alert-webhook", cfg.AlertWebhook()).
		Bool("alert-webhook-secret-set", len(cfg.AlertWebhookSecretWrapped()) > 0).
		Float64("disk-warn-threshold", cfg.DiskWarnFrac()).
		Float64("disk-critical-threshold", cfg.DiskCriticalFrac()).
		Dur("snapshot-interval", cfg.SnapshotInterval()).
		Int32("snapshot-retain", cfg.SnapshotRetain()).
		Msg("cluster config loaded")
}
