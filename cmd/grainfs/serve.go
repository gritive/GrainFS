package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/spf13/cobra"

	"crypto/rand"

	"github.com/gritive/GrainFS/internal/alerts"
	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/eventstore"
	grainotel "github.com/gritive/GrainFS/internal/otel"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/server"
	"github.com/gritive/GrainFS/internal/snapshot"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/storage/packblob"
	"github.com/gritive/GrainFS/internal/storage/pullthrough"
	"github.com/gritive/GrainFS/internal/storage/wal"
	"github.com/gritive/GrainFS/internal/transport"
)

func init() {
	serveCmd.Flags().StringP("data", "d", "./data", "data directory")
	serveCmd.Flags().IntP("port", "p", 9000, "listen port")
	serveCmd.Flags().String("node-id", "", "unique node ID (auto-generated if omitted)")
	serveCmd.Flags().String("raft-addr", "", "Raft listen address (required when --peers is set)")
	serveCmd.Flags().String("cluster-key", "", "Pre-shared key for cluster peer authentication")
	serveCmd.Flags().String("peers", "", "comma-separated list of peer Raft addresses (enables cluster mode)")
	serveCmd.Flags().Int("ec-data", cluster.DefaultDataShards, "target max data shards k; actual k scales with node count (EffectiveConfig, 3+ nodes)")
	serveCmd.Flags().Int("ec-parity", cluster.DefaultParityShards, "target max parity shards m; actual m=max(1,round(n×m/(k+m)))")
	serveCmd.Flags().Bool("cluster-ec", true, "enable cluster erasure coding; activates at 3+ nodes with proportional k,m; 1-2 nodes use N× replication")
	serveCmd.Flags().String("access-key", "", "S3 access key for authentication (enables auth when set)")
	serveCmd.Flags().String("secret-key", "", "S3 secret key for authentication")
	serveCmd.Flags().String("encryption-key-file", "", "path to 32-byte encryption key file (auto-generated if omitted)")
	serveCmd.Flags().Bool("no-encryption", false, "disable at-rest encryption")
	serveCmd.Flags().Int("nfs-port", 9002, "NFS server port (0 = disabled, volumes managed via REST API)")
	serveCmd.Flags().Int("nfs4-port", 2049, "NFSv4 server port (0 = disabled)")
	serveCmd.Flags().Int("nbd-port", 10809, "NBD server port (0 = disabled, Linux only)")
	serveCmd.Flags().Int64("nbd-volume-size", 1024*1024*1024, "default NBD volume size in bytes")
	serveCmd.Flags().Int("pack-threshold", 0, "pack objects below this size into blob files (0 = disabled, e.g. 65536)")
	serveCmd.Flags().Duration("snapshot-interval", 1*time.Hour, "auto-snapshot interval (0 to disable)")
	serveCmd.Flags().Int("snapshot-retain", 24, "number of auto-snapshots to retain")
	serveCmd.Flags().Duration("scrub-interval", 24*time.Hour, "EC shard scrub interval (0 to disable)")
	serveCmd.Flags().Duration("lifecycle-interval", 1*time.Hour, "lifecycle rule evaluation interval (0 to disable)")
	serveCmd.Flags().String("upstream", "", "upstream S3-compatible endpoint for pull-through caching (e.g. http://minio:9000)")
	serveCmd.Flags().String("upstream-access-key", "", "access key for upstream S3 endpoint")
	serveCmd.Flags().String("upstream-secret-key", "", "secret key for upstream S3 endpoint")
	serveCmd.Flags().Bool("balancer-enabled", true, "enable auto-balancing in cluster mode")
	serveCmd.Flags().Duration("balancer-gossip-interval", cluster.DefaultBalancerConfig().GossipInterval, "how often the balancer evaluates disk usage")
	serveCmd.Flags().Float64("balancer-imbalance-trigger-pct", cluster.DefaultBalancerConfig().ImbalanceTriggerPct, "start migration when max-min disk usage diff exceeds this percentage")
	serveCmd.Flags().Float64("balancer-imbalance-stop-pct", cluster.DefaultBalancerConfig().ImbalanceStopPct, "stop migration when max-min disk usage diff drops below this percentage")
	serveCmd.Flags().Int("balancer-migration-rate", cluster.DefaultBalancerConfig().MigrationRate, "max migration proposals per tick")
	serveCmd.Flags().Duration("balancer-leader-tenure-min", cluster.DefaultBalancerConfig().LeaderTenureMin, "minimum time a leader must hold tenure before load-based transfer")
	serveCmd.Flags().Duration("balancer-warmup-timeout", cluster.DefaultBalancerConfig().WarmupTimeout, "time to wait after node start before proposing disk migrations (prevents false alarms during join/recovery)")
	serveCmd.Flags().Float64("balancer-cb-threshold", cluster.DefaultBalancerConfig().CBThreshold, "disk-used fraction (0–1) at which a dst node's circuit breaker opens (e.g. 0.90 = 90%)")
	serveCmd.Flags().Int("balancer-migration-max-retries", cluster.DefaultBalancerConfig().MigrationMaxRetries, "max shard write attempts per shard during migration")
	serveCmd.Flags().Duration("balancer-migration-pending-ttl", cluster.DefaultBalancerConfig().MigrationPendingTTL, "max time a pending migration may linger before being cancelled")
	serveCmd.Flags().Bool("badger-managed-mode", false, "enable Raft log GC using quorum watermark (WARNING: on-disk format change; see docs/badger-managed-mode-rollback.md)")
	serveCmd.Flags().Duration("raft-log-gc-interval", 30*time.Second, "how often Raft log GC runs when --badger-managed-mode is enabled")
	// Phase 16 Week 4 — webhook alerts.
	serveCmd.Flags().String("alert-webhook", "", "Slack-compatible webhook URL for critical alerts (empty disables alerts)")
	serveCmd.Flags().String("alert-webhook-secret", "", "shared secret for X-GrainFS-Signature HMAC-SHA256 (empty disables signing)")
	// Phase 16 Week 5 Slice 2 — HealReceipt API + gossip.
	serveCmd.Flags().Bool("heal-receipt-enabled", true, "enable HealReceipt audit API (Phase 16 Slice 2)")
	serveCmd.Flags().String("heal-receipt-psk", "", "PSK for HealReceipt HMAC-SHA256 signing (defaults to --cluster-key in cluster mode)")
	serveCmd.Flags().Duration("heal-receipt-retention", 30*24*time.Hour, "HealReceipt retention window (older entries are GC'd)")
	serveCmd.Flags().Duration("heal-receipt-gossip-interval", 5*time.Second, "how often this node gossips its recent receipt IDs to peers")
	serveCmd.Flags().Int("heal-receipt-window", 50, "rolling window size — how many recent receipt IDs to gossip per tick")
	serveCmd.Flags().String("otel-endpoint", "", "OTLP HTTP endpoint for trace export (empty disables OTel, e.g. localhost:4318)")
	serveCmd.Flags().Float64("otel-sample-rate", 0.01, "head-based OTel trace sample rate [0.0, 1.0] (default 1%)")
	rootCmd.AddCommand(serveCmd)
}

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start the S3-compatible storage server",
	RunE:  runServe,
}

func runServe(cmd *cobra.Command, args []string) error {
	dataDir, _ := cmd.Flags().GetString("data")
	port, _ := cmd.Flags().GetInt("port")
	peersStr, _ := cmd.Flags().GetString("peers")

	addr := fmt.Sprintf(":%d", port)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var authOpts []server.Option
	accessKey, _ := cmd.Flags().GetString("access-key")
	secretKey, _ := cmd.Flags().GetString("secret-key")
	if accessKey != "" && secretKey != "" {
		authOpts = append(authOpts, server.WithAuth([]s3auth.Credentials{
			{AccessKey: accessKey, SecretKey: secretKey},
		}))
	}

	noEncryption, _ := cmd.Flags().GetBool("no-encryption")
	var shardEncryptor *encrypt.Encryptor
	if !noEncryption {
		encKeyFile, _ := cmd.Flags().GetString("encryption-key-file")
		var err error
		shardEncryptor, err = loadOrCreateEncryptionKey(encKeyFile, dataDir)
		if err != nil {
			return fmt.Errorf("encryption setup: %w", err)
		}
	}

	otelEndpoint, _ := cmd.Flags().GetString("otel-endpoint")
	otelSampleRate, _ := cmd.Flags().GetFloat64("otel-sample-rate")
	otelShutdown, err := grainotel.Init(ctx, otelEndpoint, otelSampleRate)
	if err != nil {
		slog.Warn("otel: init failed, tracing disabled", "err", err)
	} else if otelEndpoint != "" {
		slog.Info("otel: tracing enabled", "endpoint", otelEndpoint, "sample_rate", otelSampleRate)
		defer func() { _ = otelShutdown(context.Background()) }()
	}

	nodeID, _ := cmd.Flags().GetString("node-id")
	raftAddr, _ := cmd.Flags().GetString("raft-addr")
	clusterKey, _ := cmd.Flags().GetString("cluster-key")
	return runCluster(ctx, cmd, addr, dataDir, nodeID, raftAddr, peersStr, clusterKey, authOpts, shardEncryptor)
}

func runCluster(ctx context.Context, cmd *cobra.Command, addr, dataDir, nodeID, raftAddr, peersStr, clusterKey string, authOpts []server.Option, encryptor *encrypt.Encryptor) error {
	if nodeID == "" {
		nodeID = generateNodeID(dataDir)
		slog.Info("auto-generated node ID", "component", "server", "node_id", nodeID)
	}

	// strings.Split always yields at least one element — empty input or
	// trailing commas produce "" entries that waste a gossip tick each.
	peers := filterEmpty(strings.Split(peersStr, ","))

	// When no peers are configured, we boot a singleton Raft node on a
	// loopback port so a single-machine deployment still goes through the
	// unified storage path (versioning, scrubber, lifecycle, WAL all work).
	// Operators who later want to expand the cluster pick a concrete
	// --raft-addr and --peers list; the loopback default is only for the
	// "just start it" path.
	if raftAddr == "" {
		if len(peers) > 0 {
			return fmt.Errorf("--raft-addr is required when --peers is set")
		}
		// Singleton: let the kernel pick a free port so multiple instances
		// (dev, tests) coexist without collisions. No peer will ever reach it.
		raftAddr = "127.0.0.1:0"
	}

	metaDir := filepath.Join(dataDir, "meta")
	raftDir := filepath.Join(dataDir, "raft")

	// Auto-migrate BEFORE any filesystem or lock side effects. If Raft dir
	// doesn't exist but meta dir holds an existing local BadgerDB, convert
	// in place. Two things previously broke this branch on fresh cluster
	// starts with an empty dataDir:
	//   1. MkdirAll ran before this check, so os.Stat(metaDir) succeeded
	//      on a freshly-created empty dir and triggered a spurious
	//      migration.
	//   2. The migration opens the meta DB, but we had already opened it
	//      here, and BadgerDB takes an exclusive directory lock, so the
	//      migration aborted with "Another process is using this Badger
	//      database".
	// Moving the migration above both MkdirAll and badger.Open removes
	// both failure modes.
	if _, err := os.Stat(raftDir); os.IsNotExist(err) {
		if info, err := os.Stat(metaDir); err == nil && info.IsDir() {
			// A populated local meta dir has .sst / .vlog / MANIFEST files.
			// Distinguish "real data" from "empty dir someone pre-created"
			// by checking for any entries; empty → skip migration.
			if entries, err := os.ReadDir(metaDir); err == nil && len(entries) > 0 {
				slog.Info("auto-migrating local metadata to cluster format", "component", "migrate")
				if err := cluster.MigrateLegacyMetaToCluster(dataDir, nodeID); err != nil {
					return fmt.Errorf("auto-migrate: %w", err)
				}
				slog.Info("auto-migration complete", "component", "migrate")
			}
		}
	}

	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		return fmt.Errorf("create meta dir: %w", err)
	}
	dbOpts := badger.DefaultOptions(metaDir).WithLogger(nil)
	db, err := badger.Open(dbOpts)
	if err != nil {
		return fmt.Errorf("open metadata db: %w", err)
	}
	defer db.Close()
	// Phase 16 Week 3: cluster mode preflight. Same reasoning as local.
	if err := server.PreflightBadger(db, metaDir, nil); err != nil {
		return err
	}

	badgerManagedMode, _ := cmd.Flags().GetBool("badger-managed-mode")
	raftLogGCInterval, _ := cmd.Flags().GetDuration("raft-log-gc-interval")

	var storeOpts []raft.BadgerLogStoreOption
	if badgerManagedMode {
		storeOpts = append(storeOpts, raft.WithManagedMode())
	}
	logStore, err := raft.NewBadgerLogStore(raftDir, storeOpts...)
	if err != nil {
		return fmt.Errorf("open raft store: %w", err)
	}
	defer logStore.Close()

	// Start QUIC transport for inter-node communication.
	quicTransport := transport.NewQUICTransport(clusterKey)
	if err := quicTransport.Listen(ctx, raftAddr); err != nil {
		return fmt.Errorf("start QUIC transport: %w", err)
	}
	defer quicTransport.Close()
	// Resolve `raftAddr` to its actual bound port. When the operator asked
	// for 127.0.0.1:0 (singleton default) QUIC picks a free UDP port; we
	// need that concrete address in allNodes so shard placement produces
	// dialable self entries.
	if local := quicTransport.LocalAddr(); local != "" {
		raftAddr = local
	}

	// Connect to all peers
	for _, peer := range peers {
		if err := quicTransport.Connect(ctx, peer); err != nil {
			slog.Warn("failed to connect to peer (will retry lazily)", "peer", peer, "error", err)
		}
	}

	cfg := raft.DefaultConfig(nodeID, peers)
	cfg.ManagedMode = badgerManagedMode
	cfg.LogGCInterval = raftLogGCInterval
	node := raft.NewNode(cfg, logStore)

	// Wire QUIC transport to Raft RPC layer
	rpcTransport := raft.NewQUICRPCTransport(quicTransport, node)
	rpcTransport.SetTransport()

	// Create ShardService for distributed data replication
	shardSvc := cluster.NewShardService(dataDir, quicTransport, cluster.WithEncryptor(encryptor))

	// Set up StreamRouter: Raft RPCs on Control stream, Shard RPCs on Data stream
	router := transport.NewStreamRouter()
	router.Handle(transport.StreamControl, rpcTransport.Handler())
	router.Handle(transport.StreamData, shardSvc.HandleRPC())
	quicTransport.SetStreamHandler(router.Dispatch)

	node.Start()
	defer node.Stop()

	distBackend, err := cluster.NewDistributedBackend(dataDir, db, node)
	if err != nil {
		return fmt.Errorf("failed to initialize distributed storage: %w", err)
	}

	// Wire shard service for distributed fan-out replication
	allNodes := append([]string{raftAddr}, peers...)
	distBackend.SetShardService(shardSvc, allNodes)

	// Phase 18 Cluster EC: activates at MinECNodes=3+ nodes with proportional k,m.
	// 1-2 nodes always use N× replication regardless of this flag.
	clusterEC, _ := cmd.Flags().GetBool("cluster-ec")
	clusterECData, _ := cmd.Flags().GetInt("ec-data")
	clusterECParity, _ := cmd.Flags().GetInt("ec-parity")
	distBackend.SetECConfig(cluster.ECConfig{
		DataShards:   clusterECData,
		ParityShards: clusterECParity,
		Enabled:      clusterEC,
	})
	if clusterEC {
		slog.Info("cluster EC configured", "k", clusterECData, "m", clusterECParity,
			"active", len(allNodes) >= clusterECData+clusterECParity,
			"cluster_size", len(allNodes))
	}

	// Set up snapshot manager: auto-snapshot every 10000 applied entries
	fsm := cluster.NewFSM(db)
	snapMgr := raft.NewSnapshotManager(logStore, fsm, raft.SnapshotConfig{Threshold: 10000})
	distBackend.SetSnapshotManager(snapMgr, node)

	// Restore from snapshot on startup
	snapIdx, err := snapMgr.Restore()
	if err != nil {
		slog.Warn("snapshot restore failed", "error", err)
	} else if snapIdx > 0 {
		slog.Info("restored from snapshot", "index", snapIdx)
	}

	// Wrapping chain (inner → outer): distBackend → packblob → cachedBackend →
	// WAL → pullthrough. Mirrors the pre-unification local path so operators
	// get identical semantics (small-object packing, LRU cache, PITR replay,
	// upstream pull-through) regardless of peer count.
	var inner storage.Backend = distBackend

	// Pack small objects into blob files when --pack-threshold is set.
	packThreshold, _ := cmd.Flags().GetInt("pack-threshold")
	if packThreshold > 0 {
		blobDir := filepath.Join(dataDir, "blobs")
		pb, err := packblob.NewPackedBackend(inner, blobDir, int64(packThreshold))
		if err != nil {
			return fmt.Errorf("failed to initialize packed blob: %w", err)
		}
		inner = pb
		slog.Info("packed blob storage enabled", "threshold", packThreshold)
	}

	// Wrap with LRU read cache. Raft FSM-based invalidation ensures cache
	// consistency across nodes.
	cachedBackend := storage.NewCachedBackend(inner)

	// Wire OnApply callback: invalidate cache + update metrics on committed entries
	distBackend.SetOnApply(func(cmdType cluster.CommandType, bucket, key string) {
		cachedBackend.InvalidateKey(bucket, key)
	})

	stopApply := make(chan struct{})
	go distBackend.RunApplyLoop(stopApply)

	// Start balancer if enabled (cluster mode only).
	var balancerProposer *cluster.BalancerProposer
	var gossipReceiver *cluster.GossipReceiver
	balancerEnabled, _ := cmd.Flags().GetBool("balancer-enabled")
	if balancerEnabled {
		bGossipInterval, _ := cmd.Flags().GetDuration("balancer-gossip-interval")
		statsStore := cluster.NewNodeStatsStore(3 * bGossipInterval)
		ecData, _ := cmd.Flags().GetInt("ec-data")
		ecParity, _ := cmd.Flags().GetInt("ec-parity")
		var err error
		balancerProposer, gossipReceiver, err = startBalancer(ctx, cmd, nodeID, dataDir, statsStore, node, peers, fsm, quicTransport, shardSvc, ecData+ecParity)
		if err != nil {
			slog.Warn("balancer start failed", "err", err)
		}
	}

	// Ensure a single GossipReceiver drains tr.Receive() whenever a feature
	// needs StreamReceipt gossip (heal-receipt's RoutingCache lives on this
	// path). Only one consumer is allowed because Receive() is a single
	// channel — competing readers would deliver each message to only one.
	// When balancer is off but heal-receipt is on, create a bare receiver;
	// its NodeStatsStore is unused in this path but required by the ctor.
	healReceiptEnabled, _ := cmd.Flags().GetBool("heal-receipt-enabled")
	if gossipReceiver == nil && healReceiptEnabled {
		bGossipInterval, _ := cmd.Flags().GetDuration("balancer-gossip-interval")
		standaloneStats := cluster.NewNodeStatsStore(3 * bGossipInterval)
		gossipReceiver = cluster.NewGossipReceiver(quicTransport, standaloneStats)
		go gossipReceiver.Run(ctx)
		slog.Info("gossip receiver started (receipt-only, balancer disabled)", "component", "gossip")
	}

	var backend storage.Backend = cachedBackend

	// Slice 5 of refactor/unify-storage-paths: wrap with WAL so object-level
	// mutations are captured for PITR. Raft log covers metadata consistency;
	// WAL covers PUT/DELETE/CompleteMultipart replay at the object layer. WAL
	// sits outside the read cache so every mutation is recorded before the
	// cache invalidation races the next request — same ordering as runLocalNode.
	//
	walDir := filepath.Join(dataDir, "wal")
	w, err := wal.Open(walDir)
	if err != nil {
		return fmt.Errorf("open WAL: %w", err)
	}
	defer w.Close()
	backend = wal.NewBackend(backend, w)

	// Wrap with pull-through cache if upstream is configured.
	if upstreamEndpoint, _ := cmd.Flags().GetString("upstream"); upstreamEndpoint != "" {
		upstreamAccessKey, _ := cmd.Flags().GetString("upstream-access-key")
		upstreamSecretKey, _ := cmd.Flags().GetString("upstream-secret-key")
		up, err := pullthrough.NewS3Upstream(upstreamEndpoint, upstreamAccessKey, upstreamSecretKey)
		if err != nil {
			return fmt.Errorf("init upstream: %w", err)
		}
		backend = pullthrough.NewBackend(backend, up)
		slog.Info("pull-through cache enabled", "upstream", upstreamEndpoint)
	}

	// Start auto-snapshotter for object-level PITR snapshots (separate from
	// Raft snapshots above). Uses the WAL-wrapped backend so replay is
	// anchored to the object mutation log.
	snapInterval, _ := cmd.Flags().GetDuration("snapshot-interval")
	snapRetain, _ := cmd.Flags().GetInt("snapshot-retain")
	if snapInterval > 0 {
		if snapshotable, ok := backend.(storage.Snapshotable); ok {
			snapDir := filepath.Join(dataDir, "snapshots")
			objSnapMgr, err := snapshot.NewManager(snapDir, snapshotable, walDir)
			if err != nil {
				slog.Warn("auto-snapshot init failed", "err", err)
			} else {
				as := snapshot.NewAutoSnapshotter(objSnapMgr, snapInterval, snapRetain)
				as.Start(ctx)
				slog.Info("auto-snapshot enabled", "interval", snapInterval, "retain", snapRetain)
			}
		} else {
			slog.Debug("auto-snapshot skipped: backend does not implement Snapshotable")
		}
	}

	// DiskCollector exposes grainfs_disk_used_pct metric. In multi-node mode
	// the balancer owns its own collector; in singleton mode nothing else
	// would emit disk stats. Register unconditionally — duplicate registration
	// is guarded inside NewDiskCollector.
	diskCollector := cluster.NewDiskCollector(nodeID, dataDir, nil, 30*time.Second)
	go diskCollector.Run(ctx)

	// Auto-create "default" bucket on startup. In cluster mode this must
	// wait for Raft to elect a leader before the proposal can commit.
	// Retry for up to 30s with backoff so the first node's boot does not
	// fail the whole cluster when peers come up in any order.
	//
	// For nodes joining an existing cluster (i.e., cluster mode with peers),
	// the default bucket should already exist. If it doesn't, a follower can't
	// create it anyway (only the leader can propose). We log a warning but
	// don't fail startup to allow cluster reconfiguration.
	if err := createDefaultBucketWithRetry(ctx, backend, 30*time.Second); err != nil {
		if len(peers) > 0 {
			// Cluster mode with peers: joining an existing cluster.
			// Default bucket may or may not exist; either way, follower can't create it.
			slog.Warn("default bucket creation failed on follower (may already exist)", "error", err)
		} else {
			// Single-node mode: default bucket must be created.
			return fmt.Errorf("create default bucket: %w", err)
		}
	}

	slog.Info("server started", "component", "server", "version", version,
		"node_id", nodeID, "raft_addr", raftAddr, "peers", peers, "addr", addr, "data", dataDir)

	clusterAlertWebhook, _ := cmd.Flags().GetString("alert-webhook")
	clusterAlertSecret, _ := cmd.Flags().GetString("alert-webhook-secret")
	clusterAlerts := server.NewAlertsState(clusterAlertWebhook, alerts.Options{Secret: clusterAlertSecret}, alerts.DegradedConfig{})
	srvOpts := []server.Option{
		server.WithClusterInfo(&raftClusterInfo{node: node, peers: peers}),
		server.WithEventStore(eventstore.New(db)),
		server.WithAlerts(clusterAlerts),
		server.WithDataDir(dataDir),
	}
	// Propagate S3 auth from --access-key / --secret-key. Previously this
	// was local-only; cluster mode silently ran without auth regardless of
	// the flags.
	srvOpts = append(srvOpts, authOpts...)
	if balancerProposer != nil {
		srvOpts = append(srvOpts, server.WithBalancerInfo(&balancerInfoAdapter{p: balancerProposer}))
	}

	// Phase 16 Week 5 Slice 2 — HealReceipt API + gossip + broadcast fallback.
	newSrvOpts, receiptWiring, err := setupClusterReceipt(
		ctx, cmd, dataDir, nodeID, clusterKey, peers,
		quicTransport, router, gossipReceiver, srvOpts,
	)
	if err != nil {
		return fmt.Errorf("heal-receipt wiring: %w", err)
	}
	srvOpts = newSrvOpts
	defer receiptWiring.Close()

	// Slice 4 of refactor/unify-storage-paths: cluster-mode lifecycle.
	// Construct the manager before srv.New so the S3 PutBucketLifecycle API
	// can reuse the same config store the worker scans. The worker itself
	// runs leader-only — see LifecycleManager.Run.
	lifecycleInterval, _ := cmd.Flags().GetDuration("lifecycle-interval")
	var lifecycleMgr *cluster.LifecycleManager
	if lifecycleInterval > 0 {
		lifecycleMgr = cluster.NewLifecycleManager(distBackend, lifecycleInterval)
		srvOpts = append(srvOpts, server.WithLifecycleStore(lifecycleMgr.Store()))
	}

	srv := server.New(addr, backend, srvOpts...)

	// receiptWiring.keyStore may be nil when heal-receipt is disabled — in that
	// case NewReceiptTrackingEmitter degrades to a pass-through (signing unhealthy).
	var activeEmitter scrubber.Emitter = srv.HealEmitter()
	if receiptWiring != nil && receiptWiring.store != nil {
		rte := server.NewReceiptTrackingEmitter(srv.HealEmitter(), receiptWiring.store, receiptWiring.keyStore)
		defer rte.Close()
		activeEmitter = rte
	}

	// Phase 16 Week 3: cluster mode also needs startup recovery for the
	// node's local data dir (per-node multipart parts + .tmp leftovers).
	if rec, err := server.RunStartupRecovery(ctx, dataDir, activeEmitter); err != nil && !errors.Is(err, context.Canceled) {
		slog.Warn("startup recovery failed", "err", err)
	} else if rec.OrphanTmpRemoved+rec.OrphanMultipartRemoved+len(rec.Errors) > 0 {
		slog.Info("startup recovery summary",
			"orphan_tmp", rec.OrphanTmpRemoved,
			"orphan_multipart", rec.OrphanMultipartRemoved,
			"errors", len(rec.Errors))
	}

	// Cluster-mode scrubber with ShardOwner filtering.
	// Each node only verifies shards assigned to it in the placement vector,
	// avoiding redundant cross-node I/O. RepairShard is idempotent so
	// concurrent repair from multiple nodes is safe.
	//
	// ShardPlacementMonitor detects locally-missing shards between full
	// scrub cycles; its onMissing callback calls RepairShardLocal which
	// resolves the latest version and pulls survivor shards from peers.
	scrubInterval, _ := cmd.Flags().GetDuration("scrub-interval")
	if scrubInterval > 0 && distBackend.ECActive() {
		sc := scrubber.New(distBackend, scrubInterval)
		sc.SetEmitter(activeEmitter)
		sc.Start(ctx)

		placementMonitor := cluster.NewShardPlacementMonitor(fsm, shardSvc, distBackend.NodeID(), scrubInterval)
		placementMonitor.SetOnMissing(func(bucket, key string, shardIdx int) {
			if err := distBackend.RepairShardLocal(bucket, key, "", shardIdx); err != nil {
				slog.Warn("placement monitor repair failed", "bucket", bucket, "key", key, "shard", shardIdx, "err", err)
			}
		})
		go placementMonitor.Start(ctx)
		slog.Info("cluster scrubber started", "interval", scrubInterval)
	}

	// Start the leader-aware worker loop. Only the Raft leader runs the
	// worker; followers skip the scan so we don't waste IO on proposals that
	// would be rejected anyway. LifecycleManager polls node.State() and
	// starts/stops the worker on leadership transitions.
	if lifecycleMgr != nil {
		go lifecycleMgr.Run(ctx)
		slog.Info("cluster lifecycle manager started", "interval", lifecycleInterval)
	}

	go func() {
		if err := srv.Run(); err != nil {
			slog.Error("http server error", "error", err)
		}
	}()

	// Post-Phase-18 local-path merge: universal node services (NFS/NFSv4/NBD)
	// are now wired in cluster mode too, not just local. Formerly local-only
	// because runCluster never called the NFS/NBD wiring. Scrubber/lifecycle
	// remain local-specific pending ECBackend→cluster integration (A.2).
	nfsPort, _ := cmd.Flags().GetInt("nfs-port")
	nfs4Port, _ := cmd.Flags().GetInt("nfs4-port")
	nbdPort, _ := cmd.Flags().GetInt("nbd-port")
	nbdVolumeSize, _ := cmd.Flags().GetInt64("nbd-volume-size")
	nodeSvc := startNodeServices(ctx, cmd, backend, nfsPort, nfs4Port, nbdPort, nbdVolumeSize)
	defer nodeSvc.Close()

	<-ctx.Done()
	slog.Info("graceful shutdown started", "component", "server")

	// 1. Drain in-flight HTTP requests
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Warn("http server shutdown error", "error", err)
	}

	// 2. Transfer Raft leadership before stopping
	if err := node.TransferLeadership(); err != nil {
		slog.Debug("leadership transfer skipped", "reason", err)
	} else {
		slog.Info("leadership transferred", "component", "raft")
	}

	// 3. Stop Raft apply loop
	close(stopApply)

	slog.Info("server stopped", "component", "server")
	return nil
}

// loadOrCreateEncryptionKey loads a key from file or auto-generates one in the data directory.
// If keyFile is explicitly provided (non-empty) and the file does not exist, an error is returned
// rather than silently generating a new key — a missing explicit path likely means a mount failure,
// and generating a new key would make all existing shards permanently unreadable.
func loadOrCreateEncryptionKey(keyFile, dataDir string) (*encrypt.Encryptor, error) {
	explicitPath := keyFile != ""
	if !explicitPath {
		keyFile = filepath.Join(dataDir, "encryption.key")
	}

	keyData, err := os.ReadFile(keyFile)
	if err == nil {
		slog.Info("at-rest encryption enabled", "component", "server", "key_file", keyFile)
		return encrypt.NewEncryptor(keyData)
	}

	if !os.IsNotExist(err) {
		return nil, fmt.Errorf("read key file: %w", err)
	}

	if explicitPath {
		return nil, fmt.Errorf("encryption key file not found: %s (mount failure?): %w", keyFile, err)
	}

	// Auto-generate a new key only for the default path.
	if err := os.MkdirAll(filepath.Dir(keyFile), 0o755); err != nil {
		return nil, fmt.Errorf("create key dir: %w", err)
	}
	keyData = make([]byte, 32)
	if _, err := rand.Read(keyData); err != nil {
		return nil, fmt.Errorf("generate key: %w", err)
	}
	if err := os.WriteFile(keyFile, keyData, 0o600); err != nil {
		return nil, fmt.Errorf("write key file: %w", err)
	}

	slog.Info("at-rest encryption enabled (auto-generated key)", "component", "server", "key_file", keyFile)
	return encrypt.NewEncryptor(keyData)
}

// raftBalancerAdapter wraps *raft.Node to implement cluster.RaftBalancerNode.
type raftBalancerAdapter struct {
	node  *raft.Node
	peers []string // Raft peer addresses (node IDs for balancer purposes)
}

func (a *raftBalancerAdapter) Propose(data []byte) error { return a.node.Propose(data) }
func (a *raftBalancerAdapter) IsLeader() bool            { return a.node.State() == raft.Leader }
func (a *raftBalancerAdapter) NodeID() string            { return a.node.ID() }
func (a *raftBalancerAdapter) PeerIDs() []string         { return a.peers }
func (a *raftBalancerAdapter) TransferLeadership() error { return a.node.TransferLeadership() }

// startBalancer wires and launches the BalancerProposer, GossipSender, GossipReceiver,
// MigrationExecutor and migration task channel, then replays any persisted pending tasks.
// Returns the GossipReceiver so the caller can wire additional StreamType consumers
// (e.g. Phase 16 Slice 2 receipt gossip) onto the same receiver.
func startBalancer(
	ctx context.Context,
	cmd *cobra.Command,
	nodeID, dataDir string,
	statsStore *cluster.NodeStatsStore,
	node *raft.Node,
	peers []string,
	fsm *cluster.FSM,
	quicTransport transport.Transport,
	shardSvc *cluster.ShardService,
	numShards int,
) (*cluster.BalancerProposer, *cluster.GossipReceiver, error) {
	gossipInterval, _ := cmd.Flags().GetDuration("balancer-gossip-interval")
	triggerPct, _ := cmd.Flags().GetFloat64("balancer-imbalance-trigger-pct")
	stopPct, _ := cmd.Flags().GetFloat64("balancer-imbalance-stop-pct")
	migrationRate, _ := cmd.Flags().GetInt("balancer-migration-rate")
	tenureMin, _ := cmd.Flags().GetDuration("balancer-leader-tenure-min")
	warmupTimeout, _ := cmd.Flags().GetDuration("balancer-warmup-timeout")
	cbThreshold, _ := cmd.Flags().GetFloat64("balancer-cb-threshold")
	if cbThreshold < 0 || cbThreshold > 1 {
		return nil, nil, fmt.Errorf("balancer-cb-threshold must be in [0, 1], got %g", cbThreshold)
	}
	migMaxRetries, _ := cmd.Flags().GetInt("balancer-migration-max-retries")
	migPendingTTL, _ := cmd.Flags().GetDuration("balancer-migration-pending-ttl")

	def := cluster.DefaultBalancerConfig()
	cfg := cluster.BalancerConfig{
		GossipInterval:      gossipInterval,
		WarmupTimeout:       warmupTimeout,
		ImbalanceTriggerPct: triggerPct,
		ImbalanceStopPct:    stopPct,
		MigrationRate:       migrationRate,
		LeaderTenureMin:     tenureMin,
		LeaderLoadThreshold: def.LeaderLoadThreshold,
		GracePeriod:         def.GracePeriod,
		PeerSeenWindow:      def.PeerSeenWindow,
		CBThreshold:         cbThreshold,
		MigrationMaxRetries: migMaxRetries,
		MigrationPendingTTL: migPendingTTL,
	}

	adapter := &raftBalancerAdapter{node: node, peers: peers}
	balancer := cluster.NewBalancerProposer(nodeID, statsStore, adapter, cfg)

	balancer.SetObjectPicker(cluster.NewLocalObjectPicker(filepath.Join(dataDir, "shards")))

	// Migration task channel (buffered to absorb bursts).
	taskCh := make(chan cluster.MigrationTask, 256)

	exec := cluster.NewMigrationExecutorWithTTL(shardSvc, adapter, numShards, migPendingTTL)
	if migMaxRetries > 0 {
		exec.SetMaxWriteRetries(migMaxRetries)
	}
	exec.Start(ctx)

	// Wire FSM hooks: migration proposals → channel, Raft commit → executor,
	// balancer → release inflight slot on done.
	fsm.SetMigrationHooks(taskCh, exec, balancer)

	// Gossip: broadcast local stats + receive from peers.
	sender := cluster.NewGossipSender(nodeID, peers, quicTransport, statsStore, gossipInterval)
	receiver := cluster.NewGossipReceiver(quicTransport, statsStore)

	go sender.Run(ctx)
	go receiver.Run(ctx)
	// Start executor before RecoverPending so the channel consumer is ready.
	go exec.Run(ctx, taskCh)
	go balancer.Run(ctx)

	// Seed local node stats so GossipSender can broadcast immediately.
	statsStore.Set(cluster.NodeStats{
		NodeID:   nodeID,
		JoinedAt: time.Now(),
	})

	// Start DiskCollector: reads local disk stats and updates the store every gossip interval.
	// GRAINFS_TEST_DISK_PCT overrides the real syscall for integration testing.
	collector := cluster.NewDiskCollector(nodeID, dataDir, statsStore, gossipInterval)
	if testPctStr := os.Getenv("GRAINFS_TEST_DISK_PCT"); testPctStr != "" {
		var testPct float64
		if _, err := fmt.Sscanf(testPctStr, "%f", &testPct); err != nil {
			return nil, nil, fmt.Errorf("GRAINFS_TEST_DISK_PCT: invalid value %q: %w", testPctStr, err)
		}
		if testPct < 0 || testPct > 100 {
			return nil, nil, fmt.Errorf("GRAINFS_TEST_DISK_PCT: value %v out of range [0,100]", testPct)
		}
		slog.Warn("GRAINFS_TEST_DISK_PCT active — real disk stats overridden", "pct", testPct)
		collector.SetStatFunc(func(string) (float64, uint64) { return testPct, 0 })
	}
	go collector.Run(ctx)

	// Replay any tasks that were persisted during a previous channel-full event.
	if err := fsm.RecoverPending(ctx, taskCh); err != nil {
		slog.Warn("balancer: recover pending failed", "err", err)
	}

	slog.Info("balancer started", "component", "balancer",
		"gossip_interval", gossipInterval, "trigger_pct", triggerPct, "stop_pct", stopPct)
	return balancer, receiver, nil
}

// createDefaultBucketWithRetry keeps trying the "default" bucket proposal
// until Raft commits one (quorum reached, leader elected) or the deadline
// elapses. "ErrBucketAlreadyExists" is success. "not the leader" and
// transport errors are treated as transient.
// filterEmpty drops "" entries from the slice. strings.Split(",",",") and
// strings.Split("",",") both yield elements that would be wasted as peer
// addresses — gossip sends to "" log a warning every tick.
func filterEmpty(ss []string) []string {
	out := ss[:0]
	for _, s := range ss {
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

func createDefaultBucketWithRetry(ctx context.Context, backend storage.Backend, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	backoff := 100 * time.Millisecond
	const maxBackoff = 2 * time.Second
	var lastErr error
	for {
		err := backend.CreateBucket("default")
		if err == nil || errors.Is(err, storage.ErrBucketAlreadyExists) {
			return nil
		}
		lastErr = err
		if time.Now().After(deadline) {
			return fmt.Errorf("default bucket not created after %s: %w", timeout, lastErr)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

// balancerInfoAdapter adapts *cluster.BalancerProposer to server.BalancerInfo.
type balancerInfoAdapter struct {
	p *cluster.BalancerProposer
}

func (a *balancerInfoAdapter) Status() server.BalancerStatusResult {
	st := a.p.Status()
	nodes := make([]server.BalancerNodeInfo, len(st.Nodes))
	for i, n := range st.Nodes {
		nodes[i] = server.BalancerNodeInfo{
			NodeID:         n.NodeID,
			DiskUsedPct:    n.DiskUsedPct,
			DiskAvailBytes: n.DiskAvailBytes,
			RequestsPerSec: n.RequestsPerSec,
			JoinedAt:       n.JoinedAt,
			UpdatedAt:      n.UpdatedAt,
		}
	}
	return server.BalancerStatusResult{
		Active:       st.Active,
		ImbalancePct: st.ImbalancePct,
		Nodes:        nodes,
	}
}

// raftClusterInfo adapts raft.Node to server.ClusterInfo interface.
type raftClusterInfo struct {
	node  *raft.Node
	peers []string
}

func (r *raftClusterInfo) NodeID() string   { return r.node.ID() }
func (r *raftClusterInfo) State() string    { return r.node.State().String() }
func (r *raftClusterInfo) Term() uint64     { return r.node.Term() }
func (r *raftClusterInfo) LeaderID() string { return r.node.LeaderID() }
func (r *raftClusterInfo) Peers() []string  { return r.peers }
