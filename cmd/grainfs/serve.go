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
	"github.com/gritive/GrainFS/internal/erasure"
	"github.com/gritive/GrainFS/internal/eventstore"
	"github.com/gritive/GrainFS/internal/lifecycle"
	"github.com/gritive/GrainFS/internal/nfs4server"
	"github.com/gritive/GrainFS/internal/nfsserver"
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
	"github.com/gritive/GrainFS/internal/vfs"
	"github.com/gritive/GrainFS/internal/volume"
)

func init() {
	serveCmd.Flags().StringP("data", "d", "./data", "data directory")
	serveCmd.Flags().IntP("port", "p", 9000, "listen port")
	serveCmd.Flags().String("node-id", "", "unique node ID (auto-generated if omitted)")
	serveCmd.Flags().String("raft-addr", "", "Raft listen address (required when --peers is set)")
	serveCmd.Flags().String("cluster-key", "", "Pre-shared key for cluster peer authentication")
	serveCmd.Flags().String("peers", "", "comma-separated list of peer Raft addresses (enables cluster mode)")
	serveCmd.Flags().Bool("ec", true, "enable erasure coding (Reed-Solomon 4+2, use --ec=false to disable)")
	serveCmd.Flags().Int("ec-data", erasure.DefaultDataShards, "number of data shards for erasure coding")
	serveCmd.Flags().Int("ec-parity", erasure.DefaultParityShards, "number of parity shards for erasure coding")
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

	ecEnabled, _ := cmd.Flags().GetBool("ec")
	ecData, _ := cmd.Flags().GetInt("ec-data")
	ecParity, _ := cmd.Flags().GetInt("ec-parity")

	var authOpts []server.Option
	accessKey, _ := cmd.Flags().GetString("access-key")
	secretKey, _ := cmd.Flags().GetString("secret-key")
	if accessKey != "" && secretKey != "" {
		authOpts = append(authOpts, server.WithAuth([]s3auth.Credentials{
			{AccessKey: accessKey, SecretKey: secretKey},
		}))
	}

	var ecOpts []erasure.ECOption
	noEncryption, _ := cmd.Flags().GetBool("no-encryption")
	if !noEncryption {
		encKeyFile, _ := cmd.Flags().GetString("encryption-key-file")
		enc, err := loadOrCreateEncryptionKey(encKeyFile, dataDir)
		if err != nil {
			return fmt.Errorf("encryption setup: %w", err)
		}
		ecOpts = append(ecOpts, erasure.WithEncryption(enc))
	}

	nfsPort, _ := cmd.Flags().GetInt("nfs-port")
	nfs4Port, _ := cmd.Flags().GetInt("nfs4-port")
	nbdPort, _ := cmd.Flags().GetInt("nbd-port")
	nbdVolumeSize, _ := cmd.Flags().GetInt64("nbd-volume-size")
	packThreshold, _ := cmd.Flags().GetInt("pack-threshold")

	if peersStr == "" {
		var backend storage.Backend
		var err error
		var sc *scrubber.BackgroundScrubber
		var lcStore *lifecycle.Store
		var lcWorker *lifecycle.Worker
		if ecEnabled {
			ecBackend, ecErr := erasure.NewECBackend(dataDir, ecData, ecParity, ecOpts...)
			if ecErr != nil {
				return fmt.Errorf("failed to initialize storage: %w", ecErr)
			}
			scrubInterval, _ := cmd.Flags().GetDuration("scrub-interval")
			if scrubInterval > 0 {
				sc = scrubber.New(ecBackend, scrubInterval)
			}
			lifecycleInterval, _ := cmd.Flags().GetDuration("lifecycle-interval")
			if lifecycleInterval > 0 {
				lcStore = lifecycle.NewStore(ecBackend.DB())
				lcWorker = lifecycle.NewWorker(lcStore, ecBackend, &ecDeleterAdapter{ecBackend}, lifecycleInterval)
			}
			backend = ecBackend
		} else {
			backend, err = storage.NewLocalBackend(dataDir)
		}
		if err != nil {
			return fmt.Errorf("failed to initialize storage: %w", err)
		}

		// Capture DB before wrapping for event store (DBProvider is either LocalBackend or ECBackend)
		var evDB *badger.DB
		if dp, ok := backend.(storage.DBProvider); ok {
			evDB = dp.DB()
			// Phase 16 Week 3: Badger preflight. badger.Open already runs
			// internal recovery; this confirms the DB is actually writable
			// before we accept traffic and gives the operator a recovery
			// guide if it isn't. Fail-fast is the right outcome — booting a
			// dashboard against a broken DB only confuses the postmortem.
			if err := server.PreflightBadger(evDB, dataDir, nil); err != nil {
				return err
			}
		}

		// Wrap with Packed Blob if threshold is set
		if packThreshold > 0 {
			blobDir := filepath.Join(dataDir, "blobs")
			pb, err := packblob.NewPackedBackend(backend, blobDir, int64(packThreshold))
			if err != nil {
				return fmt.Errorf("failed to initialize packed blob: %w", err)
			}
			backend = pb
			slog.Info("packed blob storage enabled", "threshold", packThreshold)
		}

		// Wrap with read cache
		backend = storage.NewCachedBackend(backend)

		// Wrap with WAL for PITR support
		walDir := filepath.Join(dataDir, "wal")
		w, err := wal.Open(walDir)
		if err != nil {
			return fmt.Errorf("open WAL: %w", err)
		}
		defer w.Close()
		backend = wal.NewBackend(backend, w)

		mode := "solo"
		if ecEnabled {
			mode = "solo-ec"
		}

		// Wrap with pull-through cache if upstream is configured
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

		// Wrap backend in SwappableBackend to allow runtime cluster transition
		swappable := storage.NewSwappableBackend(backend)
		return runSoloWithNFS(ctx, cmd, addr, dataDir, mode, swappable, authOpts, sc, lcStore, lcWorker, nfsPort, nfs4Port, nbdPort, nbdVolumeSize, evDB)
	}

	nodeID, _ := cmd.Flags().GetString("node-id")
	raftAddr, _ := cmd.Flags().GetString("raft-addr")
	clusterKey, _ := cmd.Flags().GetString("cluster-key")
	return runCluster(ctx, cmd, addr, dataDir, nodeID, raftAddr, peersStr, clusterKey)
}

func runSoloWithNFS(ctx context.Context, cmd *cobra.Command, addr, dataDir, mode string, swappable *storage.SwappableBackend, opts []server.Option, sc *scrubber.BackgroundScrubber, lcStore *lifecycle.Store, lcWorker *lifecycle.Worker, nfsPort, nfs4Port, nbdPort int, nbdVolumeSize int64, evDB *badger.DB) error {
	slog.Info("server started", "component", "server", "mode", mode, "version", version, "addr", addr, "data", dataDir)

	// Start DiskCollector to expose grainfs_disk_used_pct metric even in solo mode.
	soloNodeID := generateNodeID(dataDir)
	diskCollector := cluster.NewDiskCollector(soloNodeID, dataDir, nil, 30*time.Second)
	go diskCollector.Run(ctx)

	// Auto-create "default" bucket on startup
	if err := swappable.CreateBucket("default"); err != nil {
		if !errors.Is(err, storage.ErrBucketAlreadyExists) {
			return fmt.Errorf("create default bucket: %w", err)
		}
	}

	soloManagedMode, _ := cmd.Flags().GetBool("badger-managed-mode")
	soloLogGCInterval, _ := cmd.Flags().GetDuration("raft-log-gc-interval")

	// Join cluster callback: transitions from solo to cluster mode at runtime.
	joinFn := func(nodeID, raftAddr, peersStr, clusterKey string) error {
		return joinClusterLive(ctx, swappable, dataDir, nodeID, raftAddr, peersStr, clusterKey, soloManagedMode, soloLogGCInterval)
	}
	opts = append(opts, server.WithJoinCluster(joinFn))
	opts = append(opts, server.WithDataDir(dataDir))
	if evDB != nil {
		opts = append(opts, server.WithEventStore(eventstore.New(evDB)))
	}
	if sc != nil {
		opts = append(opts, server.WithScrubber(sc))
		// NOTE: sc.Start() is called AFTER server.New so the scrubber can be
		// wired to the server-owned heal emitter. Without this ordering the
		// dashboard would never receive HealEvents.
	}

	// Phase 16 Week 4: webhook alerts (degraded mode + critical events).
	alertWebhook, _ := cmd.Flags().GetString("alert-webhook")
	alertSecret, _ := cmd.Flags().GetString("alert-webhook-secret")
	alertsState := server.NewAlertsState(alertWebhook, alerts.Options{Secret: alertSecret}, alerts.DegradedConfig{})
	opts = append(opts, server.WithAlerts(alertsState))
	if lcStore != nil {
		opts = append(opts, server.WithLifecycleStore(lcStore))
	}
	if lcWorker != nil {
		go lcWorker.Run(ctx)
		slog.Info("lifecycle worker started")
	}

	srv := server.New(addr, swappable, opts...)

	// Phase 16 Week 3: sweep crash-leftover artifacts BEFORE the scrubber
	// starts so the scrubber doesn't trip over half-written .tmp files.
	// HealEvents flow through the server emitter — operator sees the
	// "Restart Recovery" dashboard line right after boot.
	healEmitter := srv.HealEmitter()
	if rec, err := server.RunStartupRecovery(ctx, dataDir, healEmitter); err != nil && !errors.Is(err, context.Canceled) {
		slog.Warn("startup recovery failed", "err", err)
	} else if rec.OrphanTmpRemoved+rec.OrphanMultipartRemoved+len(rec.Errors) > 0 {
		slog.Info("startup recovery summary",
			"orphan_tmp", rec.OrphanTmpRemoved,
			"orphan_multipart", rec.OrphanMultipartRemoved,
			"errors", len(rec.Errors))
	}

	if sc != nil {
		sc.SetEmitter(healEmitter)
		sc.Start(ctx)
		slog.Info("background scrubber started")
	}
	go func() {
		if err := srv.Run(); err != nil {
			slog.Error("http server error", "error", err)
		}
	}()

	// Start NFS server if requested
	var nfsSrv *nfsserver.Server
	if nfsPort > 0 {
		fmt.Println("WARNING: NFS null auth enabled — all NFS access is unauthenticated")
		const defaultVolName = "default"
		const defaultVolSize = 1024 * 1024 * 1024 // 1G

		mgr := volume.NewManager(swappable)
		// Ensure a default volume exists for NFS
		if _, err := mgr.Get(defaultVolName); err != nil {
			if _, err := mgr.Create(defaultVolName, defaultVolSize); err != nil {
				slog.Warn("default nfs volume create failed (may already exist)", "error", err)
			}
		}

		nfsSrv = nfsserver.NewServer(swappable, defaultVolName, nil, // no registry in solo mode
			vfs.WithStatCacheTTL(1*time.Second),
			vfs.WithDirCacheTTL(1*time.Second),
		)
		go func() {
			nfsAddr := fmt.Sprintf(":%d", nfsPort)
			if err := nfsSrv.ListenAndServe(nfsAddr); err != nil {
				slog.Error("nfs server error", "error", err)
			}
		}()
	}

	// Start NFSv4 server if requested
	var nfs4Srv *nfs4server.Server
	if nfs4Port > 0 {
		nfs4Srv = nfs4server.NewServer(swappable)
		go func() {
			nfs4Addr := fmt.Sprintf("127.0.0.1:%d", nfs4Port) // localhost only for AUTH_SYS security
			if err := nfs4Srv.ListenAndServe(nfs4Addr); err != nil {
				slog.Error("nfs4 server error", "error", err)
			}
		}()
	}

	// Start auto-snapshotter if interval is configured
	snapInterval, _ := cmd.Flags().GetDuration("snapshot-interval")
	snapRetain, _ := cmd.Flags().GetInt("snapshot-retain")
	if snapInterval > 0 {
		snapDir := filepath.Join(dataDir, "snapshots")
		walDir := filepath.Join(dataDir, "wal")
		snapMgr, err := snapshot.NewManager(snapDir, swappable, walDir)
		if err != nil {
			slog.Warn("auto-snapshot init failed", "err", err)
		} else {
			as := snapshot.NewAutoSnapshotter(snapMgr, snapInterval, snapRetain)
			as.Start(ctx)
			slog.Info("auto-snapshot enabled", "interval", snapInterval, "retain", snapRetain)
		}
	}

	// Start NBD server if requested (Linux only)
	if nbdPort > 0 {
		const defaultVolName2 = "default"

		mgr2 := volume.NewManager(swappable)
		if _, err := mgr2.Get(defaultVolName2); err != nil {
			if _, err := mgr2.Create(defaultVolName2, nbdVolumeSize); err != nil {
				slog.Warn("default nbd volume create failed", "error", err)
			}
		}

		if _, err := startNBDServer(mgr2, defaultVolName2, nbdPort); err != nil {
			slog.Error("nbd server start failed", "error", err)
		}
	}

	<-ctx.Done()
	slog.Info("graceful shutdown started", "component", "server")

	// 1. Drain in-flight HTTP requests
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Warn("http server shutdown error", "error", err)
	}

	// 2. Close NFS server
	if nfsSrv != nil {
		if err := nfsSrv.Close(); err != nil {
			slog.Warn("nfs server close error", "error", err)
		}
	}

	// 3. Close storage backend
	if closer, ok := swappable.Inner().(interface{ Close() error }); ok {
		if err := closer.Close(); err != nil {
			slog.Warn("storage backend close error", "error", err)
		}
	}

	slog.Info("server stopped", "component", "server")
	return nil
}

func runCluster(ctx context.Context, cmd *cobra.Command, addr, dataDir, nodeID, raftAddr, peersStr, clusterKey string) error {
	if nodeID == "" {
		nodeID = generateNodeID(dataDir)
		slog.Info("auto-generated node ID", "component", "server", "node_id", nodeID)
	}
	if raftAddr == "" {
		return fmt.Errorf("--raft-addr is required when --peers is set")
	}

	peers := strings.Split(peersStr, ",")

	metaDir := filepath.Join(dataDir, "meta")
	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		return fmt.Errorf("create meta dir: %w", err)
	}
	dbOpts := badger.DefaultOptions(metaDir).WithLogger(nil)
	db, err := badger.Open(dbOpts)
	if err != nil {
		return fmt.Errorf("open metadata db: %w", err)
	}
	defer db.Close()
	// Phase 16 Week 3: cluster mode preflight. Same reasoning as solo.
	if err := server.PreflightBadger(db, metaDir, nil); err != nil {
		return err
	}

	raftDir := filepath.Join(dataDir, "raft")

	// Auto-migrate: if Raft directory doesn't exist yet but metadata does,
	// automatically bootstrap from solo metadata (zero-downtime migration)
	if _, err := os.Stat(raftDir); os.IsNotExist(err) {
		if _, err := os.Stat(filepath.Join(dataDir, "meta")); err == nil {
			slog.Info("auto-migrating solo metadata to cluster format", "component", "migrate")
			if err := cluster.MigrateSoloToCluster(dataDir, nodeID); err != nil {
				return fmt.Errorf("auto-migrate: %w", err)
			}
			slog.Info("auto-migration complete", "component", "migrate")
		}
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

	// Start QUIC transport for inter-node communication
	quicTransport := transport.NewQUICTransport(clusterKey)
	if err := quicTransport.Listen(ctx, raftAddr); err != nil {
		return fmt.Errorf("start QUIC transport: %w", err)
	}
	defer quicTransport.Close()

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
	shardSvc := cluster.NewShardService(dataDir, quicTransport)

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

	// Wrap distributed backend with LRU read cache.
	// Raft FSM-based invalidation ensures cache consistency across nodes.
	cachedBackend := storage.NewCachedBackend(distBackend)

	// Wire OnApply callback: invalidate cache + update metrics on committed entries
	distBackend.SetOnApply(func(cmdType cluster.CommandType, bucket, key string) {
		cachedBackend.InvalidateKey(bucket, key)
	})

	stopApply := make(chan struct{})
	go distBackend.RunApplyLoop(stopApply)

	// Start balancer if enabled (cluster mode only).
	var balancerProposer *cluster.BalancerProposer
	balancerEnabled, _ := cmd.Flags().GetBool("balancer-enabled")
	if balancerEnabled {
		bGossipInterval, _ := cmd.Flags().GetDuration("balancer-gossip-interval")
		statsStore := cluster.NewNodeStatsStore(3 * bGossipInterval)
		ecData, _ := cmd.Flags().GetInt("ec-data")
		ecParity, _ := cmd.Flags().GetInt("ec-parity")
		var err error
		balancerProposer, err = startBalancer(ctx, cmd, nodeID, dataDir, statsStore, node, peers, fsm, quicTransport, shardSvc, ecData+ecParity)
		if err != nil {
			slog.Warn("balancer start failed", "err", err)
		}
	}

	var backend storage.Backend = cachedBackend

	// Auto-create "default" bucket on startup
	if err := backend.CreateBucket("default"); err != nil {
		if !errors.Is(err, storage.ErrBucketAlreadyExists) {
			return fmt.Errorf("create default bucket: %w", err)
		}
	}

	slog.Info("server started", "component", "server", "mode", "cluster", "version", version,
		"node_id", nodeID, "raft_addr", raftAddr, "peers", peers, "addr", addr, "data", dataDir)

	clusterAlertWebhook, _ := cmd.Flags().GetString("alert-webhook")
	clusterAlertSecret, _ := cmd.Flags().GetString("alert-webhook-secret")
	clusterAlerts := server.NewAlertsState(clusterAlertWebhook, alerts.Options{Secret: clusterAlertSecret}, alerts.DegradedConfig{})
	srvOpts := []server.Option{
		server.WithClusterInfo(&raftClusterInfo{node: node, peers: peers}),
		server.WithEventStore(eventstore.New(db)),
		server.WithAlerts(clusterAlerts),
	}
	if balancerProposer != nil {
		srvOpts = append(srvOpts, server.WithBalancerInfo(&balancerInfoAdapter{p: balancerProposer}))
	}
	srv := server.New(addr, backend, srvOpts...)

	// Phase 16 Week 3: cluster mode also needs startup recovery for the
	// node's local data dir (per-node multipart parts + .tmp leftovers).
	if rec, err := server.RunStartupRecovery(ctx, dataDir, srv.HealEmitter()); err != nil && !errors.Is(err, context.Canceled) {
		slog.Warn("startup recovery failed", "err", err)
	} else if rec.OrphanTmpRemoved+rec.OrphanMultipartRemoved+len(rec.Errors) > 0 {
		slog.Info("startup recovery summary",
			"orphan_tmp", rec.OrphanTmpRemoved,
			"orphan_multipart", rec.OrphanMultipartRemoved,
			"errors", len(rec.Errors))
	}

	go func() {
		if err := srv.Run(); err != nil {
			slog.Error("http server error", "error", err)
		}
	}()

	<-ctx.Done()
	slog.Info("graceful shutdown started", "component", "server", "mode", "cluster")

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

	slog.Info("server stopped", "component", "server", "mode", "cluster")
	return nil
}

// loadOrCreateEncryptionKey loads a key from file or auto-generates one in the data directory.
func loadOrCreateEncryptionKey(keyFile, dataDir string) (*encrypt.Encryptor, error) {
	if keyFile == "" {
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

	// Auto-generate a new key
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

// ecDeleterAdapter adapts ECBackend to lifecycle.ObjectDeleter (ListObjectVersions signature differs).
type ecDeleterAdapter struct{ b *erasure.ECBackend }

func (a *ecDeleterAdapter) DeleteObject(bucket, key string) error {
	return a.b.DeleteObject(bucket, key)
}
func (a *ecDeleterAdapter) DeleteObjectVersion(bucket, key, versionID string) error {
	return a.b.DeleteObjectVersion(bucket, key, versionID)
}
func (a *ecDeleterAdapter) ListObjectVersions(bucket, key string) ([]*storage.ObjectVersion, error) {
	// ListObjectVersions uses prefix matching; filter to exact key to avoid pruning wrong versions.
	all, err := a.b.ListObjectVersions(bucket, key, 10000)
	if err != nil {
		return nil, err
	}
	result := all[:0]
	for _, v := range all {
		if v.Key == key {
			result = append(result, v)
		}
	}
	return result, nil
}

// raftBalancerAdapter wraps *raft.Node to implement cluster.RaftBalancerNode.
type raftBalancerAdapter struct {
	node  *raft.Node
	peers []string // Raft peer addresses (node IDs for balancer purposes)
}

func (a *raftBalancerAdapter) Propose(data []byte) error   { return a.node.Propose(data) }
func (a *raftBalancerAdapter) IsLeader() bool              { return a.node.State() == raft.Leader }
func (a *raftBalancerAdapter) NodeID() string              { return a.node.ID() }
func (a *raftBalancerAdapter) PeerIDs() []string           { return a.peers }
func (a *raftBalancerAdapter) TransferLeadership() error   { return a.node.TransferLeadership() }

// startBalancer wires and launches the BalancerProposer, GossipSender, GossipReceiver,
// MigrationExecutor and migration task channel, then replays any persisted pending tasks.
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
) (*cluster.BalancerProposer, error) {
	gossipInterval, _ := cmd.Flags().GetDuration("balancer-gossip-interval")
	triggerPct, _ := cmd.Flags().GetFloat64("balancer-imbalance-trigger-pct")
	stopPct, _ := cmd.Flags().GetFloat64("balancer-imbalance-stop-pct")
	migrationRate, _ := cmd.Flags().GetInt("balancer-migration-rate")
	tenureMin, _ := cmd.Flags().GetDuration("balancer-leader-tenure-min")
	warmupTimeout, _ := cmd.Flags().GetDuration("balancer-warmup-timeout")
	cbThreshold, _ := cmd.Flags().GetFloat64("balancer-cb-threshold")
	if cbThreshold < 0 || cbThreshold > 1 {
		return nil, fmt.Errorf("balancer-cb-threshold must be in [0, 1], got %g", cbThreshold)
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
			return nil, fmt.Errorf("GRAINFS_TEST_DISK_PCT: invalid value %q: %w", testPctStr, err)
		}
		if testPct < 0 || testPct > 100 {
			return nil, fmt.Errorf("GRAINFS_TEST_DISK_PCT: value %v out of range [0,100]", testPct)
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
	return balancer, nil
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

// joinClusterLive performs the runtime solo→cluster transition.
// It starts the QUIC transport and Raft node, migrates metadata, then swaps the backend.
func joinClusterLive(ctx context.Context, swappable *storage.SwappableBackend, dataDir, nodeID, raftAddr, peersStr, clusterKey string, managedMode bool, logGCInterval time.Duration) error {
	if nodeID == "" {
		nodeID = generateNodeID(dataDir)
	}

	peers := strings.Split(peersStr, ",")

	metaDir := filepath.Join(dataDir, "meta")
	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		return fmt.Errorf("create meta dir: %w", err)
	}
	dbOpts := badger.DefaultOptions(metaDir).WithLogger(nil)
	db, err := badger.Open(dbOpts)
	if err != nil {
		return fmt.Errorf("open metadata db: %w", err)
	}
	// Phase 16 Week 3: solo-to-cluster migration path also runs preflight.
	if err := server.PreflightBadger(db, metaDir, nil); err != nil {
		return err
	}

	raftDir := filepath.Join(dataDir, "raft")

	// Auto-migrate solo metadata
	if _, err := os.Stat(raftDir); os.IsNotExist(err) {
		slog.Info("auto-migrating solo metadata to cluster format", "component", "join")
		if err := cluster.MigrateSoloToCluster(dataDir, nodeID); err != nil {
			db.Close()
			return fmt.Errorf("auto-migrate: %w", err)
		}
	}

	var joinStoreOpts []raft.BadgerLogStoreOption
	if managedMode {
		joinStoreOpts = append(joinStoreOpts, raft.WithManagedMode())
	}
	logStore, err := raft.NewBadgerLogStore(raftDir, joinStoreOpts...)
	if err != nil {
		db.Close()
		return fmt.Errorf("open raft store: %w", err)
	}

	quicTransport := transport.NewQUICTransport(clusterKey)
	if err := quicTransport.Listen(ctx, raftAddr); err != nil {
		logStore.Close()
		db.Close()
		return fmt.Errorf("start QUIC transport: %w", err)
	}

	for _, peer := range peers {
		if err := quicTransport.Connect(ctx, peer); err != nil {
			slog.Warn("failed to connect to peer", "peer", peer, "error", err)
		}
	}

	cfg := raft.DefaultConfig(nodeID, peers)
	cfg.ManagedMode = managedMode
	cfg.LogGCInterval = logGCInterval
	node := raft.NewNode(cfg, logStore)

	rpcTransport := raft.NewQUICRPCTransport(quicTransport, node)
	rpcTransport.SetTransport()

	shardSvc := cluster.NewShardService(dataDir, quicTransport)

	router := transport.NewStreamRouter()
	router.Handle(transport.StreamControl, rpcTransport.Handler())
	router.Handle(transport.StreamData, shardSvc.HandleRPC())
	quicTransport.SetStreamHandler(router.Dispatch)

	node.Start()

	distBackend, err := cluster.NewDistributedBackend(dataDir, db, node)
	if err != nil {
		node.Stop()
		quicTransport.Close()
		logStore.Close()
		db.Close()
		return fmt.Errorf("init distributed backend: %w", err)
	}

	allNodes := append([]string{raftAddr}, peers...)
	distBackend.SetShardService(shardSvc, allNodes)

	fsm := cluster.NewFSM(db)
	snapMgr := raft.NewSnapshotManager(logStore, fsm, raft.SnapshotConfig{Threshold: 10000})
	distBackend.SetSnapshotManager(snapMgr, node)

	if snapIdx, err := snapMgr.Restore(); err != nil {
		slog.Warn("snapshot restore failed", "error", err)
	} else if snapIdx > 0 {
		slog.Info("restored from snapshot", "index", snapIdx)
	}

	cachedBackend := storage.NewCachedBackend(distBackend)
	distBackend.SetOnApply(func(cmdType cluster.CommandType, bucket, key string) {
		cachedBackend.InvalidateKey(bucket, key)
	})

	stopApply := make(chan struct{})
	go distBackend.RunApplyLoop(stopApply)

	// Swap the backend atomically — new requests go to cluster mode
	swappable.Swap(cachedBackend)

	slog.Info("live cluster join complete", "component", "join",
		"node_id", nodeID, "raft_addr", raftAddr, "peers", peers)

	return nil
}
