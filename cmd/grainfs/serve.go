package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"crypto/rand"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/alerts"
	"github.com/gritive/GrainFS/internal/cache/blockcache"
	"github.com/gritive/GrainFS/internal/cache/shardcache"
	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/eventstore"
	"github.com/gritive/GrainFS/internal/icebergcatalog"
	"github.com/gritive/GrainFS/internal/metrics/readamp"
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
	"github.com/gritive/GrainFS/internal/volume"
	"github.com/gritive/GrainFS/internal/volume/dedup"
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
	serveCmd.Flags().Int("seed-groups", 0, "number of data groups to seed at bootstrap (0 = auto: max(8, (cluster_size)*4) — covers future cluster expansion)")
	serveCmd.Flags().String("access-key", "", "S3 access key for authentication (enables auth when set)")
	serveCmd.Flags().String("secret-key", "", "S3 secret key for authentication")
	serveCmd.Flags().String("encryption-key-file", "", "path to 32-byte encryption key file (auto-generated if omitted)")
	serveCmd.Flags().Bool("no-encryption", false, "disable at-rest encryption")
	serveCmd.Flags().Int("nfs4-port", 2049, "NFSv4 server port (0 = disabled); binds 0.0.0.0 — use firewall or set 0 when exposing public interfaces")
	serveCmd.Flags().Int("nbd-port", 10809, "NBD server port (0 = disabled). Client-side nbd-client still requires Linux.")
	serveCmd.Flags().Int64("nbd-volume-size", 1024*1024*1024, "default NBD volume size in bytes")
	serveCmd.Flags().Int("pack-threshold", 0, "pack objects below this size into blob files (0 = disabled, e.g. 65536)")
	serveCmd.Flags().Duration("snapshot-interval", 1*time.Hour, "auto-snapshot interval (0 to disable)")
	serveCmd.Flags().Int("snapshot-retain", 24, "number of auto-snapshots to retain")
	serveCmd.Flags().Duration("scrub-interval", 24*time.Hour, "EC shard scrub interval (0 to disable)")
	serveCmd.Flags().Duration("lifecycle-interval", 1*time.Hour, "lifecycle rule evaluation interval (0 to disable)")
	serveCmd.Flags().Duration("degraded-check-interval", 30*time.Second, "EC degraded-mode liveness check interval")
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
	// Predictive disk warnings — fires zerolog.Warn + critical webhook on transitions
	// between OK/Warn/Critical levels. Defaults match the Phase 1 design (80%/90%).
	serveCmd.Flags().Float64("disk-warn-threshold", 0.80, "disk used fraction (0-1) at which a 'disk_warn' alert+log fires")
	serveCmd.Flags().Float64("disk-critical-threshold", 0.90, "disk used fraction (0-1) at which a 'disk_critical' alert+log fires")
	// Phase 2 — direct I/O on local shard writes. Bypasses the kernel page
	// cache (Linux O_DIRECT, macOS F_NOCACHE). On by default — the bench
	// (internal/cluster/shardio_directio_bench_test.go) showed 10x on 1MB
	// shards, 40% on 4MB, neutral on 16MB. Filesystems that reject O_DIRECT
	// (some overlayfs/tmpfs) fall back to the buffered path automatically;
	// pass --direct-io=false to force buffered everywhere.
	serveCmd.Flags().Bool("direct-io", true, "bypass page cache on local EC shard writes (Linux O_DIRECT / macOS F_NOCACHE)")
	// Phase 2 #3 evaluation flag: when on, every volume-block and EC-shard
	// read is fed to the read-amplification simulator at three cache sizes
	// (16/64/256 MB equivalent) per path. Hit/miss counters appear at
	// /metrics under grainfs_readamp_*. Off by default — production pays
	// only an atomic.Bool load per read when this is unset.
	serveCmd.Flags().Bool("measure-read-amp", false, "enable read-amplification simulator (informs Unified Buffer Cache decision)")
	// Phase 2 #3 implementation: in-memory block cache for volume.ReadAt.
	// Default 64 MB matches the simulator's measured "knee" — workloads with
	// temporal locality saturate around that budget. Set 0 to disable.
	serveCmd.Flags().Int64("block-cache-size", 64*1024*1024, "volume block cache capacity in bytes (0 disables)")
	// EC shard cache (Phase 2 #3 follow-up). Sits in front of getObjectEC's
	// per-shard fan-out. Default 256 MB — multi-node measurement on PR #71
	// showed large_repeat (16 MB×10) hits 90% at every reachable cache size,
	// so the working set is small relative to memory budget. Set 0 to
	// disable when running --measure-read-amp baselines.
	serveCmd.Flags().Int64("shard-cache-size", 256*1024*1024, "EC shard cache capacity in bytes (0 disables)")
	// Phase 16 Week 5 Slice 2 — HealReceipt API + gossip.
	serveCmd.Flags().Bool("heal-receipt-enabled", true, "enable HealReceipt audit API (Phase 16 Slice 2)")
	serveCmd.Flags().String("heal-receipt-psk", "", "PSK for HealReceipt HMAC-SHA256 signing (defaults to --cluster-key in cluster mode)")
	serveCmd.Flags().Duration("heal-receipt-retention", 30*24*time.Hour, "HealReceipt retention window (older entries are GC'd)")
	serveCmd.Flags().Duration("heal-receipt-gossip-interval", 5*time.Second, "how often this node gossips its recent receipt IDs to peers")
	serveCmd.Flags().Int("heal-receipt-window", 50, "rolling window size — how many recent receipt IDs to gossip per tick")
	serveCmd.Flags().String("otel-endpoint", "", "OTLP HTTP endpoint for trace export (empty disables OTel, e.g. localhost:4318)")
	serveCmd.Flags().Float64("otel-sample-rate", 0.01, "head-based OTel trace sample rate [0.0, 1.0] (default 1%)")
	serveCmd.Flags().Int("pprof-port", 0, "expose net/http/pprof on this port (0 = disabled, for profiling e2e/load tests)")
	serveCmd.Flags().Bool("dedup", true, "enable block-level deduplication (BadgerDB index at {data}/dedup/)")
	serveCmd.Flags().Bool("shared-badger", true, "share one raft-log BadgerDB across all groups (C2). Reduces per-process instance count when many groups are seeded. Disable with --shared-badger=false only for legacy per-group dirs.")
	// Rate limit overrides — defaults are production-safe (100/200 ip, 50/100 user).
	// Benchmarks/dev/upstream-proxied deployments can relax these. 0 disables that layer.
	serveCmd.Flags().Bool("raft-log-fsync", true, "fsync the Raft log store on every append (auto: cluster=false (consensus provides redundancy), single=true; explicit value always wins)")
	serveCmd.Flags().Duration("raft-heartbeat-interval", 200*time.Millisecond, "per-group raft heartbeat interval. Lower = faster failure detection, higher CPU/network. Default 200ms balances detection latency with QUIC stream-open cost.")
	serveCmd.Flags().Duration("raft-election-timeout", 1000*time.Millisecond, "per-group raft election timeout (must be >= 3 * heartbeat-interval). Higher = fewer spurious elections under load.")
	serveCmd.Flags().Bool("quic-mux", true, "use multiplexed QUIC streams + heartbeat coalescing for per-group raft RPCs. idle-N8 measurement: 78pct drop in CPU samples, 17x drop in recvmsg syscalls vs legacy per-message path. Falls back to legacy on peer ALPN mismatch (older binaries).")
	serveCmd.Flags().Int("quic-mux-pool", 4, "stream pool size per peer when --quic-mux=true (avoids HoL with raft pipelining)")
	serveCmd.Flags().Duration("quic-mux-flush", 2*time.Millisecond, "heartbeat coalescing flush window when --quic-mux=true (must be << heartbeat-interval)")
	serveCmd.Flags().Bool("backend-vfs-fixed-version", true, "use fixed versionID 'current' for __grainfs_vfs_* buckets to bound on-disk usage; disable for legacy multi-version behavior (cluster mode only)")
	serveCmd.Flags().Float64("rate-limit-ip-rps", 100, "per-source-IP rate limit in requests/sec (0 disables)")
	serveCmd.Flags().Int("rate-limit-ip-burst", 200, "per-source-IP rate limit burst size")
	serveCmd.Flags().Float64("rate-limit-user-rps", 50, "per-authenticated-user rate limit in requests/sec (0 disables)")
	serveCmd.Flags().Int("rate-limit-user-burst", 100, "per-authenticated-user rate limit burst size")
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
	} else {
		log.Warn().Msg("S3 authentication disabled — set --access-key and --secret-key for production")
	}

	noEncryption, _ := cmd.Flags().GetBool("no-encryption")
	var shardEncryptor *encrypt.Encryptor
	if !noEncryption {
		encKeyFile, _ := cmd.Flags().GetString("encryption-key-file")
		var err error
		shardEncryptor, err = loadOrCreateEncryptionKey(encKeyFile, dataDir)
		if err != nil {
			return fmt.Errorf("encryption setup: %w\n  recovery: pass --encryption-key-file=<path> to load an existing key, or --no-encryption to disable at-rest encryption", err)
		}
	}

	if pprofPort, _ := cmd.Flags().GetInt("pprof-port"); pprofPort > 0 {
		runtime.SetMutexProfileFraction(1)
		runtime.SetBlockProfileRate(1)
		pprofAddr := fmt.Sprintf("127.0.0.1:%d", pprofPort)
		go func() {
			log.Info().Str("addr", pprofAddr).Msg("pprof listening")
			if err := http.ListenAndServe(pprofAddr, nil); err != nil {
				log.Warn().Err(err).Msg("pprof server error")
			}
		}()
	}

	otelEndpoint, _ := cmd.Flags().GetString("otel-endpoint")
	otelSampleRate, _ := cmd.Flags().GetFloat64("otel-sample-rate")
	otelShutdown, err := grainotel.Init(ctx, otelEndpoint, otelSampleRate)
	if err != nil {
		log.Warn().Err(err).Msg("otel: init failed, tracing disabled")
	} else if otelEndpoint != "" {
		log.Info().Str("endpoint", otelEndpoint).Float64("sample_rate", otelSampleRate).Msg("otel: tracing enabled")
		defer func() { _ = otelShutdown(context.Background()) }()
	}

	if err := server.RunSystemPreflight(server.PreflightConfig{
		DataDir:  dataDir,
		HTTPAddr: addr,
		NoAuth:   accessKey == "" || secretKey == "",
	}); err != nil {
		return err
	}

	nodeID, _ := cmd.Flags().GetString("node-id")
	raftAddr, _ := cmd.Flags().GetString("raft-addr")
	clusterKey, _ := cmd.Flags().GetString("cluster-key")
	return runCluster(ctx, cmd, addr, dataDir, nodeID, raftAddr, peersStr, clusterKey, authOpts, shardEncryptor)
}

func runCluster(ctx context.Context, cmd *cobra.Command, addr, dataDir, nodeID, raftAddr, peersStr, clusterKey string, authOpts []server.Option, encryptor *encrypt.Encryptor) error {
	if nodeID == "" {
		nodeID = generateNodeID(dataDir)
		log.Info().Str("component", "server").Str("node_id", nodeID).Msg("auto-generated node ID")
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
				log.Info().Str("component", "migrate").Msg("auto-migrating local metadata to cluster format")
				if err := cluster.MigrateLegacyMetaToCluster(dataDir, nodeID); err != nil {
					return fmt.Errorf("auto-migrate: %w", err)
				}
				log.Info().Str("component", "migrate").Msg("auto-migration complete")
			}
		}
	}

	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		return fmt.Errorf("create meta dir at %s: %w\n  recovery: check that the parent directory exists and the user has write permission", metaDir, err)
	}
	dbOpts := badger.DefaultOptions(metaDir).WithLogger(nil)
	db, err := badger.Open(dbOpts)
	if err != nil {
		return fmt.Errorf("open metadata db at %s: %w\n  recovery: check disk free space, confirm no other grainfs process holds the lock (lsof %s/LOCK), see README#badger-troubleshooting", metaDir, err, metaDir)
	}
	defer db.Close()
	// Phase 16 Week 3: cluster mode preflight. Same reasoning as local.
	if err := server.PreflightBadger(db, metaDir, nil); err != nil {
		return err
	}

	badgerManagedMode, _ := cmd.Flags().GetBool("badger-managed-mode")
	raftLogGCInterval, _ := cmd.Flags().GetDuration("raft-log-gc-interval")
	raftHeartbeatInterval, _ := cmd.Flags().GetDuration("raft-heartbeat-interval")
	raftElectionTimeout, _ := cmd.Flags().GetDuration("raft-election-timeout")
	if raftElectionTimeout > 0 && raftHeartbeatInterval > 0 && raftElectionTimeout < 3*raftHeartbeatInterval {
		return fmt.Errorf("--raft-election-timeout (%s) must be >= 3 * --raft-heartbeat-interval (%s)", raftElectionTimeout, raftHeartbeatInterval)
	}
	quicMuxEnabled, _ := cmd.Flags().GetBool("quic-mux")
	quicMuxPoolSize, _ := cmd.Flags().GetInt("quic-mux-pool")
	quicMuxFlushWindow, _ := cmd.Flags().GetDuration("quic-mux-flush")
	if quicMuxEnabled && quicMuxFlushWindow > 0 && raftHeartbeatInterval > 0 && quicMuxFlushWindow >= raftHeartbeatInterval {
		return fmt.Errorf("--quic-mux-flush (%s) must be << --raft-heartbeat-interval (%s)", quicMuxFlushWindow, raftHeartbeatInterval)
	}

	var storeOpts []raft.BadgerLogStoreOption
	if badgerManagedMode {
		storeOpts = append(storeOpts, raft.WithManagedMode())
	}
	logStore, err := raft.NewBadgerLogStore(raftDir, storeOpts...)
	if err != nil {
		return fmt.Errorf("open raft store at %s: %w\n  recovery: check disk free space, confirm no other grainfs process holds the lock (lsof %s/LOCK)", raftDir, err, raftDir)
	}
	defer logStore.Close()

	// C2 P0b prototype: optionally open one shared raft-log BadgerDB so all
	// data groups share a single instance instead of opening their own. Reduces
	// process-level BadgerDB instance count from (2N+1) → (2+1) for the log
	// half. FSM state DB consolidation deferred to full C2.
	sharedBadgerEnabled, _ := cmd.Flags().GetBool("shared-badger")
	var sharedRaftLogDB *badger.DB
	if sharedBadgerEnabled {
		// Refuse to silently abandon legacy per-group raft logs. Existing
		// deployments that started before P0b have raft state under
		// <dataDir>/groups/*/raft. Ignoring those and opening a fresh shared
		// DB would silently reset every group's term/votedFor/log — i.e.,
		// data loss. Fail with a clear migration message instead.
		groupsDir := filepath.Join(dataDir, "groups")
		if entries, _ := os.ReadDir(groupsDir); len(entries) > 0 {
			for _, e := range entries {
				if !e.IsDir() {
					continue
				}
				legacyRaftDir := filepath.Join(groupsDir, e.Name(), "raft")
				if st, err := os.Stat(legacyRaftDir); err == nil && st.IsDir() {
					return fmt.Errorf("shared-badger=true incompatible with legacy per-group raft dir %s. "+
						"This deployment was started before C2 P0b. Use --shared-badger=false to keep "+
						"per-group raft logs, or wipe %s to start fresh on the new layout (DESTRUCTIVE — "+
						"only on test clusters or after a full backup)", legacyRaftDir, dataDir)
				}
			}
		}
		sharedDir := filepath.Join(dataDir, "shared-raft-log")
		if err := os.MkdirAll(sharedDir, 0o755); err != nil {
			return fmt.Errorf("mkdir shared raft-log dir: %w", err)
		}
		sharedRaftLogDB, err = badger.Open(badger.DefaultOptions(sharedDir).
			WithLogger(nil).
			WithSyncWrites(true).
			WithNumCompactors(2).
			WithNumVersionsToKeep(1))
		if err != nil {
			return fmt.Errorf("open shared raft-log badger at %s: %w", sharedDir, err)
		}
		defer sharedRaftLogDB.Close()
		log.Info().Str("dir", sharedDir).Msg("shared raft-log DB enabled (C2 P0b prototype)")
	}

	// Start QUIC transport for inter-node communication.
	quicTransport := transport.NewQUICTransport(clusterKey)
	if err := quicTransport.Listen(ctx, raftAddr); err != nil {
		return fmt.Errorf("start QUIC transport on %s: %w\n  recovery: confirm UDP port is free (lsof -i UDP:%s), check firewall, or pass --raft-addr=127.0.0.1:0 to pick any free port", raftAddr, err, raftAddr)
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
			log.Warn().Str("peer", peer).Err(err).Msg("failed to connect to peer (will retry lazily)")
		}
	}

	cfg := raft.DefaultConfig(nodeID, peers)
	cfg.ManagedMode = badgerManagedMode
	cfg.LogGCInterval = raftLogGCInterval
	node := raft.NewNode(cfg, logStore)
	if err := node.Bootstrap(); err != nil && !errors.Is(err, raft.ErrAlreadyBootstrapped) {
		return fmt.Errorf("raft bootstrap: %w", err)
	}

	// Wire QUIC transport to Raft RPC layer
	rpcTransport := raft.NewQUICRPCTransport(quicTransport, node)
	rpcTransport.SetTransport()

	// Meta-Raft: dedicated control-plane Raft group for cluster membership.
	metaRaft, err := cluster.NewMetaRaft(cluster.MetaRaftConfig{
		NodeID:  nodeID,
		Peers:   peers,
		DataDir: dataDir,
	})
	if err != nil {
		return fmt.Errorf("init meta-raft: %w", err)
	}
	metaTransport := cluster.NewMetaTransportQUIC(quicTransport, metaRaft.Node())
	metaRaft.SetTransport(metaTransport)

	// PR-D: DataGroupManager + Router — created before metaRaft.Start() so the
	// OnBucketAssigned callback is registered before the apply loop starts (race-free).
	dgMgr := cluster.NewDataGroupManager()
	clusterRouter := cluster.NewRouter(dgMgr)
	clusterRouter.SetDefault("group-0")
	// SetOnBucketAssigned uses f.mu.Lock() internally; must be called before Start().
	metaRaft.FSM().SetOnBucketAssigned(func(bucket, groupID string) {
		clusterRouter.AssignBucket(bucket, groupID)
	})

	if err := metaRaft.Bootstrap(); err != nil {
		return fmt.Errorf("meta-raft bootstrap: %w", err)
	}
	if err := metaRaft.Start(ctx); err != nil {
		return fmt.Errorf("meta-raft start: %w", err)
	}
	defer metaRaft.Close()

	// Seed Router with any bucket assignments already persisted in FSM state.
	// Start() returns before replay finishes; onBucketAssigned fires live updates.
	clusterRouter.Sync(metaRaft.FSM().BucketAssignments())

	// Propose initial shard groups asynchronously (idempotent: ProposeShardGroup is leader-only;
	// non-leader는 NotLeader 에러를 리턴하고 다음 leader가 take-over).
	//
	// Default 0 = auto-derived from cluster size: max(8, (1+len(peers))*4).
	// Solo(peers=0)=8, 5-node=20, 10-node=40 — 클러스터 확장 시 sharding 헤드룸 확보.
	// 명시값 ≥1 = 그 값 그대로, <0 = 1로 클램프.
	seedGroups, _ := cmd.Flags().GetInt("seed-groups")
	if seedGroups == 0 {
		clusterSize := 1 + len(peers)
		seedGroups = clusterSize * 4
		if seedGroups < 8 {
			seedGroups = 8
		}
	} else if seedGroups < 1 {
		seedGroups = 1
	}
	go func() {
		// Wait for meta-Raft leader to settle before seeding (single-node: ~150ms;
		// multi-node: up to election timeout). Without this, the very first
		// ProposeShardGroup races and fails with NotLeader on cold start.
		leaderDeadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(leaderDeadline) {
			if metaRaft.IsLeader() || metaRaft.Node().LeaderID() != "" {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}

		// Use raft addresses as cluster-wide identity for ShardGroupEntry.PeerIDs —
		// every node sees the same set, so pickVoters output is reproducible across
		// processes. nodeID is a local label; raftAddr is globally consistent.
		clusterPeers := append([]string{raftAddr}, peers...)
		const replicationFactor = 3
		for i := 0; i < seedGroups; i++ {
			groupID := fmt.Sprintf("group-%d", i)
			// group-0 keeps full membership for backward-compat with legacy
			// single-backend deployment. Groups 1..N-1 use rendezvous-hashed
			// voter placement (RF=3) for true sharding.
			var voters []string
			if i == 0 {
				voters = clusterPeers
			} else {
				voters = cluster.PickVoters(groupID, clusterPeers, replicationFactor)
			}
			sgCtx, sgCancel := context.WithTimeout(ctx, 30*time.Second)
			if err := metaRaft.ProposeShardGroup(sgCtx, cluster.ShardGroupEntry{
				ID:      groupID,
				PeerIDs: voters,
			}); err != nil {
				// Followers reject with NotLeader; only the leader's seed loop succeeds.
				log.Debug().Str("group", groupID).Err(err).Msg("seed shard group propose failed (non-fatal)")
			}
			sgCancel()
		}
	}()

	// Create ShardService for distributed data replication
	shardSvcOpts := []cluster.ShardServiceOption{cluster.WithEncryptor(encryptor)}
	if directIO, _ := cmd.Flags().GetBool("direct-io"); directIO {
		shardSvcOpts = append(shardSvcOpts, cluster.WithDirectIO())
		log.Info().Msg("direct I/O enabled for local shard writes (page cache bypass)")
	}
	if measureReadAmp, _ := cmd.Flags().GetBool("measure-read-amp"); measureReadAmp {
		readamp.Enable()
		log.Info().Msg("read-amplification simulator enabled — see grainfs_readamp_* counters at /metrics")
	}
	shardSvc := cluster.NewShardService(dataDir, quicTransport, shardSvcOpts...)

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

	// Live multi-raft sharding (v0.0.7.0): group-0 keeps using the shared
	// distBackend (legacy single-backend deployment is the group-0 instance);
	// groups 1..N-1 get their own per-group BadgerDB+raft via instantiateLocalGroup
	// when this node is a voter (see ownedGroups loop below).
	group0Backend := cluster.WrapDistributedBackend("group-0", distBackend)
	group0 := cluster.NewDataGroupWithBackend(
		"group-0",
		append([]string{nodeID}, peers...),
		group0Backend,
	)
	dgMgr.Add(group0)
	distBackend.SetRouter(clusterRouter)
	distBackend.SetBucketAssigner(metaRaft)
	distBackend.SetShardGroupSource(metaRaft.FSM())

	// PR-D: Rebalancer 배선 — LoadReporter가 meta-Raft FSM에 부하 스냅샷을 커밋하고
	// Rebalancer가 leader에서 주기적으로 평가해 RebalancePlan을 제안·실행한다.
	rebalancerCfg := cluster.DefaultRebalancerConfig()
	rebalancer := cluster.NewRebalancer(nodeID, metaRaft, dgMgr, rebalancerCfg)
	rebalancer.SetGroupRebalancer(
		cluster.NewDataGroupPlanExecutor(nodeID, dgMgr, metaRaft.FSM(), metaRaft),
	)
	metaRaft.FSM().SetOnRebalancePlan(func(plan *cluster.RebalancePlan) {
		execCtx, execCancel := context.WithTimeout(ctx, rebalancerCfg.PlanTimeout)
		go func() {
			defer execCancel()
			if err := rebalancer.ExecutePlan(execCtx, plan); err != nil {
				log.Error().Err(err).Str("plan_id", plan.PlanID).Msg("rebalancer: ExecutePlan failed")
			}
		}()
	})
	go rebalancer.Run(ctx)

	// EC config read early — instantiateLocalGroup needs it for per-group EC.
	clusterECData, _ := cmd.Flags().GetInt("ec-data")
	clusterECParity, _ := cmd.Flags().GetInt("ec-parity")

	// Live multi-raft sharding (v0.0.7.0): instantiate per-group raft.Node +
	// BadgerDB + GroupBackend for each group this node is a voter of. group-0
	// is already wired with the shared distBackend (legacy compat).
	//
	// Two paths cover all entries:
	//   1. Iterate ShardGroups() once for entries already in FSM.
	//   2. SetOnShardGroupAdded for entries replayed/applied later.
	// Both call into instantiateOwnedIfNeeded which is idempotent — duplicates
	// from concurrent paths are no-ops.
	ownedGroups := struct {
		mu       sync.Mutex
		m        map[string]*cluster.GroupBackend
		inFlight map[string]bool // entry.ID currently being instantiated; prevents duplicate concurrent OpenSharedLogStore / badger.Open
	}{m: make(map[string]*cluster.GroupBackend), inFlight: make(map[string]bool)}

	// Shared stop channel for all apply loops (distBackend + per-group).
	// Must be initialized before any goroutine that passes it to RunApplyLoop.
	stopApply := make(chan struct{})

	// GroupRaftQUICMux multiplexes per-group raft RPCs over StreamGroupRaft (0x09).
	// Registered once; each group uses ForGroup(groupID) as its raft transport.
	groupRaftMux := raft.NewGroupRaftQUICMux(quicTransport)
	if quicMuxEnabled {
		groupRaftMux.EnableMux(quicMuxPoolSize, quicMuxFlushWindow)
		log.Info().
			Int("pool", quicMuxPoolSize).
			Dur("flush", quicMuxFlushWindow).
			Msg("group raft mux mode enabled (R+H Phase 2 prototype)")
	}

	instantiateOwnedIfNeeded := func(entry cluster.ShardGroupEntry) {
		// group-0 is already wired with the shared distBackend.
		if entry.ID == "group-0" {
			return
		}
		// Only instantiate for groups where we are a voter. PeerIDs use the
		// cluster-wide raft address as identity (set by the seed loop above);
		// match against our raftAddr.
		isVoter := false
		for _, p := range entry.PeerIDs {
			if p == raftAddr {
				isVoter = true
				break
			}
		}
		if !isVoter {
			return
		}
		ownedGroups.mu.Lock()
		if _, ok := ownedGroups.m[entry.ID]; ok {
			ownedGroups.mu.Unlock()
			return // already instantiated
		}
		if ownedGroups.inFlight[entry.ID] {
			ownedGroups.mu.Unlock()
			return // another goroutine is currently bringing this group up
		}
		ownedGroups.inFlight[entry.ID] = true
		ownedGroups.mu.Unlock()
		// Make sure inFlight is cleared even if instantiation fails (log.Fatal
		// below would skip this; that's acceptable since the process is dying).
		defer func() {
			ownedGroups.mu.Lock()
			delete(ownedGroups.inFlight, entry.ID)
			ownedGroups.mu.Unlock()
		}()

		glc := cluster.GroupLifecycleConfig{
			NodeID:    raftAddr, // identity = raftAddr (matches PeerIDs)
			DataDir:   dataDir,
			ShardSvc:  shardSvc,
			Transport: groupRaftMux.ForGroup(entry.ID),
			EC: cluster.ECConfig{
				DataShards:   clusterECData,
				ParityShards: clusterECParity,
			},
			ElectionTimeout:  raftElectionTimeout,
			HeartbeatTimeout: raftHeartbeatInterval,
		}
		if sharedRaftLogDB != nil {
			// Forward managed-mode and any future BadgerLogStoreOption to
			// the shared store so flags don't get silently dropped on the
			// shared path.
			ls, lerr := raft.OpenSharedLogStore(sharedRaftLogDB, entry.ID, storeOpts...)
			if lerr != nil {
				log.Fatal().Err(lerr).Str("group_id", entry.ID).Msg("OpenSharedLogStore failed")
			}
			glc.LogStore = ls
		}
		gb, err := cluster.InstantiateLocalGroup(glc, entry)
		if err != nil {
			log.Fatal().Err(err).Str("group_id", entry.ID).Msg("instantiateLocalGroup failed — voter status fatal")
		}
		groupRaftMux.Register(entry.ID, gb.RaftNode())
		dgMgr.Add(cluster.NewDataGroupWithBackend(entry.ID, entry.PeerIDs, gb))
		go gb.RunApplyLoop(stopApply)
		ownedGroups.mu.Lock()
		ownedGroups.m[entry.ID] = gb
		ownedGroups.mu.Unlock()
		log.Info().Str("group_id", entry.ID).Strs("peers", entry.PeerIDs).Msg("instantiateLocalGroup ok")
	}

	// Cold-start instantiation for entries already in FSM (restart path).
	// Run async so the apply loop is not blocked by BadgerDB+raft.Node startup.
	go func() {
		for _, entry := range metaRaft.FSM().ShardGroups() {
			instantiateOwnedIfNeeded(entry)
		}
	}()
	// Runtime: handle entries replayed/applied after this point (fresh boot path).
	// Dispatch to goroutine so apply loop is not blocked by BadgerDB+raft.Node startup.
	metaRaft.FSM().SetOnShardGroupAdded(func(entry cluster.ShardGroupEntry) {
		go instantiateOwnedIfNeeded(entry)
	})

	// Shutdown hook: close all owned groups in parallel with 5s timeout each.
	defer func() {
		ownedGroups.mu.Lock()
		toClose := make([]*cluster.GroupBackend, 0, len(ownedGroups.m))
		for _, gb := range ownedGroups.m {
			toClose = append(toClose, gb)
		}
		ownedGroups.mu.Unlock()
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
	}()

	// LoadReporter: leader 전용 — NodeStatsStore에서 읽어 meta-Raft FSM에 부하 통계 커밋.
	loadReporterStore := cluster.NewNodeStatsStore(cluster.DefaultLoadReportInterval * 3)
	loadReporter := cluster.NewLoadReporter(nodeID, loadReporterStore, metaRaft, cluster.DefaultLoadReportInterval)
	go loadReporter.Run(ctx)

	// Phase 18 Cluster EC: activates at MinECNodes=3+ nodes with proportional k,m.
	// 1-2 nodes always use N× replication. (clusterECData/Parity read above for instantiateOwned.)
	distBackend.SetECConfig(cluster.ECConfig{
		DataShards:   clusterECData,
		ParityShards: clusterECParity,
	})
	log.Info().Int("k", clusterECData).Int("m", clusterECParity).
		Bool("active", len(allNodes) >= cluster.MinECNodes).
		Int("cluster_size", len(allNodes)).Msg("cluster EC configured")

	// VFS bucket fixed-versionID toggle (rollback path for the write-amp fix).
	vfsFixed, _ := cmd.Flags().GetBool("backend-vfs-fixed-version")
	distBackend.SetVFSFixedVersionEnabled(vfsFixed)
	log.Info().Bool("enabled", vfsFixed).Msg("VFS bucket fixed-versionID configured")

	// EC shard cache (Phase 2 #3 follow-up). Multi-node measurement on
	// PR #71 showed 90% hit rate on large object repeated GET — the cache
	// pays for itself any time the same large object is read more than
	// once before its shards age out. Disabled when --shard-cache-size=0
	// (used by --measure-read-amp baselines).
	shardCacheSize, _ := cmd.Flags().GetInt64("shard-cache-size")
	shardCache := shardcache.New(shardCacheSize)
	distBackend.SetShardCache(shardCache)
	log.Info().Int64("bytes", shardCacheSize).Msg("ec shard cache configured")

	// Set up snapshot manager: auto-snapshot every 10000 applied entries
	fsm := cluster.NewFSM(db)
	snapMgr := raft.NewSnapshotManager(logStore, fsm, raft.SnapshotConfig{Threshold: 10000})
	distBackend.SetSnapshotManager(snapMgr, node)

	// Restore from snapshot on startup
	snapIdx, err := snapMgr.Restore()
	if err != nil {
		log.Warn().Err(err).Msg("snapshot restore failed")
	} else if snapIdx > 0 {
		log.Info().Uint64("index", snapIdx).Msg("restored from snapshot")
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
		log.Info().Int("threshold", packThreshold).Msg("packed blob storage enabled")
	}

	// Wrap with LRU read cache. Raft FSM-based invalidation ensures cache
	// consistency across nodes.
	cachedBackend := storage.NewCachedBackend(inner)

	// Wire OnApply callback: invalidate cache + update metrics on committed entries
	distBackend.SetOnApply(func(cmdType cluster.CommandType, bucket, key string) {
		cachedBackend.InvalidateKey(bucket, key)
	})

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
			log.Warn().Err(err).Msg("balancer start failed")
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
		log.Info().Str("component", "gossip").Msg("gossip receiver started (receipt-only, balancer disabled)")
	}

	walDir := filepath.Join(dataDir, "wal")
	w, err := wal.Open(walDir)
	if err != nil {
		return fmt.Errorf("open WAL: %w", err)
	}
	defer w.Close()

	// v0.0.7.1 PR-D: Live multi-raft routing — ClusterCoordinator + ForwardSender/Receiver.
	// ClusterCoordinator implements storage.Backend and routes bucket-scoped ops to the
	// correct group leader via ForwardSender. 0x08 handler (ForwardReceiver) receives
	// forwarded calls on voter nodes and dispatches to local GroupBackend.
	forwardDialer := func(peer string, payload []byte) ([]byte, error) {
		msg := &transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload}
		reply, err := quicTransport.Call(ctx, peer, msg)
		if err != nil {
			return nil, err
		}
		return reply.Payload, nil
	}

	forwardSender := cluster.NewForwardSender(forwardDialer)
	forwardReceiver := cluster.NewForwardReceiver(dgMgr)
	forwardReceiver.Register(shardSvc)

	clusterCoord := cluster.NewClusterCoordinator(
		distBackend,    // base for cluster-wide ops (CreateBucket, etc.)
		dgMgr,          // local owned groups (self-leader shortcut)
		clusterRouter,  // bucket → group lookup
		metaRaft.FSM(), // ShardGroupSource (PeerIDs, leader hints)
		raftAddr,       // selfID for leader check
	).WithForwardSender(forwardSender)

	// Use ClusterCoordinator as the primary backend for S3, NFSv4, NBD, then
	// wrap it with WAL so routed object mutations are captured for PITR.
	var backend storage.Backend = wal.NewBackend(clusterCoord, w)
	log.Info().Msg("v0.0.7.1 PR-D: ClusterCoordinator wired — live multi-raft routing enabled")

	// Wrap with pull-through cache if upstream is configured.
	if upstreamEndpoint, _ := cmd.Flags().GetString("upstream"); upstreamEndpoint != "" {
		upstreamAccessKey, _ := cmd.Flags().GetString("upstream-access-key")
		upstreamSecretKey, _ := cmd.Flags().GetString("upstream-secret-key")
		up, err := pullthrough.NewS3Upstream(upstreamEndpoint, upstreamAccessKey, upstreamSecretKey)
		if err != nil {
			return fmt.Errorf("init upstream: %w", err)
		}
		backend = pullthrough.NewBackend(backend, up)
		log.Info().Str("upstream", upstreamEndpoint).Msg("pull-through cache enabled")
	}
	recoveryReadOnly := false
	if marker, err := cluster.LoadRecoverClusterMarker(dataDir); err != nil {
		return fmt.Errorf("load recovery marker: %w", err)
	} else if marker != nil && !marker.Writable {
		recoveryReadOnly = true
		backend = storage.NewRecoveryWriteGate(backend, storage.ErrRecoveryWriteDisabled)
		log.Warn().Str("marker", filepath.Join(dataDir, cluster.RecoverClusterMarkerPath)).Msg("recovered cluster write gate enabled")
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
				log.Warn().Err(err).Msg("auto-snapshot init failed")
			} else {
				as := snapshot.NewAutoSnapshotter(objSnapMgr, snapInterval, snapRetain)
				as.Start(ctx)
				log.Info().Dur("interval", snapInterval).Int("retain", snapRetain).Msg("auto-snapshot enabled")
			}
		} else {
			log.Debug().Msg("auto-snapshot skipped: backend does not implement Snapshotable")
		}
	}

	// DiskCollector exposes grainfs_disk_used_pct metric. In multi-node mode
	// the balancer owns its own collector; in singleton mode nothing else
	// would emit disk stats. Register unconditionally — duplicate registration
	// is guarded inside NewDiskCollector.
	diskCollector := cluster.NewDiskCollector(nodeID, dataDir, nil, 30*time.Second)
	// Wiring of OnThreshold + Run() happens after clusterAlerts is built (below)
	// so the callback can dispatch critical webhooks on transitions.

	// Auto-create "default" bucket on startup. Single-node mode does this
	// synchronously because the bucket is part of the local ready contract.
	// Cluster nodes must not block HTTP readiness on a metadata proposal:
	// during simultaneous cold start, followers can spend the whole retry
	// window returning "not leader" before the service socket is even open.
	// Recovered clusters stay read-only until verification, so skip bucket
	// creation entirely when the recovery write gate is active.
	if !recoveryReadOnly {
		if len(peers) == 0 {
			if err := createDefaultBucketWithRetry(ctx, backend, 30*time.Second); err != nil {
				return fmt.Errorf("create default bucket: %w", err)
			}
		} else {
			go func() {
				if err := createDefaultBucketWithRetry(ctx, backend, 30*time.Second); err != nil {
					log.Warn().Err(err).Msg("default bucket creation failed in background (may already exist)")
				}
			}()
		}
	}

	log.Info().Str("component", "server").Str("version", version).
		Str("node_id", nodeID).Str("raft_addr", raftAddr).Strs("peers", peers).
		Str("addr", addr).Str("data", dataDir).Msg("server started")

	// Startup config snapshot — debug-level log of every flag-derived runtime
	// value. Useful for diffing against a known-good config when an operator
	// is comparing two installs or troubleshooting drift after a restart.
	logStartupConfigSnapshot(cmd, addr, dataDir, nodeID, raftAddr, peers)

	clusterAlertWebhook, _ := cmd.Flags().GetString("alert-webhook")
	clusterAlertSecret, _ := cmd.Flags().GetString("alert-webhook-secret")
	clusterAlerts := server.NewAlertsState(clusterAlertWebhook, alerts.Options{Secret: clusterAlertSecret}, alerts.DegradedConfig{})

	// Wire predictive disk warnings into the collector now that clusterAlerts
	// exists. Thresholds are taken as fractions on the flag (more natural for
	// operators) but DiskCollector works in percent.
	diskWarnFrac, _ := cmd.Flags().GetFloat64("disk-warn-threshold")
	diskCritFrac, _ := cmd.Flags().GetFloat64("disk-critical-threshold")
	diskCollector.SetThresholds(diskWarnFrac*100, diskCritFrac*100)
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
		server.WithClusterInfo(&raftClusterInfo{node: node, peers: peers, backend: distBackend}),
		server.WithEventStore(eventstore.New(db)),
		server.WithAlerts(clusterAlerts),
		server.WithDataDir(dataDir),
	}
	if len(peers) == 0 {
		srvOpts = append(srvOpts, server.WithIcebergCatalogStore(icebergcatalog.NewStore(db, "s3://grainfs-tables/warehouse")))
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

	volMgr, blockCache, dedupDB, err := buildVolumeManager(cmd, dataDir, backend)
	if err != nil {
		return fmt.Errorf("volume manager: %w", err)
	}
	if dedupDB != nil {
		defer dedupDB.Close()
	}
	srvOpts = append(srvOpts, server.WithVolumeManager(volMgr), server.WithBlockCache(blockCache), server.WithShardCache(shardCache))
	srvOpts = append(srvOpts, server.WithReadIndexer(distBackend))
	srvOpts = append(srvOpts, server.WithRaftSnapshotter(distBackend))

	distBackend.RegisterReadIndexHandler()

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
	scrubInterval, _ := cmd.Flags().GetDuration("scrub-interval")
	if scrubInterval > 0 {
		sc := scrubber.New(distBackend, scrubInterval)
		sc.SetEmitter(activeEmitter)
		sc.Start(ctx)

		placementMonitors := newPlacementMonitorRegistry()
		startPlacementMonitor := func(monitorCtx context.Context, dg *cluster.DataGroup) {
			gb := dg.Backend()
			placementMonitor := cluster.NewShardPlacementMonitor(gb.FSMRef(), gb, shardSvc, gb.NodeID(), scrubInterval)
			placementMonitor.SetOnMissing(func(bucket, shardKey string, shardIdx int) {
				// shardKey from placement resolution is objectKey+"/"+versionID.
				// Split on the last "/" so RepairShard can skip LookupLatestVersion.
				objectKey, versionID := shardKey, ""
				if i := strings.LastIndexByte(shardKey, '/'); i >= 0 {
					objectKey, versionID = shardKey[:i], shardKey[i+1:]
				}
				if err := gb.RepairShardLocal(bucket, objectKey, versionID, shardIdx); err != nil {
					log.Warn().Str("group", dg.ID()).Str("bucket", bucket).Str("key", shardKey).Int("shard", shardIdx).Err(err).Msg("placement monitor repair failed")
				}
			})
			go placementMonitor.Start(monitorCtx)
		}
		refreshPlacementMonitors := func() {
			placementMonitors.refresh(ctx, dgMgr.All(), startPlacementMonitor)
		}
		refreshPlacementMonitors()
		go func() {
			ticker := time.NewTicker(scrubInterval)
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
		log.Info().Dur("interval", scrubInterval).Msg("cluster scrubber started")
	}

	// Start the leader-aware worker loop. Only the Raft leader runs the
	// worker; followers skip the scan so we don't waste IO on proposals that
	// would be rejected anyway. LifecycleManager polls node.State() and
	// starts/stops the worker on leadership transitions.
	if lifecycleMgr != nil {
		go lifecycleMgr.Run(ctx)
		log.Info().Dur("interval", lifecycleInterval).Msg("cluster lifecycle manager started")
	}

	// Start the degraded mode monitor — checks live node count vs EC threshold
	// every 30 s. The first check fires immediately so the server knows its
	// state before serving any requests.
	degradedInterval, _ := cmd.Flags().GetDuration("degraded-check-interval")
	degradedMon := cluster.NewDegradedMonitor(distBackend, clusterAlerts.Tracker(), degradedInterval).
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
	nfs4Port, _ := cmd.Flags().GetInt("nfs4-port")
	nbdPort, _ := cmd.Flags().GetInt("nbd-port")
	nbdVolumeSize, _ := cmd.Flags().GetInt64("nbd-volume-size")
	nodeSvc := startNodeServices(ctx, cmd, backend, volMgr, nfs4Port, nbdPort, nbdVolumeSize, distBackend)
	defer nodeSvc.Close()

	<-ctx.Done()
	log.Info().Str("component", "server").Msg("graceful shutdown started")

	// 1. Drain in-flight HTTP requests
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Warn().Err(err).Msg("http server shutdown error")
	}

	// 2. Transfer Raft leadership before stopping
	if err := node.TransferLeadership(); err != nil {
		log.Debug().Err(err).Msg("leadership transfer skipped")
	} else {
		log.Info().Str("component", "raft").Msg("leadership transferred")
	}

	// 3. Stop Raft apply loop
	close(stopApply)

	log.Info().Str("component", "server").Msg("server stopped")
	return nil
}

// buildVolumeManager creates the shared volume.Manager for the serve
// path. When --dedup is set, it opens a dedicated BadgerDB at
// {dataDir}/dedup/ and returns the manager + DB (caller closes the DB
// on shutdown). The block cache is constructed here so the same
// instance is shared with the dashboard endpoint via WithBlockCache.
func buildVolumeManager(cmd *cobra.Command, dataDir string, backend storage.Backend) (*volume.Manager, *blockcache.Cache, *badger.DB, error) {
	dedupEnabled, _ := cmd.Flags().GetBool("dedup")
	cacheSize, _ := cmd.Flags().GetInt64("block-cache-size")
	cache := blockcache.New(cacheSize)
	if cacheSize > 0 {
		log.Info().Int64("bytes", cacheSize).Msg("volume block cache enabled")
	}
	opts := volume.ManagerOptions{BlockCache: cache}
	if !dedupEnabled {
		return volume.NewManagerWithOptions(backend, opts), cache, nil, nil
	}
	dir := filepath.Join(dataDir, "dedup")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, nil, fmt.Errorf("create dedup dir: %w", err)
	}
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("open dedup db: %w", err)
	}
	opts.DedupIndex = dedup.NewBadgerIndex(db)
	mgr := volume.NewManagerWithOptions(backend, opts)
	return mgr, cache, db, nil
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
		log.Info().Str("component", "server").Str("key_file", keyFile).Msg("at-rest encryption enabled")
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

	log.Info().Str("component", "server").Str("key_file", keyFile).Msg("at-rest encryption enabled (auto-generated key)")
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
	exec.SetShardCounter(ecShardCounterFor(fsm))
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
		log.Warn().Float64("pct", testPct).Msg("GRAINFS_TEST_DISK_PCT active — real disk stats overridden")
		collector.SetStatFunc(func(string) (float64, uint64) { return testPct, 0 })
	}
	go collector.Run(ctx)

	// Replay any tasks that were persisted during a previous channel-full event.
	if err := fsm.RecoverPending(ctx, taskCh); err != nil {
		log.Warn().Err(err).Msg("balancer: recover pending failed")
	}

	log.Info().Str("component", "balancer").
		Dur("gossip_interval", gossipInterval).Float64("trigger_pct", triggerPct).Float64("stop_pct", stopPct).Msg("balancer started")
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
	node    *raft.Node
	peers   []string
	backend *cluster.DistributedBackend
}

func (r *raftClusterInfo) NodeID() string   { return r.node.ID() }
func (r *raftClusterInfo) State() string    { return r.node.State().String() }
func (r *raftClusterInfo) Term() uint64     { return r.node.Term() }
func (r *raftClusterInfo) LeaderID() string { return r.node.LeaderID() }
func (r *raftClusterInfo) Peers() []string  { return r.peers }
func (r *raftClusterInfo) LivePeers() []string {
	if r.backend == nil {
		return r.peers
	}
	return r.backend.LiveNodes()
}

// logStartupConfigSnapshot dumps every flag's resolved value at debug level
// and writes a JSON snapshot to {dataDir}/.last-config.json. On the next
// start the previous snapshot is compared and any changed key is logged at
// info level so the operator sees what was reconfigured between restarts.
//
// Limited to flag values — there is no live-config-file system in grainfs,
// so true "drift detection" (file vs runtime) is out of scope until a config
// file is added (Phase 20). What this catches is unintentional flag changes
// across restarts, which is the realistic operator failure mode today.
func logStartupConfigSnapshot(cmd *cobra.Command, addr, dataDir, nodeID, raftAddr string, peers []string) {
	snapshot := map[string]any{
		"addr":      addr,
		"data_dir":  dataDir,
		"node_id":   nodeID,
		"raft_addr": raftAddr,
		"peers":     peers,
	}
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		// Strip secrets from the snapshot — they should never hit a debug
		// log or an on-disk plaintext file.
		switch f.Name {
		case "secret-key", "cluster-key", "alert-webhook-secret", "heal-receipt-psk":
			if f.Value.String() != "" {
				snapshot[f.Name] = "<redacted>"
			}
			return
		}
		snapshot[f.Name] = f.Value.String()
	})

	log.Debug().Interface("flags", snapshot).Msg("startup config snapshot")

	// Compare against previous snapshot, log diff at info level.
	snapPath := filepath.Join(dataDir, ".last-config.json")
	if prev, err := os.ReadFile(snapPath); err == nil {
		var prevMap map[string]any
		if err := json.Unmarshal(prev, &prevMap); err == nil {
			diff := diffSnapshots(prevMap, snapshot)
			if len(diff) > 0 {
				log.Info().Interface("changed", diff).Msg("config changed since last startup")
			}
		}
	}

	// Persist current snapshot for next restart's comparison. Failure here
	// is non-fatal — drift detection is best-effort observability.
	if data, err := json.MarshalIndent(snapshot, "", "  "); err == nil {
		if err := os.WriteFile(snapPath, data, 0o600); err != nil {
			log.Debug().Err(err).Str("path", snapPath).Msg("could not persist startup config snapshot")
		}
	}
}

// diffSnapshots returns a map of key→{prev, curr} for every key whose value
// changed between snapshots. Keys present in only one side are also reported.
func diffSnapshots(prev, curr map[string]any) map[string]map[string]any {
	out := make(map[string]map[string]any)
	keys := make(map[string]struct{}, len(prev)+len(curr))
	for k := range prev {
		keys[k] = struct{}{}
	}
	for k := range curr {
		keys[k] = struct{}{}
	}
	sortedKeys := make([]string, 0, len(keys))
	for k := range keys {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)
	for _, k := range sortedKeys {
		pv, pok := prev[k]
		cv, cok := curr[k]
		if !pok || !cok || fmt.Sprintf("%v", pv) != fmt.Sprintf("%v", cv) {
			out[k] = map[string]any{"prev": pv, "curr": cv}
		}
	}
	return out
}

// ecShardCounterFor returns a per-object shard-count function for MigrationExecutor.
// Returns 1 for N× objects (no EC metadata) and k+m for EC objects.
func ecShardCounterFor(fsm *cluster.FSM) func(bucket, key, versionID string) int {
	return func(bucket, key, versionID string) int {
		k, m, err := fsm.LookupObjectECShards(bucket, key, versionID)
		if err != nil {
			// Return 0 so Execute falls back to numShards (cluster-wide k+m).
			// Returning 1 would copy only shard 0 then delete all k+m source shards — data loss.
			log.Warn().Err(err).Str("bucket", bucket).Str("key", key).Str("version", versionID).
				Msg("LookupObjectECShards failed, using numShards fallback")
			return 0
		}
		if k == 0 {
			return 1 // N× 모드: EC 메타 없음
		}
		return k + m
	}
}
