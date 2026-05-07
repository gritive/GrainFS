package main

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/badgerutil"
	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
	grainotel "github.com/gritive/GrainFS/internal/otel"
	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/internal/server"
	"github.com/gritive/GrainFS/internal/serveruntime"
)

const defaultReshardInterval = 24 * time.Hour

func init() {
	serveCmd.Flags().StringP("data", "d", "./data", "data directory")
	serveCmd.Flags().IntP("port", "p", 9000, "listen port")
	serveCmd.Flags().String("admin-socket", "", "admin Unix socket path (default <data>/admin.sock)")
	serveCmd.Flags().String("admin-group", "", "OS group name for admin socket chown (default: caller's primary group)")
	serveCmd.Flags().String("public-url", "", "public dashboard base URL (e.g. https://node1:9000); defaults to localhost in `grainfs dashboard` output")
	serveCmd.Flags().String("node-id", "", "unique node ID (auto-generated if omitted)")
	serveCmd.Flags().String("raft-addr", "", "Raft listen address (required when --peers is set)")
	serveCmd.Flags().String("cluster-key", "", "Pre-shared key for cluster peer authentication")
	serveCmd.Flags().String("peers", "", "comma-separated list of peer Raft addresses (enables cluster mode)")
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
	serveCmd.Flags().Duration("reshard-interval", defaultReshardInterval, "background EC reshard interval (0 to disable)")
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
	serveCmd.Flags().Bool("fd-watch-enabled", true, "enable predictive file descriptor exhaustion warnings")
	serveCmd.Flags().Duration("fd-watch-interval", 10*time.Second, "how often to sample process file descriptor usage")
	serveCmd.Flags().Float64("fd-warn-threshold", 0.80, "FD used fraction (0-1) at which a warning incident fires")
	serveCmd.Flags().Float64("fd-critical-threshold", 0.90, "FD used fraction (0-1) at which a critical incident fires")
	serveCmd.Flags().Duration("fd-eta-window", 30*time.Minute, "positive-trend ETA window for predictive FD warnings")
	serveCmd.Flags().Duration("fd-recovery-window", time.Minute, "stable below-threshold window before resolving FD incidents")
	serveCmd.Flags().Int("fd-classification-cap", 512, "max open file descriptors to classify by category per sample")
	// Predictive goroutine warnings — same Detector pattern as FD watcher.
	// Defaults measurement-justified: 3-node cluster idle baseline ~200
	// goroutines/node, so 5000 warn (~25× idle) and 20000 critical (~100×).
	serveCmd.Flags().Bool("goroutine-watch-enabled", true, "enable predictive goroutine count warnings")
	serveCmd.Flags().Int("goroutine-warn", 5000, "goroutine count that triggers warn-level alert (transition-only firing)")
	serveCmd.Flags().Int("goroutine-critical", 20000, "goroutine count that triggers critical-level alert")
	serveCmd.Flags().Duration("goroutine-poll-interval", 30*time.Second, "polling interval for goroutine count sampling")
	serveCmd.Flags().Duration("goroutine-eta-window", 30*time.Minute, "ETA projection window for predictive goroutine warnings")
	serveCmd.Flags().Duration("goroutine-recovery-window", time.Minute, "minimum time below warn threshold before transitioning to ok")
	serveCmd.Flags().Bool("vlog-watch-enabled", true, "enable BadgerDB vlog watcher (PR2)")
	serveCmd.Flags().Float64("vlog-warn-ratio", 0.4, "vlog/disk ratio that fires warn (transition-only)")
	serveCmd.Flags().Float64("vlog-critical-ratio", 0.7, "vlog/disk ratio that fires critical")
	serveCmd.Flags().Duration("vlog-poll-interval", 60*time.Second, "vlog watcher sampling cadence")
	serveCmd.Flags().Duration("vlog-eta-window", 30*time.Minute, "ETA projection window for vlog warnings")
	serveCmd.Flags().Duration("vlog-recovery-window", 5*time.Minute, "minimum time below warn ratio before transitioning to ok")
	serveCmd.Flags().Duration("badger-gc-interval", 5*time.Minute, "BadgerDB vlog GC ticker cadence")
	serveCmd.Flags().Bool("badger-gc-disable", false, "disable BadgerDB vlog GC ticker (debug only)")
	serveCmd.Flags().Int32("badger-gc-fail-threshold", 3, "consecutive RunValueLogGC failures before incident")
	serveCmd.Flags().Bool("strict-vlog-registry", false, "fatal on vlog registry smoke mismatch (e2e: true)")
	serveCmd.Flags().Duration("vlog-smoke-defer", 60*time.Second, "delay before vlog registry startup smoke runs")
	serveCmd.Flags().Int64("badger-value-threshold", 0, "force BadgerDB ValueThreshold (bytes) so values above this size spill to vlog; 0 keeps Badger default (1 MiB). Test-only.")
	_ = serveCmd.Flags().MarkHidden("badger-value-threshold")
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
	serveCmd.Flags().Bool("raft-log-fsync", true, "fsync the Raft log store on every append (auto: cluster=false (consensus provides redundancy), single=true; explicit value always wins)")
	serveCmd.Flags().Duration("raft-heartbeat-interval", 200*time.Millisecond, "per-group raft heartbeat interval. Lower = faster failure detection, higher CPU/network. Default 200ms balances detection latency with QUIC stream-open cost.")
	serveCmd.Flags().Duration("raft-election-timeout", 1000*time.Millisecond, "per-group raft election timeout (must be >= 3 * heartbeat-interval). Higher = fewer spurious elections under load.")
	serveCmd.Flags().Bool("quic-mux", true, "use multiplexed QUIC streams + heartbeat coalescing for per-group raft RPCs. idle-N8 measurement: 78pct drop in CPU samples, 17x drop in recvmsg syscalls vs legacy per-message path. Falls back to legacy on peer ALPN mismatch (older binaries).")
	serveCmd.Flags().Int("quic-mux-pool", 4, "stream pool size per peer when --quic-mux=true (avoids HoL with raft pipelining)")
	serveCmd.Flags().Duration("quic-mux-flush", 2*time.Millisecond, "heartbeat coalescing flush window when --quic-mux=true (must be << heartbeat-interval)")
	serveCmd.Flags().String("join", "", "join an existing cluster through this leader/follower raft address")
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

	if vt, _ := cmd.Flags().GetInt64("badger-value-threshold"); vt > 0 {
		badgerutil.SetValueThresholdOverride(vt)
	}

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
	cfg := buildClusterConfig(cmd, addr, dataDir, nodeID, raftAddr, clusterKey, authOpts, shardEncryptor)
	return serveruntime.Run(ctx, cfg)
}
