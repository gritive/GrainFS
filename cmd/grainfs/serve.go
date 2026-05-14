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
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/iam"
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
	serveCmd.Flags().String("raft-addr", "", "Raft listen address for cluster communication (required in cluster mode)")
	serveCmd.Flags().String("cluster-key", "", "Pre-shared key for cluster peer authentication")
	serveCmd.Flags().String("encryption-key-file", "", "path to 32-byte encryption key file (auto-generated if omitted)")
	serveCmd.Flags().Bool("no-encryption", false, "disable at-rest encryption")
	_ = serveCmd.Flags().MarkHidden("no-encryption")
	serveCmd.Flags().Int("nfs4-port", 2049, "NFSv4 server port (0 = disabled); binds 0.0.0.0 — use firewall or set 0 when exposing public interfaces")
	serveCmd.Flags().Int("nbd-port", 10809, "NBD server port (0 = disabled). Client-side nbd-client still requires Linux.")
	serveCmd.Flags().String("9p-bind", "127.0.0.1", "9P2000.L bind address; set 0.0.0.0 only on trusted networks")
	serveCmd.Flags().Int("9p-port", 0, "9P2000.L server port (0 = disabled); unauthenticated, use firewall")
	serveCmd.Flags().Int("pack-threshold", 0, "pack objects below this size into blob files (0 = disabled, e.g. 65536)")
	serveCmd.Flags().Duration("scrub-interval", 24*time.Hour, "EC shard scrub interval (always on; 0 resets to default 24h)")
	serveCmd.Flags().Duration("reshard-interval", defaultReshardInterval, "background EC reshard interval (always on; 0 resets to default 24h)")
	serveCmd.Flags().Duration("ring-reshard-interval", time.Hour, "ring placement reshard interval — migrates EC objects to current ring when nodes change (always on; 0 resets to default 1h)")
	serveCmd.Flags().Duration("datagroup-refresh-interval", time.Minute, "how often to scan for new DataGroups and start reshard managers (0 = only scan at startup)")
	serveCmd.Flags().Duration("lifecycle-interval", 1*time.Hour, "lifecycle rule evaluation interval (0 to disable)")
	serveCmd.Flags().Duration("degraded-check-interval", 30*time.Second, "EC degraded-mode liveness check interval")
	serveCmd.Flags().Duration("raft-log-gc-interval", 30*time.Second, "how often Raft log GC runs")
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
	_ = serveCmd.Flags().MarkHidden("direct-io")
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
	serveCmd.Flags().Bool("dedup", true, "DEPRECATED: dedup is always enabled. Block-level deduplication uses a BadgerDB index at {data}/dedup/.")
	_ = serveCmd.Flags().MarkHidden("dedup")
	_ = serveCmd.Flags().MarkDeprecated("dedup", "dedup is always enabled; this flag will be removed in v0.1.0")
	serveCmd.Flags().Duration("raft-heartbeat-interval", 200*time.Millisecond, "per-group raft heartbeat interval. Lower = faster failure detection, higher CPU/network. Default 200ms balances detection latency with QUIC stream-open cost.")
	serveCmd.Flags().Duration("raft-election-timeout", 1000*time.Millisecond, "per-group raft election timeout (must be >= 3 * heartbeat-interval). Higher = fewer spurious elections under load.")
	// Multiplexed QUIC raft RPCs are always on (idle-N8 measurement: 78pct drop
	// in CPU samples, 17x drop in recvmsg syscalls vs the legacy per-message
	// path; per-peer ALPN fallback to the legacy path is retained for older
	// binaries). These two knobs tune the always-on mux path.
	serveCmd.Flags().Int("quic-mux-pool", 4, "stream pool size per peer for multiplexed raft RPCs (avoids HoL with raft pipelining)")
	serveCmd.Flags().Duration("quic-mux-flush", 2*time.Millisecond, "heartbeat coalescing flush window for multiplexed raft RPCs (must be << raft-heartbeat-interval)")
	serveCmd.Flags().Bool("audit-iceberg", true, "enable audit log lake: S3 ops → Iceberg table on grainfs-audit bucket")
	serveCmd.Flags().Duration("audit-commit-interval", 60*time.Second, "how often the audit committer flushes the ring buffer to Iceberg")
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

	// IAM Store always exists. Static creds are no longer accepted via flag —
	// bootstrap goes through admin UDS POST /v1/iam/sa (see docs/RUNBOOK.md).
	iamStore := iam.NewStore()
	inner := s3auth.NewVerifier(nil)
	inner.SecretLookup = iam.NewSecretLookup(iamStore)
	verifier := s3auth.NewCachingVerifier(inner, 4096, 5*time.Minute)

	authOpts := []server.Option{
		server.WithVerifier(verifier),
		server.WithIAMStore(iamStore),
	}
	// IAM audit logger emits authz allow/deny events via zerolog. Always
	// wired in production; tests can override or omit by using a different
	// option set.
	auditLogger := iam.NewAuditLogger(iam.NewLogAuditEmitter())
	authOpts = append(authOpts, server.WithIAMAudit(auditLogger))

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

	// IAM Applier — only constructed when an encryptor is available, since
	// secret_key wrap/unwrap requires it. In --no-encryption mode the
	// applier is nil; cluster meta-FSM will reject IAM commands until
	// encryption is enabled.
	var iamApplier *iam.Applier
	if shardEncryptor != nil {
		iamApplier = iam.NewApplier(iamStore, shardEncryptor)
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
	}); err != nil {
		return err
	}

	nodeID, _ := cmd.Flags().GetString("node-id")
	raftAddr, _ := cmd.Flags().GetString("raft-addr")
	clusterKey, _ := cmd.Flags().GetString("cluster-key")
	cfg := buildClusterConfig(cmd, addr, dataDir, nodeID, raftAddr, clusterKey, authOpts, shardEncryptor, iamStore, iamApplier)
	return serveruntime.Run(ctx, cfg)
}
