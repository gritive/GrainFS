package main

import (
	"context"
	"errors"
	"fmt"
	"io"
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
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"golang.org/x/time/rate"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/alerts"
	"github.com/gritive/GrainFS/internal/badgerrole"
	"github.com/gritive/GrainFS/internal/badgerutil"
	"github.com/gritive/GrainFS/internal/cache/shardcache"
	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/dashboard"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/eventstore"
	"github.com/gritive/GrainFS/internal/icebergcatalog"
	"github.com/gritive/GrainFS/internal/incident"
	"github.com/gritive/GrainFS/internal/incident/badgerstore"
	"github.com/gritive/GrainFS/internal/metrics/readamp"
	grainotel "github.com/gritive/GrainFS/internal/otel"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/receipt"
	"github.com/gritive/GrainFS/internal/resourcewatch"
	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/server"
	"github.com/gritive/GrainFS/internal/server/admin"
	"github.com/gritive/GrainFS/internal/serveruntime"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/storage/packblob"
	"github.com/gritive/GrainFS/internal/storage/pullthrough"
	"github.com/gritive/GrainFS/internal/storage/wal"
	"github.com/gritive/GrainFS/internal/transport"
	"github.com/gritive/GrainFS/internal/volume"
)

const defaultReshardInterval = 24 * time.Hour

// peerHealthAdapter implements admin.PeerHealthAPI on top of *cluster.DistributedBackend.
// Converts cluster.PeerHealthEntry to the JSON-friendly admin.ClusterPeerInfo so
// the admin handler stays decoupled from the cluster package.
type peerHealthAdapter struct {
	backend *cluster.DistributedBackend
}

func (a peerHealthAdapter) Snapshot() []admin.ClusterPeerInfo {
	if a.backend == nil {
		return nil
	}
	ph := a.backend.PeerHealth()
	if ph == nil {
		return nil
	}
	src := ph.Snapshot()
	out := make([]admin.ClusterPeerInfo, 0, len(src))
	for _, e := range src {
		info := admin.ClusterPeerInfo{
			ID:                  e.ID,
			Healthy:             e.Healthy,
			CooldownRemainingMs: e.CooldownRemainingMs,
		}
		if e.LastFailure != nil {
			info.LastFailure = e.LastFailure.UTC().Format(time.RFC3339Nano)
		}
		out = append(out, info)
	}
	return out
}

// replicaRepairerFunc adapts a function to scrubber.ReplicaRepairer.
type replicaRepairerFunc func(ctx context.Context, bucket, key string) error

func (f replicaRepairerFunc) RepairReplica(ctx context.Context, bucket, key string) error {
	return f(ctx, bucket, key)
}

// scrubProposerAdapter implements admin.ScrubProposer over MetaRaft. The
// adapter does a leader-side dedup pre-check so duplicate triggers don't
// consume a fresh raft entry per call.
type scrubProposerAdapter struct {
	metaRaft *cluster.MetaRaft
	director *scrubber.Director
	nodeID   string
}

func (a *scrubProposerAdapter) Propose(ctx context.Context, req scrubber.TriggerReq) (scrubber.ScrubTriggerEntry, bool, error) {
	if existing, ok := a.director.LookupDedup(req); ok {
		return existing, false, nil
	}
	entry := scrubber.ScrubTriggerEntry{
		SessionID:        uuid.NewString(),
		Bucket:           req.Bucket,
		KeyPrefix:        req.KeyPrefix,
		Scope:            req.Scope,
		DryRun:           req.DryRun,
		RequestedAt:      time.Now().Unix(),
		OriginatorNodeID: a.nodeID,
	}
	return entry, true, a.metaRaft.ProposeScrubTrigger(ctx, entry)
}

// scrubAggregatorAdapter implements admin.ScrubAggregator over
// ClusterCoordinator's per-peer fan-out RPC.
type scrubAggregatorAdapter struct {
	coord *cluster.ClusterCoordinator
}

func (a *scrubAggregatorAdapter) Peers(ctx context.Context, sessionID string) ([]admin.ScrubJobInfo, []string, error) {
	if a.coord == nil {
		return nil, nil, nil
	}
	stats, failures, err := a.coord.ScrubSessionStat(ctx, sessionID)
	if err != nil {
		return nil, nil, err
	}
	infos := make([]admin.ScrubJobInfo, 0, len(stats))
	for _, s := range stats {
		scope := "full"
		if s.Scope == int32(scrubber.ScopeLive) {
			scope = "live"
		}
		infos = append(infos, admin.ScrubJobInfo{
			Bucket:       s.Bucket,
			KeyPrefix:    s.KeyPrefix,
			Scope:        scope,
			DryRun:       s.DryRun,
			Status:       s.Status,
			StartedAt:    s.StartedAt,
			DoneAt:       s.DoneAt,
			Checked:      s.Checked,
			Healthy:      s.Healthy,
			Detected:     s.Detected,
			Repaired:     s.Repaired,
			Unrepairable: s.Unrepairable,
			Skipped:      s.Skipped,
			OwnedHere:    s.OwnedHere,
		})
	}
	return infos, failures, nil
}

// vlogBreakdownAdapter implements admin.VlogBreakdownAPI for the
// `GET /v1/resource/vlog/breakdown` endpoint. It re-runs registry smoke on
// every call (operator-initiated, low QPS) so callers see fresh stale/live
// state rather than the stale 60s-startup snapshot.
type vlogBreakdownAdapter struct {
	registry *resourcewatch.Registry
	provider *resourcewatch.VlogProvider
	dataDir  string
	warn     float64
	critical float64
}

func newVlogBreakdownAdapter(cmd *cobra.Command, dataDir string) admin.VlogBreakdownAPI {
	if !vlogWatchEnabled(cmd) {
		return nil
	}
	warn, _ := cmd.Flags().GetFloat64("vlog-warn-ratio")
	critical, _ := cmd.Flags().GetFloat64("vlog-critical-ratio")
	return &vlogBreakdownAdapter{
		registry: resourcewatch.Default,
		provider: resourcewatch.NewVlogProvider(resourcewatch.VlogProviderOptions{DataDir: dataDir}),
		dataDir:  dataDir,
		warn:     warn,
		critical: critical,
	}
}

func (a *vlogBreakdownAdapter) Breakdown() (admin.VlogBreakdownResp, error) {
	sample, err := a.provider.Snapshot(context.Background())
	if err != nil {
		return admin.VlogBreakdownResp{}, fmt.Errorf("vlog snapshot: %w", err)
	}
	cats := make([]admin.VlogCategoryBytes, 0, len(sample.Categories))
	for k, v := range sample.Categories {
		cats = append(cats, admin.VlogCategoryBytes{Category: string(k), VlogBytes: int64(v)})
	}
	sort.Slice(cats, func(i, j int) bool { return cats[i].VlogBytes > cats[j].VlogBytes })

	gcFails := make(map[string]int32)
	for _, e := range a.registry.Snapshot() {
		gcFails[string(e.Category)] = e.ConsecutiveGCFailures()
	}

	smoke, _ := resourcewatch.VerifyVlogRegistry(a.dataDir, a.registry, false)
	live, stale := smoke.Live, smoke.Stale
	if live == nil {
		live = []string{}
	}
	if stale == nil {
		stale = []string{}
	}

	var ratio float64
	if sample.Limit > 0 {
		ratio = float64(sample.Open) / float64(sample.Limit)
	}
	level := "ok"
	switch {
	case ratio >= a.critical:
		level = "critical"
	case ratio >= a.warn:
		level = "warn"
	}
	return admin.VlogBreakdownResp{
		TotalVlogBytes: int64(sample.Open),
		LimitBytes:     int64(sample.Limit),
		Ratio:          ratio,
		Level:          level,
		Categories:     cats,
		GCFailures:     gcFails,
		SmokeReport:    admin.VlogSmokeReport{Live: live, Stale: stale},
	}, nil
}

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
	// Rate limit overrides — defaults are production-safe (100/200 ip, 50/100 user).
	// Benchmarks/dev/upstream-proxied deployments can relax these. 0 disables that layer.
	serveCmd.Flags().Bool("raft-log-fsync", true, "fsync the Raft log store on every append (auto: cluster=false (consensus provides redundancy), single=true; explicit value always wins)")
	serveCmd.Flags().Duration("raft-heartbeat-interval", 200*time.Millisecond, "per-group raft heartbeat interval. Lower = faster failure detection, higher CPU/network. Default 200ms balances detection latency with QUIC stream-open cost.")
	serveCmd.Flags().Duration("raft-election-timeout", 1000*time.Millisecond, "per-group raft election timeout (must be >= 3 * heartbeat-interval). Higher = fewer spurious elections under load.")
	serveCmd.Flags().Bool("quic-mux", true, "use multiplexed QUIC streams + heartbeat coalescing for per-group raft RPCs. idle-N8 measurement: 78pct drop in CPU samples, 17x drop in recvmsg syscalls vs legacy per-message path. Falls back to legacy on peer ALPN mismatch (older binaries).")
	serveCmd.Flags().Int("quic-mux-pool", 4, "stream pool size per peer when --quic-mux=true (avoids HoL with raft pipelining)")
	serveCmd.Flags().Duration("quic-mux-flush", 2*time.Millisecond, "heartbeat coalescing flush window when --quic-mux=true (must be << heartbeat-interval)")
	serveCmd.Flags().String("join", "", "join an existing cluster through this leader/follower raft address")
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
	ipRPS, _ := cmd.Flags().GetFloat64("rate-limit-ip-rps")
	ipBurst, _ := cmd.Flags().GetInt("rate-limit-ip-burst")
	userRPS, _ := cmd.Flags().GetFloat64("rate-limit-user-rps")
	userBurst, _ := cmd.Flags().GetInt("rate-limit-user-burst")
	authOpts = append(authOpts, server.WithRateLimits(ipRPS, ipBurst, userRPS, userBurst))

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
		var err error
		nodeID, err = serveruntime.GenerateNodeID(dataDir)
		if err != nil {
			return fmt.Errorf("generate node ID: %w", err)
		}
		log.Info().Str("component", "server").Str("node_id", nodeID).Msg("auto-generated node ID")
	}
	raftAddrExplicit := raftAddr != ""

	// strings.Split always yields at least one element — empty input or
	// trailing commas produce "" entries that waste a gossip tick each.
	peers := serveruntime.FilterEmpty(strings.Split(peersStr, ","))
	joinAddr, _ := cmd.Flags().GetString("join")
	joinMode := joinAddr != ""

	// D6/D7: --cluster-key is required when running in actual cluster mode
	// (peers > 0 || join != ""). Solo runs through this same function but
	// does not require a cluster key — runCluster handles both modes.
	clusterMode := len(peers) > 0 || joinMode
	if clusterMode {
		if err := transport.ValidateClusterKey(clusterKey); err != nil {
			if errors.Is(err, transport.ErrEmptyClusterKey) {
				return fmt.Errorf("--cluster-key is required in cluster mode (generate with: openssl rand -hex 32)")
			}
			log.Warn().Err(err).Msg("--cluster-key is below recommended length")
		}
	}
	if joinMode {
		if len(peers) > 0 {
			return fmt.Errorf("--join cannot be used with --peers")
		}
		if raftAddr == "" {
			return fmt.Errorf("--raft-addr is required when --join is set")
		}
		peers = []string{joinAddr}
	}

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
	roleRegistry := badgerrole.DefaultRegistry()
	startupDecisions := make([]badgerrole.Decision, 0, 8)
	recordStartupDecision := func(decision badgerrole.Decision) {
		startupDecisions = append(startupDecisions, decision)
	}

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
	dbOpts := badgerutil.SmallOptions(metaDir)
	db, err := badger.Open(dbOpts)
	if err != nil {
		return fmt.Errorf("open metadata db at %s: %w\n  recovery: check disk free space, confirm no other grainfs process holds the lock (lsof %s/LOCK), see README#badger-troubleshooting", metaDir, err, metaDir)
	}
	defer db.Close()
	metaVlogEntry := resourcewatch.RegisterDB(resourcewatch.DBCategoryMeta, db)
	defer resourcewatch.DeregisterDB(metaVlogEntry)
	// Phase 16 Week 3: cluster mode preflight. Same reasoning as local.
	recordStartupDecision(badgerrole.ProbeWritable(db, badgerrole.RoleMeta, "", metaDir))
	if startupDecisions[len(startupDecisions)-1].Status != badgerrole.DecisionOK {
		return server.PreflightBadger(db, metaDir, nil)
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
	// Meta-raft heartbeat is fixed (not user-configurable) and shares the
	// same coalescer flush window. If the flush window were larger than
	// the meta heartbeat, meta hb dispatch could be delayed past the meta
	// election deadline. Cap conservatively at < half of the meta heartbeat.
	if quicMuxEnabled && quicMuxFlushWindow > 0 && quicMuxFlushWindow*2 >= cluster.MetaRaftHeartbeatInterval {
		return fmt.Errorf("--quic-mux-flush (%s) must be << meta-raft heartbeat (%s); meta-raft uses a fixed 150ms heartbeat / 750ms election", quicMuxFlushWindow, cluster.MetaRaftHeartbeatInterval)
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
	if !logStore.IsShared() && logStore.DB() != nil {
		raftLogVlogEntry := resourcewatch.RegisterDB(resourcewatch.DBCategorySharedRaftLog, logStore.DB())
		defer resourcewatch.DeregisterDB(raftLogVlogEntry)
	}
	recordStartupDecision(badgerrole.Decision{
		Role:   badgerrole.RoleMetaRaftLog,
		Path:   raftDir,
		Status: badgerrole.DecisionOK,
		Action: badgerrole.RecoveryActionNone,
	})

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
		sharedRaftLogDB, err = badger.Open(badgerutil.RaftLogOptions(sharedDir, true))
		if err != nil {
			return fmt.Errorf("open shared raft-log badger at %s: %w", sharedDir, err)
		}
		defer sharedRaftLogDB.Close()
		sharedRaftLogVlogEntry := resourcewatch.RegisterDB(resourcewatch.DBCategorySharedRaftLog, sharedRaftLogDB)
		defer resourcewatch.DeregisterDB(sharedRaftLogVlogEntry)
		recordStartupDecision(badgerrole.Decision{
			Role:   badgerrole.RoleSharedRaftLog,
			Path:   sharedDir,
			Status: badgerrole.DecisionOK,
			Action: badgerrole.RecoveryActionNone,
		})
		log.Info().Str("dir", sharedDir).Msg("shared raft-log DB enabled (C2 P0b prototype)")
	}

	// Start QUIC transport for inter-node communication. Resolution order
	// (rotation-spec D10):
	//   1. keys.d/current.key wins over --cluster-key flag if both differ
	//      (warn emitted; refuse-to-start path explicitly NOT used).
	//   2. Disk only: use disk silently.
	//   3. Flag only: use flag, mirror to keys.d/current.key on first boot.
	//   4. Both empty + cluster mode: refused upstream by ValidateClusterKey.
	//      Both empty + solo mode: generate ephemeral so zero-config holds.
	resolvedKey, warn, err := serveruntime.ResolveClusterKey(dataDir, clusterKey)
	if err != nil {
		return fmt.Errorf("resolve cluster key: %w", err)
	}
	if warn != "" {
		log.Warn().Msg(warn)
	}
	transportPSK := resolvedKey
	if transportPSK == "" {
		ephemeral, err := serveruntime.GenerateEphemeralClusterKey()
		if err != nil {
			return fmt.Errorf("init QUIC transport: %w", err)
		}
		transportPSK = ephemeral
	}
	quicTransport, err := transport.NewQUICTransport(transportPSK)
	if err != nil {
		return fmt.Errorf("init QUIC transport: %w", err)
	}
	// Forwarded S3 PUTs can fan out into EC shard body streams on the bucket
	// owner. Keep enough bulk capacity for that nested data path while meta and
	// raft traffic remain independently classed.
	quicTransport.SetTrafficLimits(transport.TrafficLimits{Bulk: 64})
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
	if !joinMode {
		if err := node.Bootstrap(); err != nil && !errors.Is(err, raft.ErrAlreadyBootstrapped) {
			return fmt.Errorf("raft bootstrap: %w", err)
		}
	}

	// Wire QUIC transport to Raft RPC layer
	rpcTransport := raft.NewQUICRPCTransport(quicTransport, node)
	rpcTransport.SetTransport()

	// GroupRaftQUICMux multiplexes per-group raft RPCs over StreamGroupRaft.
	// Created BEFORE NewMetaTransportQUICMux so the meta-raft transport can
	// auto-register its node on the mux at construction time. This closes
	// the codex P1 #3 startup race: if EnableMux ran before metaNode was
	// registered, all inbound meta calls would hit "mux: unknown group
	// __meta__" and meta election would stall.
	groupRaftMux := raft.NewGroupRaftQUICMux(quicTransport)
	if quicMuxEnabled {
		groupRaftMux.EnableMux(quicMuxPoolSize, quicMuxFlushWindow)
		log.Info().
			Int("pool", quicMuxPoolSize).
			Dur("flush", quicMuxFlushWindow).
			Msg("group raft mux mode enabled (R+H Phase 2 prototype)")
	}

	// Meta-Raft: dedicated control-plane Raft group for cluster membership.
	metaRaft, err := cluster.NewMetaRaft(cluster.MetaRaftConfig{
		NodeID:  nodeID,
		Peers:   peers,
		DataDir: dataDir,
	})
	if err != nil {
		return fmt.Errorf("init meta-raft: %w", err)
	}
	// Mux-aware constructor: auto-registers metaRaft.Node() on groupRaftMux
	// under the magic groupID "__meta__" so receiver-side mux dispatch is
	// wired before any meta heartbeat hits the wire.
	metaTransport := cluster.NewMetaTransportQUICMux(quicTransport, metaRaft.Node(), groupRaftMux)
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

	// 클러스터 키 회전 — RotationWorker가 FSM phase 변경에 반응하여 디스크
	// I/O와 transport identity swap을 수행 (D16 분리). 콜백은 metaRaft.Start
	// 전에 등록해야 첫 apply 이벤트를 놓치지 않는다 (race-free).
	rotationKeystore := transport.NewKeystore(dataDir)
	rotationWorker := cluster.NewRotationWorker(rotationKeystore, quicTransport, nodeID)
	metaRaft.FSM().SetOnRotationApplied(func(st cluster.RotationState) {
		_ = rotationWorker.OnPhaseChange(st)
	})
	// Seed rotation FSM steady state with active SPKI so RotateKeyBegin can be
	// validated against the current cluster key (D10).
	if _, activeSPKI, err := transport.DeriveClusterIdentity(transportPSK); err == nil {
		metaRaft.FSM().SetRotationSteady(activeSPKI)
	} else {
		log.Warn().Err(err).Msg("failed to seed rotation FSM steady state; rotation will be unavailable until next restart")
	}

	if !joinMode {
		if err := metaRaft.Bootstrap(); err != nil {
			return fmt.Errorf("meta-raft bootstrap: %w", err)
		}
	}
	if err := metaRaft.Start(ctx); err != nil {
		return fmt.Errorf("meta-raft start: %w", err)
	}
	// previous.key cleanup goroutine — deletes keys.d/previous.key after
	// RotationPreviousGrace expires. Runs on all nodes (FSM state is
	// identical via raft); each node deletes its own local file.
	metaRaft.StartPreviousKeyCleanup(ctx, rotationKeystore)
	if err := serveruntime.StartRotationSocket(ctx, dataDir, metaRaft); err != nil {
		log.Warn().Err(err).Msg("rotation socket failed to start; cluster rotate-key CLI will be unavailable")
	}
	defer metaRaft.Close()

	// Seed Router with any bucket assignments already persisted in FSM state.
	// Start() returns before replay finishes; onBucketAssigned fires live updates.
	clusterRouter.Sync(metaRaft.FSM().BucketAssignments())

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
	if !joinMode {
		if err := serveruntime.WaitForMetaRaftLeader(ctx, metaRaft, 15*time.Second); err != nil {
			return err
		}
		addNodeCtx, addNodeCancel := context.WithTimeout(ctx, 10*time.Second)
		if err := metaRaft.ProposeAddNode(addNodeCtx, cluster.MetaNodeEntry{ID: nodeID, Address: raftAddr, Role: 0}); err != nil {
			log.Debug().Err(err).Str("node_id", nodeID).Str("addr", raftAddr).Msg("seed node metadata propose failed (non-fatal)")
		}
		addNodeCancel()

		if err := serveruntime.SeedInitialShardGroups(ctx, metaRaft, nodeID, raftAddr, peers, seedGroups); err != nil {
			return err
		}
		if err := serveruntime.WaitForShardGroupCount(ctx, metaRaft.FSM(), seedGroups, 30*time.Second); err != nil {
			return err
		}
		clusterRouter.Sync(metaRaft.FSM().BucketAssignments())
		clusterRouter.SetRequireExplicitAssignments(true)
	}

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
	shardSvcOpts = append(shardSvcOpts, cluster.WithNodeAddressBook(metaRaft.FSM()))
	shardSvc := cluster.NewShardService(dataDir, quicTransport, shardSvcOpts...)

	// Set up StreamRouter: Raft RPCs on Control stream, Shard RPCs on Data stream
	router := transport.NewStreamRouter()
	router.Handle(transport.StreamControl, rpcTransport.Handler())
	router.Handle(transport.StreamData, shardSvc.HandleRPC())
	quicTransport.SetStreamHandler(router.Dispatch)
	// Body handler must register on QUICTransport's internal router; the
	// catch-all (router.Dispatch) only sees per-message handlers and cannot
	// consume body streams. Pre-fix, every StreamShardWriteBody fell through
	// to the catch-all → returned nil → stream closed without response →
	// caller saw "decode response: read header: EOF" → N×replication produced
	// only the leader's local copy.
	quicTransport.HandleBody(transport.StreamShardWriteBody, shardSvc.HandleWriteBody())
	quicTransport.HandleRead(transport.StreamShardReadBody, shardSvc.HandleReadBody())

	node.Start()
	defer node.Stop()

	distBackend, err := cluster.NewDistributedBackend(dataDir, db, node)
	if err != nil {
		return fmt.Errorf("failed to initialize distributed storage: %w", err)
	}

	// Wire shard service for distributed fan-out replication
	allNodes := append([]string{raftAddr}, peers...)
	distBackend.SetShardService(shardSvc, allNodes)

	// EC shard cache (Phase 2 #3 follow-up). Construct it before any per-group
	// backend can be instantiated so group-1..N receive the same cache wiring
	// as the legacy group-0 backend.
	shardCacheSize, _ := cmd.Flags().GetInt64("shard-cache-size")
	shardCache := shardcache.New(shardCacheSize)
	distBackend.SetShardCache(shardCache)
	log.Info().Int64("bytes", shardCacheSize).Msg("ec shard cache configured")

	// Live multi-raft sharding (v0.0.7.0): group-0 keeps using the shared
	// distBackend (legacy single-backend deployment is the group-0 instance);
	// groups 1..N-1 get their own per-group BadgerDB+raft via instantiateLocalGroup
	// when this node is a voter (see ownedGroups loop below).
	group0Backend := cluster.WrapDistributedBackend("group-0", distBackend)
	group0 := cluster.NewDataGroupWithBackend(
		"group-0",
		serveruntime.SeedShardGroupVoters(nodeID, raftAddr, peers, metaRaft.FSM().Nodes(), "group-0", 3),
		group0Backend,
	)
	dgMgr.Add(group0)
	distBackend.SetRouter(clusterRouter)
	distBackend.SetShardGroupSource(metaRaft.FSM())

	// PR-D: Rebalancer 배선 — LoadReporter가 meta-Raft FSM에 부하 스냅샷을 커밋하고
	// Rebalancer가 leader에서 주기적으로 평가해 RebalancePlan을 제안·실행한다.
	rebalancerCfg := cluster.DefaultRebalancerConfig()
	rebalancer := cluster.NewRebalancer(nodeID, metaRaft, dgMgr, rebalancerCfg)
	rebalancer.SetGroupRebalancer(
		cluster.NewDataGroupPlanExecutor(nodeID, dgMgr, metaRaft.FSM(), metaRaft),
	)
	metaRaft.FSM().SetOnRebalancePlan(func(plan *cluster.RebalancePlan) {
		if joinMode {
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
		mu           sync.Mutex
		wg           sync.WaitGroup
		m            map[string]*cluster.GroupBackend
		inFlight     map[string]bool // entry.ID currently being instantiated; prevents duplicate concurrent OpenSharedLogStore / badger.Open
		shuttingDown bool
	}{m: make(map[string]*cluster.GroupBackend), inFlight: make(map[string]bool)}

	// Shared stop channel for all apply loops (distBackend + per-group).
	// Must be initialized before any goroutine that passes it to RunApplyLoop.
	stopApply := make(chan struct{})

	// groupRaftMux was created earlier (before NewMetaTransportQUICMux) so
	// metaTransport could auto-register its node onto the mux. Each group
	// uses ForGroup(groupID) as its raft transport.

	instantiateOwnedIfNeeded := func(entry cluster.ShardGroupEntry) error {
		// group-0 is already wired with the shared distBackend.
		if entry.ID == "group-0" {
			return nil
		}
		serveruntime.EnsureShardGroupPlaceholder(dgMgr, entry)
		// Only instantiate for groups where we are a voter. Shard groups should
		// store node IDs; raftAddr remains a local alias for legacy/static
		// entries written before that invariant existed.
		groupNodeID, isVoter := cluster.NewShardGroupPeerSet(entry).MatchLocal(nodeID, raftAddr)
		if !isVoter {
			return nil
		}
		ownedGroups.mu.Lock()
		if ownedGroups.shuttingDown {
			ownedGroups.mu.Unlock()
			return nil
		}
		if _, ok := ownedGroups.m[entry.ID]; ok {
			ownedGroups.mu.Unlock()
			return nil // already instantiated
		}
		if ownedGroups.inFlight[entry.ID] {
			ownedGroups.mu.Unlock()
			return nil // another goroutine is currently bringing this group up
		}
		ownedGroups.inFlight[entry.ID] = true
		ownedGroups.mu.Unlock()
		// Make sure inFlight is cleared even if instantiation fails.
		defer func() {
			ownedGroups.mu.Lock()
			delete(ownedGroups.inFlight, entry.ID)
			ownedGroups.mu.Unlock()
		}()

		glc := cluster.GroupLifecycleConfig{
			NodeID:    groupNodeID,
			DataDir:   dataDir,
			ShardSvc:  shardSvc,
			Transport: groupRaftMux.ForGroup(entry.ID),
			AddrBook:  metaRaft.FSM(),
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
				return fmt.Errorf("group %s: open shared log store: %w", entry.ID, lerr)
			}
			glc.LogStore = ls
		}
		gb, err := cluster.InstantiateLocalGroup(glc, entry)
		if err != nil {
			return fmt.Errorf("group %s: instantiate local group: %w", entry.ID, err)
		}
		gb.SetShardCache(shardCache)
		groupRaftMux.Register(entry.ID, gb.RaftNode())
		dgMgr.Add(cluster.NewDataGroupWithBackend(entry.ID, entry.PeerIDs, gb))
		go gb.RunApplyLoop(stopApply)
		ownedGroups.mu.Lock()
		ownedGroups.m[entry.ID] = gb
		ownedGroups.mu.Unlock()
		log.Info().Str("group_id", entry.ID).Strs("peers", entry.PeerIDs).Msg("instantiateLocalGroup ok")
		return nil
	}

	scheduleOwnedInstantiation := func(entry cluster.ShardGroupEntry) {
		ownedGroups.mu.Lock()
		if ownedGroups.shuttingDown {
			ownedGroups.mu.Unlock()
			return
		}
		ownedGroups.wg.Add(1)
		ownedGroups.mu.Unlock()
		go func() {
			defer ownedGroups.wg.Done()
			if err := instantiateOwnedIfNeeded(entry); err != nil {
				log.Error().Err(serveruntime.HandleRuntimeGroupInstantiationError(entry.ID, err)).Str("group_id", entry.ID).Msg("runtime data group instantiation failed")
			}
		}()
	}

	// Cold-start instantiation for entries already in FSM (restart path).
	// Run synchronously so Badger role failures feed the startup reducer before
	// the server accepts traffic.
	for _, entry := range metaRaft.FSM().ShardGroups() {
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
	// Runtime: handle entries replayed/applied after this point (fresh boot path).
	// Dispatch to goroutine so apply loop is not blocked by BadgerDB+raft.Node startup.
	metaRaft.FSM().SetOnShardGroupAdded(func(entry cluster.ShardGroupEntry) {
		scheduleOwnedInstantiation(entry)
	})

	// Shutdown hook: close all owned groups in parallel with 5s timeout each.
	defer func() {
		metaRaft.FSM().SetOnShardGroupAdded(nil)
		ownedGroups.mu.Lock()
		ownedGroups.shuttingDown = true
		ownedGroups.mu.Unlock()
		ownedGroups.wg.Wait()

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

	distBackend.RegisterCacheInvalidator("s3-cache", cluster.CacheInvalidatorFunc(cachedBackend.InvalidateKey))

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
		WithReadStreamDialer(forwardReadStreamDialer)
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
		return metaForwardSender.Send(ctx, serveruntime.MetaProposalTargets(metaRaft.Node().LeaderID(), peers), command)
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
	).WithForwardSender(forwardSender).WithNodeAddressResolver(metaRaft.FSM()).WithSelfPeerAlias(raftAddr)
	metaReadReceiver := cluster.NewMetaCatalogReadReceiver(cluster.NewMetaCatalog(metaRaft, clusterCoord, "s3://grainfs-tables/warehouse"))
	router.Handle(transport.StreamMetaCatalogRead, metaReadReceiver.Handle)
	if joinMode {
		if err := serveruntime.PerformMetaJoin(ctx, quicTransport, peers, nodeID, raftAddr); err != nil {
			return err
		}
		if err := serveruntime.WaitForShardGroupCount(ctx, metaRaft.FSM(), seedGroups, 30*time.Second); err != nil {
			return err
		}
		clusterRouter.Sync(metaRaft.FSM().BucketAssignments())
		clusterRouter.SetRequireExplicitAssignments(true)
	}

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
	snapInterval, _ := cmd.Flags().GetDuration("snapshot-interval")
	snapRetain, _ := cmd.Flags().GetInt("snapshot-retain")

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
	if serveruntime.ShouldCreateDefaultBucketOnStartup(peers, recoveryReadOnly) {
		if err := serveruntime.CreateDefaultBucketWithRetry(ctx, backend, 30*time.Second); err != nil {
			return fmt.Errorf("create default bucket: %w", err)
		}
	}

	// Start auto-snapshotter for object-level PITR snapshots (separate from
	// Raft snapshots above). Uses the WAL-wrapped backend so replay is
	// anchored to the object mutation log. Start only after startup bucket
	// metadata exists and the routed snapshot enumeration path is usable; data
	// groups are instantiated asynchronously during boot.
	if err := serveruntime.StartAutoSnapshotterWhenReady(ctx, dataDir, walDir, backend, snapInterval, snapRetain, 30*time.Second); err != nil {
		log.Warn().Err(err).Msg("auto-snapshot init failed")
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
		// cluster status / remove-peer must reflect *meta-raft* membership —
		// that is the cluster-wide membership ledger that `serve --join`
		// updates via performMetaJoin. The node initialised at line 872 is a
		// legacy per-process raft instance whose Peers() never grows on join.
		server.WithClusterInfo(&raftClusterInfo{node: metaRaft.Node(), peers: peers, backend: distBackend, addrBook: metaRaft.FSM()}),
		server.WithClusterMembership(&raftMembership{node: metaRaft.Node()}),
		server.WithEventStore(eventstore.New(db)),
		server.WithAlerts(clusterAlerts),
		server.WithDataDir(dataDir),
	}
	if len(peers) == 0 && !raftAddrExplicit && !joinMode {
		legacyStore := icebergcatalog.NewStore(db, "s3://grainfs-tables/warehouse")
		metaCatalog := cluster.NewMetaCatalog(metaRaft, backend, "s3://grainfs-tables/warehouse")
		if err := serveruntime.MigrateLegacySingletonIcebergCatalog(ctx, legacyStore, metaCatalog, backend); err != nil {
			return fmt.Errorf("migrate singleton Iceberg catalog: %w", err)
		}
		srvOpts = append(srvOpts, server.WithIcebergCatalog(metaCatalog))
	} else {
		metaForward := func(ctx context.Context, command []byte) error {
			return metaForwardSender.Send(ctx, serveruntime.MetaProposalTargets(metaRaft.Node().LeaderID(), peers), command)
		}
		metaReadTargets := func() []string {
			return serveruntime.MetaProposalTargets(metaRaft.Node().LeaderID(), peers)
		}
		srvOpts = append(srvOpts, server.WithIcebergCatalog(cluster.NewMetaCatalogWithForwarders(metaRaft, backend, "s3://grainfs-tables/warehouse", metaForward, metaReadSender, metaReadTargets)))
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

	var incidentRecorder *incident.Recorder
	incidentDB, incidentDecision, err := badgerrole.OpenRole(roleRegistry, badgerrole.RoleIncidentState, badgerrole.PathContext{DataDir: dataDir})
	if err != nil {
		if feature, ok := serveruntime.OptionalRoleDisabled(roleRegistry, incidentDecision); ok {
			serveruntime.LogOptionalRoleDisabled(badgerrole.RoleIncidentState, feature, err)
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
		startResourceGuards(ctx, cmd, nodeID, dataDir, incidentRecorder, clusterAlerts)
	}
	clusterIncidentRecorder, scrubberIncidentRecorder := serveruntime.IncidentRecorderInterfaces(incidentRecorder)

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

	// --- Admin / dashboard wiring (Volume CLI Phase B) ---
	// Open the dashboard auth token (creates <data>/dashboard.token mode 0600
	// on first run). Install the middleware on /ui/* and register /ui/api/*
	// admin routes BEFORE the data-plane Hertz starts serving.
	tokenStore, err := dashboard.Open(filepath.Join(dataDir, "dashboard.token"))
	if err != nil {
		return fmt.Errorf("dashboard token: %w", err)
	}
	publicURL, _ := cmd.Flags().GetString("public-url")
	adminDeps := &admin.Deps{
		Manager:       srv.VolumeManager(),
		Token:         tokenStore,
		PublicURL:     publicURL,
		NodeID:        nodeID,
		PeerHealth:    peerHealthAdapter{distBackend},
		VlogBreakdown: newVlogBreakdownAdapter(cmd, dataDir),
	}
	dataHertz := srv.HertzEngine()
	dataHertz.Use(server.DashboardTokenMiddleware(tokenStore))
	admin.RegisterUI(dataHertz, adminDeps)

	// Open the admin Unix socket. Operator commands (`grainfs volume *`, `grainfs
	// dashboard`) reach this socket; permissions are governed by the file mode
	// (0660) and optional --admin-group chown.
	adminSocket, _ := cmd.Flags().GetString("admin-socket")
	if adminSocket == "" {
		adminSocket = filepath.Join(dataDir, "admin.sock")
	}
	adminGroup, _ := cmd.Flags().GetString("admin-group")
	adminSrv, err := admin.Start(admin.Config{
		SocketPath: adminSocket,
		Group:      adminGroup,
		Deps:       adminDeps,
	})
	if err != nil {
		return fmt.Errorf("admin server: %w", err)
	}
	// Best-effort fallback: ensures the socket is unlinked even if the explicit
	// shutdown path is bypassed (e.g. early return). The explicit shutdown
	// sequence below stops admin BEFORE the data plane drains so operator
	// commands cannot land on a half-shutdown server (spec A5).
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = adminSrv.Stop(stopCtx)
	}()

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
	repairer := scrubber.ReplicaRepairer(replicaRepairerFunc(func(rctx context.Context, bucket, key string) error {
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
	adminDeps.ScrubProposer = &scrubProposerAdapter{metaRaft: metaRaft, director: director, nodeID: nodeID}
	adminDeps.ScrubAggregator = &scrubAggregatorAdapter{coord: clusterCoord}

	scrubInterval, _ := cmd.Flags().GetDuration("scrub-interval")
	if scrubInterval > 0 {
		sc := scrubber.New(distBackend, scrubInterval)
		sc.SetEmitter(activeEmitter)
		sc.RegisterSource("replication", replSource, replVerifier)
		sc.Start(ctx)

		placementMonitors := serveruntime.NewPlacementMonitorRegistry()
		startPlacementMonitor := func(monitorCtx context.Context, dg *cluster.DataGroup) {
			gb := dg.Backend()
			if clusterIncidentRecorder != nil {
				gb.SetIncidentRecorder(clusterIncidentRecorder)
			}
			placementMonitor := cluster.NewShardPlacementMonitor(gb.FSMRef(), gb, shardSvc, gb.NodeID(), scrubInterval)
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
				} else if receiptWiring != nil && receiptWiring.store != nil && receiptWiring.keyStore != nil {
					r := &receipt.HealReceipt{
						ReceiptID:     receiptID,
						Timestamp:     time.Now().UTC(),
						Object:        receipt.ObjectRef{Bucket: bucket, Key: objectKey, VersionID: versionID},
						ShardsLost:    []int32{int32(shardIdx)},
						ShardsRebuilt: []int32{int32(shardIdx)},
						EventIDs:      []string{correlationID},
						CorrelationID: correlationID,
					}
					if err := receipt.Sign(r, receiptWiring.keyStore); err != nil {
						log.Warn().Str("correlation_id", correlationID).Err(err).Msg("placement monitor receipt sign failed")
					} else if err := receiptWiring.store.Put(r); err != nil {
						log.Warn().Str("correlation_id", correlationID).Str("receipt_id", receiptID).Err(err).Msg("placement monitor receipt store failed")
					} else if err := receiptWiring.store.Flush(); err != nil {
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

	// Background EC resharding is group-local: each locally-owned DataGroup has
	// its own Raft leader and object metadata DB. Followers skip each pass in
	// ReshardManager, but we still start one manager per local group so
	// leadership changes are picked up without serve-level rewiring.
	reshardInterval, _ := cmd.Flags().GetDuration("reshard-interval")
	if reshardInterval > 0 {
		reshardManagers := serveruntime.NewReshardManagerRegistry()
		startReshardManager := func(managerCtx context.Context, dg *cluster.DataGroup) {
			gb := dg.Backend()
			leader := gb.RaftNode()
			if leader == nil {
				log.Warn().Str("group", dg.ID()).Msg("reshard manager skipped: group has no raft node")
				return
			}
			go cluster.NewReshardManager(gb, leader, reshardInterval).Start(managerCtx)
		}
		refreshReshardManagers := func() {
			reshardManagers.Refresh(ctx, dgMgr.All(), startReshardManager)
		}
		refreshReshardManagers()
		go func() {
			ticker := time.NewTicker(reshardInterval)
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
		log.Info().Dur("interval", reshardInterval).Msg("cluster reshard manager started")
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
	exec.SetShardCounter(serveruntime.ECShardCounterFor(fsm))
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
//
// Peers() / LivePeers() always read from the running raft.Node so dynamic-join
// clusters reflect membership changes (joins, removes) the moment they commit.
// The peers field is preserved for compatibility with callers that still pass
// the bootstrap list, but it is not consulted on the read path.
type raftClusterInfo struct {
	node     *raft.Node
	peers    []string
	backend  *cluster.DistributedBackend
	addrBook cluster.NodeAddressBook
}

func (r *raftClusterInfo) NodeID() string   { return r.node.ID() }
func (r *raftClusterInfo) State() string    { return r.node.State().String() }
func (r *raftClusterInfo) Term() uint64     { return r.node.Term() }
func (r *raftClusterInfo) LeaderID() string { return r.node.LeaderID() }
func (r *raftClusterInfo) Peers() []string {
	return nilToEmpty(r.normalizePeerIDs(r.node.Peers()))
}

// LivePeers reports all metaRaft voters as live: self plus every remote.
// The legacy DistributedBackend's LiveNodes() lives on a separate raft +
// peerHealth map whose identifiers don't line up with metaRaft's voter
// addresses, so consulting it here would mark every voter "down" right
// after dynamic-join until peerHealth catches up. Treating voters as live
// keeps the happy-path pre-flight working; voter-count math still blocks
// the only-1-voter-left scenario, which is the truly dangerous case.
// PR-D unifies peer identity so this fallback no longer mixes node IDs and
// raft addresses. Fine-grained liveness remains a later peer-health signal.
func (r *raftClusterInfo) LivePeers() []string {
	peers := r.normalizePeerIDs(r.node.Peers())
	out := make([]string, 0, len(peers)+1)
	if id := r.node.ID(); id != "" {
		out = append(out, id)
	}
	out = append(out, peers...)
	return out
}

func (r *raftClusterInfo) PeerAddrs() map[string]string {
	out := make(map[string]string)
	if r.addrBook == nil {
		return out
	}
	for _, peer := range r.node.Peers() {
		resolved := cluster.ResolveShardGroupPeer(r.addrBook, peer)
		if resolved.NodeID != "" && resolved.RaftAddr != "" {
			out[resolved.NodeID] = resolved.RaftAddr
		}
	}
	return out
}

func (r *raftClusterInfo) PeerStates() map[string]string {
	out := make(map[string]string)
	for _, peer := range r.node.Peers() {
		resolved := cluster.ResolveShardGroupPeer(r.addrBook, peer)
		id := resolved.NodeID
		if id == "" {
			id = peer
		}
		if resolved.Unresolved {
			out[id] = "unresolved_legacy"
			continue
		}
		out[id] = "configured"
	}
	return out
}

func (r *raftClusterInfo) PeerSnapshot() []cluster.PeerLivenessRow {
	return cluster.BuildPeerLivenessSnapshot(cluster.PeerLivenessInput{
		SelfID:      r.node.ID(),
		Voters:      r.node.Peers(),
		AddressBook: r.addrBook,
	})
}

func (r *raftClusterInfo) BucketAssignments() map[string]string {
	src, ok := r.addrBook.(interface {
		BucketAssignments() map[string]string
	})
	if !ok {
		return nil
	}
	return src.BucketAssignments()
}

func (r *raftClusterInfo) ShardGroups() []cluster.ShardGroupEntry {
	src, ok := r.addrBook.(interface {
		ShardGroups() []cluster.ShardGroupEntry
	})
	if !ok {
		return nil
	}
	return src.ShardGroups()
}

func (r *raftClusterInfo) normalizePeerIDs(peers []string) []string {
	if len(peers) == 0 {
		return nil
	}
	if r.addrBook == nil {
		out := make([]string, len(peers))
		copy(out, peers)
		return out
	}
	out := make([]string, len(peers))
	for i, peer := range peers {
		out[i] = cluster.ResolveShardGroupPeer(r.addrBook, peer).NodeID
		if out[i] == "" {
			out[i] = peer
		}
	}
	return out
}

// nilToEmpty normalises a nil slice to an empty one so JSON marshals it as
// "[]" instead of "null". The cluster status CLI assertion path treats
// missing keys and null identically — keep the wire shape stable.
func nilToEmpty(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// raftMembership adapts raft.Node to server.ClusterMembership for the
// remove-peer endpoint. Joint consensus (§4.3) is used so the change is
// atomic and the engine handles leader self-removal via commit-time wakeup.
type raftMembership struct{ node *raft.Node }

func (r *raftMembership) RemoveVoter(ctx context.Context, id string) error {
	return r.node.ChangeMembership(ctx, nil, []string{id})
}
