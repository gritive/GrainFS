package main

import (
	"context"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/serveruntime"
)

const defaultReshardInterval = 24 * time.Hour

// registerAllServeFlags registers every cobra flag the `grainfs serve` command
// accepts. Extracted from init() so unit tests can build a fresh cobra.Command
// and exercise serveOptionsFromCmd without touching the global serveCmd.
func registerAllServeFlags(cmd *cobra.Command) {
	cmd.Flags().StringP("data", "d", "./data", "data directory (comma-separated for multi-root)")
	cmd.Flags().String("meta-dir", "", "dedicated fast metadata storage directory (defaults to first path in --data)")
	cmd.Flags().IntP("port", "p", 9000, "listen port")
	cmd.Flags().String("admin-socket", "", "admin Unix socket path (default <data>/admin.sock)")
	cmd.Flags().String("admin-group", "", "OS group name for admin socket chown (default: caller's primary group)")
	cmd.Flags().String("public-url", "", "public dashboard base URL (e.g. https://node1:9000); defaults to localhost in `grainfs dashboard` output")
	cmd.Flags().String("node-id", "", "unique node ID (auto-generated if omitted)")
	cmd.Flags().String("raft-addr", "", "Raft listen address for cluster communication (required in cluster mode)")
	cmd.Flags().String("cluster-key", "", "Pre-shared key for cluster peer authentication")
	cmd.Flags().Int64("cluster-append-forward-buffer-total-bytes", 512*1024*1024,
		"Total byte budget for AppendObject forward-body reservation pool (default 512 MiB).")
	cmd.Flags().Int64("cluster-append-forward-buffer-max-per-request", 64*1024*1024,
		"Max bytes any single AppendObject forward request may reserve (default 64 MiB).")
	cmd.Flags().Int64("append-size-cap-bytes", 5*1024*1024*1024*1024,
		"Per-object total size cap for AppendObject in bytes (default 5 TiB, S3 PutObject parity).")
	cmd.Flags().String("encryption-key-file", "", "path to 32-byte encryption key file (auto-generated only for solo bootstrap if omitted)")
	cmd.Flags().Int("nfs4-port", 2049, "NFSv4 server port (0 = disabled); binds 0.0.0.0 — use firewall or set 0 when exposing public interfaces")
	cmd.Flags().String("nfs-write-buffer-dir", "", "directory for NFS write coalescing buffer files (empty = derive from --data)")
	cmd.Flags().Duration("nfs-write-buffer-idle", 30*time.Second, "idle timeout before write buffer auto-flushes (0 = disable buffering)")
	cmd.Flags().Int("nbd-port", 10809, "NBD server port (0 = disabled). Client-side nbd-client still requires Linux.")
	cmd.Flags().String("9p-bind", "127.0.0.1", "9P2000.L bind address; set 0.0.0.0 only on trusted networks")
	cmd.Flags().Int("9p-port", 0, "9P2000.L server port (0 = disabled); unauthenticated, use firewall")
	cmd.Flags().Int("pack-threshold", 65537, "pack objects below this size into blob files (0 = disabled, e.g. 65537)")
	cmd.Flags().Int("shard-pack-threshold", 65545, "pack cluster shards below this size into node-local append-only shard packs (0 = disabled, e.g. 65545)")
	cmd.Flags().Duration("scrub-interval", 24*time.Hour, "EC shard scrub interval (always on; 0 resets to default 24h)")
	cmd.Flags().Duration("scrub-orphan-age", 5*time.Minute,
		"minimum filesystem mtime age before an orphan raw segment is eligible for sweep")
	cmd.Flags().Duration("reshard-interval", defaultReshardInterval, "background EC reshard interval (always on; 0 resets to default 24h)")
	cmd.Flags().Duration("datagroup-refresh-interval", time.Minute, "how often to scan for new DataGroups and start reshard managers (0 = only scan at startup)")
	cmd.Flags().Duration("lifecycle-interval", 1*time.Hour, "lifecycle rule evaluation interval (0 to disable)")
	cmd.Flags().Duration("degraded-check-interval", 30*time.Second, "EC degraded-mode liveness check interval")
	cmd.Flags().Duration("raft-log-gc-interval", 30*time.Second, "how often Raft log GC runs")
	cmd.Flags().Bool("fd-watch-enabled", true, "enable predictive file descriptor exhaustion warnings")
	cmd.Flags().Duration("fd-watch-interval", 10*time.Second, "how often to sample process file descriptor usage")
	cmd.Flags().Float64("fd-warn-threshold", 0.80, "FD used fraction (0-1) at which a warning incident fires")
	cmd.Flags().Float64("fd-critical-threshold", 0.90, "FD used fraction (0-1) at which a critical incident fires")
	cmd.Flags().Duration("fd-eta-window", 30*time.Minute, "positive-trend ETA window for predictive FD warnings")
	cmd.Flags().Duration("fd-recovery-window", time.Minute, "stable below-threshold window before resolving FD incidents")
	cmd.Flags().Int("fd-classification-cap", 512, "max open file descriptors to classify by category per sample")
	// Predictive goroutine warnings — same Detector pattern as FD watcher.
	// Defaults measurement-justified: 3-node cluster idle baseline ~200
	// goroutines/node, so 5000 warn (~25× idle) and 20000 critical (~100×).
	cmd.Flags().Bool("goroutine-watch-enabled", true, "enable predictive goroutine count warnings")
	cmd.Flags().Int("goroutine-warn", 5000, "goroutine count that triggers warn-level alert (transition-only firing)")
	cmd.Flags().Int("goroutine-critical", 20000, "goroutine count that triggers critical-level alert")
	cmd.Flags().Duration("goroutine-poll-interval", 30*time.Second, "polling interval for goroutine count sampling")
	cmd.Flags().Duration("goroutine-eta-window", 30*time.Minute, "ETA projection window for predictive goroutine warnings")
	cmd.Flags().Duration("goroutine-recovery-window", time.Minute, "minimum time below warn threshold before transitioning to ok")
	cmd.Flags().Bool("vlog-watch-enabled", true, "enable BadgerDB vlog watcher (PR2)")
	cmd.Flags().Float64("vlog-warn-ratio", 0.4, "vlog/disk ratio that fires warn (transition-only)")
	cmd.Flags().Float64("vlog-critical-ratio", 0.7, "vlog/disk ratio that fires critical")
	cmd.Flags().Duration("vlog-poll-interval", 60*time.Second, "vlog watcher sampling cadence")
	cmd.Flags().Duration("vlog-eta-window", 30*time.Minute, "ETA projection window for vlog warnings")
	cmd.Flags().Duration("vlog-recovery-window", 5*time.Minute, "minimum time below warn ratio before transitioning to ok")
	cmd.Flags().Duration("badger-gc-interval", 5*time.Minute, "BadgerDB vlog GC ticker cadence")
	cmd.Flags().Bool("badger-gc-disable", false, "disable BadgerDB vlog GC ticker (debug only)")
	cmd.Flags().Int32("badger-gc-fail-threshold", 3, "consecutive RunValueLogGC failures before incident")
	cmd.Flags().Bool("strict-vlog-registry", false, "fatal on vlog registry smoke mismatch (e2e: true)")
	cmd.Flags().Duration("vlog-smoke-defer", 60*time.Second, "delay before vlog registry startup smoke runs")
	cmd.Flags().Int64("badger-value-threshold", 0, "force BadgerDB ValueThreshold (bytes) so values above this size spill to vlog; 0 keeps GrainFS small-store default. Test-only.")
	_ = cmd.Flags().MarkHidden("badger-value-threshold")
	// Direct I/O on local shard writes bypasses the kernel page cache (Linux
	// O_DIRECT, macOS F_NOCACHE). On by default — the bench
	// (internal/cluster/shardio_directio_bench_test.go) showed 10x on 1MB
	// shards, 40% on 4MB, neutral on 16MB. Filesystems that reject O_DIRECT
	// (some overlayfs/tmpfs) fall back to the buffered path automatically;
	// pass --direct-io=false to force buffered everywhere.
	cmd.Flags().Bool("direct-io", true, "bypass page cache on local EC shard writes (Linux O_DIRECT / macOS F_NOCACHE)")
	_ = cmd.Flags().MarkHidden("direct-io")
	// When on, every volume-block and EC-shard read is fed to the
	// read-amplification simulator at three cache sizes (16/64/256 MB
	// equivalent) per path. Hit/miss counters appear at /metrics under
	// grainfs_readamp_*. Off by default — production pays only an atomic.Bool
	// load per read when this is unset.
	cmd.Flags().Bool("measure-read-amp", false, "enable read-amplification simulator (informs Unified Buffer Cache decision)")
	// In-memory block cache for volume.ReadAt. Default 64 MB matches the
	// simulator's measured "knee" — workloads with temporal locality saturate
	// around that budget. Set 0 to disable.
	cmd.Flags().Int64("block-cache-size", 64*1024*1024, "volume block cache capacity in bytes (0 disables)")
	// EC shard cache sits in front of getObjectEC's per-shard fan-out. Default
	// 1 GiB keeps repeated multipart range reads resident in 4-node cluster
	// runs without the RSS jump seen at 2 GiB. Set 0 to disable when running
	// --measure-read-amp baselines.
	cmd.Flags().Int64("shard-cache-size", 1024*1024*1024, "EC shard cache capacity in bytes (0 disables)")
	// HealReceipt API + gossip.
	cmd.Flags().Bool("heal-receipt-enabled", true, "enable HealReceipt audit API")
	cmd.Flags().String("heal-receipt-psk", "", "PSK for HealReceipt HMAC-SHA256 signing (defaults to --cluster-key in cluster mode)")
	cmd.Flags().Duration("heal-receipt-retention", 30*24*time.Hour, "HealReceipt retention window (older entries are GC'd)")
	cmd.Flags().Duration("heal-receipt-gossip-interval", 5*time.Second, "how often this node gossips its recent receipt IDs to peers")
	cmd.Flags().Int("heal-receipt-window", 50, "rolling window size — how many recent receipt IDs to gossip per tick")
	cmd.Flags().String("otel-endpoint", "", "OTLP HTTP endpoint for trace export (empty disables OTel, e.g. localhost:4318)")
	cmd.Flags().Float64("otel-sample-rate", 0.01, "head-based OTel trace sample rate [0.0, 1.0] (default 1%)")
	cmd.Flags().Int("pprof-port", 0, "expose net/http/pprof on this port (0 = disabled, for profiling e2e/load tests)")
	cmd.Flags().Duration("raft-heartbeat-interval", 200*time.Millisecond, "per-group raft heartbeat interval. Lower = faster failure detection, higher CPU/network. Default 200ms balances detection latency with QUIC stream-open cost.")
	cmd.Flags().Duration("raft-election-timeout", 1000*time.Millisecond, "per-group raft election timeout (must be >= 3 * heartbeat-interval). Higher = fewer spurious elections under load.")
	// Multiplexed QUIC raft RPCs are always on (idle-N8 measurement: 78pct drop
	// in CPU samples, 17x drop in recvmsg syscalls vs the legacy per-message
	// path; per-peer ALPN fallback to the legacy path is retained for older
	// binaries). These two knobs tune the always-on mux path.
	cmd.Flags().Int("quic-mux-pool", 4, "stream pool size per peer for multiplexed raft RPCs (avoids HoL with raft pipelining)")
	cmd.Flags().Duration("quic-mux-flush", 2*time.Millisecond, "heartbeat coalescing flush window for multiplexed raft RPCs (must be << raft-heartbeat-interval)")
	cmd.Flags().Bool("audit-iceberg", true, "enable audit log lake: S3 ops → Iceberg table on grainfs-audit bucket")
	cmd.Flags().Duration("audit-commit-interval", 60*time.Second, "how often the audit committer flushes the ring buffer to Iceberg")
}

func init() {
	registerAllServeFlags(serveCmd)
	rootCmd.AddCommand(serveCmd)
}

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start the S3-compatible storage server",
	Long: `Start the S3-compatible GrainFS storage server.

You can configure multi-root storage directories (Multi-Drive) to enable disk-level
resilience and Erasure Coding (EC) even on a single node. 

To specify multiple data directories, pass a comma-separated list of paths to the 
--data (-d) flag. It is highly recommended to also specify a fast SSD path via the
--meta-dir flag to keep the BadgerDB metadata separate from payload shards.

Examples:
  # Single-node mode with 3 data drives (automatically configures 2+1 EC):
  grainfs serve -d /mnt/hdd1,/mnt/hdd2,/mnt/hdd3 --meta-dir /mnt/ssd-meta

  # Default single-drive mode (1+0 Solo):
  grainfs serve -d ./data`,
	RunE: runServe,
}

func runServe(cmd *cobra.Command, args []string) error {
	opts, err := serveOptionsFromCmd(cmd)
	if err != nil {
		return err
	}
	opts.Version = version
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return serveruntime.RunFromOptions(ctx, opts)
}
