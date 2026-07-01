package main

import (
	"context"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/serveruntime"
)

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
	cmd.Flags().String("join-listen-addr", "", "Zero-CA join-listener bind address (leader serves the invite handler); empty derives an ephemeral port on the raft-addr host")
	cmd.Flags().Int("bootstrap-expect-nodes", 0, "EXPERIMENTAL: declared target node count for a fresh cluster. >1 defers genesis shard-group seeding until this many nodes have joined, then seeds all initial groups at the target size's uniform EC width (no RF=1 batch). 0/unset = today's behavior (seed immediately at solo genesis).")
	cmd.Flags().Duration("bootstrap-expect-timeout", 10*time.Minute, "EXPERIMENTAL: with --bootstrap-expect-nodes, max wait for the target node count before seeding with whatever joined (loud WARN). Never blocks forever.")
	cmd.Flags().Int64("append-size-cap-bytes", 5*1024*1024*1024*1024,
		"Per-object total size cap for AppendObject in bytes (default 5 TiB, S3 PutObject parity).")
	cmd.Flags().Int("pack-threshold", 65537, "pack objects below this size into blob files (0 = disabled, e.g. 65537)")
	cmd.Flags().Duration("scrub-interval", 24*time.Hour, "EC shard scrub interval (always on; 0 resets to default 24h)")
	cmd.Flags().Duration("scrub-orphan-age", 5*time.Minute,
		"minimum filesystem mtime age before an orphan raw segment is eligible for sweep")
	cmd.Flags().Duration("segment-gc-retention", 24*time.Hour,
		"grace period before unreferenced raw segment blobs are GC'd")
	cmd.Flags().Bool("ec-redundancy-upgrade", true,
		"relocate non-redundant (1+0) objects into a redundant EC group after the cluster grows (background sweep; default on)")
	cmd.Flags().Int("ec-redundancy-upgrade-max", 8,
		"max objects relocated per scrub cycle by the EC-redundancy-upgrade sweep")
	cmd.Flags().Duration("ec-redundancy-upgrade-min-age", 5*time.Minute,
		"minimum object age before the EC-redundancy-upgrade sweep relocates it (avoids racing in-flight writes)")
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
	// When on, every volume-block and EC-shard read is fed to the
	// read-amplification simulator at three cache sizes (16/64/256 MB
	// equivalent) per path. Hit/miss counters appear at /metrics under
	// grainfs_readamp_*. Off by default — production pays only an atomic.Bool
	// load per read when this is unset.
	cmd.Flags().Bool("measure-read-amp", false, "enable read-amplification simulator (informs Unified Buffer Cache decision)")
	// EC shard cache sits in front of getObjectEC's per-shard fan-out. Default
	// 1 GiB keeps repeated multipart range reads resident in 4-node cluster
	// runs without the RSS jump seen at 2 GiB. Set 0 to disable when running
	// --measure-read-amp baselines.
	cmd.Flags().Int64("shard-cache-size", 1024*1024*1024, "EC shard cache capacity in bytes (0 disables). Held in-heap: larger speeds warm GET but raises RSS; smaller leans on the OS page cache (warm sets that fit RAM stay fast). Tune down to trade GET-cache footprint for lower RSS.")
	// HealReceipt API + gossip.
	cmd.Flags().Bool("heal-receipt-enabled", true, "enable HealReceipt audit API")
	cmd.Flags().String("heal-receipt-psk", "", "PSK for HealReceipt HMAC-SHA256 signing (defaults to the resolved cluster transport key in cluster mode)")
	cmd.Flags().Duration("heal-receipt-retention", 30*24*time.Hour, "HealReceipt retention window (older entries are GC'd)")
	cmd.Flags().Duration("heal-receipt-gossip-interval", 5*time.Second, "how often this node gossips its recent receipt IDs to peers")
	cmd.Flags().Int("heal-receipt-window", 50, "rolling window size — how many recent receipt IDs to gossip per tick")
	cmd.Flags().String("kek-protector", "plaintext", "at-rest KEK protection: \"plaintext\" (raw 32-byte key files) or \"env\" (machine-bound wrap + recovery passphrase slot)")
	cmd.Flags().String("kek-recovery-secret-file", "", "file path holding the recovery passphrase for --kek-protector=env (GRAINFS_KEK_RECOVERY_SECRET env var takes precedence)")
	cmd.Flags().String("otel-endpoint", "", "OTLP HTTP endpoint for trace export (empty disables OTel, e.g. localhost:4318)")
	cmd.Flags().Float64("otel-sample-rate", 0.01, "head-based OTel trace sample rate [0.0, 1.0] (default 1%)")
	cmd.Flags().Int("pprof-port", 0, "expose net/http/pprof on this port (0 = disabled, for profiling e2e/load tests)")
	cmd.Flags().Duration("raft-heartbeat-interval", 200*time.Millisecond, "local data-group raft heartbeat interval. Default 200ms.")
	cmd.Flags().Duration("raft-election-timeout", 1000*time.Millisecond, "local data-group raft election timeout (must be >= 3 * heartbeat-interval).")
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
