package main

import (
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/gritive/GrainFS/internal/serveruntime"
)

// serveOptionsFromCmd reads every cobra flag registered by
// registerAllServeFlags and assembles a flat serveruntime.ServeOptions.
// Version, Stdout, and Stderr are intentionally not populated here —
// the caller (runServe) sets Version from the cmd-level `version` global,
// and RunFromOptions fills Stdout/Stderr defaults.
func serveOptionsFromCmd(cmd *cobra.Command) (serveruntime.ServeOptions, error) {
	opts := serveruntime.ServeOptions{}

	// Listen + addressing.
	rawDirs, _ := cmd.Flags().GetString("data")
	opts.DataDir = rawDirs
	if rawDirs != "" {
		for _, part := range strings.Split(rawDirs, ",") {
			trimmed := strings.TrimSpace(part)
			if trimmed != "" {
				opts.DataDirs = append(opts.DataDirs, trimmed)
			}
		}
	}
	if len(opts.DataDirs) == 0 {
		opts.DataDirs = []string{"./data"}
	}
	opts.MetaDir, _ = cmd.Flags().GetString("meta-dir")

	opts.Port, _ = cmd.Flags().GetInt("port")
	opts.AdminSocket, _ = cmd.Flags().GetString("admin-socket")
	opts.AdminGroup, _ = cmd.Flags().GetString("admin-group")
	opts.PublicURL, _ = cmd.Flags().GetString("public-url")

	// Cluster identity.
	opts.NodeID, _ = cmd.Flags().GetString("node-id")
	opts.RaftAddr, _ = cmd.Flags().GetString("raft-addr")
	opts.ClusterKey, _ = cmd.Flags().GetString("cluster-key")
	opts.EncryptionKeyFile, _ = cmd.Flags().GetString("encryption-key-file")

	// Cluster transport tuning.
	opts.AppendForwardBufferTotalBytes, _ = cmd.Flags().GetInt64("cluster-append-forward-buffer-total-bytes")
	opts.AppendForwardBufferMaxPerRequest, _ = cmd.Flags().GetInt64("cluster-append-forward-buffer-max-per-request")
	opts.AppendSizeCapBytes, _ = cmd.Flags().GetInt64("append-size-cap-bytes")
	opts.QUICMuxPoolSize, _ = cmd.Flags().GetInt("quic-mux-pool")
	opts.QUICMuxFlushWindow, _ = cmd.Flags().GetDuration("quic-mux-flush")

	// Storage knobs.
	opts.PackThreshold, _ = cmd.Flags().GetInt("pack-threshold")
	opts.ShardPackThreshold, _ = cmd.Flags().GetInt("shard-pack-threshold")
	opts.DirectIO, _ = cmd.Flags().GetBool("direct-io")
	opts.MeasureReadAmp, _ = cmd.Flags().GetBool("measure-read-amp")
	opts.BlockCacheSize, _ = cmd.Flags().GetInt64("block-cache-size")
	opts.ShardCacheSize, _ = cmd.Flags().GetInt64("shard-cache-size")

	// Protocols.
	opts.NFS4Port, _ = cmd.Flags().GetInt("nfs4-port")
	opts.NFSWriteBufferDir, _ = cmd.Flags().GetString("nfs-write-buffer-dir")
	opts.NFSWriteBufferIdle, _ = cmd.Flags().GetDuration("nfs-write-buffer-idle")
	opts.NBDPort, _ = cmd.Flags().GetInt("nbd-port")
	opts.P9Bind, _ = cmd.Flags().GetString("9p-bind")
	opts.P9Port, _ = cmd.Flags().GetInt("9p-port")

	// Intervals.
	opts.ScrubInterval, _ = cmd.Flags().GetDuration("scrub-interval")
	opts.ScrubOrphanAge, _ = cmd.Flags().GetDuration("scrub-orphan-age")
	opts.ReshardInterval, _ = cmd.Flags().GetDuration("reshard-interval")
	opts.DataGroupRefreshInterval, _ = cmd.Flags().GetDuration("datagroup-refresh-interval")
	opts.DegradedInterval, _ = cmd.Flags().GetDuration("degraded-check-interval")
	opts.LifecycleInterval, _ = cmd.Flags().GetDuration("lifecycle-interval")
	opts.RaftLogGCInterval, _ = cmd.Flags().GetDuration("raft-log-gc-interval")
	opts.RaftHeartbeatInterval, _ = cmd.Flags().GetDuration("raft-heartbeat-interval")
	opts.RaftElectionTimeout, _ = cmd.Flags().GetDuration("raft-election-timeout")

	// Heal receipts.
	opts.HealReceiptEnabled, _ = cmd.Flags().GetBool("heal-receipt-enabled")
	opts.HealReceiptPSK, _ = cmd.Flags().GetString("heal-receipt-psk")
	opts.HealReceiptRetention, _ = cmd.Flags().GetDuration("heal-receipt-retention")
	opts.HealReceiptGossipInterval, _ = cmd.Flags().GetDuration("heal-receipt-gossip-interval")
	opts.HealReceiptWindow, _ = cmd.Flags().GetInt("heal-receipt-window")

	// Audit.
	opts.AuditIceberg, _ = cmd.Flags().GetBool("audit-iceberg")
	opts.AuditCommitInterval, _ = cmd.Flags().GetDuration("audit-commit-interval")

	// Observability.
	opts.OTelEndpoint, _ = cmd.Flags().GetString("otel-endpoint")
	opts.OTelSampleRate, _ = cmd.Flags().GetFloat64("otel-sample-rate")
	opts.PprofPort, _ = cmd.Flags().GetInt("pprof-port")

	// Misc / hidden / deprecated.
	opts.DedupEnabled, _ = cmd.Flags().GetBool("dedup")
	opts.BadgerValueThreshold, _ = cmd.Flags().GetInt64("badger-value-threshold")
	opts.StrictVlogRegistry, _ = cmd.Flags().GetBool("strict-vlog-registry")
	opts.VlogSmokeDefer, _ = cmd.Flags().GetDuration("vlog-smoke-defer")

	// Resource guards — delegate to existing helpers (cobra-bound).
	opts.FDWatchEnabled = fdWatchEnabled(cmd)
	opts.FDOpts = fdOptionsFromCmd(cmd)
	opts.GoroutineWatchEnabled = goroutineWatchEnabled(cmd)
	opts.GoroutineOpts = goroutineOptionsFromCmd(cmd)
	opts.VlogWatchEnabled = vlogWatchEnabled(cmd)
	opts.VlogOpts = vlogOptionsFromCmd(cmd, opts.DataDir)

	// Startup flag snapshot for structured logs.
	opts.FlagsSnapshot = collectFlagsSnapshot(cmd)

	return opts, nil
}

// collectFlagsSnapshot walks every cobra flag once and produces the
// map[string]string consumed by serveruntime.LogStartupConfigSnapshot.
// Secret-bearing flags (cluster-key, alert-webhook-secret, heal-receipt-psk)
// are redacted at the source so neither the structured log nor the on-disk
// snapshot ever sees the raw value.
//
// Lives in cmd because it walks *pflag.Flag — cobra-bound.
func collectFlagsSnapshot(cmd *cobra.Command) map[string]string {
	snap := make(map[string]string, 64)
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		switch f.Name {
		case "cluster-key", "alert-webhook-secret", "heal-receipt-psk":
			if f.Value.String() != "" {
				snap[f.Name] = "<redacted>"
			}
			return
		}
		snap[f.Name] = f.Value.String()
	})
	return snap
}
