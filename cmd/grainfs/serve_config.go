package main

import (
	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/server"
	"github.com/gritive/GrainFS/internal/serveruntime"
)

// buildClusterConfig captures every cobra-derived input serveruntime.Run needs.
// All cmd.Flags().Get* calls live here so the Run body is cobra-free.
//
// Pre-resolved arguments (addr, dataDir, nodeID, raftAddr, clusterKey,
// authOpts, encryptor) are still produced by runServe — they sit upstream
// of cobra (Q9 of the cmd-thin grill).
func buildClusterConfig(
	cmd *cobra.Command,
	addr, dataDir, nodeID, raftAddr, clusterKey string,
	authOpts []server.Option,
	encryptor *encrypt.Encryptor,
	iamStore *iam.Store,
	iamApplier *iam.Applier,
) serveruntime.Config {
	cfg := serveruntime.Config{
		Version:          version,
		Addr:             addr,
		DataDir:          dataDir,
		NodeID:           nodeID,
		RaftAddr:         raftAddr,
		RaftAddrExplicit: raftAddr != "",
		ClusterKey:       clusterKey,
		AuthOpts:         authOpts,
		Encryptor:        encryptor,
		IAMStore:         iamStore,
		IAMApplier:       iamApplier,
	}

	cfg.RaftLogGCInterval, _ = cmd.Flags().GetDuration("raft-log-gc-interval")
	cfg.RaftHeartbeatInterval, _ = cmd.Flags().GetDuration("raft-heartbeat-interval")
	cfg.RaftElectionTimeout, _ = cmd.Flags().GetDuration("raft-election-timeout")
	cfg.QUICMuxEnabled = true // mux is always on; the --quic-mux flag was removed
	cfg.QUICMuxPoolSize, _ = cmd.Flags().GetInt("quic-mux-pool")
	cfg.QUICMuxFlushWindow, _ = cmd.Flags().GetDuration("quic-mux-flush")

	cfg.AppendForwardBufferTotalBytes, _ = cmd.Flags().GetInt64("cluster-append-forward-buffer-total-bytes")
	cfg.AppendForwardBufferMaxPerRequest, _ = cmd.Flags().GetInt64("cluster-append-forward-buffer-max-per-request")
	cfg.AppendSizeCapBytes, _ = cmd.Flags().GetInt64("append-size-cap-bytes")

	cfg.DirectIO, _ = cmd.Flags().GetBool("direct-io")
	cfg.MeasureReadAmp, _ = cmd.Flags().GetBool("measure-read-amp")
	cfg.ShardCacheSize, _ = cmd.Flags().GetInt64("shard-cache-size")
	cfg.PackThreshold, _ = cmd.Flags().GetInt("pack-threshold")
	cfg.ShardPackThreshold, _ = cmd.Flags().GetInt("shard-pack-threshold")

	cfg.HealReceiptEnabled, _ = cmd.Flags().GetBool("heal-receipt-enabled")
	cfg.HealReceiptPSK, _ = cmd.Flags().GetString("heal-receipt-psk")
	cfg.HealReceiptRetention, _ = cmd.Flags().GetDuration("heal-receipt-retention")
	cfg.HealReceiptGossipInterval, _ = cmd.Flags().GetDuration("heal-receipt-gossip-interval")
	cfg.HealReceiptWindow, _ = cmd.Flags().GetInt("heal-receipt-window")

	cfg.LifecycleInterval, _ = cmd.Flags().GetDuration("lifecycle-interval")
	cfg.DedupEnabled, _ = cmd.Flags().GetBool("dedup")
	cfg.BlockCacheSize, _ = cmd.Flags().GetInt64("block-cache-size")

	cfg.PublicURL, _ = cmd.Flags().GetString("public-url")
	cfg.VlogWarnRatio, _ = cmd.Flags().GetFloat64("vlog-warn-ratio")
	cfg.VlogCriticalRatio, _ = cmd.Flags().GetFloat64("vlog-critical-ratio")

	cfg.AdminSocket, _ = cmd.Flags().GetString("admin-socket")
	cfg.AdminGroup, _ = cmd.Flags().GetString("admin-group")

	cfg.ScrubInterval, _ = cmd.Flags().GetDuration("scrub-interval")
	cfg.ScrubOrphanAge, _ = cmd.Flags().GetDuration("scrub-orphan-age")
	cfg.ReshardInterval, _ = cmd.Flags().GetDuration("reshard-interval")
	cfg.RingReshardInterval, _ = cmd.Flags().GetDuration("ring-reshard-interval")
	cfg.DataGroupRefreshInterval, _ = cmd.Flags().GetDuration("datagroup-refresh-interval")
	cfg.DegradedInterval, _ = cmd.Flags().GetDuration("degraded-check-interval")

	cfg.AuditIceberg, _ = cmd.Flags().GetBool("audit-iceberg")
	cfg.AuditCommitInterval, _ = cmd.Flags().GetDuration("audit-commit-interval")

	cfg.NFS4Port, _ = cmd.Flags().GetInt("nfs4-port")
	cfg.NBDPort, _ = cmd.Flags().GetInt("nbd-port")
	cfg.P9Bind, _ = cmd.Flags().GetString("9p-bind")
	cfg.P9Port, _ = cmd.Flags().GetInt("9p-port")

	cfg.FDWatchEnabled = fdWatchEnabled(cmd)
	cfg.FDOpts = fdOptionsFromCmd(cmd)
	cfg.GoroutineWatchEnabled = goroutineWatchEnabled(cmd)
	cfg.GoroutineOpts = goroutineOptionsFromCmd(cmd)
	cfg.VlogWatchEnabled = vlogWatchEnabled(cmd)
	cfg.VlogResourceGuardOpts = vlogOptionsFromCmd(cmd, dataDir)

	cfg.FlagsSnapshot = collectFlagsSnapshot(cmd)

	serveruntime.ValidateRequiredIntervals(&cfg)
	return cfg
}
