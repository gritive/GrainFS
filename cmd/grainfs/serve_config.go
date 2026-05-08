package main

import (
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

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

	peersStr, _ := cmd.Flags().GetString("peers")
	cfg.Peers = serveruntime.FilterEmpty(strings.Split(peersStr, ","))
	cfg.JoinAddr, _ = cmd.Flags().GetString("join")
	cfg.JoinMode = cfg.JoinAddr != ""

	cfg.BadgerManagedMode, _ = cmd.Flags().GetBool("badger-managed-mode")
	cfg.RaftLogGCInterval, _ = cmd.Flags().GetDuration("raft-log-gc-interval")
	cfg.RaftHeartbeatInterval, _ = cmd.Flags().GetDuration("raft-heartbeat-interval")
	cfg.RaftElectionTimeout, _ = cmd.Flags().GetDuration("raft-election-timeout")
	cfg.QUICMuxEnabled, _ = cmd.Flags().GetBool("quic-mux")
	cfg.QUICMuxPoolSize, _ = cmd.Flags().GetInt("quic-mux-pool")
	cfg.QUICMuxFlushWindow, _ = cmd.Flags().GetDuration("quic-mux-flush")
	cfg.SharedBadgerEnabled, _ = cmd.Flags().GetBool("shared-badger")

	cfg.DirectIO, _ = cmd.Flags().GetBool("direct-io")
	cfg.MeasureReadAmp, _ = cmd.Flags().GetBool("measure-read-amp")
	cfg.ShardCacheSize, _ = cmd.Flags().GetInt64("shard-cache-size")
	cfg.PackThreshold, _ = cmd.Flags().GetInt("pack-threshold")

	cfg.BalancerEnabled, _ = cmd.Flags().GetBool("balancer-enabled")
	cfg.BalancerGossipInterval, _ = cmd.Flags().GetDuration("balancer-gossip-interval")
	cfg.BalancerImbalanceTriggerPct, _ = cmd.Flags().GetFloat64("balancer-imbalance-trigger-pct")
	cfg.BalancerImbalanceStopPct, _ = cmd.Flags().GetFloat64("balancer-imbalance-stop-pct")
	cfg.BalancerMigrationRate, _ = cmd.Flags().GetInt("balancer-migration-rate")
	cfg.BalancerLeaderTenureMin, _ = cmd.Flags().GetDuration("balancer-leader-tenure-min")
	cfg.BalancerWarmupTimeout, _ = cmd.Flags().GetDuration("balancer-warmup-timeout")
	cfg.BalancerCBThreshold, _ = cmd.Flags().GetFloat64("balancer-cb-threshold")
	cfg.BalancerMigrationMaxRetries, _ = cmd.Flags().GetInt("balancer-migration-max-retries")
	cfg.BalancerMigrationPendingTTL, _ = cmd.Flags().GetDuration("balancer-migration-pending-ttl")

	cfg.HealReceiptEnabled, _ = cmd.Flags().GetBool("heal-receipt-enabled")
	cfg.HealReceiptPSK, _ = cmd.Flags().GetString("heal-receipt-psk")
	cfg.HealReceiptRetention, _ = cmd.Flags().GetDuration("heal-receipt-retention")
	cfg.HealReceiptGossipInterval, _ = cmd.Flags().GetDuration("heal-receipt-gossip-interval")
	cfg.HealReceiptWindow, _ = cmd.Flags().GetInt("heal-receipt-window")

	cfg.UpstreamEndpoint, _ = cmd.Flags().GetString("upstream")
	cfg.UpstreamAccessKey, _ = cmd.Flags().GetString("upstream-access-key")
	cfg.UpstreamSecretKey, _ = cmd.Flags().GetString("upstream-secret-key")

	cfg.SnapInterval, _ = cmd.Flags().GetDuration("snapshot-interval")
	cfg.SnapRetain, _ = cmd.Flags().GetInt("snapshot-retain")

	cfg.AlertWebhook, _ = cmd.Flags().GetString("alert-webhook")
	cfg.AlertSecret, _ = cmd.Flags().GetString("alert-webhook-secret")
	cfg.DiskWarnFrac, _ = cmd.Flags().GetFloat64("disk-warn-threshold")
	cfg.DiskCritFrac, _ = cmd.Flags().GetFloat64("disk-critical-threshold")

	cfg.LifecycleInterval, _ = cmd.Flags().GetDuration("lifecycle-interval")
	cfg.DedupEnabled, _ = cmd.Flags().GetBool("dedup")
	cfg.BlockCacheSize, _ = cmd.Flags().GetInt64("block-cache-size")

	cfg.PublicURL, _ = cmd.Flags().GetString("public-url")
	cfg.VlogWarnRatio, _ = cmd.Flags().GetFloat64("vlog-warn-ratio")
	cfg.VlogCriticalRatio, _ = cmd.Flags().GetFloat64("vlog-critical-ratio")

	cfg.AdminSocket, _ = cmd.Flags().GetString("admin-socket")
	cfg.AdminGroup, _ = cmd.Flags().GetString("admin-group")

	cfg.ScrubInterval, _ = cmd.Flags().GetDuration("scrub-interval")
	cfg.ReshardInterval, _ = cmd.Flags().GetDuration("reshard-interval")
	cfg.DegradedInterval, _ = cmd.Flags().GetDuration("degraded-check-interval")

	cfg.NFS4Port, _ = cmd.Flags().GetInt("nfs4-port")
	cfg.NBDPort, _ = cmd.Flags().GetInt("nbd-port")
	cfg.NBDVolumeSize, _ = cmd.Flags().GetInt64("nbd-volume-size")

	cfg.FDWatchEnabled = fdWatchEnabled(cmd)
	cfg.FDOpts = fdOptionsFromCmd(cmd)
	cfg.GoroutineWatchEnabled = goroutineWatchEnabled(cmd)
	cfg.GoroutineOpts = goroutineOptionsFromCmd(cmd)
	cfg.VlogWatchEnabled = vlogWatchEnabled(cmd)
	cfg.VlogResourceGuardOpts = vlogOptionsFromCmd(cmd, dataDir)

	cfg.FlagsSnapshot = collectFlagsSnapshot(cmd)

	return cfg
}

// collectFlagsSnapshot walks every cobra flag once and produces the
// map[string]string consumed by serveruntime.LogStartupConfigSnapshot.
// Secret-bearing flags (secret-key, cluster-key, alert-webhook-secret,
// heal-receipt-psk) are redacted at the source so neither the structured log
// nor the on-disk snapshot ever sees the raw value.
func collectFlagsSnapshot(cmd *cobra.Command) map[string]string {
	snap := make(map[string]string, 64)
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		switch f.Name {
		case "secret-key", "cluster-key", "alert-webhook-secret", "heal-receipt-psk":
			if f.Value.String() != "" {
				snap[f.Name] = "<redacted>"
			}
			return
		}
		snap[f.Name] = f.Value.String()
	})
	return snap
}
