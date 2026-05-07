package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/resourceguard"
	"github.com/gritive/GrainFS/internal/server"
	"github.com/gritive/GrainFS/internal/serveruntime"
)

// clusterConfig is a flat snapshot of every flag and pre-resolved input that
// runCluster needs, captured once in runServe so the body itself reads no
// cobra state. PR2b will lift this struct into internal/serveruntime.Config.
type clusterConfig struct {
	// Network / identity
	Addr             string
	DataDir          string
	NodeID           string
	RaftAddr         string
	RaftAddrExplicit bool
	ClusterKey       string
	JoinAddr         string
	JoinMode         bool
	Peers            []string

	// Pre-built per Q9 of the cmd-thin grill
	AuthOpts  []server.Option
	Encryptor *encrypt.Encryptor

	// Raft tuning
	BadgerManagedMode     bool
	RaftLogGCInterval     time.Duration
	RaftHeartbeatInterval time.Duration
	RaftElectionTimeout   time.Duration
	QUICMuxEnabled        bool
	QUICMuxPoolSize       int
	QUICMuxFlushWindow    time.Duration
	SharedBadgerEnabled   bool
	SeedGroups            int

	// Storage / EC
	DirectIO       bool
	MeasureReadAmp bool
	ShardCacheSize int64
	ECData         int
	ECParity       int
	ECExplicit     bool
	VFSFixed       bool
	PackThreshold  int

	// Balancer
	BalancerEnabled             bool
	BalancerGossipInterval      time.Duration
	BalancerImbalanceTriggerPct float64
	BalancerImbalanceStopPct    float64
	BalancerMigrationRate       int
	BalancerLeaderTenureMin     time.Duration
	BalancerWarmupTimeout       time.Duration
	BalancerCBThreshold         float64
	BalancerMigrationMaxRetries int
	BalancerMigrationPendingTTL time.Duration

	// Heal receipts
	HealReceiptEnabled        bool
	HealReceiptPSK            string
	HealReceiptRetention      time.Duration
	HealReceiptGossipInterval time.Duration
	HealReceiptWindow         int

	// Backend chain
	UpstreamEndpoint  string
	UpstreamAccessKey string
	UpstreamSecretKey string

	// Snapshots (object PITR)
	SnapInterval time.Duration
	SnapRetain   int

	// Alerts
	AlertWebhook string
	AlertSecret  string
	DiskWarnFrac float64
	DiskCritFrac float64

	// Lifecycle / dedup / cache
	LifecycleInterval time.Duration
	DedupEnabled      bool
	BlockCacheSize    int64

	// Dashboard / vlog
	PublicURL         string
	VlogWarnRatio     float64
	VlogCriticalRatio float64

	// Admin socket
	AdminSocket string
	AdminGroup  string

	// Scrub / reshard / degraded
	ScrubInterval    time.Duration
	ReshardInterval  time.Duration
	DegradedInterval time.Duration

	// Node services
	NFS4Port      int
	NBDPort       int
	NBDVolumeSize int64

	// Resource guards (pre-resolved from cobra; runCluster body reads only these)
	FDWatchEnabled        bool
	FDOpts                resourceguard.FDOptions
	GoroutineWatchEnabled bool
	GoroutineOpts         resourceguard.GoroutineOptions
	VlogWatchEnabled      bool
	VlogResourceGuardOpts resourceguard.VlogOptions

	// Startup snapshot map (built once via cmd.Flags().VisitAll with secrets redacted)
	FlagsSnapshot map[string]string
}

// buildClusterConfig captures every cobra-derived input runCluster needs.
// All cmd.Flags().Get* calls live here so the runCluster body is cobra-free
// (preparation for PR2b which moves the body to internal/serveruntime.Run).
//
// Pre-resolved arguments (addr, dataDir, nodeID, raftAddr, clusterKey,
// authOpts, encryptor) are still produced by runServe — they sit upstream
// of cobra (Q9 of the cmd-thin grill).
func buildClusterConfig(
	cmd *cobra.Command,
	addr, dataDir, nodeID, raftAddr, clusterKey string,
	authOpts []server.Option,
	encryptor *encrypt.Encryptor,
) clusterConfig {
	cfg := clusterConfig{
		Addr:             addr,
		DataDir:          dataDir,
		NodeID:           nodeID,
		RaftAddr:         raftAddr,
		RaftAddrExplicit: raftAddr != "",
		ClusterKey:       clusterKey,
		AuthOpts:         authOpts,
		Encryptor:        encryptor,
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
	cfg.SeedGroups, _ = cmd.Flags().GetInt("seed-groups")

	cfg.DirectIO, _ = cmd.Flags().GetBool("direct-io")
	cfg.MeasureReadAmp, _ = cmd.Flags().GetBool("measure-read-amp")
	cfg.ShardCacheSize, _ = cmd.Flags().GetInt64("shard-cache-size")
	cfg.ECData, _ = cmd.Flags().GetInt("ec-data")
	cfg.ECParity, _ = cmd.Flags().GetInt("ec-parity")
	cfg.ECExplicit = cmd.Flags().Changed("ec-data") || cmd.Flags().Changed("ec-parity")
	cfg.VFSFixed, _ = cmd.Flags().GetBool("backend-vfs-fixed-version")
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

// collectFlagsSnapshot walks every cobra flag once and produces the same
// map[string]string logStartupConfigSnapshot used to write before PR2a.
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

// logStartupConfigSnapshotFromMap is the cobra-free counterpart of the
// original logStartupConfigSnapshot. It accepts a pre-redacted snapshot map
// (built upstream by collectFlagsSnapshot) plus the resolved-at-runtime
// fields (addr/dataDir/nodeID/raftAddr/peers) that aren't cobra-derived.
//
// PR2b will lift this verbatim into internal/serveruntime.
func logStartupConfigSnapshotFromMap(
	flagsSnap map[string]string,
	addr, dataDir, nodeID, raftAddr string,
	peers []string,
) {
	snapshot := make(map[string]any, len(flagsSnap)+5)
	snapshot["addr"] = addr
	snapshot["data_dir"] = dataDir
	snapshot["node_id"] = nodeID
	snapshot["raft_addr"] = raftAddr
	snapshot["peers"] = peers
	for k, v := range flagsSnap {
		snapshot[k] = v
	}

	log.Debug().Interface("flags", snapshot).Msg("startup config snapshot")

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

	if data, err := json.MarshalIndent(snapshot, "", "  "); err == nil {
		if err := os.WriteFile(snapPath, data, 0o600); err != nil {
			log.Debug().Err(err).Str("path", snapPath).Msg("could not persist startup config snapshot")
		}
	}
}

// diffSnapshots returns keys whose values differ between two snapshots,
// preserving the previous on-disk emit format used by the changed-flags log.
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
