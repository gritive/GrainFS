package serveruntime

import (
	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/server"
)

// optionsToConfig is the cobra-free mapping from ServeOptions to Config.
// Field-for-field identical: every Config field is set from the matching
// ServeOptions field.
//
// Pre-resolved arguments (addr, authOpts, iamStore, iamApplier)
// sit upstream of cobra and are passed through unchanged.
func optionsToConfig(
	opts ServeOptions,
	addr string,
	authOpts []server.Option,
	iamStore *iam.Store,
	iamApplier *iam.Applier,
) Config {
	cfg := Config{
		Version:          opts.Version,
		Addr:             addr,
		DataDir:          opts.DataDir,
		DataDirs:         opts.DataDirs,
		MetaDir:          opts.MetaDir,
		NodeID:           opts.NodeID,
		RaftAddr:         opts.RaftAddr,
		RaftAddrExplicit: opts.RaftAddr != "",
		JoinListenAddr:   opts.JoinListenAddr,
		ClusterKey:       opts.ClusterKey,
		AuthOpts:         authOpts,

		BootstrapExpectNodes:   opts.BootstrapExpectNodes,
		BootstrapExpectTimeout: opts.BootstrapExpectTimeout,
		IAMStore:               iamStore,
		IAMApplier:             iamApplier,
	}

	if len(cfg.DataDirs) > 0 {
		cfg.DataDir = cfg.DataDirs[0]
	}

	cfg.RaftLogGCInterval = opts.RaftLogGCInterval
	cfg.RaftHeartbeatInterval = opts.RaftHeartbeatInterval
	cfg.RaftElectionTimeout = opts.RaftElectionTimeout

	cfg.AppendForwardBufferTotalBytes = opts.AppendForwardBufferTotalBytes
	cfg.AppendForwardBufferMaxPerRequest = opts.AppendForwardBufferMaxPerRequest
	cfg.AppendSizeCapBytes = opts.AppendSizeCapBytes

	cfg.MeasureReadAmp = opts.MeasureReadAmp
	cfg.ShardCacheSize = opts.ShardCacheSize
	cfg.PackThreshold = opts.PackThreshold
	cfg.ShardPackThreshold = opts.ShardPackThreshold

	cfg.HealReceiptEnabled = opts.HealReceiptEnabled
	cfg.HealReceiptPSK = opts.HealReceiptPSK
	cfg.HealReceiptRetention = opts.HealReceiptRetention
	cfg.HealReceiptGossipInterval = opts.HealReceiptGossipInterval
	cfg.HealReceiptWindow = opts.HealReceiptWindow

	cfg.LifecycleInterval = opts.LifecycleInterval

	cfg.PublicURL = opts.PublicURL
	cfg.VlogWarnRatio = opts.VlogOpts.WarnRatio
	cfg.VlogCriticalRatio = opts.VlogOpts.CriticalRatio

	cfg.AdminSocket = opts.AdminSocket
	cfg.AdminGroup = opts.AdminGroup

	cfg.ScrubInterval = opts.ScrubInterval
	cfg.ScrubOrphanAge = opts.ScrubOrphanAge
	cfg.SegmentGCRetention = opts.SegmentGCRetention
	cfg.ECRedundancyUpgrade = opts.ECRedundancyUpgrade
	cfg.ECRedundancyUpgradeMax = opts.ECRedundancyUpgradeMax
	cfg.ECRedundancyUpgradeMinAge = opts.ECRedundancyUpgradeMinAge
	cfg.DegradedInterval = opts.DegradedInterval

	cfg.AuditIceberg = opts.AuditIceberg
	cfg.AuditCommitInterval = opts.AuditCommitInterval

	cfg.KEKProtector = opts.KEKProtector
	cfg.KEKRecoverySecretFile = opts.KEKRecoverySecretFile

	cfg.EnableIceberg = opts.EnableIceberg

	cfg.FDWatchEnabled = opts.FDWatchEnabled
	cfg.FDOpts = opts.FDOpts
	cfg.GoroutineWatchEnabled = opts.GoroutineWatchEnabled
	cfg.GoroutineOpts = opts.GoroutineOpts
	cfg.VlogWatchEnabled = opts.VlogWatchEnabled
	cfg.VlogResourceGuardOpts = opts.VlogOpts

	cfg.FlagsSnapshot = opts.FlagsSnapshot

	ValidateRequiredIntervals(&cfg)
	return cfg
}
