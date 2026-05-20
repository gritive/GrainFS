package serveruntime

import (
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/server"
)

// optionsToConfig is the cobra-free mapping from ServeOptions to Config.
// Field-for-field identical: every Config field is set from the matching
// ServeOptions field.
//
// Pre-resolved arguments (addr, authOpts, encryptor, iamStore, iamApplier)
// sit upstream of cobra and are passed through unchanged.
func optionsToConfig(
	opts ServeOptions,
	addr string,
	authOpts []server.Option,
	encryptor *encrypt.Encryptor,
	iamStore *iam.Store,
	iamApplier *iam.Applier,
) Config {
	cfg := Config{
		Version:          opts.Version,
		Addr:             addr,
		DataDir:          opts.DataDir,
		NodeID:           opts.NodeID,
		RaftAddr:         opts.RaftAddr,
		RaftAddrExplicit: opts.RaftAddr != "",
		ClusterKey:       opts.ClusterKey,
		AuthOpts:         authOpts,
		Encryptor:        encryptor,
		IAMStore:         iamStore,
		IAMApplier:       iamApplier,
	}

	cfg.RaftLogGCInterval = opts.RaftLogGCInterval
	cfg.RaftHeartbeatInterval = opts.RaftHeartbeatInterval
	cfg.RaftElectionTimeout = opts.RaftElectionTimeout
	cfg.QUICMuxEnabled = true // mux is always on; the --quic-mux flag was removed
	cfg.QUICMuxPoolSize = opts.QUICMuxPoolSize
	cfg.QUICMuxFlushWindow = opts.QUICMuxFlushWindow

	cfg.AppendForwardBufferTotalBytes = opts.AppendForwardBufferTotalBytes
	cfg.AppendForwardBufferMaxPerRequest = opts.AppendForwardBufferMaxPerRequest
	cfg.AppendSizeCapBytes = opts.AppendSizeCapBytes

	cfg.DirectIO = opts.DirectIO
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
	cfg.DedupEnabled = opts.DedupEnabled
	cfg.BlockCacheSize = opts.BlockCacheSize

	cfg.PublicURL = opts.PublicURL
	cfg.VlogWarnRatio = opts.VlogOpts.WarnRatio
	cfg.VlogCriticalRatio = opts.VlogOpts.CriticalRatio

	cfg.AdminSocket = opts.AdminSocket
	cfg.AdminGroup = opts.AdminGroup

	cfg.ScrubInterval = opts.ScrubInterval
	cfg.ScrubOrphanAge = opts.ScrubOrphanAge
	cfg.ReshardInterval = opts.ReshardInterval
	cfg.RingReshardInterval = opts.RingReshardInterval
	cfg.DataGroupRefreshInterval = opts.DataGroupRefreshInterval
	cfg.DegradedInterval = opts.DegradedInterval

	cfg.AuditIceberg = opts.AuditIceberg
	cfg.AuditCommitInterval = opts.AuditCommitInterval

	cfg.NFS4Port = opts.NFS4Port
	cfg.NBDPort = opts.NBDPort
	cfg.P9Bind = opts.P9Bind
	cfg.P9Port = opts.P9Port

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
