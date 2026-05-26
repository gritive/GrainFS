package serveruntime

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/resourceguard"
)

// TestOptionsToConfigFieldParity asserts that every ServeOptions field that
// maps into Config is propagated under optionsToConfig. Sentinels per type use
// distinguishable non-zero values so an accidental field swap is caught.
//
// Known asymmetry: cfg.VlogWarnRatio and cfg.VlogCriticalRatio derive from the
// same flag origin as opts.VlogOpts.WarnRatio / .CriticalRatio, so they're
// sourced from opts.VlogOpts here.
func TestOptionsToConfigFieldParity(t *testing.T) {
	opts := ServeOptions{
		Version:     "v9.9.9-test",
		DataDir:     "/tmp/sentinel-data",
		NodeID:      "sentinel-node",
		RaftAddr:    "sentinel-raft:7000",
		ClusterKey:  "sentinel-cluster-key",
		AdminSocket: "/tmp/sentinel-admin.sock",
		AdminGroup:  "sentinel-admin-grp",
		PublicURL:   "https://sentinel.example.com",

		AppendForwardBufferTotalBytes:    int64(1 << 24),
		AppendForwardBufferMaxPerRequest: int64(1 << 20),
		AppendSizeCapBytes:               int64(1 << 26),
		QUICMuxPoolSize:                  17,
		QUICMuxFlushWindow:               3 * time.Millisecond,

		PackThreshold:      31,
		ShardPackThreshold: 41,
		DirectIO:           true,
		MeasureReadAmp:     true,
		BlockCacheSize:     int64(1 << 28),
		ShardCacheSize:     int64(1 << 27),

		NFS4Port: 12049,
		NBDPort:  10809,
		P9Bind:   "127.0.0.99",
		P9Port:   1564,

		ScrubInterval:            7 * time.Second,
		ScrubOrphanAge:           11 * time.Minute,
		ReshardInterval:          13 * time.Second,
		DataGroupRefreshInterval: 19 * time.Second,
		DegradedInterval:         23 * time.Second,
		LifecycleInterval:        29 * time.Second,
		RaftLogGCInterval:        31 * time.Second,
		RaftHeartbeatInterval:    37 * time.Millisecond,
		RaftElectionTimeout:      41 * time.Millisecond,

		HealReceiptEnabled:        true,
		HealReceiptPSK:            "sentinel-psk",
		HealReceiptRetention:      53 * time.Minute,
		HealReceiptGossipInterval: 59 * time.Second,
		HealReceiptWindow:         61,

		AuditIceberg:        true,
		AuditCommitInterval: 67 * time.Second,

		FDWatchEnabled: true,
		FDOpts: resourceguard.FDOptions{
			PollInterval:      71 * time.Second,
			WarnRatio:         0.71,
			CriticalRatio:     0.79,
			ETAWindow:         83 * time.Second,
			RecoveryWindow:    89 * time.Second,
			ClassificationCap: 97,
		},
		GoroutineWatchEnabled: true,
		GoroutineOpts: resourceguard.GoroutineOptions{
			PollInterval:   101 * time.Second,
			WarnCount:      103,
			CriticalCount:  107,
			ETAWindow:      109 * time.Second,
			RecoveryWindow: 113 * time.Second,
		},
		VlogWatchEnabled: true,
		VlogOpts: resourceguard.VlogOptions{
			DataDir:         "/tmp/sentinel-data",
			PollInterval:    127 * time.Second,
			WarnRatio:       0.42,
			CriticalRatio:   0.77,
			ETAWindow:       131 * time.Second,
			RecoveryWindow:  137 * time.Second,
			GCInterval:      139 * time.Second,
			GCDisable:       true,
			GCFailThreshold: 7,
			StrictRegistry:  true,
			SmokeDefer:      149 * time.Millisecond,
		},

		FlagsSnapshot: map[string]string{"sentinel": "yes"},
	}

	const addr = ":19000"
	cfg := optionsToConfig(opts, addr, nil, nil, nil, nil)

	// Pre-built / passthrough.
	require.Equal(t, opts.Version, cfg.Version)
	require.Equal(t, addr, cfg.Addr)
	require.Equal(t, opts.DataDir, cfg.DataDir)
	require.Equal(t, opts.NodeID, cfg.NodeID)
	require.Equal(t, opts.RaftAddr, cfg.RaftAddr)
	require.True(t, cfg.RaftAddrExplicit, "RaftAddr non-empty implies explicit=true")
	require.Equal(t, opts.ClusterKey, cfg.ClusterKey)
	require.Nil(t, cfg.AuthOpts)
	require.Nil(t, cfg.Encryptor)
	require.Nil(t, cfg.IAMStore)
	require.Nil(t, cfg.IAMApplier)

	// Raft / QUIC.
	require.Equal(t, opts.RaftLogGCInterval, cfg.RaftLogGCInterval)
	require.Equal(t, opts.RaftHeartbeatInterval, cfg.RaftHeartbeatInterval)
	require.Equal(t, opts.RaftElectionTimeout, cfg.RaftElectionTimeout)
	require.True(t, cfg.QUICMuxEnabled, "QUICMuxEnabled is always true")
	require.Equal(t, opts.QUICMuxPoolSize, cfg.QUICMuxPoolSize)
	require.Equal(t, opts.QUICMuxFlushWindow, cfg.QUICMuxFlushWindow)

	// Append forward buffer + size cap.
	require.Equal(t, opts.AppendForwardBufferTotalBytes, cfg.AppendForwardBufferTotalBytes)
	require.Equal(t, opts.AppendForwardBufferMaxPerRequest, cfg.AppendForwardBufferMaxPerRequest)
	require.Equal(t, opts.AppendSizeCapBytes, cfg.AppendSizeCapBytes)

	// Storage.
	require.Equal(t, opts.DirectIO, cfg.DirectIO)
	require.Equal(t, opts.MeasureReadAmp, cfg.MeasureReadAmp)
	require.Equal(t, opts.ShardCacheSize, cfg.ShardCacheSize)
	require.Equal(t, opts.PackThreshold, cfg.PackThreshold)
	require.Equal(t, opts.ShardPackThreshold, cfg.ShardPackThreshold)

	// Heal receipts.
	require.Equal(t, opts.HealReceiptEnabled, cfg.HealReceiptEnabled)
	require.Equal(t, opts.HealReceiptPSK, cfg.HealReceiptPSK)
	require.Equal(t, opts.HealReceiptRetention, cfg.HealReceiptRetention)
	require.Equal(t, opts.HealReceiptGossipInterval, cfg.HealReceiptGossipInterval)
	require.Equal(t, opts.HealReceiptWindow, cfg.HealReceiptWindow)

	// Lifecycle / cache.
	require.Equal(t, opts.LifecycleInterval, cfg.LifecycleInterval)
	require.Equal(t, opts.BlockCacheSize, cfg.BlockCacheSize)

	// Dashboard + vlog ratios (sourced from VlogOpts — single flag origin).
	require.Equal(t, opts.PublicURL, cfg.PublicURL)
	require.Equal(t, opts.VlogOpts.WarnRatio, cfg.VlogWarnRatio)
	require.Equal(t, opts.VlogOpts.CriticalRatio, cfg.VlogCriticalRatio)

	// Admin socket.
	require.Equal(t, opts.AdminSocket, cfg.AdminSocket)
	require.Equal(t, opts.AdminGroup, cfg.AdminGroup)

	// Scrub / reshard / degraded.
	require.Equal(t, opts.ScrubInterval, cfg.ScrubInterval)
	require.Equal(t, opts.ScrubOrphanAge, cfg.ScrubOrphanAge)
	require.Equal(t, opts.ReshardInterval, cfg.ReshardInterval)
	require.Equal(t, opts.DataGroupRefreshInterval, cfg.DataGroupRefreshInterval)
	require.Equal(t, opts.DegradedInterval, cfg.DegradedInterval)

	// Audit.
	require.Equal(t, opts.AuditIceberg, cfg.AuditIceberg)
	require.Equal(t, opts.AuditCommitInterval, cfg.AuditCommitInterval)

	// Node services.
	require.Equal(t, opts.NFS4Port, cfg.NFS4Port)
	require.Equal(t, opts.NBDPort, cfg.NBDPort)
	require.Equal(t, opts.P9Bind, cfg.P9Bind)
	require.Equal(t, opts.P9Port, cfg.P9Port)

	// Resource guards.
	require.Equal(t, opts.FDWatchEnabled, cfg.FDWatchEnabled)
	require.Equal(t, opts.FDOpts, cfg.FDOpts)
	require.Equal(t, opts.GoroutineWatchEnabled, cfg.GoroutineWatchEnabled)
	require.Equal(t, opts.GoroutineOpts, cfg.GoroutineOpts)
	require.Equal(t, opts.VlogWatchEnabled, cfg.VlogWatchEnabled)
	require.Equal(t, opts.VlogOpts, cfg.VlogResourceGuardOpts)

	// Startup snapshot map.
	require.Equal(t, opts.FlagsSnapshot, cfg.FlagsSnapshot)
}

// TestOptionsToConfigRaftAddrExplicitFalse covers the empty-RaftAddr branch.
func TestOptionsToConfigRaftAddrExplicitFalse(t *testing.T) {
	opts := ServeOptions{RaftAddr: ""}
	cfg := optionsToConfig(opts, ":0", nil, nil, nil, nil)
	require.False(t, cfg.RaftAddrExplicit, "empty RaftAddr implies explicit=false")
	require.Equal(t, "", cfg.RaftAddr)
}
