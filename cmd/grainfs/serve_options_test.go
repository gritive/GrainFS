package main

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

// TestServeOptionsFromCmdReadsAllFlags asserts that registerAllServeFlags +
// serveOptionsFromCmd form a complete cobra→ServeOptions transport. We register
// every flag on a fresh cobra.Command, ParseFlags with non-default sentinel
// values, then check the resulting ServeOptions carries each value.
//
// Version / Stdout / Stderr are intentionally NOT covered — those are not
// flag-sourced (Version is set inside runServe from the cmd global, Stdout
// and Stderr default inside RunFromOptions).
func TestServeOptionsFromCmdReadsAllFlags(t *testing.T) {
	cmd := &cobra.Command{Use: "serve"}
	registerAllServeFlags(cmd)

	args := []string{
		"--data", "/tmp/sentinel-data",
		"--port", "9001",
		"--admin-socket", "/tmp/sentinel-admin.sock",
		"--admin-group", "sentinel-grp",
		"--public-url", "https://sentinel.example.com",
		"--node-id", "sentinel-node",
		"--raft-addr", "sentinel-raft:7000",
		"--cluster-key", "sentinel-cluster-key",
		"--cluster-append-forward-buffer-total-bytes", "16777216",
		"--cluster-append-forward-buffer-max-per-request", "1048576",
		"--append-size-cap-bytes", "67108864",
		"--encryption-key-file", "/tmp/sentinel.key",
		"--nfs4-port", "12049",
		"--nfs-write-buffer-dir", "/tmp/nfs-writebuf",
		"--nfs-write-buffer-idle", "15s",
		"--nbd-port", "20809",
		"--9p-bind", "127.0.0.99",
		"--9p-port", "1564",
		"--pack-threshold", "31",
		"--shard-pack-threshold", "41",
		"--scrub-interval", "7s",
		"--scrub-orphan-age", "11m",
		"--reshard-interval", "13s",
		"--datagroup-refresh-interval", "19s",
		"--lifecycle-interval", "29s",
		"--degraded-check-interval", "23s",
		"--raft-log-gc-interval", "31s",
		"--fd-watch-enabled=false",
		"--fd-watch-interval", "71s",
		"--fd-warn-threshold", "0.71",
		"--fd-critical-threshold", "0.79",
		"--fd-eta-window", "83s",
		"--fd-recovery-window", "89s",
		"--fd-classification-cap", "97",
		"--goroutine-watch-enabled=false",
		"--goroutine-warn", "103",
		"--goroutine-critical", "107",
		"--goroutine-poll-interval", "101s",
		"--goroutine-eta-window", "109s",
		"--goroutine-recovery-window", "113s",
		"--vlog-watch-enabled=false",
		"--vlog-warn-ratio", "0.42",
		"--vlog-critical-ratio", "0.77",
		"--vlog-poll-interval", "127s",
		"--vlog-eta-window", "131s",
		"--vlog-recovery-window", "137s",
		"--badger-gc-interval", "139s",
		"--badger-gc-disable=true",
		"--badger-gc-fail-threshold", "7",
		"--strict-vlog-registry=true",
		"--vlog-smoke-defer", "149ms",
		"--badger-value-threshold", "12345",
		"--direct-io=false",
		"--measure-read-amp=true",
		"--block-cache-size", "268435456",
		"--shard-cache-size", "134217728",
		"--heal-receipt-enabled=false",
		"--heal-receipt-psk", "sentinel-psk",
		"--heal-receipt-retention", "53m",
		"--heal-receipt-gossip-interval", "59s",
		"--heal-receipt-window", "61",
		"--otel-endpoint", "localhost:4318",
		"--otel-sample-rate", "0.42",
		"--pprof-port", "6060",
		"--raft-heartbeat-interval", "37ms",
		"--raft-election-timeout", "41ms",
		"--quic-mux-pool", "17",
		"--quic-mux-flush", "3ms",
		"--audit-iceberg=false",
		"--audit-commit-interval", "67s",
	}
	require.NoError(t, cmd.ParseFlags(args))

	opts, err := serveOptionsFromCmd(cmd)
	require.NoError(t, err)

	// Listen + addressing.
	require.Equal(t, "/tmp/sentinel-data", opts.DataDir)
	require.Equal(t, 9001, opts.Port)
	require.Equal(t, "/tmp/sentinel-admin.sock", opts.AdminSocket)
	require.Equal(t, "sentinel-grp", opts.AdminGroup)
	require.Equal(t, "https://sentinel.example.com", opts.PublicURL)

	// Cluster identity.
	require.Equal(t, "sentinel-node", opts.NodeID)
	require.Equal(t, "sentinel-raft:7000", opts.RaftAddr)
	require.Equal(t, "sentinel-cluster-key", opts.ClusterKey)
	require.Equal(t, "/tmp/sentinel.key", opts.EncryptionKeyFile)

	// Cluster transport tuning.
	require.Equal(t, int64(16777216), opts.AppendForwardBufferTotalBytes)
	require.Equal(t, int64(1048576), opts.AppendForwardBufferMaxPerRequest)
	require.Equal(t, int64(67108864), opts.AppendSizeCapBytes)
	require.Equal(t, 17, opts.QUICMuxPoolSize)
	require.Equal(t, 3*1000*1000, int(opts.QUICMuxFlushWindow.Nanoseconds()))

	// Storage knobs.
	require.Equal(t, 31, opts.PackThreshold)
	require.Equal(t, 41, opts.ShardPackThreshold)
	require.False(t, opts.DirectIO)
	require.True(t, opts.MeasureReadAmp)
	require.Equal(t, int64(268435456), opts.BlockCacheSize)
	require.Equal(t, int64(134217728), opts.ShardCacheSize)

	// Protocols.
	require.Equal(t, 12049, opts.NFS4Port)
	require.Equal(t, "/tmp/nfs-writebuf", opts.NFSWriteBufferDir)
	require.Equal(t, "15s", opts.NFSWriteBufferIdle.String())
	require.Equal(t, 20809, opts.NBDPort)
	require.Equal(t, "127.0.0.99", opts.P9Bind)
	require.Equal(t, 1564, opts.P9Port)

	// Intervals.
	require.Equal(t, "7s", opts.ScrubInterval.String())
	require.Equal(t, "11m0s", opts.ScrubOrphanAge.String())
	require.Equal(t, "13s", opts.ReshardInterval.String())
	require.Equal(t, "19s", opts.DataGroupRefreshInterval.String())
	require.Equal(t, "23s", opts.DegradedInterval.String())
	require.Equal(t, "29s", opts.LifecycleInterval.String())
	require.Equal(t, "31s", opts.RaftLogGCInterval.String())
	require.Equal(t, "37ms", opts.RaftHeartbeatInterval.String())
	require.Equal(t, "41ms", opts.RaftElectionTimeout.String())

	// Heal receipts.
	require.False(t, opts.HealReceiptEnabled)
	require.Equal(t, "sentinel-psk", opts.HealReceiptPSK)
	require.Equal(t, "53m0s", opts.HealReceiptRetention.String())
	require.Equal(t, "59s", opts.HealReceiptGossipInterval.String())
	require.Equal(t, 61, opts.HealReceiptWindow)

	// Audit.
	require.False(t, opts.AuditIceberg)
	require.Equal(t, "1m7s", opts.AuditCommitInterval.String())

	// Observability.
	require.Equal(t, "localhost:4318", opts.OTelEndpoint)
	require.Equal(t, 0.42, opts.OTelSampleRate)
	require.Equal(t, 6060, opts.PprofPort)

	// Resource guards (one inner field per group — helpers are tested elsewhere).
	require.False(t, opts.FDWatchEnabled)
	require.Equal(t, 0.71, opts.FDOpts.WarnRatio)
	require.Equal(t, 0.79, opts.FDOpts.CriticalRatio)
	require.Equal(t, 97, opts.FDOpts.ClassificationCap)
	require.False(t, opts.GoroutineWatchEnabled)
	require.Equal(t, 103, opts.GoroutineOpts.WarnCount)
	require.Equal(t, 107, opts.GoroutineOpts.CriticalCount)
	require.False(t, opts.VlogWatchEnabled)
	require.Equal(t, 0.42, opts.VlogOpts.WarnRatio)
	require.Equal(t, 0.77, opts.VlogOpts.CriticalRatio)
	require.True(t, opts.VlogOpts.GCDisable)
	require.Equal(t, int32(7), opts.VlogOpts.GCFailThreshold)
	require.Equal(t, "/tmp/sentinel-data", opts.VlogOpts.DataDir, "VlogOpts.DataDir threads through DataDir")

	// Misc / hidden / deprecated.
	require.Equal(t, int64(12345), opts.BadgerValueThreshold)
	require.True(t, opts.StrictVlogRegistry)
	require.Equal(t, "149ms", opts.VlogSmokeDefer.String())

	// Flags snapshot — non-empty + a few keys.
	require.NotEmpty(t, opts.FlagsSnapshot)
	require.Equal(t, "9001", opts.FlagsSnapshot["port"])
	require.Equal(t, "/tmp/sentinel-data", opts.FlagsSnapshot["data"])
	require.Equal(t, "<redacted>", opts.FlagsSnapshot["cluster-key"], "secret redaction")
	require.Equal(t, "<redacted>", opts.FlagsSnapshot["heal-receipt-psk"], "secret redaction")
}
