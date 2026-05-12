package cluster

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClusterConfig_Validate_OK(t *testing.T) {
	cfg := NewClusterConfig()
	cfg.applyPatch(ClusterConfigPatch{
		BalancerImbalanceTriggerPct: ptrFloat(25.0),
		BalancerImbalanceStopPct:    ptrFloat(5.0),
		DiskWarnFrac:                ptrFloat(0.7),
		DiskCriticalFrac:            ptrFloat(0.9),
		AlertWebhook:                ptrString("https://hooks.slack.com/services/X"),
	}, time.Now())
	require.NoError(t, cfg.Validate())
}

func TestClusterConfig_Validate_Invariants(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		patch ClusterConfigPatch
		want  string // substring of error message
	}{
		{
			name:  "trigger_below_zero",
			patch: ClusterConfigPatch{BalancerImbalanceTriggerPct: ptrFloat(-1)},
			want:  "balancer-imbalance-trigger-pct",
		},
		{
			name:  "trigger_above_100",
			patch: ClusterConfigPatch{BalancerImbalanceTriggerPct: ptrFloat(101)},
			want:  "balancer-imbalance-trigger-pct",
		},
		{
			name: "stop_above_trigger",
			patch: ClusterConfigPatch{
				BalancerImbalanceTriggerPct: ptrFloat(10),
				BalancerImbalanceStopPct:    ptrFloat(15),
			},
			want: "stop-pct must be <= trigger-pct",
		},
		{
			name:  "cb_threshold_zero",
			patch: ClusterConfigPatch{BalancerCBThreshold: ptrFloat(0)},
			want:  "balancer-cb-threshold",
		},
		{
			name:  "migration_rate_zero",
			patch: ClusterConfigPatch{BalancerMigrationRate: ptrInt32(0)},
			want:  "balancer-migration-rate",
		},
		{
			name: "disk_warn_above_critical",
			patch: ClusterConfigPatch{
				DiskWarnFrac:     ptrFloat(0.95),
				DiskCriticalFrac: ptrFloat(0.90),
			},
			want: "disk-warn-threshold must be <= disk-critical-threshold",
		},
		{
			name:  "alert_webhook_unsupported_scheme",
			patch: ClusterConfigPatch{AlertWebhook: ptrString("ftp://example.com/x")},
			want:  "alert-webhook",
		},
		{
			name:  "balancer_gossip_interval_negative",
			patch: ClusterConfigPatch{BalancerGossipInterval: ptrDuration(-time.Second)},
			want:  "balancer-gossip-interval",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := NewClusterConfig()
			cfg.applyPatch(tc.patch, time.Now())
			err := cfg.Validate()
			require.Error(t, err)
			require.Contains(t, strings.ToLower(err.Error()), strings.ToLower(tc.want))
		})
	}
}

func ptrDuration(v time.Duration) *time.Duration { return &v }

func TestClusterConfig_Validate_SnapshotBounds(t *testing.T) {
	c := NewClusterConfig()

	// Negative interval rejected
	negD := -1 * time.Second
	c.applyPatch(ClusterConfigPatch{SnapshotInterval: &negD}, time.UnixMilli(0))
	if err := c.Validate(); err == nil {
		t.Fatal("expected error for negative snapshot-interval")
	}

	// retain=0 rejected
	c = NewClusterConfig()
	z := int32(0)
	c.applyPatch(ClusterConfigPatch{SnapshotRetain: &z}, time.UnixMilli(0))
	if err := c.Validate(); err == nil {
		t.Fatal("expected error for snapshot-retain=0")
	}

	// interval=0 allowed (disable)
	c = NewClusterConfig()
	zd := time.Duration(0)
	c.applyPatch(ClusterConfigPatch{SnapshotInterval: &zd}, time.UnixMilli(0))
	if err := c.Validate(); err != nil {
		t.Fatalf("interval=0 must be allowed (disable): %v", err)
	}

	// sub-second positive interval rejected
	c = NewClusterConfig()
	tiny := time.Nanosecond
	c.applyPatch(ClusterConfigPatch{SnapshotInterval: &tiny}, time.UnixMilli(0))
	if err := c.Validate(); err == nil {
		t.Fatal("expected error for snapshot-interval=1ns")
	}
}
