package cluster

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClusterConfig_EmptyReturnsDefaults(t *testing.T) {
	cfg := NewClusterConfig()
	require.Equal(t, DefaultClusterBalancerEnabled, cfg.BalancerEnabled())
	require.InDelta(t, DefaultClusterBalancerImbalanceTriggerPct, cfg.BalancerImbalanceTriggerPct(), 0.0001)
	require.Equal(t, DefaultClusterBalancerGossipInterval, cfg.BalancerGossipInterval())
	require.Equal(t, "", cfg.AlertWebhook())
	require.Nil(t, cfg.AlertWebhookSecretWrapped())
	require.InDelta(t, DefaultClusterDiskWarnFrac, cfg.DiskWarnFrac(), 0.0001)
	require.Equal(t, uint64(0), cfg.Rev())
}

func TestClusterConfig_PatchSetsExplicit(t *testing.T) {
	cfg := NewClusterConfig()
	patch := ClusterConfigPatch{
		BalancerImbalanceTriggerPct: ptrFloat(25.0),
		AlertWebhook:                ptrString("https://hooks.example/abc"),
		DiskCriticalFrac:            ptrFloat(0.95),
	}
	cfg.applyPatch(patch, time.UnixMilli(1715520000000))

	require.InDelta(t, 25.0, cfg.BalancerImbalanceTriggerPct(), 0.0001)
	require.Equal(t, "https://hooks.example/abc", cfg.AlertWebhook())
	require.InDelta(t, 0.95, cfg.DiskCriticalFrac(), 0.0001)
	// untouched field returns default
	require.InDelta(t, DefaultClusterDiskWarnFrac, cfg.DiskWarnFrac(), 0.0001)
	require.Equal(t, uint64(1), cfg.Rev())
}

func TestClusterConfig_ResetKeyRestoresDefault(t *testing.T) {
	cfg := NewClusterConfig()
	cfg.applyPatch(ClusterConfigPatch{BalancerImbalanceTriggerPct: ptrFloat(50.0)}, time.Unix(0, 0))
	require.InDelta(t, 50.0, cfg.BalancerImbalanceTriggerPct(), 0.0001)

	cfg.applyPatch(ClusterConfigPatch{ResetKeys: []string{"balancer-imbalance-trigger-pct"}}, time.Unix(0, 0))
	require.InDelta(t, DefaultClusterBalancerImbalanceTriggerPct, cfg.BalancerImbalanceTriggerPct(), 0.0001)
}

func ptrFloat(v float64) *float64 { return &v }
func ptrString(v string) *string  { return &v }
func ptrInt32(v int32) *int32     { return &v }
func ptrInt64(v int64) *int64     { return &v }
func ptrBool(v bool) *bool        { return &v }
