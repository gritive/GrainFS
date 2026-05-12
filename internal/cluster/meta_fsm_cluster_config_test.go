package cluster

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// buildClusterConfigPatchCmd wraps a ClusterConfigPatch in a MetaCmd envelope,
// ready to feed into MetaFSM.applyCmd. Mirrors the helper pattern used in
// meta_fsm_test.go (e.g. makePutShardGroupCmd).
func buildClusterConfigPatchCmd(t *testing.T, p ClusterConfigPatch) []byte {
	t.Helper()
	data, err := EncodeClusterConfigPatchCmd(p)
	require.NoError(t, err)
	return data
}

func TestMetaFSM_Apply_ClusterConfigPatch_SetsValueAndBumpsRev(t *testing.T) {
	f := NewMetaFSM()

	patch := ClusterConfigPatch{
		BalancerImbalanceTriggerPct: ptrFloat(25.0),
		AlertWebhook:                ptrString("https://hooks.example/a"),
	}
	require.NoError(t, f.applyCmd(buildClusterConfigPatchCmd(t, patch)))

	cfg := f.ClusterConfig()
	require.InDelta(t, 25.0, cfg.BalancerImbalanceTriggerPct(), 0.0001)
	require.Equal(t, "https://hooks.example/a", cfg.AlertWebhook())
	require.Equal(t, uint64(1), cfg.Rev())
}

func TestMetaFSM_Apply_ClusterConfigPatch_InvariantViolation_Reject(t *testing.T) {
	f := NewMetaFSM()

	bad := ClusterConfigPatch{
		BalancerImbalanceTriggerPct: ptrFloat(150.0), // > 100, invalid
	}
	require.Error(t, f.applyCmd(buildClusterConfigPatchCmd(t, bad)))

	// FSM unchanged — rev still 0, getter returns default.
	cfg := f.ClusterConfig()
	require.Equal(t, uint64(0), cfg.Rev())
	require.InDelta(t, DefaultClusterBalancerImbalanceTriggerPct, cfg.BalancerImbalanceTriggerPct(), 0.0001)
}

func TestMetaFSM_Apply_ClusterConfigPatch_CAS_Mismatch_Reject(t *testing.T) {
	f := NewMetaFSM()

	// First patch — rev 0 → 1
	require.NoError(t, f.applyCmd(buildClusterConfigPatchCmd(t, ClusterConfigPatch{
		BalancerImbalanceTriggerPct: ptrFloat(20.5),
	})))
	require.Equal(t, uint64(1), f.ClusterConfig().Rev())

	// CAS-failing patch — expects rev 5, current is 1
	err := f.applyCmd(buildClusterConfigPatchCmd(t, ClusterConfigPatch{
		BalancerImbalanceTriggerPct: ptrFloat(30.0),
		ExpectedRev:                 5,
	}))
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrClusterConfigCAS), "error must wrap ErrClusterConfigCAS, got %v", err)

	// State unchanged
	require.Equal(t, uint64(1), f.ClusterConfig().Rev())
	require.InDelta(t, 20.5, f.ClusterConfig().BalancerImbalanceTriggerPct(), 0.0001)
}

func TestMetaFSM_Apply_ClusterConfigPatch_ResetKeys(t *testing.T) {
	f := NewMetaFSM()

	require.NoError(t, f.applyCmd(buildClusterConfigPatchCmd(t, ClusterConfigPatch{
		BalancerImbalanceTriggerPct: ptrFloat(50.0),
	})))
	require.InDelta(t, 50.0, f.ClusterConfig().BalancerImbalanceTriggerPct(), 0.0001)

	require.NoError(t, f.applyCmd(buildClusterConfigPatchCmd(t, ClusterConfigPatch{
		ResetKeys: []string{"balancer-imbalance-trigger-pct"},
	})))
	require.InDelta(t, DefaultClusterBalancerImbalanceTriggerPct, f.ClusterConfig().BalancerImbalanceTriggerPct(), 0.0001)
	require.Equal(t, uint64(2), f.ClusterConfig().Rev())
}
