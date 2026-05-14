package cluster

import (
	"bytes"
	"errors"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
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

// TestMetaFSM_Apply_ClusterConfigPatch_RejectsSecretWithoutEncryptor verifies
// that a patch carrying a wrapped webhook secret is rejected when the FSM has
// no encryptor. The error must mention
// "encryption disabled" so the HTTP handler (Task 10) can map it to 403.
func TestMetaFSM_Apply_ClusterConfigPatch_RejectsSecretWithoutEncryptor(t *testing.T) {
	f := NewMetaFSM()
	// f.encryptor stays nil.

	patch := ClusterConfigPatch{
		AlertWebhook:              ptrString("https://hooks.example/x"),
		AlertWebhookSecretWrapped: []byte{0x01, 0x02, 0x03}, // pretending wrapped
	}
	err := f.applyCmd(buildClusterConfigPatchCmd(t, patch))
	require.Error(t, err)
	require.Contains(t, err.Error(), "encryption disabled")

	// FSM unchanged — rev still 0, no webhook set.
	cfg := f.ClusterConfig()
	require.Equal(t, uint64(0), cfg.Rev())
	require.Equal(t, "", cfg.AlertWebhook())
}

// TestMetaFSM_Apply_ClusterConfigPatch_EmitsAuditLog verifies that a successful
// patch apply emits a structured zerolog event with event=cluster_config_changed
// and the changed keys present as fields. Keys not in the patch must not leak.
func TestMetaFSM_Apply_ClusterConfigPatch_EmitsAuditLog(t *testing.T) {
	f := NewMetaFSM()

	var buf bytes.Buffer
	prev := log.Logger
	log.Logger = zerolog.New(&buf).With().Timestamp().Logger()
	defer func() { log.Logger = prev }()

	require.NoError(t, f.applyCmd(buildClusterConfigPatchCmd(t, ClusterConfigPatch{
		BalancerImbalanceTriggerPct: ptrFloat(40.0),
	})))

	s := buf.String()
	require.Contains(t, s, `"event":"cluster_config_changed"`)
	require.Contains(t, s, `"rev":1`)
	require.Contains(t, s, `balancer-imbalance-trigger-pct`)
	require.NotContains(t, s, `alert-webhook-secret`) // not present in this patch — must not leak
}

// TestMetaFSM_Apply_ClusterConfigPatch_RedactsSecret verifies that when a patch
// carries AlertWebhookSecretWrapped, the audit log emits "<redacted>" instead
// of the raw bytes.
func TestMetaFSM_Apply_ClusterConfigPatch_RedactsSecret(t *testing.T) {
	f := NewMetaFSM()
	f.SetEncryptor(newIAMTestEncryptor(t)) // gate for AlertWebhookSecretWrapped

	var buf bytes.Buffer
	prev := log.Logger
	log.Logger = zerolog.New(&buf).With().Timestamp().Logger()
	defer func() { log.Logger = prev }()

	require.NoError(t, f.applyCmd(buildClusterConfigPatchCmd(t, ClusterConfigPatch{
		AlertWebhook:              ptrString("https://hooks.example/r"),
		AlertWebhookSecretWrapped: []byte{0xAA, 0xBB},
	})))

	s := buf.String()
	require.Contains(t, s, `"alert_webhook_secret":"<redacted>"`)
	require.NotContains(t, s, "AABB") // raw bytes hex must not appear
	require.NotContains(t, s, "aabb")
}

func TestMetaFSM_Snapshot_Restore_ClusterConfig(t *testing.T) {
	src := NewMetaFSM()
	src.SetEncryptor(newIAMTestEncryptor(t)) // gate for AlertWebhookSecretWrapped
	require.NoError(t, src.applyCmd(buildClusterConfigPatchCmd(t, ClusterConfigPatch{
		BalancerImbalanceTriggerPct: ptrFloat(33.0),
		AlertWebhook:                ptrString("https://hooks.example/sr"),
		AlertWebhookSecretWrapped:   []byte{0xde, 0xad, 0xbe, 0xef},
		DiskCriticalFrac:            ptrFloat(0.85),
	})))

	// Snapshot
	buf, err := src.Snapshot()
	require.NoError(t, err)
	require.NotEmpty(t, buf)

	// Restore on a fresh FSM — outer ClusterConfig handle must remain valid
	// after Restore (A3: ReplaceSnap, not pointer assignment).
	dst := NewMetaFSM()
	cfgHandle := dst.ClusterConfig()
	require.NoError(t, dst.Restore(raft.SnapshotMeta{}, buf))

	cfg := dst.ClusterConfig()
	require.Same(t, cfgHandle, cfg, "Restore must keep the outer ClusterConfig pointer (A3)")
	require.Equal(t, uint64(1), cfg.Rev())
	require.InDelta(t, 33.0, cfg.BalancerImbalanceTriggerPct(), 0.0001)
	require.Equal(t, "https://hooks.example/sr", cfg.AlertWebhook())
	require.Equal(t, []byte{0xde, 0xad, 0xbe, 0xef}, cfg.AlertWebhookSecretWrapped())
	require.InDelta(t, 0.85, cfg.DiskCriticalFrac(), 0.0001)
	// Unchanged values still default
	require.InDelta(t, DefaultClusterDiskWarnFrac, cfg.DiskWarnFrac(), 0.0001)
	// "explicit" vs "default" source preserved
	require.Equal(t, "explicit", cfg.SourceForKey("alert-webhook"))
	require.Equal(t, "default", cfg.SourceForKey("disk-warn-threshold"))
}

// TestMetaFSM_Snapshot_Restore_ClusterConfig_ExplicitEmptyWebhook verifies that
// an explicit "" alert-webhook survives the round trip and stays distinguishable
// from the unset default. Without this, a Reset of alert-webhook would silently
// collapse to "default" after restore.
func TestMetaFSM_Snapshot_Restore_ClusterConfig_ExplicitEmptyWebhook(t *testing.T) {
	src := NewMetaFSM()
	require.NoError(t, src.applyCmd(buildClusterConfigPatchCmd(t, ClusterConfigPatch{
		AlertWebhook: ptrString(""),
	})))
	require.Equal(t, "explicit", src.ClusterConfig().SourceForKey("alert-webhook"))

	buf, err := src.Snapshot()
	require.NoError(t, err)

	dst := NewMetaFSM()
	require.NoError(t, dst.Restore(raft.SnapshotMeta{}, buf))
	require.Equal(t, "explicit", dst.ClusterConfig().SourceForKey("alert-webhook"))
	require.Equal(t, "", dst.ClusterConfig().AlertWebhook())
}
