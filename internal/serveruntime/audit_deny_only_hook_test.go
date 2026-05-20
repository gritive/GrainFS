// §6 T52': Production-wiring integration test for the audit.deny-only reload
// hook.
//
// This builds the exact closure shape used by bootMetaRaftWiring — a hook that
// captures *bootState and dereferences state.auditOutbox at fire time — and
// asserts that setting "audit.deny-only" via the config store propagates to
// the outbox's atomic. This is the join point the runtime relies on.
package serveruntime

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/audit"
	"github.com/gritive/GrainFS/internal/config"
)

// TestAuditDenyOnlyHook_PropagatesToOutbox — wires the OnAuditDenyOnly hook
// the same way bootMetaRaftWiring does (closure over *bootState), then sets
// state.auditOutbox as boot_phases_srvopts would, and verifies cfgStore.Set
// updates the outbox's atomic.
func TestAuditDenyOnlyHook_PropagatesToOutbox(t *testing.T) {
	ctx := context.Background()

	state := &bootState{}
	cfgStore := config.NewStore()
	config.RegisterClusterKeys(cfgStore, config.ReloadHooks{
		OnAuditDenyOnly: func(_ context.Context, v bool) error {
			state.auditOutbox.SetDenyOnly(v) // nil-safe; same shape as production
			return nil
		},
	})

	box, err := audit.OpenOutbox(filepath.Join(t.TempDir(), "audit-outbox"))
	require.NoError(t, err)
	defer box.Close()
	state.auditOutbox = box

	require.False(t, box.DenyOnly(), "initial state must be off")

	require.NoError(t, cfgStore.Set(ctx, "audit.deny-only", "true"))
	require.True(t, box.DenyOnly(), "cfgStore.Set true must reach the outbox")

	require.NoError(t, cfgStore.Set(ctx, "audit.deny-only", "false"))
	require.False(t, box.DenyOnly(), "cfgStore.Set false must reach the outbox")
}

// TestAuditDenyOnlyHook_NilSafeBeforeOutbox — the hook fires before the
// outbox is constructed (cfg.AuditIceberg=false, or a snapshot Restore that
// replays audit.deny-only on a node that never created an outbox). This must
// not panic; SetDenyOnly is nil-safe.
func TestAuditDenyOnlyHook_NilSafeBeforeOutbox(t *testing.T) {
	ctx := context.Background()
	state := &bootState{} // state.auditOutbox stays nil

	cfgStore := config.NewStore()
	config.RegisterClusterKeys(cfgStore, config.ReloadHooks{
		OnAuditDenyOnly: func(_ context.Context, v bool) error {
			state.auditOutbox.SetDenyOnly(v)
			return nil
		},
	})

	require.NotPanics(t, func() {
		_ = cfgStore.Set(ctx, "audit.deny-only", "true")
	})
}
