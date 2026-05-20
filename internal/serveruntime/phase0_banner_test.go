// §5 T46: Phase 0 banner wiring tests.
//
// Two surfaces covered here:
//
//   - bootPhase0Banner: drives the boot-time emission directly against a
//     freshly-built config.Store, bypassing the full Run() pipeline.
//   - composeAnonHookWithBanner: drives the runtime flip-detection hook
//     wired into config.Store via RegisterClusterKeys, exercising the same
//     code path that production hits when an operator runs
//     `grainfs config set iam.anon-enabled false`.
//
// The Run()-level e2e is intentionally NOT replicated here. The unit-level
// boot-phase test plus the hook-composition test together cover the exact
// stdout-writing branches; a full process-level e2e would only re-verify that
// fmt.Fprintln works.
package serveruntime

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/gritive/GrainFS/internal/config"
)

// TestBootPhase0Banner_EmitsWhenAnonEnabled — default cfgStore (anon-enabled
// defaults to true) → bootPhase0Banner writes the WARN banner.
func TestBootPhase0Banner_EmitsWhenAnonEnabled(t *testing.T) {
	var buf bytes.Buffer
	state := &bootState{
		cfgStore:     newTestCfgStore(t),
		bannerWriter: &buf,
	}
	if err := bootPhase0Banner(state); err != nil {
		t.Fatalf("bootPhase0Banner: %v", err)
	}
	if !strings.Contains(buf.String(), "Phase 0 anonymous") {
		t.Fatalf("expected banner with 'Phase 0 anonymous', got: %q", buf.String())
	}
}

// TestBootPhase0Banner_SuppressedWhenAnonDisabled — anon flipped off via a
// trusted-proxy posture (so Set succeeds) → bootPhase0Banner emits nothing.
func TestBootPhase0Banner_SuppressedWhenAnonDisabled(t *testing.T) {
	cfgStore := config.NewStore()
	onAnon, onProxy, _ := wireTLSPostureHooks("")
	config.RegisterClusterKeys(cfgStore, config.ReloadHooks{
		OnAnonEnabledChange: onAnon,
		OnTrustedProxyCIDR:  onProxy,
	})
	if err := cfgStore.Set(context.Background(), "trusted-proxy.cidr", "10.0.0.0/8"); err != nil {
		t.Fatalf("set proxy: %v", err)
	}
	if err := cfgStore.Set(context.Background(), "iam.anon-enabled", "false"); err != nil {
		t.Fatalf("set anon false: %v", err)
	}

	var buf bytes.Buffer
	state := &bootState{cfgStore: cfgStore, bannerWriter: &buf}
	if err := bootPhase0Banner(state); err != nil {
		t.Fatalf("bootPhase0Banner: %v", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("expected no banner when anon disabled, got: %q", buf.String())
	}
}

// TestBootPhase0Banner_NilWriter — defensive: nil bannerWriter must not panic
// (covers the case where a test harness builds a partial bootState).
func TestBootPhase0Banner_NilWriter(t *testing.T) {
	state := &bootState{cfgStore: newTestCfgStore(t)}
	if err := bootPhase0Banner(state); err != nil {
		t.Fatalf("nil writer should be a no-op, got: %v", err)
	}
}

// TestComposeAnonHook_EmitsDisabledBannerOnTrueToFalse — the integration test
// for the runtime flip. Set proxy first, then flip anon off → the composed
// hook emits the "remains public" INFO banner exactly once.
func TestComposeAnonHook_EmitsDisabledBannerOnTrueToFalse(t *testing.T) {
	var buf bytes.Buffer
	cfgStore := config.NewStore()
	onAnon, onProxy, _ := wireTLSPostureHooks("")
	composed, _ := composeAnonHookWithBanner(onAnon, true, &buf)
	config.RegisterClusterKeys(cfgStore, config.ReloadHooks{
		OnAnonEnabledChange: composed,
		OnTrustedProxyCIDR:  onProxy,
	})

	// Make the flip safe by setting trusted-proxy first.
	if err := cfgStore.Set(context.Background(), "trusted-proxy.cidr", "10.0.0.0/8"); err != nil {
		t.Fatalf("set proxy: %v", err)
	}
	if err := cfgStore.Set(context.Background(), "iam.anon-enabled", "false"); err != nil {
		t.Fatalf("set anon false: %v", err)
	}

	if !strings.Contains(buf.String(), "s3://default") {
		t.Fatalf("expected disabled banner, got: %q", buf.String())
	}
	if !strings.Contains(buf.String(), "remains public") {
		t.Fatalf("expected 'remains public' in disabled banner, got: %q", buf.String())
	}
}

// TestComposeAnonHook_NoOpFlipIgnored — Setting anon back to true and then
// to true again must NOT trigger the disabled banner. Only a true→false
// transition emits.
func TestComposeAnonHook_NoOpFlipIgnored(t *testing.T) {
	var buf bytes.Buffer
	cfgStore := config.NewStore()
	onAnon, onProxy, _ := wireTLSPostureHooks("")
	composed, _ := composeAnonHookWithBanner(onAnon, true, &buf)
	config.RegisterClusterKeys(cfgStore, config.ReloadHooks{
		OnAnonEnabledChange: composed,
		OnTrustedProxyCIDR:  onProxy,
	})

	// No-op write (true→true).
	if err := cfgStore.Set(context.Background(), "iam.anon-enabled", "true"); err != nil {
		t.Fatalf("set anon true: %v", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("expected no banner on true→true, got: %q", buf.String())
	}
}

// TestComposeAnonHook_RejectedFlipDoesNotEmit — when the inner posture hook
// rejects the flip (no trusted proxy, anon→false), Set rolls back and the
// banner must NOT emit. Verifies the early-return ordering: inner error is
// checked before swapping the prev snapshot, so a subsequent successful flip
// still works.
func TestComposeAnonHook_RejectedFlipDoesNotEmit(t *testing.T) {
	var buf bytes.Buffer
	cfgStore := config.NewStore()
	onAnon, onProxy, _ := wireTLSPostureHooks("")
	composed, _ := composeAnonHookWithBanner(onAnon, true, &buf)
	config.RegisterClusterKeys(cfgStore, config.ReloadHooks{
		OnAnonEnabledChange: composed,
		OnTrustedProxyCIDR:  onProxy,
	})

	// No trusted proxy → posture hook rejects → Set rolls back.
	if err := cfgStore.Set(context.Background(), "iam.anon-enabled", "false"); err == nil {
		t.Fatalf("expected Set to fail without trusted proxy")
	}
	if buf.Len() != 0 {
		t.Fatalf("rejected flip should not emit banner, got: %q", buf.String())
	}

	// Now make it safe and flip — banner MUST emit. Confirms the snapshot
	// wasn't corrupted by the earlier rejected attempt.
	if err := cfgStore.Set(context.Background(), "trusted-proxy.cidr", "10.0.0.0/8"); err != nil {
		t.Fatalf("set proxy: %v", err)
	}
	if err := cfgStore.Set(context.Background(), "iam.anon-enabled", "false"); err != nil {
		t.Fatalf("set anon false: %v", err)
	}
	if !strings.Contains(buf.String(), "remains public") {
		t.Fatalf("expected disabled banner after successful flip, got: %q", buf.String())
	}
}

// TestComposeAnonHook_NilWriter — nil bannerWriter must not panic; the inner
// posture check must still run (covers a test-harness path that does not
// supply a banner sink).
func TestComposeAnonHook_NilWriter(t *testing.T) {
	cfgStore := config.NewStore()
	onAnon, onProxy, _ := wireTLSPostureHooks("")
	composed, _ := composeAnonHookWithBanner(onAnon, true, nil)
	config.RegisterClusterKeys(cfgStore, config.ReloadHooks{
		OnAnonEnabledChange: composed,
		OnTrustedProxyCIDR:  onProxy,
	})

	if err := cfgStore.Set(context.Background(), "trusted-proxy.cidr", "10.0.0.0/8"); err != nil {
		t.Fatalf("set proxy: %v", err)
	}
	// Should succeed silently — no panic from nil writer.
	if err := cfgStore.Set(context.Background(), "iam.anon-enabled", "false"); err != nil {
		t.Fatalf("flip should succeed with nil writer, got: %v", err)
	}
}

// TestComposeAnonHook_SeedPrev_RestoredFalse — F26: after a Restore installs
// iam.anon-enabled=false, seedPrev(false) corrects the banner-prev baseline.
// A subsequent Set("iam.anon-enabled", "false") must NOT emit a spurious banner
// (prev=false → new=false, no flip).
func TestComposeAnonHook_SeedPrev_RestoredFalse(t *testing.T) {
	var buf bytes.Buffer
	onAnon, onProxy, _ := wireTLSPostureHooks("")
	hook, seedPrev := composeAnonHookWithBanner(onAnon, true, &buf) // wire-time default: true

	cfgStore := config.NewStore()
	config.RegisterClusterKeys(cfgStore, config.ReloadHooks{
		OnAnonEnabledChange: hook,
		OnTrustedProxyCIDR:  onProxy,
	})

	// Simulate Restore installing iam.anon-enabled=false.
	seedPrev(false)

	// Now a Set("false") must not emit a banner: prev=false, new=false — no flip.
	if err := cfgStore.Set(context.Background(), "trusted-proxy.cidr", "10.0.0.0/8"); err != nil {
		t.Fatalf("set proxy: %v", err)
	}
	if err := cfgStore.Set(context.Background(), "iam.anon-enabled", "false"); err != nil {
		t.Fatalf("set anon false: %v", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("no banner expected after seedPrev(false)+false→false, got: %q", buf.String())
	}
}

// TestComposeAnonHook_SeedPrev_RestoredTrueAfterFalse — F26: after a Restore
// re-installs iam.anon-enabled=true (e.g., operator re-enables anon), seedPrev
// re-seeds the baseline to true so a subsequent false flip emits the banner.
func TestComposeAnonHook_SeedPrev_RestoredTrueAfterFalse(t *testing.T) {
	var buf bytes.Buffer
	onAnon, onProxy, _ := wireTLSPostureHooks("")
	hook, seedPrev := composeAnonHookWithBanner(onAnon, true, &buf)

	cfgStore := config.NewStore()
	config.RegisterClusterKeys(cfgStore, config.ReloadHooks{
		OnAnonEnabledChange: hook,
		OnTrustedProxyCIDR:  onProxy,
	})
	if err := cfgStore.Set(context.Background(), "trusted-proxy.cidr", "10.0.0.0/8"); err != nil {
		t.Fatalf("set proxy: %v", err)
	}
	// First flip true→false consumes the prev snapshot.
	if err := cfgStore.Set(context.Background(), "iam.anon-enabled", "false"); err != nil {
		t.Fatalf("first flip: %v", err)
	}
	buf.Reset()

	// Simulate a Restore that re-installs anon=true.
	seedPrev(true)

	// Another true→false flip must emit the banner again.
	if err := cfgStore.Set(context.Background(), "iam.anon-enabled", "true"); err != nil {
		t.Fatalf("reset anon: %v", err)
	}
	if err := cfgStore.Set(context.Background(), "iam.anon-enabled", "false"); err != nil {
		t.Fatalf("second flip: %v", err)
	}
	if !strings.Contains(buf.String(), "remains public") {
		t.Fatalf("expected banner on second true→false flip, got: %q", buf.String())
	}
}
