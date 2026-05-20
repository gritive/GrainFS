package serveruntime

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gritive/GrainFS/internal/config"
	"github.com/gritive/GrainFS/internal/nodeconfig"
)

// newTestCfgStore returns a config.Store with the cluster keys registered and
// no reload hooks wired — sufficient for posture-gate scenarios where we set
// values directly via Set without exercising downstream subsystems.
func newTestCfgStore(t *testing.T) *config.Store {
	t.Helper()
	s := config.NewStore()
	config.RegisterClusterKeys(s, config.ReloadHooks{})
	return s
}

// TestTLSPosture_RefusesAuthRequiredWithoutCertOrProxy is the §5 T44 spec test:
// anon=false + no cert + no trusted-proxy.cidr → error containing
// "config set trusted-proxy.cidr".
func TestTLSPosture_RefusesAuthRequiredWithoutCertOrProxy(t *testing.T) {
	// Isolate from any GRAINFS_TLS_CERT/KEY env on the host runner.
	t.Setenv("GRAINFS_TLS_CERT", "")
	t.Setenv("GRAINFS_TLS_KEY", "")

	dataDir := t.TempDir()
	nc := nodeconfig.New(dataDir)
	cfg := newTestCfgStore(t)

	// anon-enabled defaults to true; flip it off to require auth.
	if err := cfg.Set(context.Background(), "iam.anon-enabled", "false"); err != nil {
		t.Fatalf("set iam.anon-enabled=false: %v", err)
	}

	err := enforceTLSPosture(cfg, nc)
	if err == nil {
		t.Fatalf("expected posture error, got nil")
	}
	if !strings.Contains(err.Error(), "config set trusted-proxy.cidr") {
		t.Fatalf("error missing 'config set trusted-proxy.cidr' substring: %v", err)
	}
}

// TestTLSPosture_AllowsAnonEnabled — default posture (anon=true) is always
// permitted regardless of cert / proxy state.
func TestTLSPosture_AllowsAnonEnabled(t *testing.T) {
	t.Setenv("GRAINFS_TLS_CERT", "")
	t.Setenv("GRAINFS_TLS_KEY", "")
	dataDir := t.TempDir()
	nc := nodeconfig.New(dataDir)
	cfg := newTestCfgStore(t)
	// iam.anon-enabled defaults to true; do not Set it.
	if err := enforceTLSPosture(cfg, nc); err != nil {
		t.Fatalf("anon=true should pass posture gate, got: %v", err)
	}
}

// TestTLSPosture_AllowsCertOnDisk — anon=false + cert file present → safe.
func TestTLSPosture_AllowsCertOnDisk(t *testing.T) {
	t.Setenv("GRAINFS_TLS_CERT", "")
	t.Setenv("GRAINFS_TLS_KEY", "")
	dataDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dataDir, "tls"), 0o755); err != nil {
		t.Fatalf("mkdir tls: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "tls", "cert.pem"), []byte("dummy"), 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	nc := nodeconfig.New(dataDir)
	cfg := newTestCfgStore(t)
	if err := cfg.Set(context.Background(), "iam.anon-enabled", "false"); err != nil {
		t.Fatalf("set iam.anon-enabled=false: %v", err)
	}
	if err := enforceTLSPosture(cfg, nc); err != nil {
		t.Fatalf("cert-present posture should pass, got: %v", err)
	}
}

// TestTLSPosture_AllowsTrustedProxy — anon=false + no cert + trusted-proxy.cidr
// set → safe (front-end TLS terminator carries client identity).
func TestTLSPosture_AllowsTrustedProxy(t *testing.T) {
	t.Setenv("GRAINFS_TLS_CERT", "")
	t.Setenv("GRAINFS_TLS_KEY", "")
	dataDir := t.TempDir()
	nc := nodeconfig.New(dataDir)
	cfg := newTestCfgStore(t)
	if err := cfg.Set(context.Background(), "iam.anon-enabled", "false"); err != nil {
		t.Fatalf("set iam.anon-enabled=false: %v", err)
	}
	if err := cfg.Set(context.Background(), "trusted-proxy.cidr", "10.0.0.0/8"); err != nil {
		t.Fatalf("set trusted-proxy.cidr: %v", err)
	}
	if err := enforceTLSPosture(cfg, nc); err != nil {
		t.Fatalf("trusted-proxy posture should pass, got: %v", err)
	}
}

// TestTLSPosture_ReloadHookRollsBackOnUnsafeFlip — the iam.anon-enabled reload
// hook returns the posture error, and config.Store.Set rolls back the value.
// This is the runtime-safety leg: an operator cannot `grainfs config set
// iam.anon-enabled false` into an unsafe posture (no trusted proxy).
//
// The hook is INTENTIONALLY cert-independent — see the "Determinism split"
// note in tls_posture.go. Per-node cert state cannot drive a cluster-replicated
// config decision without risking split-brain. Cert presence is checked only
// at boot (TestTLSPosture_RefusesAuthRequiredWithoutCertOrProxy).
//
// The hook also fires under the store's write lock, so it MUST NOT re-query
// the store (would self-deadlock on RLock). This test exercises that path —
// it would deadlock if wireTLSPostureHooks regressed to reading from cfgStore.
func TestTLSPosture_ReloadHookRollsBackOnUnsafeFlip(t *testing.T) {
	cfgStore := config.NewStore()
	onAnon, onProxy, _ := wireTLSPostureHooks("")
	config.RegisterClusterKeys(cfgStore, config.ReloadHooks{
		OnAnonEnabledChange: onAnon,
		OnTrustedProxyCIDR:  onProxy,
	})

	// Flip anon off — no proxy → hook errors, Set rolls back.
	err := cfgStore.Set(context.Background(), "iam.anon-enabled", "false")
	if err == nil {
		t.Fatalf("expected Set to fail due to unsafe posture, got nil")
	}
	if !strings.Contains(err.Error(), "config set trusted-proxy.cidr") {
		t.Fatalf("error missing 'config set trusted-proxy.cidr' substring: %v", err)
	}

	// Verify rollback: the value remained at the default (true).
	v, ok := cfgStore.GetBool("iam.anon-enabled")
	if !ok {
		t.Fatalf("iam.anon-enabled not registered")
	}
	if !v {
		t.Fatalf("iam.anon-enabled should remain true after rejected flip, got false")
	}
}

// TestTLSPosture_ReloadHookAcceptsAfterProxySet — set trusted-proxy.cidr first,
// then flipping anon off must succeed (the OnTrustedProxyCIDR hook updates the
// atomic snapshot so the anon-change hook observes the new posture).
func TestTLSPosture_ReloadHookAcceptsAfterProxySet(t *testing.T) {
	cfgStore := config.NewStore()
	onAnon, onProxy, _ := wireTLSPostureHooks("")
	config.RegisterClusterKeys(cfgStore, config.ReloadHooks{
		OnAnonEnabledChange: onAnon,
		OnTrustedProxyCIDR:  onProxy,
	})

	if err := cfgStore.Set(context.Background(), "trusted-proxy.cidr", "10.0.0.0/8"); err != nil {
		t.Fatalf("set trusted-proxy.cidr: %v", err)
	}
	if err := cfgStore.Set(context.Background(), "iam.anon-enabled", "false"); err != nil {
		t.Fatalf("anon flip should succeed once proxy is set, got: %v", err)
	}
}
