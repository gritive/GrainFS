package serveruntime

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestNFSBoot_AnonOn_NoCertNoProxy_OK — when anon=true, the NFS/9P posture
// gate is not enforced (Phase 0: open access is intentional).
func TestNFSBoot_AnonOn_NoCertNoProxy_OK(t *testing.T) {
	t.Setenv("GRAINFS_TLS_CERT", "")
	t.Setenv("GRAINFS_TLS_KEY", "")
	dataDir := t.TempDir()
	cfg := newTestCfgStore(t)
	// iam.anon-enabled defaults to true — do not flip.
	state := &bootState{
		cfg:      Config{DataDir: dataDir},
		cfgStore: cfg,
	}
	if err := bootNodeServicesPostureGate(state); err != nil {
		t.Fatalf("anon=true: expected no error, got: %v", err)
	}
}

// TestNFSBoot_AnonOff_NoCertNoProxy_BootFail — anon=false + no cert + no
// trusted proxy → NFS/9P boot must be refused with a posture error.
func TestNFSBoot_AnonOff_NoCertNoProxy_BootFail(t *testing.T) {
	t.Setenv("GRAINFS_TLS_CERT", "")
	t.Setenv("GRAINFS_TLS_KEY", "")
	dataDir := t.TempDir()
	cfg := newTestCfgStore(t)
	if err := cfg.Set(context.Background(), "iam.anon-enabled", "false"); err != nil {
		t.Fatalf("set iam.anon-enabled=false: %v", err)
	}
	state := &bootState{
		cfg:      Config{DataDir: dataDir},
		cfgStore: cfg,
	}
	err := bootNodeServicesPostureGate(state)
	if err == nil {
		t.Fatal("expected posture boot-fail, got nil")
	}
	// Confirm NFS/9P prefix so the operator sees which component refused.
	if !strings.HasPrefix(err.Error(), "NFS/9P boot:") {
		t.Fatalf("error should start with %q, got: %v", "NFS/9P boot:", err)
	}
}

// TestNFSBoot_AnonOff_WithCert_OK — anon=false but a cert is on disk → safe.
func TestNFSBoot_AnonOff_WithCert_OK(t *testing.T) {
	t.Setenv("GRAINFS_TLS_CERT", "")
	t.Setenv("GRAINFS_TLS_KEY", "")
	dataDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dataDir, "tls"), 0o755); err != nil {
		t.Fatalf("mkdir tls: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "tls", "cert.pem"), []byte("dummy"), 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	cfg := newTestCfgStore(t)
	if err := cfg.Set(context.Background(), "iam.anon-enabled", "false"); err != nil {
		t.Fatalf("set iam.anon-enabled=false: %v", err)
	}
	state := &bootState{
		cfg:      Config{DataDir: dataDir},
		cfgStore: cfg,
	}
	if err := bootNodeServicesPostureGate(state); err != nil {
		t.Fatalf("cert-present posture should pass NFS/9P gate, got: %v", err)
	}
}

// TestNFSBoot_AnonOff_WithTrustedProxy_OK — anon=false + no cert but
// trusted-proxy.cidr set → safe (front-end TLS terminator carries identity).
func TestNFSBoot_AnonOff_WithTrustedProxy_OK(t *testing.T) {
	t.Setenv("GRAINFS_TLS_CERT", "")
	t.Setenv("GRAINFS_TLS_KEY", "")
	dataDir := t.TempDir()
	cfg := newTestCfgStore(t)
	if err := cfg.Set(context.Background(), "iam.anon-enabled", "false"); err != nil {
		t.Fatalf("set iam.anon-enabled=false: %v", err)
	}
	if err := cfg.Set(context.Background(), "trusted-proxy.cidr", "10.0.0.0/8"); err != nil {
		t.Fatalf("set trusted-proxy.cidr: %v", err)
	}
	state := &bootState{
		cfg:      Config{DataDir: dataDir},
		cfgStore: cfg,
	}
	if err := bootNodeServicesPostureGate(state); err != nil {
		t.Fatalf("trusted-proxy posture should pass NFS/9P gate, got: %v", err)
	}
}
