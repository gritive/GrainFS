package nodeconfig

import (
	"path/filepath"
	"testing"
)

func TestTLSCertPath_DefaultConvention(t *testing.T) {
	t.Setenv("GRAINFS_TLS_CERT", "")
	nc := New("/tmp/data-x")
	if got, want := nc.TLSCertPath(), filepath.Join("/tmp/data-x", "tls", "cert.pem"); got != want {
		t.Fatalf("TLSCertPath() = %q, want %q", got, want)
	}
}

func TestTLSKeyPath_EnvOverride(t *testing.T) {
	t.Setenv("GRAINFS_TLS_KEY", "/custom/key.pem")
	nc := New("/tmp/data-x")
	if got, want := nc.TLSKeyPath(), "/custom/key.pem"; got != want {
		t.Fatalf("TLSKeyPath() = %q, want %q", got, want)
	}
}

func TestLogLevel_Default(t *testing.T) {
	t.Setenv("GRAINFS_LOG_LEVEL", "")
	nc := New("/tmp/data-x")
	if got, want := nc.LogLevel(), "info"; got != want {
		t.Fatalf("LogLevel() = %q, want %q", got, want)
	}
}
