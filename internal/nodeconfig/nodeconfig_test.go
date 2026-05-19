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
