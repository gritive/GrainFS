package serveruntime

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestLoadOrCreateJoinListenerCertStableSPKI asserts that two loads over the
// same data dir yield the SAME SPKI — the property outstanding invite bundles
// rely on across leader restarts.
func TestLoadOrCreateJoinListenerCertStableSPKI(t *testing.T) {
	dir := t.TempDir()

	cert1, spki1, err := LoadOrCreateJoinListenerCert(dir)
	require.NoError(t, err)
	require.NotNil(t, cert1.PrivateKey)
	require.NotEqual(t, [32]byte{}, spki1, "SPKI must be non-zero")

	_, spki2, err := LoadOrCreateJoinListenerCert(dir)
	require.NoError(t, err)
	require.Equal(t, spki1, spki2, "SPKI must be stable across reloads of the same dir")

	// A different dir must yield a different identity.
	_, spki3, err := LoadOrCreateJoinListenerCert(t.TempDir())
	require.NoError(t, err)
	require.NotEqual(t, spki1, spki3, "distinct dirs must yield distinct SPKIs")
}

func TestResolveJoinListenAddr(t *testing.T) {
	tests := []struct {
		name       string
		joinAddr   string
		raftAddr   string
		want       string
		wantPrefix string
	}{
		{name: "explicit wins", joinAddr: "0.0.0.0:7100", raftAddr: "10.0.0.1:6000", want: "0.0.0.0:7100"},
		{name: "derive host from raft addr", raftAddr: "10.0.0.1:6000", want: "10.0.0.1:0"},
		{name: "raft addr ephemeral port", raftAddr: "127.0.0.1:0", want: "127.0.0.1:0"},
		{name: "no raft addr falls back to loopback", want: "127.0.0.1:0"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveJoinListenAddr(tc.joinAddr, tc.raftAddr)
			require.Equal(t, tc.want, got)
		})
	}
}
