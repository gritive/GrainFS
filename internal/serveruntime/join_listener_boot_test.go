package serveruntime

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/transport"
)

// TestStartJoinListener_TCPBranch proves startJoinListener starts a
// *transport.TCPJoinListener (TCP is the sole cluster transport), binds a
// loopback port, and exposes the persisted cert's SPKI. The handler is not
// invoked here (no joiner dials), so a nil receiver is fine.
func TestStartJoinListener_TCPBranch(t *testing.T) {
	dir := t.TempDir()
	_, wantSPKI, err := LoadOrCreateJoinListenerCert(dir)
	require.NoError(t, err)

	state := newBootState(Config{DataDir: dir, NodeID: "n1"})
	t.Cleanup(state.Cleanup)

	require.NoError(t, startJoinListener(state, nil))
	_, isTCP := state.joinListener.(*transport.TCPJoinListener)
	assert.True(t, isTCP, "startJoinListener must start a *TCPJoinListener")
	assert.True(t, strings.HasPrefix(state.JoinListenerAddr(), "127.0.0.1:"),
		"join listener bound on loopback, got %q", state.JoinListenerAddr())
	assert.Equal(t, wantSPKI, state.JoinListenerSPKI(), "listener SPKI matches the persisted cert")
}

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
