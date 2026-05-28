package transport

import (
	"crypto/tls"
	"testing"

	"github.com/stretchr/testify/require"
)

func loadSnap(t *testing.T, tr *QUICTransport) *IdentitySnapshot {
	t.Helper()
	s := tr.identity.Load()
	require.NotNil(t, s, "identity snapshot present")
	return s
}

func TestQUICTransport_FlipPresent_PinsPresent(t *testing.T) {
	tr, err := NewQUICTransport("test-psk-flip")
	require.NoError(t, err)
	defer tr.Close()

	perNode := spkiN(9)
	tr.FlipPresent(tlsCertForTest(t), perNode)
	require.Equal(t, perNode, loadSnap(t, tr).PresentSPKI, "FlipPresent pins present SPKI")

	tr.UpdateRegistryAccept([][32]byte{spkiN(7)})
	require.Equal(t, perNode, loadSnap(t, tr).PresentSPKI, "registry update keeps pinned present")
}

func TestQUICTransport_RecycleConns_NoConns_NoError(t *testing.T) {
	tr, err := NewQUICTransport("test-psk-recycle")
	require.NoError(t, err)
	defer tr.Close()
	tr.RecycleConns()
	tr.mu.RLock()
	require.Empty(t, tr.conns)
	require.Empty(t, tr.muxConns)
	tr.mu.RUnlock()
}

func TestQUICTransport_SetDropped_ExcludesBase(t *testing.T) {
	tr, err := NewQUICTransport("test-psk-dropped")
	require.NoError(t, err)
	defer tr.Close()
	base := loadSnap(t, tr).PresentSPKI
	tr.UpdateRegistryAccept([][32]byte{spkiN(7)})
	require.True(t, acceptsContains(loadSnap(t, tr), base), "base accepted before drop")

	tr.SetDropped()
	require.False(t, acceptsContains(loadSnap(t, tr), base), "SetDropped removes cluster-key base")
}

func tlsCertForTest(t *testing.T) tls.Certificate {
	t.Helper()
	cert, _, err := GenerateNodeIdentity("test-cluster", "test-node")
	require.NoError(t, err)
	return cert
}
