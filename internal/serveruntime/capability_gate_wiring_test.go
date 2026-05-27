package serveruntime

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/compat"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/transport"
)

// buildCapGateWiringState assembles the minimal bootState that
// wireCapabilityGateDirectProbe touches: a real MetaRaft (for FSM()), a real
// QUIC transport (for Handle), a CapabilityGate, a KEKStore, and a
// HandshakeVerifier bound to a 16-byte cluster.id.
func buildCapGateWiringState(t *testing.T) *bootState {
	t.Helper()
	meta, err := cluster.NewMetaRaft(cluster.MetaRaftConfig{NodeID: "node-1", DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { meta.Close() })

	quic, err := transport.NewQUICTransport("test-psk")
	require.NoError(t, err)
	t.Cleanup(func() { _ = quic.Close() })

	kekStore := encrypt.NewKEKStore()
	require.NoError(t, kekStore.Add(0, make([]byte, encrypt.KEKSize)))

	clusterID := make([]byte, 16)
	for i := range clusterID {
		clusterID[i] = byte(i + 1)
	}
	verifier := encrypt.NewHandshakeVerifier(kekStore, clusterID)

	gate := cluster.NewCapabilityGate(compat.DefaultRegistry, 30*time.Second)
	meta.SetCapabilityGate(gate)

	return &bootState{
		cfg:               Config{Version: "0.0.356.0"},
		metaRaft:          meta,
		quicTransport:     quic,
		kekStore:          kekStore,
		capabilityGate:    gate,
		handshakeVerifier: verifier,
	}
}

// TestWireCapabilityGateDirectProbe_WiresWhenKEKPresent proves that the boot
// wiring populates the gate's direct signed-assertion path when a KEK store is
// present — the security gap this change closes (previously directCfg stayed
// nil and Allow silently fell back to gossip in production).
func TestWireCapabilityGateDirectProbe_WiresWhenKEKPresent(t *testing.T) {
	state := buildCapGateWiringState(t)
	require.False(t, state.capabilityGate.HasDirectProbe(), "precondition: gate must start unwired")

	require.NoError(t, wireCapabilityGateDirectProbe(state, state.metaRaft.Node().ID()))
	require.True(t, state.capabilityGate.HasDirectProbe(),
		"wireCapabilityGateDirectProbe must populate the gate's direct-RPC path when kekStore is present")
}

// TestWireCapabilityGateDirectProbe_NilGateNoop verifies the wiring is a no-op
// (no panic) when no capability gate is configured (non-cluster test configs).
func TestWireCapabilityGateDirectProbe_NilGateNoop(t *testing.T) {
	state := buildCapGateWiringState(t)
	state.capabilityGate = nil
	require.NoError(t, wireCapabilityGateDirectProbe(state, state.metaRaft.Node().ID()))
}
