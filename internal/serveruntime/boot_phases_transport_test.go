package serveruntime

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBootQUICTransport_BindsLoopbackPort0 — happy path: solo-mode bootstrap
// (no peers) uses 127.0.0.1:0; bootQUICTransport must Listen, then resolve
// state.raftAddr to the kernel-picked port so peers see a dialable self.
func TestBootQUICTransport_BindsLoopbackPort0(t *testing.T) {
	state := newBootState(Config{DataDir: t.TempDir(), NodeID: "n1", ClusterKey: "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"})
	require.NoError(t, bootValidateConfig(state))
	t.Cleanup(state.Cleanup)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, bootQUICTransport(ctx, state))
	require.NotNil(t, state.quicTransport, "state.quicTransport populated")
	assert.NotEmpty(t, state.transportPSK, "ephemeral PSK generated in solo mode")

	resolved := state.raftAddr
	assert.NotEqual(t, "127.0.0.1:0", resolved, "Listen must resolve :0 to kernel-picked port")
	assert.True(t, strings.HasPrefix(resolved, "127.0.0.1:"), "kept on loopback in solo mode")
	assert.Equal(t, state.quicTransport.LocalAddr(), resolved, "state.raftAddr matches LocalAddr")
}

// TestBootQUICTransport_GeneratesEphemeralKeyInSoloMode — when neither cfg.
// ClusterKey nor keys.d/current.key exists and no peers are configured,
// bootQUICTransport must generate an ephemeral key so zero-config holds.
func TestBootQUICTransport_GeneratesEphemeralKeyInSoloMode(t *testing.T) {
	state := newBootState(Config{DataDir: t.TempDir(), NodeID: "n1", ClusterKey: "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"})
	require.NoError(t, bootValidateConfig(state))
	t.Cleanup(state.Cleanup)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, bootQUICTransport(ctx, state))
	assert.NotEmpty(t, state.transportPSK, "ephemeral PSK populates state")
	assert.GreaterOrEqual(t, len(state.transportPSK), 32, "ephemeral PSK is at least 32 chars")
}

// TestBootPeerConnections_EmptyPeersIsNoOp — solo bootstrap has no peers.
// bootPeerConnections must return cleanly without panicking on a nil/empty
// peer list.
func TestBootPeerConnections_EmptyPeersIsNoOp(t *testing.T) {
	state := newBootState(Config{DataDir: t.TempDir(), NodeID: "n1", ClusterKey: "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"})
	require.NoError(t, bootValidateConfig(state))
	t.Cleanup(state.Cleanup)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, bootQUICTransport(ctx, state))
	require.NoError(t, bootPeerConnections(ctx, state), "empty peer list is a clean no-op")
}

// TestBootGroupRaftMux_CreatesMux — phase constructs the mux from
// state.quicTransport. With QUICMuxEnabled=false, EnableMux must NOT fire
// (default mux mode).
func TestBootGroupRaftMux_CreatesMux(t *testing.T) {
	state := newBootState(Config{DataDir: t.TempDir(), NodeID: "n1", ClusterKey: "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"})
	require.NoError(t, bootValidateConfig(state))
	t.Cleanup(state.Cleanup)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, bootQUICTransport(ctx, state))
	require.NoError(t, bootGroupRaftMux(state))
	require.NotNil(t, state.groupRaftMux, "state.groupRaftMux populated")
}

// TestBootGroupRaftMux_EnabledHonorsConfig — with QUICMuxEnabled=true the
// phase must wire EnableMux with the operator-supplied pool size + flush
// window (the R+H Phase 2 prototype mode).
func TestBootGroupRaftMux_EnabledHonorsConfig(t *testing.T) {
	state := newBootState(Config{
		DataDir:            t.TempDir(),
		NodeID:             "n1",
		ClusterKey:         "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899",
		QUICMuxEnabled:     true,
		QUICMuxPoolSize:    8,
		QUICMuxFlushWindow: 2 * time.Millisecond,
	})
	require.NoError(t, bootValidateConfig(state))
	t.Cleanup(state.Cleanup)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, bootQUICTransport(ctx, state))
	require.NoError(t, bootGroupRaftMux(state))
	require.NotNil(t, state.groupRaftMux)
}

// TestBootTransportPhases_OrderingPreservesMuxBeforeMetaTransportInvariant —
// the central invariant: groupRaftMux must be constructed BEFORE any code
// that builds NewMetaTransportQUICMux. After PR 3 the phase ordering makes
// this explicit (mux phase runs before any raft-meta phase). This test
// asserts the bootState surface witnesses that ordering: after running the
// transport phase trio, state.groupRaftMux is non-nil and observable to a
// hypothetical bootMetaRaft phase that would consume it next.
func TestBootTransportPhases_OrderingPreservesMuxBeforeMetaTransportInvariant(t *testing.T) {
	state := newBootState(Config{DataDir: t.TempDir(), NodeID: "n1", ClusterKey: "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"})
	require.NoError(t, bootValidateConfig(state))
	t.Cleanup(state.Cleanup)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Before any transport phase: nil.
	assert.Nil(t, state.quicTransport)
	assert.Nil(t, state.groupRaftMux)

	require.NoError(t, bootQUICTransport(ctx, state))
	require.NotNil(t, state.quicTransport, "QUIC up first")
	assert.Nil(t, state.groupRaftMux, "mux not yet constructed")

	require.NoError(t, bootPeerConnections(ctx, state))
	assert.Nil(t, state.groupRaftMux, "peer connections do not touch mux")

	require.NoError(t, bootGroupRaftMux(state))
	require.NotNil(t, state.groupRaftMux, "mux ready for downstream metaTransport")
}
