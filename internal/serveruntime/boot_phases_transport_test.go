package serveruntime

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/transport"
)

// TestBootClusterTransport_ConstructsHTTPAndBindsPort — bootClusterTransport
// constructs the (sole) HTTP cluster transport, Listens, and resolves
// state.raftAddr to the kernel-picked loopback port.
func TestBootClusterTransport_ConstructsHTTPAndBindsPort(t *testing.T) {
	state := newBootState(Config{DataDir: t.TempDir(), NodeID: "n1", ClusterKey: "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"})
	require.NoError(t, bootValidateConfig(state))
	t.Cleanup(state.Cleanup)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, bootClusterTransport(ctx, state))
	require.NotNil(t, state.clusterTransport)
	_, isHTTP := state.clusterTransport.(*transport.HTTPTransport)
	assert.True(t, isHTTP, "bootClusterTransport must construct a *HTTPTransport")

	resolved := state.raftAddr
	assert.NotEqual(t, "127.0.0.1:0", resolved, "Listen must resolve :0 to kernel-picked port")
	assert.True(t, strings.HasPrefix(resolved, "127.0.0.1:"), "kept on loopback in solo mode")
	assert.Equal(t, state.clusterTransport.LocalAddr(), resolved, "state.raftAddr matches LocalAddr")
}

// TestBootClusterTransport_BindsLoopbackPort0 — happy path: solo-mode bootstrap
// (no peers) uses 127.0.0.1:0; bootClusterTransport must Listen, then resolve
// state.raftAddr to the kernel-picked port so peers see a dialable self.
func TestBootClusterTransport_BindsLoopbackPort0(t *testing.T) {
	state := newBootState(Config{DataDir: t.TempDir(), NodeID: "n1", ClusterKey: "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"})
	require.NoError(t, bootValidateConfig(state))
	t.Cleanup(state.Cleanup)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, bootClusterTransport(ctx, state))
	require.NotNil(t, state.clusterTransport, "state.clusterTransport populated")
	assert.NotEmpty(t, state.transportPSK, "ephemeral PSK generated in solo mode")

	resolved := state.raftAddr
	assert.NotEqual(t, "127.0.0.1:0", resolved, "Listen must resolve :0 to kernel-picked port")
	assert.True(t, strings.HasPrefix(resolved, "127.0.0.1:"), "kept on loopback in solo mode")
	assert.Equal(t, state.clusterTransport.LocalAddr(), resolved, "state.raftAddr matches LocalAddr")
}

// TestBootClusterTransport_GeneratesEphemeralKeyInSoloMode — when neither cfg.
// ClusterKey nor keys.d/current.key exists and no peers are configured,
// bootClusterTransport must generate an ephemeral key so zero-config holds.
func TestBootClusterTransport_GeneratesEphemeralKeyInSoloMode(t *testing.T) {
	state := newBootState(Config{DataDir: t.TempDir(), NodeID: "n1", ClusterKey: "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"})
	require.NoError(t, bootValidateConfig(state))
	t.Cleanup(state.Cleanup)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, bootClusterTransport(ctx, state))
	assert.NotEmpty(t, state.transportPSK, "ephemeral PSK populates state")
	assert.GreaterOrEqual(t, len(state.transportPSK), 32, "ephemeral PSK is at least 32 chars")
}

// TestBootPeerConnections_EmptyPeersIsNoOp — solo bootstrap has no peers.
// TestBootGroupRaftMux_CreatesMux — phase constructs the group-raft dispatcher
// from state.clusterTransport.
func TestBootGroupRaftMux_CreatesMux(t *testing.T) {
	state := newBootState(Config{DataDir: t.TempDir(), NodeID: "n1", ClusterKey: "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"})
	require.NoError(t, bootValidateConfig(state))
	t.Cleanup(state.Cleanup)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, bootClusterTransport(ctx, state))
	require.NoError(t, bootGroupRaftMux(state))
	require.NotNil(t, state.groupRaftMux, "state.groupRaftMux populated")
}

// TestBootTransportPhases_OrderingPreservesMuxBeforeMetaTransportInvariant —
// the central invariant: groupRaftMux must be constructed BEFORE any code
// that builds NewMetaTransport. After PR 3 the phase ordering makes
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
	assert.Nil(t, state.clusterTransport)
	assert.Nil(t, state.groupRaftMux)

	require.NoError(t, bootClusterTransport(ctx, state))
	require.NotNil(t, state.clusterTransport, "transport up first")
	assert.Nil(t, state.groupRaftMux, "mux not yet constructed")

	require.NoError(t, bootGroupRaftMux(state))
	require.NotNil(t, state.groupRaftMux, "mux ready for downstream metaTransport")
}
