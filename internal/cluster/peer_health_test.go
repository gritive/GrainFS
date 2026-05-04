package cluster

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPeerHealth_NewPeersAreHealthy(t *testing.T) {
	ph := NewPeerHealth([]string{"node-a", "node-b"}, 1*time.Second)
	assert.True(t, ph.IsHealthy("node-a"))
	assert.True(t, ph.IsHealthy("node-b"))
}

func TestPeerHealth_MarkUnhealthy(t *testing.T) {
	ph := NewPeerHealth([]string{"node-a", "node-b"}, 1*time.Second)

	ph.MarkUnhealthy("node-a")
	assert.False(t, ph.IsHealthy("node-a"))
	assert.True(t, ph.IsHealthy("node-b"))
}

func TestPeerHealth_RecoverAfterCooldown(t *testing.T) {
	ph := NewPeerHealth([]string{"node-a"}, 50*time.Millisecond)

	ph.MarkUnhealthy("node-a")
	assert.False(t, ph.IsHealthy("node-a"))

	time.Sleep(60 * time.Millisecond)
	assert.True(t, ph.IsHealthy("node-a"))
}

func TestPeerHealth_MarkHealthy(t *testing.T) {
	ph := NewPeerHealth([]string{"node-a"}, 1*time.Second)

	ph.MarkUnhealthy("node-a")
	assert.False(t, ph.IsHealthy("node-a"))

	ph.MarkHealthy("node-a")
	assert.True(t, ph.IsHealthy("node-a"))
}

func TestPeerHealth_HealthyPeers(t *testing.T) {
	ph := NewPeerHealth([]string{"node-a", "node-b", "node-c"}, 1*time.Second)

	ph.MarkUnhealthy("node-b")
	healthy := ph.HealthyPeers()
	require.Len(t, healthy, 2)
	assert.Contains(t, healthy, "node-a")
	assert.Contains(t, healthy, "node-c")
}

func TestPeerHealth_UnknownPeerIsHealthy(t *testing.T) {
	ph := NewPeerHealth(nil, 1*time.Second)
	// Unknown peers default to healthy (conservative — try them)
	assert.True(t, ph.IsHealthy("unknown-node"))
}

func TestPeerHealth_MarkUnhealthy_TransitionReturn(t *testing.T) {
	ph := NewPeerHealth([]string{"node-a"}, 1*time.Second)

	// First MarkUnhealthy on a healthy peer = transition.
	assert.True(t, ph.MarkUnhealthy("node-a"), "first call should report transition")

	// Second MarkUnhealthy while still in cooldown = NOT a transition.
	assert.False(t, ph.MarkUnhealthy("node-a"), "second call within cooldown should not report transition")
}

func TestPeerHealth_MarkHealthy_TransitionReturn(t *testing.T) {
	ph := NewPeerHealth([]string{"node-a"}, 1*time.Second)

	// MarkHealthy on a peer that's already healthy = NOT a transition.
	assert.False(t, ph.MarkHealthy("node-a"), "no-op MarkHealthy should not report transition")

	ph.MarkUnhealthy("node-a")
	// MarkHealthy on a peer in cooldown = transition.
	assert.True(t, ph.MarkHealthy("node-a"), "recovery should report transition")
}

func TestPeerHealth_Snapshot(t *testing.T) {
	ph := NewPeerHealth([]string{"node-a", "node-b", "node-c"}, 1*time.Second)
	ph.MarkUnhealthy("node-b")

	snap := ph.Snapshot()
	require.Len(t, snap, 3)

	// Snapshot is sorted by ID for deterministic output.
	assert.Equal(t, "node-a", snap[0].ID)
	assert.True(t, snap[0].Healthy)
	assert.Nil(t, snap[0].LastFailure)
	assert.Equal(t, int64(0), snap[0].CooldownRemainingMs)

	assert.Equal(t, "node-b", snap[1].ID)
	assert.False(t, snap[1].Healthy)
	require.NotNil(t, snap[1].LastFailure)
	assert.Greater(t, snap[1].CooldownRemainingMs, int64(0))

	assert.Equal(t, "node-c", snap[2].ID)
	assert.True(t, snap[2].Healthy)
}
