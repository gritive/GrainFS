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
