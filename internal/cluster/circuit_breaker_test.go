package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCircuitBreaker_OpenOnDiskFull(t *testing.T) {
	cb := newCircuitBreaker()
	cb.update(NodeStats{NodeID: "n1", DiskUsedPct: 91.0}, 90.0)
	assert.False(t, cb.allow(), "CB must be open when DiskUsedPct >= threshold")
}

func TestCircuitBreaker_RecoveryOnGossip(t *testing.T) {
	cb := newCircuitBreaker()
	cb.update(NodeStats{NodeID: "n1", DiskUsedPct: 95.0}, 90.0) // open
	assert.False(t, cb.allow())

	cb.update(NodeStats{NodeID: "n1", DiskUsedPct: 80.0}, 90.0) // recovered
	assert.True(t, cb.allow(), "CB must close when DiskUsedPct < threshold")
}

func TestCircuitBreaker_AllowBlocks(t *testing.T) {
	cb := newCircuitBreaker()
	assert.True(t, cb.allow(), "new CB must allow (closed)")

	cb.update(NodeStats{NodeID: "n1", DiskUsedPct: 100.0}, 90.0)
	assert.False(t, cb.allow(), "CB must block when open")
}

func TestCircuitBreaker_ThresholdBoundary(t *testing.T) {
	cb := newCircuitBreaker()
	// exactly at threshold → open
	cb.update(NodeStats{NodeID: "n1", DiskUsedPct: 90.0}, 90.0)
	assert.False(t, cb.allow(), "CB must open at exactly threshold")

	// one tick below threshold → closed
	cb2 := newCircuitBreaker()
	cb2.update(NodeStats{NodeID: "n1", DiskUsedPct: 89.99}, 90.0)
	assert.True(t, cb2.allow(), "CB must stay closed below threshold")
}

func TestCircuitBreaker_ThresholdHotReloadAcrossUpdates(t *testing.T) {
	cb := newCircuitBreaker()
	// First tick at threshold=85%: 88% disk → open.
	cb.update(NodeStats{DiskUsedPct: 88.0}, 85.0)
	require.False(t, cb.allow())
	// Threshold raised to 90% (hot-reload), same disk → closed.
	cb.update(NodeStats{DiskUsedPct: 88.0}, 90.0)
	require.True(t, cb.allow())
	// Lowered to 80% — open again.
	cb.update(NodeStats{DiskUsedPct: 88.0}, 80.0)
	require.False(t, cb.allow())
}
