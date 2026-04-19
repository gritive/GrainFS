package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCircuitBreaker_OpenOnDiskFull(t *testing.T) {
	cb := newCircuitBreaker(0.90) // 90% threshold
	cb.update(NodeStats{NodeID: "n1", DiskUsedPct: 91.0})
	assert.False(t, cb.allow(), "CB must be open when DiskUsedPct >= threshold")
}

func TestCircuitBreaker_RecoveryOnGossip(t *testing.T) {
	cb := newCircuitBreaker(0.90)
	cb.update(NodeStats{NodeID: "n1", DiskUsedPct: 95.0}) // open
	assert.False(t, cb.allow())

	cb.update(NodeStats{NodeID: "n1", DiskUsedPct: 80.0}) // recovered
	assert.True(t, cb.allow(), "CB must close when DiskUsedPct < threshold")
}

func TestCircuitBreaker_AllowBlocks(t *testing.T) {
	cb := newCircuitBreaker(0.90)
	assert.True(t, cb.allow(), "new CB must allow (closed)")

	cb.update(NodeStats{NodeID: "n1", DiskUsedPct: 100.0})
	assert.False(t, cb.allow(), "CB must block when open")
}

func TestCircuitBreaker_ThresholdBoundary(t *testing.T) {
	cb := newCircuitBreaker(0.90)
	// exactly at threshold → open
	cb.update(NodeStats{NodeID: "n1", DiskUsedPct: 90.0})
	assert.False(t, cb.allow(), "CB must open at exactly threshold")

	// one tick below threshold → closed
	cb2 := newCircuitBreaker(0.90)
	cb2.update(NodeStats{NodeID: "n1", DiskUsedPct: 89.99})
	assert.True(t, cb2.allow(), "CB must stay closed below threshold")
}
