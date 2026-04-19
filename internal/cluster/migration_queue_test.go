package cluster

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMigrationQueue_PriorityOrder: 80% node dequeued before 50% node.
func TestMigrationQueue_PriorityOrder(t *testing.T) {
	q := NewMigrationPriorityQueue(10) // high rate: don't interfere

	now := time.Now()
	q.upsertAt("node-a", 80.0, now)
	q.upsertAt("node-b", 50.0, now)

	first, ok := q.TryDequeue()
	require.True(t, ok)
	assert.Equal(t, "node-a", first, "80%% node must dequeue first")

	second, ok := q.TryDequeue()
	require.True(t, ok)
	assert.Equal(t, "node-b", second)
}

// TestMigrationQueue_AgingFactor: item queued 10 min ago beats a newer high-pct item.
func TestMigrationQueue_AgingFactor(t *testing.T) {
	q := NewMigrationPriorityQueue(10)

	// node-a at 50% but queued 10 minutes ago
	q.upsertAt("node-a", 50.0, time.Now().Add(-10*time.Minute))
	// node-b at 80% but just queued
	q.upsertAt("node-b", 80.0, time.Now())

	// effectivePriority(node-a) = 50 × (1 + 10/10) = 100
	// effectivePriority(node-b) = 80 × (1 + 0/10)  = 80
	// node-a should win
	first, ok := q.TryDequeue()
	require.True(t, ok)
	assert.Equal(t, "node-a", first, "aged item must be promoted past fresher high-pct item")
}

// TestMigrationQueue_TokenBucket: rate=2/s — 3rd dequeue within 1s window fails.
func TestMigrationQueue_TokenBucket(t *testing.T) {
	q := NewMigrationPriorityQueue(2) // burst=2

	now := time.Now()
	q.upsertAt("node-a", 80.0, now)
	q.upsertAt("node-b", 70.0, now)
	q.upsertAt("node-c", 60.0, now)

	_, ok1 := q.TryDequeue()
	_, ok2 := q.TryDequeue()
	_, ok3 := q.TryDequeue() // token bucket exhausted

	assert.True(t, ok1, "first dequeue must succeed")
	assert.True(t, ok2, "second dequeue must succeed")
	assert.False(t, ok3, "third dequeue must fail — rate=2/s token bucket exhausted")
}

// TestMigrationQueue_Empty: empty queue returns false.
func TestMigrationQueue_Empty(t *testing.T) {
	q := NewMigrationPriorityQueue(10)
	_, ok := q.TryDequeue()
	assert.False(t, ok)
}

// TestMigrationQueue_Upsert_UpdatesPriority: updating diskUsedPct re-orders the queue.
func TestMigrationQueue_Upsert_UpdatesPriority(t *testing.T) {
	q := NewMigrationPriorityQueue(10)

	now := time.Now()
	q.upsertAt("node-a", 50.0, now)
	q.upsertAt("node-b", 80.0, now)

	// Now update node-a to 90%
	q.upsertAt("node-a", 90.0, now)

	first, ok := q.TryDequeue()
	require.True(t, ok)
	assert.Equal(t, "node-a", first, "updated 90%% node must beat 80%% node")
}

// TestMigrationQueue_SingleNode: single node is always dequeued.
func TestMigrationQueue_SingleNode(t *testing.T) {
	q := NewMigrationPriorityQueue(10)
	q.upsertAt("node-a", 75.0, time.Now())

	src, ok := q.TryDequeue()
	require.True(t, ok)
	assert.Equal(t, "node-a", src)

	// now empty
	_, ok = q.TryDequeue()
	assert.False(t, ok)
}
