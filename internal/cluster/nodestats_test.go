package cluster

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNodeStatsStore_SetAndGet(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)

	store.Set(NodeStats{
		NodeID:         "node-a",
		DiskUsedPct:    60.0,
		DiskAvailBytes: 100 << 30,
		RequestsPerSec: 50.0,
	})

	stats, ok := store.Get("node-a")
	require.True(t, ok)
	assert.Equal(t, "node-a", stats.NodeID)
	assert.Equal(t, 60.0, stats.DiskUsedPct)
	assert.Equal(t, uint64(100<<30), stats.DiskAvailBytes)
	assert.Equal(t, 50.0, stats.RequestsPerSec)
}

func TestNodeStatsStore_GetMissing(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)

	_, ok := store.Get("node-x")
	assert.False(t, ok)
}

func TestNodeStatsStore_TTL(t *testing.T) {
	store := NewNodeStatsStore(50 * time.Millisecond)

	store.Set(NodeStats{NodeID: "node-a", DiskUsedPct: 50.0})

	_, ok := store.Get("node-a")
	require.True(t, ok)

	time.Sleep(70 * time.Millisecond)

	_, ok = store.Get("node-a")
	assert.False(t, ok, "expired entry should not be returned")
}

func TestNodeStatsStore_TTL_GetAll(t *testing.T) {
	store := NewNodeStatsStore(50 * time.Millisecond)

	store.Set(NodeStats{NodeID: "node-a", DiskUsedPct: 50.0})
	store.Set(NodeStats{NodeID: "node-b", DiskUsedPct: 40.0})

	time.Sleep(70 * time.Millisecond)

	all := store.GetAll()
	assert.Empty(t, all, "expired entries should not appear in GetAll")
}

func TestNodeStatsStore_GetAll(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)

	store.Set(NodeStats{NodeID: "node-a", DiskUsedPct: 75.0})
	store.Set(NodeStats{NodeID: "node-b", DiskUsedPct: 40.0})
	store.Set(NodeStats{NodeID: "node-c", DiskUsedPct: 55.0})

	all := store.GetAll()
	assert.Len(t, all, 3)

	byID := make(map[string]NodeStats)
	for _, s := range all {
		byID[s.NodeID] = s
	}
	assert.Equal(t, 75.0, byID["node-a"].DiskUsedPct)
	assert.Equal(t, 40.0, byID["node-b"].DiskUsedPct)
	assert.Equal(t, 55.0, byID["node-c"].DiskUsedPct)
}

func TestNodeStatsStore_Overwrite(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)

	store.Set(NodeStats{NodeID: "node-a", DiskUsedPct: 50.0, RequestsPerSec: 10.0})
	store.Set(NodeStats{NodeID: "node-a", DiskUsedPct: 80.0, RequestsPerSec: 200.0})

	stats, ok := store.Get("node-a")
	require.True(t, ok)
	assert.Equal(t, 80.0, stats.DiskUsedPct)
	assert.Equal(t, 200.0, stats.RequestsPerSec)
}

func TestNodeStatsStore_Len(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)

	assert.Equal(t, 0, store.Len())

	store.Set(NodeStats{NodeID: "node-a"})
	store.Set(NodeStats{NodeID: "node-b"})
	assert.Equal(t, 2, store.Len())
}

func TestNodeStatsStore_Len_ExcludesExpired(t *testing.T) {
	store := NewNodeStatsStore(50 * time.Millisecond)

	store.Set(NodeStats{NodeID: "node-a"})
	time.Sleep(70 * time.Millisecond)

	assert.Equal(t, 0, store.Len())
}

func TestNodeStatsStore_Concurrent(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)

	var wg sync.WaitGroup
	for i := range 20 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			nodeID := "node-a"
			if i%2 == 0 {
				nodeID = "node-b"
			}
			store.Set(NodeStats{NodeID: nodeID, DiskUsedPct: float64(i)})
			store.Get(nodeID)
			store.GetAll()
			store.Len()
		}(i)
	}
	wg.Wait()
}

func TestNodeStatsStore_UpdatedAt(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)

	before := time.Now()
	store.Set(NodeStats{NodeID: "node-a", DiskUsedPct: 50.0})
	after := time.Now()

	stats, ok := store.Get("node-a")
	require.True(t, ok)
	assert.True(t, !stats.UpdatedAt.Before(before))
	assert.True(t, !stats.UpdatedAt.After(after))
}

func TestNodeStatsStore_PurgesExpiredOnSet(t *testing.T) {
	store := NewNodeStatsStore(50 * time.Millisecond)

	store.Set(NodeStats{NodeID: "node-a"})
	time.Sleep(70 * time.Millisecond)

	// trigger purge via a new Set
	store.Set(NodeStats{NodeID: "node-b"})

	store.mu.RLock()
	_, stillInMap := store.stats["node-a"]
	store.mu.RUnlock()

	assert.False(t, stillInMap, "expired entry should be purged from map on Set()")
}

func TestNodeStatsStore_ClampsInvalidValues(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)

	store.Set(NodeStats{NodeID: "bad", DiskUsedPct: -5.0, RequestsPerSec: -100.0})
	s, ok := store.Get("bad")
	require.True(t, ok)
	assert.Equal(t, 0.0, s.DiskUsedPct, "negative DiskUsedPct should be clamped to 0")
	assert.Equal(t, 0.0, s.RequestsPerSec, "negative RequestsPerSec should be clamped to 0")

	store.Set(NodeStats{NodeID: "over", DiskUsedPct: 150.0, RequestsPerSec: 999.0})
	s, ok = store.Get("over")
	require.True(t, ok)
	assert.Equal(t, 100.0, s.DiskUsedPct, "DiskUsedPct > 100 should be clamped to 100")
	assert.Equal(t, 999.0, s.RequestsPerSec, "valid RequestsPerSec should pass through unchanged")
}

func TestNodeStatsStore_TTL_PartialExpiry(t *testing.T) {
	store := NewNodeStatsStore(50 * time.Millisecond)

	store.Set(NodeStats{NodeID: "node-a", DiskUsedPct: 50.0})
	time.Sleep(70 * time.Millisecond)

	// node-b는 TTL 만료 후에 추가 — 유효해야 함
	store.Set(NodeStats{NodeID: "node-b", DiskUsedPct: 40.0})

	_, ok := store.Get("node-a")
	assert.False(t, ok, "node-a should be expired")

	_, ok = store.Get("node-b")
	assert.True(t, ok, "node-b should still be valid")

	all := store.GetAll()
	assert.Len(t, all, 1)
}
