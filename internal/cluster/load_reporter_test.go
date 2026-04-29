package cluster

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockLoadProposer struct {
	calls atomic.Int32
	mu    sync.Mutex
	last  []LoadStatEntry
}

func (m *mockLoadProposer) ProposeLoadSnapshot(_ context.Context, entries []LoadStatEntry) error {
	m.calls.Add(1)
	m.mu.Lock()
	m.last = entries
	m.mu.Unlock()
	return nil
}

func (m *mockLoadProposer) getLast() []LoadStatEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.last
}

func (m *mockLoadProposer) IsLeader() bool { return true }

type nonLeaderProposer struct{ mockLoadProposer }

func (n *nonLeaderProposer) IsLeader() bool { return false }

func TestLoadReporter_FiresOnTick(t *testing.T) {
	store := NewNodeStatsStore(5 * time.Minute)
	store.Set(NodeStats{NodeID: "n1", DiskUsedPct: 55.0, DiskAvailBytes: 8000, RequestsPerSec: 10, UpdatedAt: time.Now()})

	proposer := &mockLoadProposer{}
	r := NewLoadReporter("n1", store, proposer, 50*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	go r.Run(ctx)

	require.Eventually(t, func() bool {
		return proposer.calls.Load() >= 2
	}, 500*time.Millisecond, 10*time.Millisecond, "must fire at least twice within 500ms")

	last := proposer.getLast()
	require.NotEmpty(t, last)
	assert.InDelta(t, 55.0, last[0].DiskUsedPct, 0.01)
}

func TestLoadReporter_SkipsIfNotLeader(t *testing.T) {
	store := NewNodeStatsStore(5 * time.Minute)
	store.Set(NodeStats{NodeID: "n1", DiskUsedPct: 50.0, DiskAvailBytes: 5000, UpdatedAt: time.Now()})

	proposer := &nonLeaderProposer{}
	r := NewLoadReporterWithLeaderCheck("n1", store, &proposer.mockLoadProposer, proposer, 50*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	go r.Run(ctx)

	<-ctx.Done()
	assert.Equal(t, int32(0), proposer.calls.Load(), "non-leader must not propose")
}

func TestLoadReporter_EmptyStoreSkips(t *testing.T) {
	store := NewNodeStatsStore(5 * time.Minute)
	proposer := &mockLoadProposer{}
	r := NewLoadReporter("n1", store, proposer, 50*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	go r.Run(ctx)

	<-ctx.Done()
	assert.Equal(t, int32(0), proposer.calls.Load(), "empty store must not propose")
}
