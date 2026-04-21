package raft

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockStore is a minimal in-memory LogStore that counts AppendEntries calls.
// Used to verify batching: 100 concurrent proposals → fewer than 100 persist calls.
type mockStore struct {
	mu        sync.Mutex
	entries   []LogEntry
	term      uint64
	votedFor  string
	callCount atomic.Int32
	appendErr error
}

func newMockStore() *mockStore {
	return &mockStore{entries: make([]LogEntry, 0)}
}

func (m *mockStore) AppendEntries(entries []LogEntry) error {
	m.callCount.Add(1)
	if m.appendErr != nil {
		return m.appendErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = append(m.entries, entries...)
	return nil
}

func (m *mockStore) GetEntry(index uint64) (*LogEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.entries {
		if m.entries[i].Index == index {
			e := m.entries[i]
			return &e, nil
		}
	}
	return nil, fmt.Errorf("entry %d not found", index)
}

func (m *mockStore) GetEntries(lo, hi uint64) ([]LogEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []LogEntry
	for _, e := range m.entries {
		if e.Index >= lo && e.Index <= hi {
			result = append(result, e)
		}
	}
	return result, nil
}

func (m *mockStore) LastIndex() (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.entries) == 0 {
		return 0, nil
	}
	return m.entries[len(m.entries)-1].Index, nil
}

func (m *mockStore) TruncateAfter(afterIndex uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, e := range m.entries {
		if e.Index <= afterIndex {
			m.entries[n] = e
			n++
		}
	}
	m.entries = m.entries[:n]
	return nil
}

func (m *mockStore) TruncateBefore(beforeIndex uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, e := range m.entries {
		if e.Index >= beforeIndex {
			m.entries[n] = e
			n++
		}
	}
	m.entries = m.entries[:n]
	return nil
}

func (m *mockStore) SaveState(term uint64, votedFor string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.term = term
	m.votedFor = votedFor
	return nil
}

func (m *mockStore) LoadState() (uint64, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.term, m.votedFor, nil
}

func (m *mockStore) SaveSnapshot(index, term uint64, data []byte) error { return nil }
func (m *mockStore) LoadSnapshot() (uint64, uint64, []byte, error)       { return 0, 0, nil, nil }
func (m *mockStore) Close() error                                         { return nil }

// newSingletonLeader creates a single-peer node that becomes leader quickly, for batcher tests.
func newSingletonLeader(t *testing.T, store ...LogStore) *Node {
	t.Helper()
	cfg := DefaultConfig("node1", nil)
	cfg.ElectionTimeout = 50 * time.Millisecond
	cfg.HeartbeatTimeout = 30 * time.Millisecond

	var node *Node
	if len(store) > 0 && store[0] != nil {
		node = NewNode(cfg, store[0])
	} else {
		node = NewNode(cfg)
	}
	node.SetTransport(
		func(_ string, _ *RequestVoteArgs) (*RequestVoteReply, error) { return nil, nil },
		func(_ string, _ *AppendEntriesArgs) (*AppendEntriesReply, error) { return nil, nil },
	)
	node.Start()
	t.Cleanup(func() { node.Stop() })
	go func() { for range node.ApplyCh() {} }()

	require.Eventually(t, func() bool {
		return node.State() == Leader
	}, 2*time.Second, 10*time.Millisecond, "node did not become leader")

	return node
}

// TestBatcher_HighLoad sends 100 concurrent proposals and asserts that batching
// reduces the number of AppendEntries (persist) calls below 100.
func TestBatcher_HighLoad(t *testing.T) {
	store := newMockStore()
	node := newSingletonLeader(t, store)

	const N = 100
	ctx := context.Background()
	indices := make([]uint64, N)
	errs := make([]error, N)

	var wg sync.WaitGroup
	wg.Add(N)
	for i := range N {
		go func(i int) {
			defer wg.Done()
			idx, err := node.ProposeWait(ctx, []byte(fmt.Sprintf("cmd%d", i)))
			indices[i] = idx
			errs[i] = err
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		require.NoError(t, err, "proposal %d failed", i)
	}

	// All indices must be unique and within [1, N].
	seen := make(map[uint64]bool)
	for _, idx := range indices {
		assert.False(t, seen[idx], "duplicate index %d", idx)
		seen[idx] = true
		assert.True(t, idx >= 1 && idx <= uint64(N), "index %d out of [1, %d]", idx, N)
	}

	// Batching effect: persist calls must be fewer than proposals.
	calls := store.callCount.Load()
	assert.Less(t, calls, int32(N),
		"batching did not occur: AppendEntries calls (%d) should be < proposals (%d)", calls, N)
	t.Logf("HighLoad: %d proposals → %d persist calls (%.1f%%)", N, calls, float64(calls)/float64(N)*100)
}

// TestBatcher_LowLoad verifies that a single proposal at low load flushes within 1ms
// and that the node reports low-load adaptive metrics (batchTimeout = 100µs).
func TestBatcher_LowLoad(t *testing.T) {
	node := newSingletonLeader(t)

	ctx := context.Background()
	start := time.Now()
	idx, err := node.ProposeWait(ctx, []byte("low-load"))
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.Equal(t, uint64(1), idx)
	assert.Less(t, elapsed, time.Millisecond,
		"low-load flush latency %v should be < 1ms", elapsed)

	// Verify adaptive metrics reflect the low-load state.
	// BatchMetrics() is the new accessor introduced by this feature.
	bm := node.BatchMetrics()
	assert.Equal(t, 100*time.Microsecond, bm.BatchTimeout,
		"low-load batchTimeout should be 100µs")
}

// TestBatcher_NotLeader verifies that ProposeWait on a non-leader returns ErrNotLeader
// without enqueuing to proposalCh.
func TestBatcher_NotLeader(t *testing.T) {
	cfg := DefaultConfig("follower", []string{"leader"})
	node := NewNode(cfg)
	// Do not Start() — node stays Follower.

	_, err := node.ProposeWait(context.Background(), []byte("cmd"))
	assert.ErrorIs(t, err, ErrNotLeader)
}

// TestBatcher_Shutdown verifies that a pending proposal receives ErrProposalFailed
// when the node stops, and that batcherLoop cleans up without goroutine leaks.
func TestBatcher_Shutdown(t *testing.T) {
	cfg := DefaultConfig("node1", nil)
	cfg.ElectionTimeout = 50 * time.Millisecond
	node := NewNode(cfg)
	node.SetTransport(
		func(_ string, _ *RequestVoteArgs) (*RequestVoteReply, error) { return nil, nil },
		func(_ string, _ *AppendEntriesArgs) (*AppendEntriesReply, error) { return nil, nil },
	)
	node.Start()
	go func() { for range node.ApplyCh() {} }()

	require.Eventually(t, func() bool {
		return node.State() == Leader
	}, 2*time.Second, 10*time.Millisecond)

	// Inject a proposal directly into proposalCh to test the shutdown drain path.
	doneCh := make(chan proposalResult, 1)
	p := proposal{
		command: []byte("pending"),
		doneCh:  doneCh,
		ctx:     context.Background(),
	}
	select {
	case node.proposalCh <- p:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("proposalCh full or not started")
	}

	node.Stop()

	select {
	case result := <-doneCh:
		assert.ErrorIs(t, result.err, ErrProposalFailed,
			"pending proposal should return ErrProposalFailed on shutdown")
	case <-time.After(500 * time.Millisecond):
		t.Fatal("pending proposal not flushed after node.Stop()")
	}
}

// TestBatcher_ReplicationTrigger verifies that flushBatch triggers immediate replication
// (well below HeartbeatTimeout = 30ms).
func TestBatcher_ReplicationTrigger(t *testing.T) {
	var replicateCalls atomic.Int32

	cfg := DefaultConfig("node1", []string{"node2"})
	cfg.ElectionTimeout = 50 * time.Millisecond
	cfg.HeartbeatTimeout = 30 * time.Millisecond
	node := NewNode(cfg)
	node.SetTransport(
		func(_ string, _ *RequestVoteArgs) (*RequestVoteReply, error) {
			return &RequestVoteReply{Term: 0, VoteGranted: true}, nil
		},
		func(_ string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
			if len(args.Entries) > 0 {
				replicateCalls.Add(1)
			}
			return &AppendEntriesReply{Term: args.Term, Success: true}, nil
		},
	)
	node.Start()
	defer node.Stop()
	go func() { for range node.ApplyCh() {} }()

	require.Eventually(t, func() bool {
		return node.State() == Leader
	}, 2*time.Second, 10*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := node.ProposeWait(ctx, []byte("trigger"))
	require.NoError(t, err)

	// Replication must happen within 5ms of ProposeWait returning — not waiting
	// for the 30ms HeartbeatTimeout. This proves the replicationCh trigger works.
	require.Eventually(t, func() bool {
		return replicateCalls.Load() > 0
	}, 5*time.Millisecond, 100*time.Microsecond,
		"replication should be triggered immediately after flush, not wait for heartbeat")
}

// TestAdaptiveMetrics_Transition verifies EWMA threshold transitions and maxBatch/batchTimeout.
func TestAdaptiveMetrics_Transition(t *testing.T) {
	cases := []struct {
		name        string
		ewmaRate    float64
		wantTimeout time.Duration
		wantMax     int
	}{
		{"low (<100 req/s)", 50.0, 100 * time.Microsecond, 4},
		{"medium (100-500 req/s)", 300.0, 1 * time.Millisecond, 32},
		{"high (>500 req/s)", 600.0, 5 * time.Millisecond, 128},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := adaptiveMetrics{ewmaRate: tc.ewmaRate, alpha: 0.3}
			assert.Equal(t, tc.wantTimeout, m.batchTimeout())
			assert.Equal(t, tc.wantMax, m.maxBatch())
		})
	}

	// EWMA update: newRate = alpha*instantRate + (1-alpha)*ewmaRate
	// With ewmaRate=0, alpha=0.3, 10 entries over ~1s → newRate ≈ 3.0
	m := adaptiveMetrics{ewmaRate: 0, alpha: 0.3, lastFlushAt: time.Now().Add(-time.Second)}
	m.update(10)
	assert.InDelta(t, 3.0, m.ewmaRate, 1.0, "EWMA update: expected ~3.0")
}

// TestBatcher_PersistPanic verifies the persist-failure-must-panic invariant
// by calling persistLogEntries directly with a failing store.
// This invariant is independent of the batcher: any path that calls
// persistLogEntries must panic on error (prior learning: raft-persist-error-discarded).
func TestBatcher_PersistPanic(t *testing.T) {
	store := newMockStore()
	store.appendErr = fmt.Errorf("simulated disk full")

	cfg := DefaultConfig("node1", nil)
	node := NewNode(cfg, store)

	// Manually set leader state so persistLogEntries runs (not a no-op).
	node.mu.Lock()
	node.state = Leader
	node.currentTerm = 1
	node.matchIndex[node.id] = 0
	entry := LogEntry{Term: 1, Index: 1, Command: []byte("cmd")}
	node.log = append(node.log, entry)
	node.mu.Unlock()

	assert.Panics(t, func() {
		node.mu.Lock()
		defer node.mu.Unlock()
		node.persistLogEntries([]LogEntry{entry})
	})
}
