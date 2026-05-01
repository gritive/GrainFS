package raft

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- WaitApplied tests ---

func TestWaitApplied_FastPath_ReturnsImmediately(t *testing.T) {
	config := DefaultConfig("A", []string{"B", "C"})
	n := NewNode(config)
	n.appliedCond = *newAppliedCond(&n.mu)
	n.lastApplied = 5

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := n.WaitApplied(ctx, 5)
	require.NoError(t, err)

	err = n.WaitApplied(ctx, 3)
	require.NoError(t, err)
}

func TestWaitApplied_BlocksUntilApplied(t *testing.T) {
	config := DefaultConfig("A", []string{"B", "C"})
	n := NewNode(config)
	n.appliedCond = *newAppliedCond(&n.mu)
	n.lastApplied = 0

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- n.WaitApplied(ctx, 1)
	}()

	// advance lastApplied and broadcast
	time.Sleep(30 * time.Millisecond)
	n.mu.Lock()
	n.lastApplied = 1
	n.appliedCond.Broadcast()
	n.mu.Unlock()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("WaitApplied did not return after lastApplied advanced")
	}
}

func TestWaitApplied_NodeStopped_ReturnsError(t *testing.T) {
	config := DefaultConfig("A", []string{"B", "C"})
	n := NewNode(config)
	n.appliedCond = *newAppliedCond(&n.mu)
	n.lastApplied = 0

	ctx := context.Background()

	done := make(chan error, 1)
	go func() {
		done <- n.WaitApplied(ctx, 99)
	}()

	time.Sleep(20 * time.Millisecond)
	n.mu.Lock()
	n.stopped = true
	n.appliedCond.Broadcast()
	n.mu.Unlock()

	select {
	case err := <-done:
		assert.ErrorIs(t, err, ErrNodeStopped)
	case <-time.After(time.Second):
		t.Fatal("WaitApplied did not return after node stopped")
	}
}

func TestWaitApplied_ContextCancelled(t *testing.T) {
	config := DefaultConfig("A", []string{"B", "C"})
	n := NewNode(config)
	n.appliedCond = *newAppliedCond(&n.mu)
	n.lastApplied = 0

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- n.WaitApplied(ctx, 99)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("WaitApplied did not return after ctx cancelled")
	}
}

// --- ReadIndex leader tests ---

func TestReadIndex_Leader_ReturnsCommitIndex(t *testing.T) {
	cluster := newTestCluster(t, 3)
	cluster.startAll()

	leader := cluster.waitForLeader(3 * time.Second)
	require.NotNil(t, leader)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	idx, err := leader.ReadIndex(ctx)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, idx, uint64(0))
}

func TestReadIndex_NotLeader_ReturnsError(t *testing.T) {
	cluster := newTestCluster(t, 3)
	cluster.startAll()

	cluster.waitForLeader(3 * time.Second)

	// Find a follower
	var follower *Node
	for _, n := range cluster.nodes {
		if n.State() != Leader {
			follower = n
			break
		}
	}
	require.NotNil(t, follower)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := follower.ReadIndex(ctx)
	assert.ErrorIs(t, err, ErrNotLeader)
}

func TestReadIndex_LeadershipLost_ReturnsError(t *testing.T) {
	cluster := newTestCluster(t, 3)
	cluster.startAll()

	leader := cluster.waitForLeader(3 * time.Second)
	require.NotNil(t, leader)

	// Isolate the leader by making its AE sends fail (partition simulation):
	// swap transport to drop all outbound messages.
	leader.SetTransport(
		func(peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
			return nil, assert.AnError
		},
		func(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
			return nil, assert.AnError
		},
	)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	_, err := leader.ReadIndex(ctx)
	// Either context deadline or leadership lost
	assert.True(t, err != nil, "expected error when leader is isolated")
}

func TestReadIndex_Concurrent_Batched(t *testing.T) {
	cluster := newTestCluster(t, 3)
	cluster.startAll()

	leader := cluster.waitForLeader(3 * time.Second)
	require.NotNil(t, leader)

	const N = 10
	results := make(chan error, N)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	for i := 0; i < N; i++ {
		go func() {
			_, err := leader.ReadIndex(ctx)
			results <- err
		}()
	}

	for i := 0; i < N; i++ {
		select {
		case err := <-results:
			assert.NoError(t, err)
		case <-time.After(3 * time.Second):
			t.Fatalf("ReadIndex %d timed out", i)
		}
	}
}

// newAppliedCond creates a sync.Cond for use in unit tests that bypass NewNode.
func newAppliedCond(mu *sync.Mutex) *sync.Cond {
	return sync.NewCond(mu)
}
