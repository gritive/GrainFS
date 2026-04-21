package raft

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProposeWait_Singleton(t *testing.T) {
	// Single node = immediate leader, immediate commit
	cfg := DefaultConfig("node1", nil)
	cfg.ElectionTimeout = 50 * time.Millisecond
	node := NewNode(cfg)
	node.SetTransport(
		func(peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
			return nil, nil
		},
		func(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
			return nil, nil
		},
	)
	node.Start()
	defer node.Stop()

	// Wait for leader
	require.Eventually(t, func() bool {
		return node.State() == Leader
	}, 2*time.Second, 10*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	idx, err := node.ProposeWait(ctx, []byte("cmd1"))
	require.NoError(t, err)
	assert.Equal(t, uint64(1), idx)

	// Second proposal
	idx2, err := node.ProposeWait(ctx, []byte("cmd2"))
	require.NoError(t, err)
	assert.Equal(t, uint64(2), idx2)
}

func TestProposeWait_NotLeader(t *testing.T) {
	cfg := DefaultConfig("follower1", []string{"leader1"})
	node := NewNode(cfg)
	node.SetTransport(
		func(peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
			return &RequestVoteReply{Term: 0, VoteGranted: false}, nil
		},
		func(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
			return &AppendEntriesReply{Term: 0, Success: false}, nil
		},
	)
	// Don't start — stays as Follower

	ctx := context.Background()
	_, err := node.ProposeWait(ctx, []byte("cmd"))
	assert.ErrorIs(t, err, ErrNotLeader)
}

func TestProposeWait_ContextCancelled(t *testing.T) {
	// Create a 3-node cluster but only start one — it will become candidate
	// but never leader (can't get majority)
	cfg := DefaultConfig("node1", []string{"node2", "node3"})
	cfg.ElectionTimeout = 50 * time.Millisecond
	node := NewNode(cfg)
	node.SetTransport(
		func(peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
			return &RequestVoteReply{Term: 0, VoteGranted: false}, nil
		},
		func(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
			return &AppendEntriesReply{Term: 0, Success: false}, nil
		},
	)

	// Force leader state for testing context cancellation
	node.mu.Lock()
	node.state = Leader
	node.currentTerm = 1
	node.matchIndex["node1"] = 0
	node.mu.Unlock()

	// Don't start the apply/commit loops — proposal will be proposed but never committed
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := node.ProposeWait(ctx, []byte("cmd"))
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}
