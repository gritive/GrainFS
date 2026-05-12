package raft

// ported_persistence_test.go: after-Stop RPC safety tests ported from
// internal/raft/persistence_test.go (PR 19 batch 1).
//
// v1 equivalents:
//   TestNode_RequestVoteAfterCloseDoesNotPersist
//   TestNode_AppendEntriesAfterCloseDoesNotPersist
//   TestNode_InstallSnapshotAfterCloseDoesNotPersist
//
// Mapping: v1 node.Close() → v2 node.Stop().
// v2 already documents this behaviour in node.go:311 ("matches v1's
// stopped-node behaviour"). These tests provide explicit regression coverage.

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestNode_RequestVoteAfterStopDoesNotPanic verifies that HandleRequestVote
// called after Stop returns a zero-value reply without panicking. The actor
// may have already flushed its cmdCh, so the call must not block or attempt
// to write to a closed store.
func TestNode_RequestVoteAfterStopDoesNotPanic(t *testing.T) {
	n, err := NewNode(Config{ID: "A", Peers: []string{"B"}})
	require.NoError(t, err)
	n.Start()
	go func() {
		for range n.ApplyCh() {
		}
	}()

	n.Stop()

	require.NotPanics(t, func() {
		reply := n.HandleRequestVote(&RequestVoteArgs{
			Term:        2,
			CandidateID: "B",
		})
		require.NotNil(t, reply)
		require.False(t, reply.VoteGranted)
	})
}

// TestNode_AppendEntriesAfterStopDoesNotPanic verifies that HandleAppendEntries
// called after Stop returns a reply without panicking or blocking.
func TestNode_AppendEntriesAfterStopDoesNotPanic(t *testing.T) {
	n, err := NewNode(Config{ID: "A", Peers: []string{"B"}})
	require.NoError(t, err)
	n.Start()
	go func() {
		for range n.ApplyCh() {
		}
	}()

	n.Stop()

	require.NotPanics(t, func() {
		reply := n.HandleAppendEntries(&AppendEntriesArgs{
			Term:     2,
			LeaderID: "B",
		})
		require.NotNil(t, reply)
		require.False(t, reply.Success)
	})
}

// TestNode_InstallSnapshotAfterStopDoesNotPanic verifies that
// HandleInstallSnapshot called after Stop returns a reply without panicking.
func TestNode_InstallSnapshotAfterStopDoesNotPanic(t *testing.T) {
	n, err := NewNode(Config{ID: "A", Peers: []string{"B"}})
	require.NoError(t, err)
	n.Start()
	go func() {
		for range n.ApplyCh() {
		}
	}()

	n.Stop()

	require.NotPanics(t, func() {
		reply := n.HandleInstallSnapshot(&InstallSnapshotArgs{
			Term:              2,
			LeaderID:          "B",
			LastIncludedIndex: 10,
			LastIncludedTerm:  2,
			Data:              []byte("snapshot"),
			Configuration:     []string{"A"},
		})
		require.NotNil(t, reply)
	})
}

// TestNode_StopIsIdempotent verifies that calling Stop multiple times is safe.
func TestNode_StopIsIdempotent(t *testing.T) {
	n, err := NewNode(Config{ID: "n1"})
	require.NoError(t, err)
	n.Start()

	go func() {
		for range n.ApplyCh() {
		}
	}()

	require.NoError(t, waitFor(time.Second, func() bool { return n.IsLeader() }))

	require.NotPanics(t, func() {
		n.Stop()
		n.Stop()
		n.Stop()
	})
}
