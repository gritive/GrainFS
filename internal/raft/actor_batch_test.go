package raft

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type countingLogStore struct {
	LogStore
	appendLens []int
}

func (s *countingLogStore) Append(entries []LogEntry) error {
	s.appendLens = append(s.appendLens, len(entries))
	return s.LogStore.Append(entries)
}

func TestHandleProposeBatchAppendsQueuedProposalsTogether(t *testing.T) {
	prev := proposeAppendBatchLimit
	proposeAppendBatchLimit = 8
	t.Cleanup(func() { proposeAppendBatchLimit = prev })

	store := &countingLogStore{LogStore: newMemLogStore()}
	n, err := NewNode(Config{ID: "n1", LogStore: store})
	require.NoError(t, err)
	n.st.state = Leader
	n.st.currentTerm = 1

	replies := make([]chan proposalResult, 8)
	for i := range replies {
		replies[i] = make(chan proposalResult, 1)
	}
	first := command{kind: cmdPropose, proposeCommand: []byte("cmd-0"), proposeReply: replies[0]}
	for i := 1; i < len(replies); i++ {
		n.cmdCh <- command{kind: cmdPropose, proposeCommand: []byte{byte(i)}, proposeReply: replies[i]}
	}

	postponed, ok := n.handleProposeBatch(first)
	require.False(t, ok, "only queued proposals were present, so no command should be postponed: %#v", postponed.kind)
	require.Equal(t, []int{8}, store.appendLens)

	for i, reply := range replies {
		got := <-reply
		require.NoError(t, got.err)
		require.Equal(t, uint64(i+1), got.index)
	}
	require.Equal(t, uint64(8), n.st.commitIndex)
}

// With linger disabled, the actor's drain stays non-blocking — a propose that
// arrives after the queue empties is NOT folded into the in-flight batch.
func TestHandleProposeBatchLingerDisabledCommitsImmediately(t *testing.T) {
	prevLinger := proposeLingerWindow
	proposeLingerWindow = 0
	t.Cleanup(func() { proposeLingerWindow = prevLinger })

	store := &countingLogStore{LogStore: newMemLogStore()}
	n, err := NewNode(Config{ID: "n1", LogStore: store})
	require.NoError(t, err)
	n.st.state = Leader
	n.st.currentTerm = 1

	reply := make(chan proposalResult, 1)
	first := command{kind: cmdPropose, proposeCommand: []byte("first"), proposeReply: reply}
	_, ok := n.handleProposeBatch(first)
	require.False(t, ok)
	require.Equal(t, []int{1}, store.appendLens)
	got := <-reply
	require.NoError(t, got.err)
}

// With linger enabled, a propose that arrives shortly after handleProposeBatch
// drains the queue must be folded into the same batch.
func TestHandleProposeBatchLingerCoalescesLateArrival(t *testing.T) {
	prevLinger := proposeLingerWindow
	proposeLingerWindow = 50 * time.Millisecond
	t.Cleanup(func() { proposeLingerWindow = prevLinger })

	store := &countingLogStore{LogStore: newMemLogStore()}
	n, err := NewNode(Config{ID: "n1", LogStore: store})
	require.NoError(t, err)
	n.st.state = Leader
	n.st.currentTerm = 1

	reply1 := make(chan proposalResult, 1)
	reply2 := make(chan proposalResult, 1)
	first := command{kind: cmdPropose, proposeCommand: []byte("first"), proposeReply: reply1}
	late := command{kind: cmdPropose, proposeCommand: []byte("late"), proposeReply: reply2}

	go func() {
		// Send the late propose after the actor has likely entered linger.
		// 2 ms is comfortably within the 50 ms linger window but past any
		// initial non-blocking drain.
		time.Sleep(2 * time.Millisecond)
		n.cmdCh <- late
	}()

	_, ok := n.handleProposeBatch(first)
	require.False(t, ok)
	require.Equal(t, []int{2}, store.appendLens, "linger window must fold the late propose into the same Append batch")

	got1 := <-reply1
	require.NoError(t, got1.err)
	require.Equal(t, uint64(1), got1.index)
	got2 := <-reply2
	require.NoError(t, got2.err)
	require.Equal(t, uint64(2), got2.index)
}

// When no second propose arrives within the linger window the actor must
// stop waiting and commit what it has — linger must not block indefinitely.
func TestHandleProposeBatchLingerExpiresWithoutNewArrival(t *testing.T) {
	prevLinger := proposeLingerWindow
	proposeLingerWindow = 5 * time.Millisecond
	t.Cleanup(func() { proposeLingerWindow = prevLinger })

	store := &countingLogStore{LogStore: newMemLogStore()}
	n, err := NewNode(Config{ID: "n1", LogStore: store})
	require.NoError(t, err)
	n.st.state = Leader
	n.st.currentTerm = 1

	reply := make(chan proposalResult, 1)
	first := command{kind: cmdPropose, proposeCommand: []byte("alone"), proposeReply: reply}

	start := time.Now()
	_, ok := n.handleProposeBatch(first)
	elapsed := time.Since(start)

	require.False(t, ok)
	require.Equal(t, []int{1}, store.appendLens)
	require.GreaterOrEqual(t, elapsed, 5*time.Millisecond, "linger should hold the actor for the configured window")
	require.Less(t, elapsed, 100*time.Millisecond, "linger must release after the deadline, not block forever")

	got := <-reply
	require.NoError(t, got.err)
}
