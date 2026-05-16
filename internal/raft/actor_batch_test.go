package raft

import (
	"testing"

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
