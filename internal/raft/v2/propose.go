package raftv2

import (
	"context"
	"fmt"
)

// Propose submits a command for replication and returns once the actor has
// accepted it onto cmdCh. Returns ErrNotLeader if the snapshot says we are
// not leader; the actor re-checks on receive (the snapshot may be stale).
//
// PR 1 only — does not wait for commit. Use ProposeWait to block until the
// entry is committed.
func (n *Node) Propose(cmd []byte) error {
	if !n.IsLeader() {
		return ErrNotLeader
	}
	select {
	case n.cmdCh <- command{kind: cmdPropose, proposeCommand: cmd}:
		return nil
	case <-n.stopCh:
		return ErrNodeStopped
	}
}

// ProposeWait submits a command and blocks until it is committed (entry index
// returned) or the context is cancelled. PR 1 commits synchronously inside
// the actor (single-voter), so the wait is bounded by actor scheduling.
func (n *Node) ProposeWait(ctx context.Context, cmd []byte) (uint64, error) {
	if !n.IsLeader() {
		return 0, ErrNotLeader
	}
	reply := make(chan proposalResult, 1)
	select {
	case n.cmdCh <- command{kind: cmdPropose, proposeCommand: cmd, proposeReply: reply}:
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-n.stopCh:
		return 0, ErrNodeStopped
	}

	select {
	case res := <-reply:
		if res.err != nil {
			return 0, fmt.Errorf("propose: %w", res.err)
		}
		return res.index, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-n.stopCh:
		return 0, ErrNodeStopped
	}
}
