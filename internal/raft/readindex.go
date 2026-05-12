package raft

import "context"

// ReadIndex implements the Raft §6.4 linearizable read protocol from the
// leader side. On return, the caller has a barrier index such that any
// FSM read taken after the FSM's lastApplied >= barrier sees a state at
// least as fresh as the moment ReadIndex was called.
//
// Caller contract:
//   - Caller must be FSM-side; Node does not track FSM lastApplied.
//   - Caller waits (via its own mechanism) until FSM lastApplied >= returned
//     index before serving the linearizable read.
//   - Single-voter clusters return commitIndex inline (no heartbeat round
//     needed; self-quorum confirms leadership at every commit).
//
// Returns ErrNotLeader if not currently leader (snapshot check + actor
// re-check). Returns ErrProposalFailed if leadership is lost while the
// request is queued (Leader→Follower step-down drains the queue).
// Returns ctx.Err() / ErrNodeStopped on cancellation / shutdown.
func (n *Node) ReadIndex(ctx context.Context) (uint64, error) {
	if !n.IsLeader() {
		return 0, ErrNotLeader
	}
	reply := make(chan readIndexResult, 1)
	select {
	case n.cmdCh <- command{kind: cmdReadIndex, riReply: reply}:
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-n.stopCh:
		return 0, ErrNodeStopped
	}
	select {
	case res := <-reply:
		return res.index, res.err
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-n.stopCh:
		return 0, ErrNodeStopped
	}
}
