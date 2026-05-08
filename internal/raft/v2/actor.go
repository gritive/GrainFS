package raftv2

// command is the unit of work passed from public methods to the actor
// goroutine over cmdCh. The actor handles each command serially; reply (if
// any) is delivered via the embedded reply channel inside cmd.
type command struct {
	kind cmdKind

	// proposeCmd payload (kind == cmdPropose).
	proposeCommand []byte
	proposeType    LogEntryType
	proposeReply   chan proposalResult
}

type cmdKind int

const (
	cmdPropose cmdKind = iota
)

// proposalResult is delivered to ProposeWait callers when their entry commits
// (or fails). PR 1 single-node always succeeds synchronously.
type proposalResult struct {
	index uint64
	err   error
}

// run is the actor goroutine. It is the sole writer of actorState and the
// sole publisher of readState. All state transitions flow through this loop.
func (n *Node) run() {
	defer close(n.doneCh)

	// Bootstrap: a single-voter cluster (Peers empty == only self) auto-
	// promotes to Leader at term 1. This is a PR 1 shortcut; real elections
	// arrive in PR 4-5. The condition follows v1's "Peers excludes self"
	// convention (internal/raft/raft.go:184).
	if len(n.cfg.Peers) == 0 {
		n.st.currentTerm = 1
		n.st.state = Leader
		n.st.leaderID = n.cfg.ID
		n.publish()
	}

	for {
		select {
		case <-n.stopCh:
			return
		case cmd := <-n.cmdCh:
			n.handle(cmd)
		}
	}
}

// handle dispatches a command to its handler. Adding new command kinds means
// adding a case here and a public method that enqueues it.
func (n *Node) handle(cmd command) {
	switch cmd.kind {
	case cmdPropose:
		n.handlePropose(cmd)
	}
}

// handlePropose appends the proposed command to the in-memory log, commits it
// (single-voter == self-quorum), publishes the new readState, sends the entry
// to ApplyCh, and replies to the waiter.
//
// PR 1 simplification: commit happens synchronously inside the actor. Once
// replication lands (PR 4-5), commit will lag the propose and a waiter map
// will gate the reply on the commit watermark.
func (n *Node) handlePropose(cmd command) {
	if n.st.state != Leader {
		if cmd.proposeReply != nil {
			cmd.proposeReply <- proposalResult{err: ErrNotLeader}
		}
		return
	}

	idx := uint64(len(n.st.log)) + 1
	entry := LogEntry{
		Term:    n.st.currentTerm,
		Index:   idx,
		Command: cmd.proposeCommand,
		Type:    cmd.proposeType,
	}
	n.st.log = append(n.st.log, entry)
	n.st.commitIndex = idx
	n.publish()

	// Deliver to FSM. Select on stopCh so Stop() never deadlocks against a
	// slow ApplyCh reader (mirrors v1 raft.go:707-711).
	select {
	case n.applyCh <- entry:
	case <-n.stopCh:
		if cmd.proposeReply != nil {
			cmd.proposeReply <- proposalResult{err: ErrNodeStopped}
		}
		return
	}

	if cmd.proposeReply != nil {
		cmd.proposeReply <- proposalResult{index: idx}
	}
}

// publish stores a fresh readState snapshot for hot-path readers. Called by
// the actor after every state mutation.
func (n *Node) publish() {
	n.rs.Store(n.st.snapshot())
}
