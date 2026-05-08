package raftv2

import (
	"sync"
	"sync/atomic"
)

// Channel sizing constants. cmdCh follows v1's proposalCh-style buffering for
// burst tolerance; applyCh mirrors v1's 64-entry buffer (internal/raft/raft.go:409).
const (
	cmdChBuffer   = 256
	applyChBuffer = 64
)

// Node is a single Raft consensus node, actor-pattern edition. The public
// API mirrors internal/raft.Node so the M5 import flip is mechanical.
type Node struct {
	cfg Config

	// Read-mostly snapshot for hot-path callers. Written only by the actor
	// goroutine (see actor.go). Always non-nil after NewNode.
	rs atomic.Pointer[readState]

	// Actor channels. cmdCh accepts commands from public methods; doneCh is
	// closed when the actor exits; stopCh signals shutdown.
	cmdCh  chan command
	stopCh chan struct{}
	doneCh chan struct{}

	// applyCh delivers committed entries to the FSM consumer.
	applyCh chan LogEntry

	// Actor-owned mutable state. Access is single-goroutine by construction;
	// no locking. Public methods MUST NOT touch st directly.
	st actorState

	// stopOnce guards multi-call Stop().
	stopOnce sync.Once

	// transport is the outbound RPC sender. PR 4 stores it but never sends;
	// PR 5 wires it into the election state machine. Set via SetTransport
	// before Start. Read-only after Start, so no synchronization needed.
	transport Transport
}

// NewNode creates a Node from cfg. Call Start to launch the actor goroutine.
//
// PR 1 ignores all Config fields except ID and Peers. Persistence (LogStore)
// lands in PR 6+.
func NewNode(cfg Config) *Node {
	n := &Node{
		cfg:     cfg,
		cmdCh:   make(chan command, cmdChBuffer),
		stopCh:  make(chan struct{}),
		doneCh:  make(chan struct{}),
		applyCh: make(chan LogEntry, applyChBuffer),
		st: actorState{
			id:    cfg.ID,
			state: Follower,
			log:   make([]LogEntry, 0),
		},
	}
	// Publish an initial readState so callers between NewNode and the actor's
	// first publish see a coherent snapshot rather than a nil load.
	n.rs.Store(n.st.snapshot())
	return n
}

// Start launches the actor goroutine. Safe to call exactly once per Node.
func (n *Node) Start() {
	go n.run()
}

// Stop signals the actor to exit and waits for it to finish. Safe to call
// multiple times; subsequent calls are no-ops.
func (n *Node) Stop() {
	n.stopOnce.Do(func() {
		close(n.stopCh)
	})
	<-n.doneCh
}

// ID returns the node's configured ID. Immutable post-construction.
func (n *Node) ID() string { return n.cfg.ID }

// State returns the node's current role. Hot path — atomic snapshot only.
func (n *Node) State() NodeState { return n.rs.Load().state }

// Term returns the current term. Hot path — atomic snapshot only.
func (n *Node) Term() uint64 { return n.rs.Load().term }

// IsLeader reports whether this node is the current leader. Hot path.
func (n *Node) IsLeader() bool { return n.rs.Load().isLeader }

// LeaderID returns the ID of the current leader (empty if unknown). Hot path.
func (n *Node) LeaderID() string { return n.rs.Load().leaderID }

// CommittedIndex returns the latest committed log index. Hot path.
func (n *Node) CommittedIndex() uint64 { return n.rs.Load().commitIndex }

// SetTransport configures the outbound Transport. Must be called before
// Start. PR 4 stores but does not exercise the transport for sending; this
// is forward-compatibility for PR 5's election state machine.
func (n *Node) SetTransport(t Transport) { n.transport = t }

// HandleRequestVote processes an incoming RequestVote RPC and returns the
// reply. Safe to call from any goroutine; the call routes through the actor
// via cmdCh and waits synchronously for the actor's reply.
//
// If the node has been Stopped, returns a zero-value reply with the snapshot
// term (matches v1's stopped-node behaviour at internal/raft/raft.go:1748-1750).
func (n *Node) HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	reply := make(chan *RequestVoteReply, 1)
	select {
	case n.cmdCh <- command{kind: cmdRequestVote, rvArgs: args, rvReply: reply}:
	case <-n.stopCh:
		return &RequestVoteReply{Term: n.Term()}
	}
	select {
	case r := <-reply:
		return r
	case <-n.stopCh:
		return &RequestVoteReply{Term: n.Term()}
	}
}

// HandleAppendEntries processes an incoming AppendEntries RPC and returns
// the reply. Safe to call from any goroutine. PR 4 returns Success=false
// for all requests; full log-replication semantics land in PR 5+ (heartbeat)
// and PR 6+ (entry append). The handler still respects the term-step-down
// rule so the term invariant holds.
func (n *Node) HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	reply := make(chan *AppendEntriesReply, 1)
	select {
	case n.cmdCh <- command{kind: cmdAppendEntries, aeArgs: args, aeReply: reply}:
	case <-n.stopCh:
		return &AppendEntriesReply{Term: n.Term()}
	}
	select {
	case r := <-reply:
		return r
	case <-n.stopCh:
		return &AppendEntriesReply{Term: n.Term()}
	}
}

// ApplyCh returns the channel on which committed log entries are delivered.
// The caller must drain this channel to avoid back-pressuring the actor.
// The channel is closed after Stop completes, so consumers may use
// "for range ApplyCh()" to drain until shutdown.
func (n *Node) ApplyCh() <-chan LogEntry { return n.applyCh }
