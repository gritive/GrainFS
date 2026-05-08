package raftv2

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// Timer defaults applied when Config leaves the corresponding field zero.
// Mirrors v1's defaults (internal/raft/raft.go) closely enough that tests
// using zero-config Nodes inherit familiar election cadence.
const (
	defaultElectionTimeout  = 150 * time.Millisecond
	defaultHeartbeatTimeout = 50 * time.Millisecond
	// idleElectionTimeout parks the timer for single-voter Nodes (no election
	// is needed; the bootstrap path makes us Leader). One hour is "effectively
	// never" without complicating the select with a nil-channel branch.
	idleElectionTimeout = time.Hour
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

	// electionTimer fires when no leader contact has been received within the
	// randomized election timeout. Owned by the actor; constructed in NewNode
	// so the actor's select can range on its C channel from the first tick.
	// For single-voter Nodes it is parked at idleElectionTimeout (never fires
	// in practice).
	electionTimer *time.Timer

	// heartbeatTicker is non-nil only while this Node is Leader. The actor
	// creates it on becomeLeader and stops + nils it on step-down, leveraging
	// the nil-channel select trick to skip heartbeat ticks when not leader.
	heartbeatTicker *time.Ticker

	// rng powers the randomized election-timeout window. Seeded per-Node so
	// concurrent multi-voter test clusters do not draw identical timeouts.
	rng *rand.Rand

	// bootstrapped records whether Bootstrap() has been called. PR 7 keeps
	// Bootstrap a no-op and the auto-bootstrap path inside actor.run() fires
	// regardless; this flag exists purely so a second Bootstrap() returns
	// ErrAlreadyBootstrapped for v1 API parity.
	bootstrapped atomic.Bool
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
			log:   newMemLogStore(),
		},
		// Per-Node rng seeded with current time + ID hash so two Nodes started
		// in the same nanosecond still draw different timeouts. Sole user is
		// the actor goroutine, so no locking needed.
		rng: rand.New(rand.NewSource(time.Now().UnixNano() ^ int64(hashID(cfg.ID)))),
	}
	// Allocate the election timer in a stopped state. The actor's run() will
	// arm it (multi-voter) or leave it parked (single-voter) on first tick.
	// Stopped + drained timers are safe to Reset later.
	n.electionTimer = time.NewTimer(idleElectionTimeout)
	if !n.electionTimer.Stop() {
		<-n.electionTimer.C
	}
	// Publish an initial readState so callers between NewNode and the actor's
	// first publish see a coherent snapshot rather than a nil load.
	n.rs.Store(n.st.snapshot())
	return n
}

// hashID is a tiny non-cryptographic mixer for rng seeding. FNV-1a 32-bit so
// two Nodes started in the same nanosecond pick distinct timeout draws.
func hashID(id string) uint32 {
	const offset, prime = uint32(2166136261), uint32(16777619)
	h := offset
	for i := 0; i < len(id); i++ {
		h ^= uint32(id[i])
		h *= prime
	}
	return h
}

// Start launches the actor goroutine. Safe to call exactly once per Node.
func (n *Node) Start() {
	go n.run()
}

// Stop signals the actor to exit and waits for it to finish. Safe to call
// multiple times; subsequent calls are no-ops. Timer cleanup happens inside
// run() (via defer) once the actor observes stopCh; doing it here would race
// with concurrent timer Reset calls inside the actor.
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

// Bootstrap initializes the cluster with the configured peer list. For a
// single-voter cluster (cfg.Peers empty), Bootstrap is a no-op — Start auto-
// promotes to Leader as before. For multi-voter clusters, Bootstrap is also
// a no-op in PR 7; multi-voter elections begin via the election timer once
// Start runs. Future PRs may make Bootstrap meaningful (e.g., persist the
// initial configuration to LogStore in M2).
//
// API parity with v1's Node.Bootstrap. Returns nil on first call; calling
// twice returns ErrAlreadyBootstrapped.
func (n *Node) Bootstrap() error {
	if n.bootstrapped.CompareAndSwap(false, true) {
		return nil
	}
	return ErrAlreadyBootstrapped
}

// Configuration returns a point-in-time snapshot of the cluster's voter set.
// Reads from the actor's published readState; lock-free.
//
// PR 7: cfg.Peers is static (set at NewNode). Configuration just returns
// the initial set. M2 will make it dynamic via readState as joint consensus
// lands.
func (n *Node) Configuration() Configuration {
	rs := n.rs.Load()
	servers := make([]Server, 0, len(n.cfg.Peers)+1)
	servers = append(servers, Server{ID: n.cfg.ID, Suffrage: Voter})
	for _, peer := range n.cfg.Peers {
		servers = append(servers, Server{ID: peer, Suffrage: Voter})
	}
	_ = rs // not used yet; future PRs may reflect membership changes through readState
	return Configuration{Servers: servers}
}

// AddVoter is a stub for v1 API parity. Real implementation lands in M2
// (configuration changes via joint consensus).
func (n *Node) AddVoter(id, addr string) error { return ErrNotImplemented }

// AddVoterCtx is a stub for v1 API parity. Real implementation lands in M2
// (configuration changes via joint consensus).
func (n *Node) AddVoterCtx(ctx context.Context, id, addr string) error {
	return ErrNotImplemented
}

// RemoveVoter is a stub for v1 API parity. Real implementation lands in M2
// (configuration changes via joint consensus).
func (n *Node) RemoveVoter(id string) error { return ErrNotImplemented }

// AddLearner is a stub for v1 API parity. Real implementation lands in M2
// (configuration changes via joint consensus).
func (n *Node) AddLearner(id, addr string) error { return ErrNotImplemented }

// PromoteToVoter is a stub for v1 API parity. Real implementation lands in M2
// (configuration changes via joint consensus).
func (n *Node) PromoteToVoter(id string) error { return ErrNotImplemented }

// TransferLeadership is a stub for v1 API parity. Real implementation lands
// in M2 (configuration changes via joint consensus).
func (n *Node) TransferLeadership() error { return ErrNotImplemented }
