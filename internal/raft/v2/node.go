package raftv2

import (
	"context"
	"fmt"
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
// applyInChBuffer absorbs scheduling jitter between actor's enqueue and applyLoop's
// dequeue; the unbounded buf-in-goroutine inside applyLoop handles sustained slow
// FSM consumers without back-pressuring the actor.
const (
	cmdChBuffer     = 256
	applyChBuffer   = 64
	applyInChBuffer = 256
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

	// applyCh delivers committed entries to the FSM consumer. Owned by the
	// applyLoop goroutine; closed by applyLoop on shutdown after draining.
	applyCh chan LogEntry

	// applyInCh is the actor → applyLoop pipeline. The actor enqueues every
	// committed entry here; the applyLoop forwards to applyCh from an
	// unbounded in-goroutine buffer. This decouples actor liveness from FSM
	// consumer speed (TODO raft/v2: cmdCh backpressure cascade): a slow FSM
	// can no longer wedge the actor's applyCh send → applyCommitted →
	// AE/heartbeat handling → cascade to election storms.
	applyInCh chan LogEntry

	// applyDoneCh is closed when the applyLoop goroutine exits. Stop waits
	// on it after doneCh so callers can rely on applyCh being closed before
	// Stop returns.
	applyDoneCh chan struct{}

	// Actor-owned mutable state. Access is single-goroutine by construction;
	// no locking. Public methods MUST NOT touch st directly.
	st actorState

	// stopOnce guards multi-call Stop().
	stopOnce sync.Once

	// stable persists HardState (currentTerm, votedFor) across crashes.
	// Always non-nil after NewNode; defaults to memStableStore if Config.StableStore is nil.
	stable StableStore

	// snaps persists Raft snapshots (§7). Always non-nil after NewNode;
	// defaults to memSnapshotStore if Config.SnapshotStore is nil. Writes
	// happen only inside the actor goroutine (via cmdCreateSnapshot or
	// cmdInstallSnapshot); reads via LatestSnapshot are safe from any
	// goroutine because the underlying impl synchronises through Badger
	// or is single-writer (memSnapshotStore is only read after Save).
	snaps SnapshotStore

	// lastPersistedHS caches the most recently saved HardState so persistHardState
	// can short-circuit redundant SaveHardState calls (e.g. becomeFollower
	// followed by handleRequestVote in the same tick at the same term/vote).
	// Actor-only access; no locking required.
	lastPersistedHS HardState

	// transport is the outbound RPC sender. SetTransport stores it via atomic
	// swap so a Late SetTransport (post-Start) is data-race-free, even though
	// the documented contract is "set before Start". Reads from the actor
	// goroutine and from sendRequestVote/dispatchAppendEntries dispatch
	// goroutines all go through atomic.Pointer.Load.
	transport atomic.Pointer[Transport]

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

	// bootstrapped records whether Bootstrap() has been called. CURRENTLY
	// INFORMATIONAL ONLY: actor.run() auto-promotes a single-voter cluster
	// to Leader regardless of this flag, and multi-voter Nodes start as
	// Follower regardless. The flag exists so a second Bootstrap() returns
	// ErrAlreadyBootstrapped for v1 API parity. Callers MUST NOT depend on
	// "Start without Bootstrap stays Follower" — that contract is reserved
	// for a future PR (M2/M5) once the FSM caller migration begins. See the
	// SetTransport convention as another "documented but unenforced" surface.
	bootstrapped atomic.Bool
}

// NewNode creates a Node from cfg. Call Start to launch the actor goroutine.
//
// NewNode restores HardState (currentTerm, votedFor) from cfg.StableStore and
// the log from cfg.LogStore on startup. This satisfies Raft §5.4.1: a restarted
// node recovers the same term and vote as before the crash, so it cannot grant
// a conflicting vote in an already-decided term.
//
// Returns an error only if loading HardState fails (I/O error on restart).
// A missing key (first start) is not an error — zero HardState is used.
func NewNode(cfg Config) (*Node, error) {
	logStore := cfg.LogStore
	if logStore == nil {
		logStore = newMemLogStore()
	}
	stable := cfg.StableStore
	if stable == nil {
		stable = newMemStableStore()
	}
	snaps := cfg.SnapshotStore
	if snaps == nil {
		snaps = newMemSnapshotStore()
	}

	// Recover HardState. A zero HardState (CurrentTerm=0, VotedFor="") is the
	// "first start" case — equivalent to behaviour prior to PR 11.
	hs, err := stable.HardState()
	if err != nil {
		return nil, fmt.Errorf("raftv2: NewNode: load HardState: %w", err)
	}

	// Recover snapshot, if any. The snapshot's LastIncludedIndex bounds
	// commitIndex from below — those entries are by definition committed
	// (the snapshot contains their effect on the FSM). The log itself, if
	// persistent, must already have FirstIndex == LastIncludedIndex+1
	// because CompactBefore was called atomically with Save. A mismatch
	// here is a stale-state bug (e.g. crash mid-CreateSnapshot leaving log
	// uncompacted) — we panic loudly rather than silently risk a follower
	// applying entries the leader thinks are gone.
	snap, err := snaps.Latest()
	if err != nil {
		return nil, fmt.Errorf("raftv2: NewNode: load snapshot: %w", err)
	}
	initialCommitIndex := uint64(0)
	if snap != nil {
		initialCommitIndex = snap.LastIncludedIndex
		if logStore.FirstIndex() < snap.LastIncludedIndex+1 {
			panic(fmt.Sprintf(
				"raftv2: NewNode: log FirstIndex (%d) lags snapshot LastIncludedIndex+1 (%d) — "+
					"likely crashed mid-CreateSnapshot; manual recovery required",
				logStore.FirstIndex(), snap.LastIncludedIndex+1))
		}
	}

	// Reconstruct the effective configuration. Per Raft §4.3, the live config
	// is whatever the most recent config-bearing log entry says — even if
	// uncommitted. Bootstrap order:
	//  1. Seed from snapshot voters if a snapshot exists; otherwise from
	//     {cfg.ID} ∪ cfg.Peers (cfg.Peers is "peers excludes self" per v1).
	//  2. Walk log entries from snap.LastIncludedIndex+1 → LastIndex, applying
	//     any LogEntryConfChange / LogEntryJointConfChange to advance the
	//     effective config. configHistory is rebuilt in lockstep so any
	//     subsequent truncation can revert correctly.
	currentConfig, history, appendedIdx := reconstructConfig(snap, logStore, cfg.ID, cfg.Peers)

	n := &Node{
		cfg:             cfg,
		cmdCh:           make(chan command, cmdChBuffer),
		stopCh:          make(chan struct{}),
		doneCh:          make(chan struct{}),
		applyCh:         make(chan LogEntry, applyChBuffer),
		applyInCh:       make(chan LogEntry, applyInChBuffer),
		applyDoneCh:     make(chan struct{}),
		stable:          stable,
		snaps:           snaps,
		lastPersistedHS: hs,
		st: actorState{
			id:          cfg.ID,
			state:       Follower,
			currentTerm: hs.CurrentTerm,
			votedFor:    hs.VotedFor,
			log:         logStore,
			// commitIndex is volatile per Raft §5.3, but the snapshot floor
			// is durable: any entry covered by the snapshot is by definition
			// committed (the snapshot encodes the FSM state at that point).
			// On a non-snapshotted restart, this is 0; the next leader's
			// AE will re-establish via LeaderCommit.
			commitIndex:         initialCommitIndex,
			currentConfig:       currentConfig,
			configHistory:       history,
			appendedConfigIndex: appendedIdx,
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
	return n, nil
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

// Start launches the actor and applyLoop goroutines. Safe to call exactly once.
func (n *Node) Start() {
	go n.run()
	go n.applyLoop()
}

// Stop signals the actor to exit and waits for it to finish. Safe to call
// multiple times; subsequent calls are no-ops. Timer cleanup happens inside
// run() (via defer) once the actor observes stopCh; doing it here would race
// with concurrent timer Reset calls inside the actor.
//
// Lifecycle ordering: doneCh closes first (actor exit + close(applyInCh)),
// then applyLoop drains its buffer to applyCh and exits, closing applyDoneCh
// + applyCh. Callers that want a guarantee that applyCh has been closed by
// the time Stop returns can rely on this ordering.
func (n *Node) Stop() {
	n.stopOnce.Do(func() {
		close(n.stopCh)
	})
	<-n.doneCh
	<-n.applyDoneCh
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

// SetTransport configures the outbound Transport. Documented contract:
// call before Start. The atomic swap makes a late call data-race-free vs
// concurrent transport reads from dispatch goroutines, but a late
// SetTransport mid-flight is a logical bug — replies still in flight on
// the old transport may arrive after the swap and be processed against
// stale state. Best practice: set once at construction.
func (n *Node) SetTransport(t Transport) { n.transport.Store(&t) }

// loadTransport returns the configured Transport, or nil if SetTransport
// was never called. Safe to call from any goroutine.
func (n *Node) loadTransport() Transport {
	p := n.transport.Load()
	if p == nil {
		return nil
	}
	return *p
}

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

// LatestSnapshot returns the most recent snapshot persisted by SnapshotStore,
// or (nil, nil) if none exists. Safe to call from any goroutine — reads pass
// through to the underlying SnapshotStore impl.
//
// FSM-restore ordering: callers SHOULD invoke LatestSnapshot immediately
// after Start (before draining ApplyCh), reset their FSM state from
// snap.Data, then drain ApplyCh — which delivers entries from
// snap.LastIncludedIndex+1 onwards (or, if the leader installs a fresher
// snapshot mid-flight, a LogEntrySnapshot signal followed by entries past
// the new boundary). FIFO order across ApplyCh is preserved.
func (n *Node) LatestSnapshot() (*Snapshot, error) {
	return n.snaps.Latest()
}

// CreateSnapshot persists a snapshot at lastIncludedIndex with the given FSM
// state bytes, then compacts the log up to (and including) lastIncludedIndex.
// Routed through the actor goroutine so the log compaction is atomic with
// respect to AE handling.
//
// Returns an error if:
//   - lastIncludedIndex > commitIndex (cannot snapshot uncommitted state)
//   - lastIncludedIndex < FirstIndex (already covered by an earlier snapshot)
//
// Storage failures during Save or CompactBefore panic the actor — durability
// is unrecoverable, mirroring StableStore semantics.
func (n *Node) CreateSnapshot(lastIncludedIndex uint64, data []byte) error {
	reply := make(chan error, 1)
	select {
	case n.cmdCh <- command{kind: cmdCreateSnapshot, csIndex: lastIncludedIndex, csData: data, csReply: reply}:
	case <-n.stopCh:
		return ErrNodeStopped
	}
	select {
	case err := <-reply:
		return err
	case <-n.stopCh:
		return ErrNodeStopped
	}
}

// HandleInstallSnapshot processes an incoming InstallSnapshot RPC and returns
// the reply. Safe to call from any goroutine; the call routes through the
// actor via cmdCh and waits synchronously for the actor's reply.
//
// If the node has been Stopped, returns a zero-value reply with the snapshot
// term (matches HandleAppendEntries' stopped-node behaviour).
func (n *Node) HandleInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply {
	reply := make(chan *InstallSnapshotReply, 1)
	select {
	case n.cmdCh <- command{kind: cmdInstallSnapshot, isArgs: args, isReply: reply}:
	case <-n.stopCh:
		return &InstallSnapshotReply{Term: n.Term()}
	}
	select {
	case r := <-reply:
		return r
	case <-n.stopCh:
		return &InstallSnapshotReply{Term: n.Term()}
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

// Bootstrap is currently INFORMATIONAL ONLY — it sets a CAS flag so a second
// call returns ErrAlreadyBootstrapped (v1 API parity). It does NOT gate
// cluster initialisation: actor.run() auto-promotes single-voter clusters and
// arms the election timer for multi-voter clusters regardless of whether
// Bootstrap was called. Calling Bootstrap is therefore optional today.
//
// This is a temporary stub. A future PR (timed with the M5 caller migration
// from v1) will tighten the contract so Bootstrap actually persists initial
// configuration to LogStore and gates cluster init. At that point callers
// MUST call Bootstrap before Start; expect a documented breaking change.
//
// Returns nil on first call; calling twice returns ErrAlreadyBootstrapped.
func (n *Node) Bootstrap() error {
	if n.bootstrapped.CompareAndSwap(false, true) {
		return nil
	}
	return ErrAlreadyBootstrapped
}

// Configuration returns a point-in-time snapshot of the cluster's voter set
// per the most recent effective configuration. Reads from the actor's
// published readState; lock-free.
//
// In joint state (Raft §4.3), Servers contains the union Cold ∪ Cnew with
// Cnew first; callers that need the joint distinction should use the
// internal readState path. The flat Server list matches v1's API shape.
func (n *Node) Configuration() Configuration {
	rs := n.rs.Load()
	all := rs.config.allVoters()
	servers := make([]Server, 0, len(all))
	for _, id := range all {
		servers = append(servers, Server{ID: id, Suffrage: Voter})
	}
	return Configuration{Servers: servers}
}

// AddVoter submits an Add-voter membership change and blocks until both
// phases of the Raft §4.3 joint-consensus protocol commit. Equivalent to
// AddVoterCtx with a background context.
//
// addr is the transport address; v2 does not interpret it (the Transport
// implementation is responsible for resolving peer IDs to addresses), so
// the parameter is preserved for v1 API parity but otherwise unused.
func (n *Node) AddVoter(id, addr string) error {
	return n.AddVoterCtx(context.Background(), id, addr)
}

// AddVoterCtx submits an Add-voter membership change. Returns once the
// final LogEntryConfChange entry (Cnew alone) has committed, or the
// context cancels, or the node steps down.
//
// Errors:
//   - ErrNotLeader: caller-side snapshot says we are not leader, or the
//     actor's recheck finds the same.
//   - ErrConfChangeInFlight: a previous membership change has not yet
//     completed both phases.
//   - ErrProposalFailed: leader stepped down before phase 2 committed.
//   - ctx.Err: context cancelled.
//
// addr is unused; see AddVoter docstring.
func (n *Node) AddVoterCtx(ctx context.Context, id, addr string) error {
	_ = addr
	return n.submitConfChange(ctx, id, true)
}

// RemoveVoter submits a Remove-voter membership change and blocks until
// both phases of the Raft §4.3 joint-consensus protocol commit. The
// removed voter MAY be the current leader: in that case the leader stays
// at its post until the final entry commits, then steps down to Follower
// (Diego's thesis §4.3, "self-removed leader" rule).
func (n *Node) RemoveVoter(id string) error {
	return n.submitConfChange(context.Background(), id, false)
}

// submitConfChange is the shared implementation backing AddVoter,
// AddVoterCtx, and RemoveVoter. It enqueues a cmdConfChange and waits for
// the actor's reply.
func (n *Node) submitConfChange(ctx context.Context, id string, add bool) error {
	if !n.IsLeader() {
		return ErrNotLeader
	}
	reply := make(chan confChangeResult, 1)
	select {
	case n.cmdCh <- command{kind: cmdConfChange, ccID: id, ccAdd: add, ccReply: reply}:
	case <-ctx.Done():
		return ctx.Err()
	case <-n.stopCh:
		return ErrNodeStopped
	}
	select {
	case res := <-reply:
		return res.err
	case <-ctx.Done():
		return ctx.Err()
	case <-n.stopCh:
		return ErrNodeStopped
	}
}

// AddLearner is a stub for v1 API parity. Real implementation lands in M2
// (configuration changes via joint consensus).
func (n *Node) AddLearner(id, addr string) error { return ErrNotImplemented }

// PromoteToVoter is a stub for v1 API parity. Real implementation lands in M2
// (configuration changes via joint consensus).
func (n *Node) PromoteToVoter(id string) error { return ErrNotImplemented }

// TransferLeadership is a stub for v1 API parity. Real implementation lands
// in M2 (configuration changes via joint consensus).
func (n *Node) TransferLeadership() error { return ErrNotImplemented }
