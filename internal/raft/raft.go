package raft

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
)

var raftTraceEnabled = os.Getenv("GRAINFS_VOLUME_TRACE") == "1"

var (
	ErrNotLeader           = errors.New("not the leader")
	ErrProposalFailed      = errors.New("proposal failed: node stepped down")
	ErrNoPeers             = errors.New("no peers available for leadership transfer")
	ErrAlreadyBootstrapped = errors.New("raft: already bootstrapped")
)

// ServerSuffrage describes whether a cluster member has a vote.
type ServerSuffrage int

const (
	Voter    ServerSuffrage = iota // full voting member
	NonVoter                       // read-only observer (reserved)
)

// Server describes a single node in the cluster configuration.
type Server struct {
	ID       string
	Suffrage ServerSuffrage
}

// Configuration is a point-in-time snapshot of cluster membership.
type Configuration struct {
	Servers []Server
}

// NodeState represents the current role of a Raft node.
type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

func (s NodeState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return fmt.Sprintf("Unknown(%d)", int(s))
	}
}

// LogEntry represents a single entry in the Raft log.
type LogEntry struct {
	Term    uint64
	Index   uint64
	Command []byte
	Type    LogEntryType // 0 = Command (default, backward-compatible)
}

// Config holds Raft node configuration.
// proposal carries a single command through the batcher pipeline.
type proposal struct {
	command   []byte
	entryType LogEntryType // 0 = Command (default)
	doneCh    chan proposalResult
	ctx       context.Context
	submitAt  time.Time
}

// proposalResult is the outcome delivered to ProposeWait callers.
type proposalResult struct {
	index uint64
	err   error
}

// adaptiveMetrics tracks EWMA request rate to dynamically tune batch parameters.
// It has its own mutex so callers do not need to hold Node.mu for metrics reads.
type adaptiveMetrics struct {
	mu          sync.Mutex
	ewmaRate    float64
	lastFlushAt time.Time
	alpha       float64
}

func (m *adaptiveMetrics) update(batchSize int) {
	now := time.Now()
	m.mu.Lock()
	elapsed := now.Sub(m.lastFlushAt).Seconds()
	if elapsed <= 0 {
		elapsed = 0.001
	}
	instantRate := float64(batchSize) / elapsed
	m.ewmaRate = m.alpha*instantRate + (1-m.alpha)*m.ewmaRate
	m.lastFlushAt = now
	m.mu.Unlock()
}

func (m *adaptiveMetrics) snapshot() (ewmaRate float64, timeout time.Duration, maxBatch int) {
	m.mu.Lock()
	ewmaRate = m.ewmaRate
	m.mu.Unlock()
	timeout = calcBatchTimeout(ewmaRate)
	maxBatch = calcMaxBatch(ewmaRate)
	return
}

func (m *adaptiveMetrics) batchTimeout() time.Duration {
	m.mu.Lock()
	rate := m.ewmaRate
	m.mu.Unlock()
	return calcBatchTimeout(rate)
}

func (m *adaptiveMetrics) maxBatch() int {
	m.mu.Lock()
	rate := m.ewmaRate
	m.mu.Unlock()
	return calcMaxBatch(rate)
}

func calcBatchTimeout(rate float64) time.Duration {
	switch {
	case rate >= 500:
		return 5 * time.Millisecond
	case rate >= 100:
		return time.Millisecond
	default:
		return 100 * time.Microsecond
	}
}

func calcMaxBatch(rate float64) int {
	switch {
	case rate >= 500:
		return 128
	case rate >= 100:
		return 32
	default:
		return 4
	}
}

// BatchMetricsSnapshot is a point-in-time view of the adaptive batcher state.
type BatchMetricsSnapshot struct {
	EWMARate     float64
	BatchTimeout time.Duration
	MaxBatch     int
}

type Config struct {
	ID               string
	Peers            []string      // addresses of other nodes
	ElectionTimeout  time.Duration // base election timeout
	HeartbeatTimeout time.Duration
	ManagedMode      bool          // enable periodic Raft log GC via quorum watermark
	LogGCInterval    time.Duration // how often log GC runs (default 30s)
	MaxEntriesPerAE  uint64        // 0 = unlimited; default via DefaultConfig = 512
	TrailingLogs     uint64        // entries to keep after snapshot; default 10240
	// LearnerCatchupThreshold: matchIndex+threshold >= commitIndex 시 watcher가 PromoteToVoter propose.
	// 0 면 100으로 fallback. AddVoter 자동 learner-first의 promote 트리거.
	LearnerCatchupThreshold uint64
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig(id string, peers []string) Config {
	return Config{
		ID:                      id,
		Peers:                   peers,
		ElectionTimeout:         150 * time.Millisecond,
		HeartbeatTimeout:        50 * time.Millisecond,
		MaxEntriesPerAE:         512,
		TrailingLogs:            10240,
		LearnerCatchupThreshold: 100,
	}
}

// RPC message types for Raft protocol.

// RequestVoteArgs is sent by candidates to gather votes.
type RequestVoteArgs struct {
	Term           uint64
	CandidateID    string
	LastLogIndex   uint64
	LastLogTerm    uint64
	PreVote        bool // true = pre-vote round; receiver must not update state/term
	LeaderTransfer bool // true = leadership transfer; receiver must bypass stickiness
}

// RequestVoteReply is the response to a RequestVote RPC.
type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

// AppendEntriesArgs is sent by the leader to replicate log entries.
type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

// AppendEntriesReply is the response to an AppendEntries RPC.
type AppendEntriesReply struct {
	Term          uint64
	Success       bool
	ConflictTerm  uint64 // term of conflicting entry; 0 = not set or old peer
	ConflictIndex uint64 // first index of ConflictTerm; 0 = not set
}

// Node is a single Raft consensus node.
type Node struct {
	mu sync.Mutex

	// persistent state
	id          string
	currentTerm uint64
	votedFor    string
	log         []LogEntry
	firstIndex  uint64 // Raft index of log[0]; enables log compaction after snapshots

	// volatile state
	state         NodeState
	commitIndex   uint64
	lastApplied   uint64
	snapshotIndex uint64 // Raft index of the most recent snapshot; 0 = no snapshot yet

	// leader volatile state
	nextIndex        map[string]uint64
	matchIndex       map[string]uint64
	learnerIDs       map[string]string        // nodeID → peerKey; learners not counted for quorum
	learnerPromoteCh map[string]chan struct{} // nodeID → ch; closed in apply loop when PromoteToVoter commits (AddVoter caller wakeup)

	// jointManagedLearners tracks learners that ChangeMembership added in the
	// pre-joint window. checkLearnerCatchup skips these to prevent the
	// auto-promote watcher from racing with the upcoming JointEnter. Cleared
	// on joint apply success or defer cleanup. Leader-only volatile in K1/K2;
	// PR-K3 makes this a state-machine output via ConfChange.managed_by_joint.
	jointManagedLearners map[string]struct{}

	// removedFromCluster is set when the local node observed its own removal
	// via JointLeave (commit-time self-removal hook). Disables election
	// participation so the orphan node does not split votes against C_new.
	// Cleared on apply of ConfChangeAddVoter / ConfChangeAddLearner for self
	// id, or on JointEnter where self is in C_new.
	removedFromCluster bool

	// changeMembershipDefaults configures default behavior of ChangeMembership.
	// Mutated under mu via SetChangeMembershipDefaults. Zero CatchUpTimeout
	// falls back to 30s.
	changeMembershipDefaults ChangeMembershipOpts

	// config
	config Config

	// channels
	applyCh  chan LogEntry
	stopCh   chan struct{}
	resetCh  chan struct{} // signals election timer reset
	commitCh chan struct{} // signals applyLoop when commitIndex advances
	stopped  bool
	wg       sync.WaitGroup // tracks goroutines started by Start()

	// transport callback for sending RPCs
	sendRequestVote     func(peer string, args *RequestVoteArgs) (*RequestVoteReply, error)
	sendAppendEntries   func(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error)
	sendInstallSnapshot func(peer string, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
	sendTimeoutNow      func(peer string) error

	// durable storage (optional; when nil, state is in-memory only)
	store LogStore

	// leader tracking (observable from outside)
	leaderID string

	// leader stickiness: time of last valid AppendEntries from a live leader.
	// Only followers update this; the leader never refreshes it (it sends AE, not receives).
	// The leader relies on pre-vote and CheckQuorum for self-defense, not stickiness.
	lastLeaderContact time.Time

	// CheckQuorum: per-peer time of last AppendEntries reply (leader only)
	checkQuorumAcks map[string]time.Time

	// leaderTransfer is set by HandleTimeoutNow to signal runCandidate that
	// this election is a leader-transfer; RequestVote must bypass stickiness on receivers.
	leaderTransfer bool

	// proposal waiters: log index -> channel signaled when committed (nil = success, error = failure)
	waiters map[uint64]chan error

	// §4.4 membership change tracking
	pendingConfChangeIndex uint64   // index of in-flight ConfChange entry; 0 = none
	initialPeers           []string // bootstrap peers; used to rebuild config after log truncation
	mixedVersion           bool     // true = cluster has nodes on different versions; blocks membership changes

	// §4.3 joint consensus state (Sub-project 2)
	jointPhase         jointPhase    // current phase; JointNone unless C_old+new is committed
	jointOldVoters     []string      // C_old voter ids; valid only when jointPhase == JointEntering
	jointNewVoters     []string      // C_new voter ids; valid only when jointPhase == JointEntering
	jointEnterIndex    uint64        // log index of committed JointEnter entry; 0 = none
	jointLeaveProposed bool          // leader-only idempotency flag for checkJointAdvance
	jointPromoteCh     chan struct{} // closed by every node's apply path on JointLeave commit; wakes proposeJointConfChangeWait callers

	// log GC tracking (Phase 14d)
	lastLogGC time.Time
	gcRunning atomic.Bool // prevents overlapping GC goroutines

	// batcher pipeline (Phase 14d')
	proposalCh    chan proposal
	replicationCh chan struct{}
	metrics       adaptiveMetrics

	// noOpCmd is proposed immediately when a node becomes leader so that
	// advanceCommitIndex can commit entries from previous terms. Set via
	// SetNoOpCommand before Start().
	noOpCmd []byte

	// Observer pattern (PR3): event delivery to external consumers.
	observerState
}

// NewNode creates a new Raft node. Call Start() to begin operation.
// If store is non-nil, it restores persisted state on creation.
func NewNode(config Config, store ...LogStore) *Node {
	initialPeers := make([]string, len(config.Peers))
	copy(initialPeers, config.Peers)

	n := &Node{
		id:                   config.ID,
		state:                Follower,
		config:               config,
		log:                  make([]LogEntry, 0),
		firstIndex:           1, // Raft indices start at 1
		nextIndex:            make(map[string]uint64),
		matchIndex:           make(map[string]uint64),
		learnerIDs:           make(map[string]string),
		learnerPromoteCh:     make(map[string]chan struct{}),
		jointManagedLearners: make(map[string]struct{}),
		checkQuorumAcks:      make(map[string]time.Time),
		applyCh:              make(chan LogEntry, 64),
		stopCh:               make(chan struct{}),
		resetCh:              make(chan struct{}, 1),
		commitCh:             make(chan struct{}, 1),
		waiters:              make(map[uint64]chan error),
		proposalCh:           make(chan proposal, 4096),
		replicationCh:        make(chan struct{}, 1),
		metrics:              adaptiveMetrics{alpha: 0.3, lastFlushAt: time.Now()},
		initialPeers:         initialPeers,
	}

	if len(store) > 0 && store[0] != nil {
		n.store = store[0]
		n.restoreFromStore()
	}

	return n
}

func (n *Node) restoreFromStore() {
	if n.store == nil {
		return
	}

	// Restore term and votedFor
	term, votedFor, err := n.store.LoadState()
	if err == nil {
		n.currentTerm = term
		n.votedFor = votedFor
	}

	// Restore snapshot watermark and cluster config from snapshot.
	snap, snapErr := n.store.LoadSnapshot()
	if snapErr == nil && snap.Index > 0 {
		n.snapshotIndex = snap.Index
		n.lastApplied = snap.Index
		n.commitIndex = snap.Index
		n.currentTerm = snap.Term

		if len(snap.Servers) == 0 {
			log.Warn().
				Uint64("snap_index", snap.Index).
				Msg("raft: legacy snapshot has no server list; replaying full log for membership")
		} else {
			basePeers, baseLearners := restoreConfigFromServers(snap.Servers, n.id)
			n.config.Peers = basePeers
			n.learnerIDs = baseLearners
			// Derive removedFromCluster: if self is not in the snapshot's
			// server list, we were an orphan at snapshot time. The orphan
			// election guard must persist so we don't split votes on cold-start.
			n.removedFromCluster = !containsServer(snap.Servers, n.id)
		}
	}

	// Legacy snapshot fallback: when servers were not persisted, best-effort
	// replay all ConfChange entries from initialPeers to reconstruct membership.
	// Marker: do this BEFORE log entries are loaded so the rebuild covers the
	// full log range below. Effective only after n.log is populated.
	legacySnapshot := snapErr == nil && snap.Index > 0 && len(snap.Servers) == 0

	// Restore log entries
	lastIdx, err := n.store.LastIndex()
	if err == nil && lastIdx > 0 {
		firstIdx := uint64(1)
		for i := uint64(1); i <= lastIdx; i++ {
			if _, err := n.store.GetEntry(i); err == nil {
				firstIdx = i
				break
			}
		}
		entries, err := n.store.GetEntries(firstIdx, lastIdx)
		if err == nil {
			n.log = entries
			n.firstIndex = firstIdx
		}
	}

	// Replay post-snapshot ConfChange entries to apply membership changes after the snapshot.
	if snapErr == nil && snap.Index > 0 && len(snap.Servers) > 0 {
		basePeers, baseLearners := restoreConfigFromServers(snap.Servers, n.id)
		n.rebuildConfigFromLog(snap.Index+1, basePeers, baseLearners)
	} else if legacySnapshot {
		// Legacy fallback: replay full log from initialPeers (best-effort).
		n.rebuildConfigFromLog(0, n.initialPeers, map[string]string{})
	}
}

// SetNoOpCommand sets the encoded command that will be proposed when this node
// becomes leader. The command must be recognised and ignored by the FSM (no-op).
// Call before Start().
func (n *Node) SetNoOpCommand(cmd []byte) {
	n.noOpCmd = cmd
}

// Bootstrap initializes the cluster for the first time by saving a bootstrap
// marker to the durable store. Returns ErrAlreadyBootstrapped if the store
// already has a bootstrap marker or existing hard state (term > 0 or votedFor set).
//
// Call Bootstrap once before the very first Start(). On subsequent restarts
// Bootstrap returns ErrAlreadyBootstrapped, which callers should ignore.
// No-op when store is nil (in-memory nodes).
func (n *Node) Bootstrap() error {
	if n.store == nil {
		return nil
	}
	bootstrapped, err := n.store.IsBootstrapped()
	if err != nil {
		return fmt.Errorf("raft: check bootstrap: %w", err)
	}
	if bootstrapped {
		return ErrAlreadyBootstrapped
	}
	// Detect pre-PR3 nodes that already have hard state but no marker.
	term, votedFor, err := n.store.LoadState()
	if err == nil && (term > 0 || votedFor != "") {
		return ErrAlreadyBootstrapped
	}
	return n.store.SaveBootstrapMarker()
}

// SetTransport sets the RPC callbacks for sending messages to peers.
func (n *Node) SetTransport(
	sendVote func(peer string, args *RequestVoteArgs) (*RequestVoteReply, error),
	sendAppend func(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error),
) {
	n.sendRequestVote = sendVote
	n.sendAppendEntries = sendAppend
}

// SetInstallSnapshotTransport sets the callback for sending snapshots to slow followers.
func (n *Node) SetInstallSnapshotTransport(
	send func(peer string, args *InstallSnapshotArgs) (*InstallSnapshotReply, error),
) {
	n.sendInstallSnapshot = send
}

// SetTimeoutNowTransport sets the callback for sending TimeoutNow during leadership transfer.
func (n *Node) SetTimeoutNowTransport(send func(peer string) error) {
	n.sendTimeoutNow = send
}

// Start begins the Raft node's main loop.
func (n *Node) Start() {
	n.wg.Add(3)
	go func() { defer n.wg.Done(); n.run() }()
	go func() { defer n.wg.Done(); n.applyLoop() }()
	go func() { defer n.wg.Done(); n.batcherLoop() }()
}

// Propose appends a command to the leader's log for replication.
// Returns ErrNotLeader if this node is not the leader.
func (n *Node) Propose(command []byte) error {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return ErrNotLeader
	}
	n.mu.Unlock()

	doneCh := make(chan proposalResult, 1)
	select {
	case n.proposalCh <- proposal{command: command, doneCh: doneCh, ctx: context.Background()}:
		return nil
	case <-n.stopCh:
		return ErrProposalFailed
	}
}

// ProposeWait appends a command and blocks until it is committed or the context is cancelled.
// Returns the log index of the committed entry.
func (n *Node) ProposeWait(ctx context.Context, command []byte) (uint64, error) {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return 0, ErrNotLeader
	}
	n.mu.Unlock()

	doneCh := make(chan proposalResult, 1)
	select {
	case n.proposalCh <- proposal{command: command, doneCh: doneCh, ctx: ctx, submitAt: time.Now()}:
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-n.stopCh:
		return 0, ErrProposalFailed
	}

	select {
	case result := <-doneCh:
		return result.index, result.err
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-n.stopCh:
		return 0, ErrProposalFailed
	}
}

// BatchMetrics returns a snapshot of the adaptive batcher's current state.
func (n *Node) BatchMetrics() BatchMetricsSnapshot {
	rate, timeout, maxBatch := n.metrics.snapshot()
	return BatchMetricsSnapshot{
		EWMARate:     rate,
		BatchTimeout: timeout,
		MaxBatch:     maxBatch,
	}
}

func (n *Node) applyLoop() {
	for {
		n.mu.Lock()
		for n.commitIndex <= n.lastApplied || !n.hasLogEntry(n.lastApplied+1) {
			n.mu.Unlock()
			select {
			case <-n.stopCh:
				return
			case <-n.commitCh:
			}
			n.mu.Lock()
		}

		n.lastApplied++
		entry := n.log[n.toSliceIdx(n.lastApplied)]
		idx := entry.Index

		// ConfChange entries are already applied on append (§4.4).
		// On commit, only clear the pending index so the next change may proceed.
		if entry.Type == LogEntryConfChange {
			if n.pendingConfChangeIndex == idx {
				n.pendingConfChangeIndex = 0
			}
			// Close promoteCh on commit (truncation-safe wakeup for AddVoter caller).
			cc := decodeConfChange(entry.Command)
			if cc.Op == ConfChangePromote {
				if ch, ok := n.learnerPromoteCh[cc.ID]; ok {
					close(ch)
					delete(n.learnerPromoteCh, cc.ID)
				}
			}
		}

		// JointConfChange entries are already applied on append for the §4.3
		// dual-quorum invariant. On JointLeave commit, perform truncation-safe
		// caller wakeup and self-removal step-down — both gated to commit so an
		// uncommitted JointLeave that gets truncated by a new leader does not
		// falsely release callers or step the leader down prematurely.
		if entry.Type == LogEntryJointConfChange {
			jc := decodeJointConfChange(entry.Command)
			if jc.Op == JointOpLeave {
				if n.jointPromoteCh != nil {
					close(n.jointPromoteCh)
					n.jointPromoteCh = nil
				}
				newPeers := serverPeerKeys(jc.NewServers)
				if !containsPeer(newPeers, n.id) {
					n.removedFromCluster = true
					if n.state == Leader {
						n.state = Follower
						n.leaderID = ""
						n.signalReset()
					}
				} else {
					n.removedFromCluster = false
				}
			}
		}

		// Signal any waiter for this index
		if ch, ok := n.waiters[idx]; ok {
			close(ch)
			delete(n.waiters, idx)
		}
		n.mu.Unlock()

		// Deliver to FSM (config changes are also delivered so FSM can track membership)
		select {
		case n.applyCh <- entry:
		case <-n.stopCh:
			return
		}
	}
}

// Stop shuts down the Raft node. Safe to call multiple times.
func (n *Node) Stop() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.stopped {
		n.stopped = true
		close(n.stopCh)
	}
}

// Close stops the node and waits for all goroutines started by Start() and all
// replicateTo goroutines to exit. Safe to call multiple times.
func (n *Node) Close() {
	n.Stop()
	n.wg.Wait()
}

// State returns the node's current state.
func (n *Node) State() NodeState {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state
}

// Term returns the node's current term.
func (n *Node) Term() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
}

// LeaderID returns the ID of the current leader (empty if unknown).
func (n *Node) LeaderID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

// IsLeader reports whether this node is the current Raft leader.
func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID == n.id && n.leaderID != ""
}

// ID returns the node's ID.
func (n *Node) ID() string {
	return n.id
}

// CommittedIndex returns the current commitIndex. Safe to call concurrently.
func (n *Node) CommittedIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

// PeerMatchIndex returns the last log index replicated to peer.
// peerKey is a QUIC address for data-Raft or a nodeID for meta-Raft.
// Returns (0, false) if the peer is not tracked.
func (n *Node) PeerMatchIndex(peerKey string) (uint64, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	idx, ok := n.matchIndex[peerKey]
	return idx, ok
}

// Peers returns a snapshot of the current peer list. The caller receives a
// copy so mutations to the returned slice do not affect the node's state.
func (n *Node) Peers() []string {
	n.mu.Lock()
	out := make([]string, len(n.config.Peers))
	copy(out, n.config.Peers)
	n.mu.Unlock()
	return out
}

// ApplyCh returns the channel on which committed log entries are delivered.
func (n *Node) ApplyCh() <-chan LogEntry {
	return n.applyCh
}

// Configuration returns a race-safe snapshot of the current cluster membership
// (self as Voter + peers as Voter + learners as NonVoter).
//
// During §4.3 JointEntering, returns the union of C_old and C_new voters so
// callers see all servers participating in the dual quorum. Address lookup is
// the caller's responsibility (peer keys are returned as Server.ID).
func (n *Node) Configuration() Configuration {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.jointPhase == JointEntering {
		seen := make(map[string]struct{},
			len(n.jointOldVoters)+len(n.jointNewVoters)+len(n.learnerIDs))
		servers := make([]Server, 0, len(seen))
		for _, v := range n.jointOldVoters {
			if _, ok := seen[v]; !ok {
				seen[v] = struct{}{}
				servers = append(servers, Server{ID: v, Suffrage: Voter})
			}
		}
		for _, v := range n.jointNewVoters {
			if _, ok := seen[v]; !ok {
				seen[v] = struct{}{}
				servers = append(servers, Server{ID: v, Suffrage: Voter})
			}
		}
		for _, pk := range n.learnerIDs {
			if _, ok := seen[pk]; !ok {
				seen[pk] = struct{}{}
				servers = append(servers, Server{ID: pk, Suffrage: NonVoter})
			}
		}
		return Configuration{Servers: servers}
	}

	servers := make([]Server, 0, len(n.config.Peers)+1+len(n.learnerIDs))
	servers = append(servers, Server{ID: n.id, Suffrage: Voter})
	for _, p := range n.config.Peers {
		servers = append(servers, Server{ID: p, Suffrage: Voter})
	}
	for _, pk := range n.learnerIDs {
		servers = append(servers, Server{ID: pk, Suffrage: NonVoter})
	}
	return Configuration{Servers: servers}
}

// JointPhase returns the current §4.3 joint state, including the dual voter
// sets when in JointEntering. enterIndex is 0 when phase == JointNone.
//
// The returned phase is the unexported jointPhase type promoted via int8 cast
// at package boundaries; callers can use raft.JointNone / raft.JointEntering
// constants for comparison.
func (n *Node) JointPhase() (phase JointPhase, oldVoters []string, newVoters []string, enterIndex uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	phase = JointPhase(n.jointPhase)
	if n.jointPhase == JointEntering {
		oldVoters = append([]string(nil), n.jointOldVoters...)
		newVoters = append([]string(nil), n.jointNewVoters...)
		enterIndex = n.jointEnterIndex
	}
	return
}

// JointPhase is the exported alias of jointPhase for callers querying joint
// state via Node.JointPhase().
type JointPhase = jointPhase

// JointSnapshotState captures the §4.3 joint state for snapshot persistence.
// Use as JointStateProvider for SnapshotManager. Returns int8 phase to keep the
// jointPhase type unexported across package boundaries.
func (n *Node) JointSnapshotState() (phase int8, jointOldVoters, jointNewVoters []string, jointEnterIndex uint64, managedLearners []string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	ml := make([]string, 0, len(n.jointManagedLearners))
	for id := range n.jointManagedLearners {
		ml = append(ml, id)
	}
	return int8(n.jointPhase),
		append([]string(nil), n.jointOldVoters...),
		append([]string(nil), n.jointNewVoters...),
		n.jointEnterIndex,
		ml
}

// RestoreJointStateFromSnapshot adopts §4.3 joint state read from a snapshot.
// Use as JointStateRestorer for SnapshotManager. jointLeaveProposed is reset to
// false so the leader's heartbeat watcher re-evaluates and re-proposes JointLeave
// if the JointEnter entry is still committed but Leave hasn't run yet.
func (n *Node) RestoreJointStateFromSnapshot(phase int8, jointOldVoters, jointNewVoters []string, jointEnterIndex uint64, managedLearners []string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.jointPhase = jointPhase(phase)
	n.jointOldVoters = jointOldVoters
	n.jointNewVoters = jointNewVoters
	n.jointEnterIndex = jointEnterIndex
	n.jointLeaveProposed = false
	n.jointManagedLearners = make(map[string]struct{}, len(managedLearners))
	for _, id := range managedLearners {
		n.jointManagedLearners[id] = struct{}{}
	}
}

// containsServer reports whether servers contains an entry with the given id.
// Used to derive removedFromCluster from snapshot Servers (cold-start +
// InstallSnapshot paths) — replay path uses containsPeer over peer-key slices.
func containsServer(servers []Server, id string) bool {
	for _, sv := range servers {
		if sv.ID == id {
			return true
		}
	}
	return false
}

// currentConfigServers returns a copy of all known servers (self + voters + learners).
// Learners are included with NonVoter suffrage so snapshots capture full membership.
// Self is omitted when removedFromCluster — a snapshotted orphan node must not
// claim to be a voter in its own snapshot. The restore path uses self ∉ Servers
// to derive removedFromCluster on cold-start.
// MUST be called with n.mu held.
func (n *Node) currentConfigServers() []Server {
	out := make([]Server, 0, len(n.config.Peers)+1+len(n.learnerIDs))
	if !n.removedFromCluster {
		out = append(out, Server{ID: n.id, Suffrage: Voter})
	}
	for _, p := range n.config.Peers {
		out = append(out, Server{ID: p, Suffrage: Voter})
	}
	for _, pk := range n.learnerIDs {
		out = append(out, Server{ID: pk, Suffrage: NonVoter})
	}
	return out
}

// checkLearnerCatchup proposes PromoteToVoter for any learner that has caught up.
// MUST be called with n.mu held; called from runLeader after heartbeat tick.
// Respects single-pending-change invariant: skips if a ConfChange is in flight.
func (n *Node) checkLearnerCatchup() {
	if n.state != Leader {
		return
	}
	if n.pendingConfChangeIndex != 0 {
		return
	}
	// Guard 1: jointPhase != None blocks auto-promote during the joint window.
	// Once a JointEnter has been proposed, the joint will atomically promote
	// new voters via C_new; auto-promote here would race.
	if n.jointPhase != JointNone {
		return
	}
	threshold := n.config.LearnerCatchupThreshold
	if threshold == 0 {
		threshold = 100
	}
	for nodeID, peerKey := range n.learnerIDs {
		// Guard 2: jointManagedLearners blocks auto-promote during the
		// pre-joint AddLearner→catch-up window on the leader that initiated
		// ChangeMembership.
		if _, joint := n.jointManagedLearners[nodeID]; joint {
			continue
		}
		mi := n.matchIndex[peerKey]
		if mi+threshold < n.commitIndex {
			continue
		}
		cmd := encodeConfChange(ConfChangePayload{Op: ConfChangePromote, ID: nodeID, Address: "", ManagedByJoint: false})
		select {
		case n.proposalCh <- proposal{
			command:   cmd,
			entryType: LogEntryConfChange,
			doneCh:    nil,
			ctx:       context.Background(),
		}:
		default: // proposal channel full; retry next tick
		}
		return // 한 번에 하나만 (single-pending invariant)
	}
}

// SetLearnerCatchupThreshold updates the threshold at runtime (operations / tests).
func (n *Node) SetLearnerCatchupThreshold(threshold uint64) {
	n.mu.Lock()
	n.config.LearnerCatchupThreshold = threshold
	n.mu.Unlock()
}

func (n *Node) run() {
	var prevState NodeState = Follower
	for {
		select {
		case <-n.stopCh:
			return
		default:
		}

		n.mu.Lock()
		state := n.state
		leaderID := n.leaderID
		term := n.currentTerm
		n.mu.Unlock()

		if state != prevState {
			if state == Leader {
				n.notifyObservers(Event{Type: EventLeaderChange, IsLeader: true, LeaderID: n.id, Term: term})
			} else if prevState == Leader {
				n.notifyObservers(Event{Type: EventLeaderChange, IsLeader: false, LeaderID: leaderID, Term: term})
			}
		}
		prevState = state

		switch state {
		case Follower:
			n.runFollower()
		case Candidate:
			n.runCandidate()
		case Leader:
			n.runLeader()
		}
	}
}

func (n *Node) randomElectionTimeout() time.Duration {
	base := n.config.ElectionTimeout
	jitter := time.Duration(rand.Int63n(int64(base)))
	return base + jitter
}

func (n *Node) runFollower() {
	timeout := n.randomElectionTimeout()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-n.stopCh:
		return
	case <-n.resetCh:
		return // restart the loop, new timeout
	case <-timer.C:
		// Skip elections if self was removed from the cluster — orphan nodes
		// must not split votes against the surviving voter set.
		n.mu.Lock()
		removed := n.removedFromCluster
		n.mu.Unlock()
		if removed {
			return
		}
		if !n.runPreVote() {
			return // pre-vote failed; stay follower for next timeout
		}
		// S1: re-check lastLeaderContact — a leader may have contacted us
		// while pre-vote RPCs were in flight. Stay follower if so.
		n.mu.Lock()
		if time.Since(n.lastLeaderContact) < n.config.ElectionTimeout {
			n.mu.Unlock()
			return
		}
		n.state = Candidate
		n.mu.Unlock()
	}
}

// runPreVote sends pre-vote RPCs to all peers before the node increments its
// term. Returns true if a majority granted the pre-vote (caller may proceed to
// a real election). In joint mode (jointPhase == JointEntering), majority is
// required in BOTH old and new voter sets (§4.3).
// Returns false if peers indicate the cluster is healthy.
func (n *Node) runPreVote() bool {
	n.mu.Lock()
	proposedTerm := n.currentTerm + 1
	lastLogIndex, lastLogTerm := n.lastLogInfo()
	// Snapshot of all voters across both quorum sets (peers we need to contact).
	voterSet := make(map[string]struct{}, len(n.config.Peers))
	for _, p := range n.config.Peers {
		voterSet[p] = struct{}{}
	}
	for _, p := range n.jointOldVoters {
		voterSet[p] = struct{}{}
	}
	for _, p := range n.jointNewVoters {
		voterSet[p] = struct{}{}
	}
	delete(voterSet, n.id)
	peers := make([]string, 0, len(voterSet))
	for p := range voterSet {
		peers = append(peers, p)
	}
	n.mu.Unlock()

	if len(peers) == 0 {
		return true // single-node: always wins
	}

	type result struct {
		peer    string
		granted bool
		term    uint64
	}
	resultCh := make(chan result, len(peers))
	for _, peer := range peers {
		go func(p string) {
			args := &RequestVoteArgs{
				Term:         proposedTerm,
				CandidateID:  n.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
				PreVote:      true,
			}
			reply, err := n.sendRequestVote(p, args)
			if err != nil {
				resultCh <- result{peer: p}
				return
			}
			resultCh <- result{peer: p, granted: reply.VoteGranted, term: reply.Term}
		}(peer)
	}

	granted := make(map[string]bool, len(peers))
	var maxReplyTerm uint64
	timer := time.NewTimer(n.randomElectionTimeout())
	defer timer.Stop()

	pending := len(peers)
	for {
		n.mu.Lock()
		if n.dualMajority(granted) {
			n.mu.Unlock()
			break
		}
		n.mu.Unlock()
		if pending == 0 {
			return false
		}
		select {
		case <-n.stopCh:
			return false
		case <-timer.C:
			return false
		case r := <-resultCh:
			pending--
			if r.granted {
				granted[r.peer] = true
			}
			if r.term > maxReplyTerm {
				maxReplyTerm = r.term
			}
		}
	}

	// M2: if a peer carries a higher term, there may be a live leader we are
	// unaware of. Signal reset so we don't spin-retry immediately; the real
	// election (if we proceed) will fail on term mismatch and the next
	// AppendEntries from the actual leader will update our term naturally.
	// (Returning false here would create a liveness issue: the node stays at
	// its stale term and can never win pre-vote until it hears from the leader.)
	n.mu.Lock()
	if maxReplyTerm > n.currentTerm {
		n.signalReset()
	}
	n.mu.Unlock()

	return true
}

func (n *Node) runCandidate() {
	n.mu.Lock()
	n.currentTerm++
	n.votedFor = n.id
	n.persistState()
	term := n.currentTerm
	lastLogIndex, lastLogTerm := n.lastLogInfo()
	// Snapshot of all voters across both quorum sets (peers we need to contact).
	voterSet := make(map[string]struct{}, len(n.config.Peers))
	for _, p := range n.config.Peers {
		voterSet[p] = struct{}{}
	}
	for _, p := range n.jointOldVoters {
		voterSet[p] = struct{}{}
	}
	for _, p := range n.jointNewVoters {
		voterSet[p] = struct{}{}
	}
	delete(voterSet, n.id)
	peers := make([]string, 0, len(voterSet))
	for p := range voterSet {
		peers = append(peers, p)
	}
	isTransfer := n.leaderTransfer
	n.leaderTransfer = false // consume the flag
	n.mu.Unlock()

	// If we don't win the election, revert to Follower so run() re-enters
	// runFollower with a fresh random timeout instead of looping back into
	// runCandidate and incrementing the term again on every iteration.
	defer func() {
		n.mu.Lock()
		if n.state == Candidate {
			n.state = Follower
		}
		n.mu.Unlock()
	}()

	// Single-peer node: dualMajority with empty granted map already passes (self-only).
	if len(peers) == 0 {
		n.mu.Lock()
		if n.dualMajority(map[string]bool{}) {
			n.state = Leader
			n.leaderID = n.id
			n.initLeaderState()
		}
		n.mu.Unlock()
		return
	}

	type voteResult struct {
		peer    string
		granted bool
	}
	voteCh := make(chan voteResult, len(peers))

	for _, peer := range peers {
		go func(p string) {
			args := &RequestVoteArgs{
				Term:           term,
				CandidateID:    n.id,
				LastLogIndex:   lastLogIndex,
				LastLogTerm:    lastLogTerm,
				LeaderTransfer: isTransfer,
			}
			reply, err := n.sendRequestVote(p, args)
			if err != nil {
				voteCh <- voteResult{peer: p}
				return
			}

			n.mu.Lock()
			if reply.Term > n.currentTerm {
				n.currentTerm = reply.Term
				n.state = Follower
				n.votedFor = ""
				n.persistState()
				n.mu.Unlock()
				voteCh <- voteResult{peer: p}
				return
			}
			n.mu.Unlock()
			voteCh <- voteResult{peer: p, granted: reply.VoteGranted}
		}(peer)
	}

	timeout := n.randomElectionTimeout()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	granted := make(map[string]bool, len(peers))
	for range len(peers) {
		select {
		case <-n.stopCh:
			return
		case <-timer.C:
			return
		case <-n.resetCh:
			return
		case r := <-voteCh:
			if r.granted {
				granted[r.peer] = true
			}
			n.mu.Lock()
			if n.dualMajority(granted) {
				if n.state == Candidate && n.currentTerm == term {
					n.state = Leader
					n.leaderID = n.id
					n.initLeaderState()
				}
				n.mu.Unlock()
				return
			}
			n.mu.Unlock()
		}
	}
}

func (n *Node) initLeaderState() {
	nextIdx := n.lastLogIdx() + 1
	now := time.Now()
	for _, peer := range n.config.Peers {
		n.nextIndex[peer] = nextIdx
		n.matchIndex[peer] = 0
		// Seed with now so CheckQuorum doesn't fire before the first heartbeat
		// round completes (grace period = 3×HeartbeatTimeout from now).
		n.checkQuorumAcks[peer] = now
	}
	// Initialize learner replication state too (learners receive AppendEntries
	// for catch-up but don't count toward quorum). Without this, a new leader
	// elected after a learner was added in a previous term would have unset
	// nextIndex for that learner, breaking learner replication and the
	// catch-up watcher.
	for _, peerKey := range n.learnerIDs {
		n.nextIndex[peerKey] = nextIdx
		n.matchIndex[peerKey] = 0
	}
	// Track self's matchIndex
	n.matchIndex[n.id] = n.lastLogIdx()
	// When this node steps down via CheckQuorum it becomes a follower with
	// knowledge of a recent leader (itself). Seed lastLeaderContact so the
	// stickiness window applies and we don't immediately grant votes.
	n.lastLeaderContact = now
}

// hasQuorum returns true if a majority of the cluster (including self) has
// replied to at least one AppendEntries within 3×HeartbeatTimeout.
// In joint mode (jointPhase == JointEntering), majority is required in BOTH
// the old and new voter sets (§4.3).
// Must be called with n.mu held.
func (n *Node) hasQuorum() bool {
	current, _ := n.quorumSets()
	if len(current) == 0 {
		return true // single-node bootstrap
	}
	threshold := time.Now().Add(-3 * n.config.HeartbeatTimeout)
	matched := make(map[string]bool, len(n.checkQuorumAcks))
	for peer, last := range n.checkQuorumAcks {
		if last.After(threshold) {
			matched[peer] = true
		}
	}
	return n.dualMajority(matched)
}

func (n *Node) runLeader() {
	// Propose a no-op in the new term so advanceCommitIndex can commit entries
	// from previous terms (Raft §8: leader completeness via no-op).
	if len(n.noOpCmd) > 0 {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, _ = n.ProposeWait(ctx, n.noOpCmd)
		}()
	}

	n.replicateToAll()

	ticker := time.NewTicker(n.config.HeartbeatTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.mu.Lock()
			if !n.hasQuorum() {
				// Cannot hear from majority: step down rather than accepting
				// Propose calls that will never commit.
				n.state = Follower
				n.leaderID = ""
				n.mu.Unlock()
				n.signalReset()
				return
			}
			n.mu.Unlock()
			n.replicateToAll()
			// Learner-first: check if any learner caught up and propose Promote.
			// Joint consensus: drive C_old+new → C_new auto-advance.
			n.mu.Lock()
			n.checkLearnerCatchup()
			n.checkJointAdvance()
			n.mu.Unlock()
			if !n.gcRunning.Swap(true) {
				go func() {
					defer n.gcRunning.Store(false)
					n.maybeRunLogGC()
				}()
			}
		case <-n.replicationCh:
			n.replicateToAll()
		case <-n.resetCh:
			return
		}
	}
}

func (n *Node) replicateToAll() {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}
	peers := n.config.Peers
	learners := make([]string, 0, len(n.learnerIDs))
	for _, pk := range n.learnerIDs {
		learners = append(learners, pk)
	}
	n.mu.Unlock()

	for _, peer := range peers {
		n.wg.Add(1)
		go func(p string) { defer n.wg.Done(); n.replicateTo(p) }(peer)
	}
	// Learners receive log entries for catch-up but do not count toward quorum.
	for _, peerKey := range learners {
		n.wg.Add(1)
		go func(p string) { defer n.wg.Done(); n.replicateTo(p) }(peerKey)
	}
}

func (n *Node) replicateTo(peer string) {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}
	term := n.currentTerm
	leaderID := n.id
	commitIndex := n.commitIndex

	nextIdx := n.nextIndex[peer]

	// If nextIdx is behind the compacted log, send snapshot instead
	if nextIdx < n.firstIndex && n.sendInstallSnapshot != nil && n.store != nil {
		snap, err := n.store.LoadSnapshot()
		if err == nil && snap.Data != nil {
			// Capture current config while holding the lock (read just above)
			snapshotServers := n.currentConfigServers()
			args := &InstallSnapshotArgs{
				Term:              term,
				LeaderID:          leaderID,
				LastIncludedIndex: snap.Index,
				LastIncludedTerm:  snap.Term,
				Data:              snap.Data,
				Servers:           snapshotServers,
			}
			n.mu.Unlock()

			reply, err := n.sendInstallSnapshot(peer, args)
			if err != nil {
				return
			}

			n.mu.Lock()
			if reply.Term > n.currentTerm {
				n.currentTerm = reply.Term
				n.state = Follower
				n.votedFor = ""
				n.leaderID = ""
				n.persistState()
				n.signalReset()
				n.mu.Unlock()
				return
			}
			n.nextIndex[peer] = snap.Index + 1
			n.matchIndex[peer] = snap.Index
			n.mu.Unlock()
			return
		}
	}

	prevLogIndex := nextIdx - 1
	prevLogTerm := uint64(0)
	if prevLogIndex > 0 && n.hasLogEntry(prevLogIndex) {
		prevLogTerm = n.log[n.toSliceIdx(prevLogIndex)].Term
	}

	// Collect entries to send
	var entries []LogEntry
	if n.hasLogEntry(nextIdx) {
		si := n.toSliceIdx(nextIdx)
		entries = make([]LogEntry, len(n.log)-si)
		copy(entries, n.log[si:])
	}

	// Cap payload size to avoid oversized AE messages.
	if n.config.MaxEntriesPerAE > 0 && uint64(len(entries)) > n.config.MaxEntriesPerAE {
		entries = entries[:n.config.MaxEntriesPerAE]
		raftAESplitCountTotal.Inc()
	}
	n.mu.Unlock()

	args := &AppendEntriesArgs{
		Term:         term,
		LeaderID:     leaderID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: commitIndex,
	}
	reply, err := n.sendAppendEntries(peer, args)
	if err != nil {
		n.notifyObservers(Event{Type: EventFailedHeartbeat, PeerID: peer})
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.currentTerm {
		n.currentTerm = reply.Term
		n.state = Follower
		n.votedFor = ""
		n.leaderID = ""
		n.persistState()
		n.signalReset()
		return
	}

	if n.state != Leader || n.currentTerm != term {
		return
	}

	// Record peer responded (CheckQuorum: any reply counts, not just Success).
	n.checkQuorumAcks[peer] = time.Now()

	if reply.Success {
		n.nextIndex[peer] = nextIdx + uint64(len(entries))
		n.matchIndex[peer] = n.nextIndex[peer] - 1
		n.advanceCommitIndex()
	} else {
		n.applyConflictHint(peer, reply, nextIdx, entries)
	}
}

// applyConflictHint updates nextIndex[peer] based on a failed AppendEntries reply.
// Must be called with n.mu held.
// sentNextIdx is the nextIndex that was sent in the failed request.
func (n *Node) applyConflictHint(peer string, reply *AppendEntriesReply, sentNextIdx uint64, _ []LogEntry) {
	if reply.ConflictIndex > 0 {
		if reply.ConflictTerm == 0 {
			// Follower missing entries: jump directly to ConflictIndex.
			n.nextIndex[peer] = reply.ConflictIndex
		} else {
			// Scan leader's log backwards for the last entry with ConflictTerm.
			// If found, set nextIndex = that entry's index + 1 (skip past our own copy).
			// If not found, use follower's ConflictIndex (skip the follower's bad term).
			newNext := reply.ConflictIndex
			for i := len(n.log) - 1; i >= 0; i-- {
				raftIdx := n.firstIndex + uint64(i)
				switch {
				case n.log[i].Term == reply.ConflictTerm:
					newNext = raftIdx + 1
					raftConflictTermJumpsTotal.Inc()
					goto applyDone
				case n.log[i].Term < reply.ConflictTerm:
					goto applyDone // not in our log, use follower hint
				}
			}
		applyDone:
			n.nextIndex[peer] = newNext
		}
	} else {
		// Old peer without hint support: decrement by 1.
		if sentNextIdx > 1 {
			n.nextIndex[peer] = sentNextIdx - 1
		}
	}
}

func (n *Node) advanceCommitIndex() {
	// Find the highest N such that a majority of matchIndex[i] >= N (in BOTH
	// quorums during joint mode) and log[N].term == currentTerm.
	for idx := n.lastLogIdx(); idx > n.commitIndex; idx-- {
		if !n.hasLogEntry(idx) {
			continue
		}
		if n.log[n.toSliceIdx(idx)].Term != n.currentTerm {
			continue
		}

		matched := make(map[string]bool, len(n.matchIndex))
		for peer, mi := range n.matchIndex {
			if mi >= idx {
				matched[peer] = true
			}
		}
		if !n.dualMajority(matched) {
			continue
		}

		n.commitIndex = idx
		n.signalCommit()
		return
	}
}

// QuorumMinMatchIndex returns the highest log index replicated to at least a
// quorum of nodes (including self). Safe to use as a GC watermark.
// For a 3-node cluster with matchIndex [100, 80, 50], returns 80 (index 1 in
// descending sort): that index is guaranteed on ≥2 of 3 nodes.
func (n *Node) QuorumMinMatchIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.quorumMinMatchIndexLocked()
}

func (n *Node) quorumMinMatchIndexLocked() uint64 {
	current, old := n.quorumSets()

	medianMatch := func(set []string) uint64 {
		if len(set) == 0 {
			return 0
		}
		vals := make([]uint64, 0, len(set))
		for _, peer := range set {
			vals = append(vals, n.matchIndex[peer])
		}
		sort.Slice(vals, func(i, j int) bool { return vals[i] > vals[j] })
		return vals[len(vals)/2]
	}

	cur := medianMatch(current)
	if old == nil {
		return cur
	}
	o := medianMatch(old)
	if cur < o {
		return cur
	}
	return o
}

// maybeRunLogGC truncates the Raft log store up to the quorum watermark when
// managed mode is enabled and the GC interval has elapsed.
func (n *Node) maybeRunLogGC() {
	n.mu.Lock()
	if n.store == nil || !n.config.ManagedMode {
		n.mu.Unlock()
		return
	}
	interval := n.config.LogGCInterval
	if interval == 0 {
		interval = 30 * time.Second
	}
	now := time.Now()
	if !n.lastLogGC.IsZero() && now.Sub(n.lastLogGC) < interval {
		n.mu.Unlock()
		return
	}
	// Leader: quorum watermark (highest index confirmed on majority).
	// Follower: commitIndex (set by leader only after quorum majority).
	var watermark uint64
	if n.state == Leader {
		watermark = n.quorumMinMatchIndexLocked()
	} else {
		watermark = n.commitIndex
	}
	n.lastLogGC = now
	n.mu.Unlock()

	if watermark == 0 {
		return
	}
	// Snapshot gate: without a snapshot covering the watermark, lagging
	// followers that need pre-GC entries cannot recover via InstallSnapshot.
	snap, err := n.store.LoadSnapshot()
	snapIdx := snap.Index
	if err != nil {
		log.Warn().Err(err).Msg("raft: log GC skipped — snapshot load error")
		return
	}
	if snapIdx < watermark {
		log.Warn().Uint64("snapIdx", snapIdx).Uint64("watermark", watermark).Msg("raft: log GC skipped — no snapshot covers watermark; take a snapshot first")
		return
	}
	if err := n.store.TruncateBefore(watermark); err != nil {
		log.Warn().Uint64("watermark", watermark).Err(err).Msg("raft: log GC failed")
		return
	}
	log.Info().Uint64("watermark", watermark).Msg("raft: log GC complete")
}

// HandleRequestVote processes an incoming RequestVote RPC.
func (n *Node) HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Pre-vote path: assess candidacy without mutating any state.
	// The caller wants to know "would you vote for me?" before committing to
	// incrementing its term. We must not change currentTerm, votedFor, or state.
	if args.PreVote {
		reply := &RequestVoteReply{Term: n.currentTerm}
		// Reject if we ARE the leader (there is definitely a live leader in the
		// cluster), or if we heard from one recently.
		// The leader never updates lastLeaderContact (it sends AE, not receives),
		// so the time-based check alone would cause the leader to grant pre-votes.
		if n.state == Leader || time.Since(n.lastLeaderContact) < n.config.ElectionTimeout {
			return reply
		}
		reply.VoteGranted = args.Term > n.currentTerm &&
			n.isLogUpToDate(args.LastLogIndex, args.LastLogTerm)
		return reply
	}

	reply := &RequestVoteReply{Term: n.currentTerm}

	// Reply false if term < currentTerm.
	if args.Term < n.currentTerm {
		return reply
	}

	// Leader stickiness: reject RequestVote (and suppress term update) if we
	// heard from a live leader within ElectionTimeout. This blocks a
	// partition-returning node from disrupting the cluster even if it bypassed
	// pre-vote (e.g. direct RPC injection).
	//
	// Critical: do NOT update currentTerm here. Updating would cause the leader
	// to see our next AppendEntriesReply with a higher term and step down —
	// the exact disruption we're preventing.
	//
	// Exception: leader-transfer elections are intentional disruptions;
	// the sender explicitly asked us to step aside.
	if !args.LeaderTransfer && time.Since(n.lastLeaderContact) < n.config.ElectionTimeout {
		return reply
	}

	// If RPC request's term > currentTerm, update and convert to follower.
	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.state = Follower
		n.votedFor = ""
		n.leaderID = ""
		n.persistState()
	}
	reply.Term = n.currentTerm

	// Grant vote if we haven't voted or already voted for this candidate,
	// and candidate's log is at least as up-to-date as ours.
	if (n.votedFor == "" || n.votedFor == args.CandidateID) && n.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		n.votedFor = args.CandidateID
		n.persistState()
		reply.VoteGranted = true
		n.signalReset()
	}

	return reply
}

// HandleAppendEntries processes an incoming AppendEntries RPC.
func (n *Node) HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := &AppendEntriesReply{Term: n.currentTerm}

	if args.Term < n.currentTerm {
		return reply
	}

	// Valid leader contact: reset election timer
	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.votedFor = ""
		n.persistState()
	}
	n.state = Follower
	n.leaderID = args.LeaderID
	n.lastLeaderContact = time.Now()
	n.signalReset()

	reply.Term = n.currentTerm

	// Log consistency check
	if args.PrevLogIndex > 0 {
		if !n.hasLogEntry(args.PrevLogIndex) {
			// Follower is behind: tell leader our last index so it can jump directly.
			reply.ConflictIndex = n.lastLogIdx() + 1
			return reply
		}
		if n.log[n.toSliceIdx(args.PrevLogIndex)].Term != args.PrevLogTerm {
			// Term conflict: find the first index of the conflicting term
			// so the leader can skip the entire bad term in one step.
			conflictTerm := n.log[n.toSliceIdx(args.PrevLogIndex)].Term
			conflictIdx := args.PrevLogIndex
			for conflictIdx > n.firstIndex && n.hasLogEntry(conflictIdx-1) &&
				n.log[n.toSliceIdx(conflictIdx-1)].Term == conflictTerm {
				conflictIdx--
			}
			reply.ConflictTerm = conflictTerm
			reply.ConflictIndex = conflictIdx
			n.abortWaitersFrom(args.PrevLogIndex)
			n.log = n.log[:n.toSliceIdx(args.PrevLogIndex)]
			return reply
		}
	}

	// Append new entries (skip already present)
	var newEntries []LogEntry
	truncated := false
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + uint64(i) + 1
		if n.hasLogEntry(idx) {
			if n.log[n.toSliceIdx(idx)].Term != entry.Term {
				n.abortWaitersFrom(idx)
				n.log = n.log[:n.toSliceIdx(idx)]
				newEntries = args.Entries[i:]
				n.log = append(n.log, newEntries...)
				truncated = true
				break
			}
		} else {
			newEntries = args.Entries[i:]
			n.log = append(n.log, newEntries...)
			break
		}
	}
	n.persistLogEntries(newEntries)

	// §4.4 / §4.3: after truncation, rebuild config (and joint state) from
	// initial peers + remaining log, then apply membership entries in the newly
	// appended batch. rebuildConfigFromLog reverts an uncommitted JointLeave
	// back to JointEntering so dual-quorum stays in force until the next leader
	// re-commits Leave.
	if truncated {
		n.rebuildConfigFromLog(0, n.initialPeers, map[string]string{})
	}
	for i := range newEntries {
		switch newEntries[i].Type {
		case LogEntryConfChange, LogEntryJointConfChange:
			n.applyConfigChangeLocked(newEntries[i])
		}
	}

	// Update commit index
	if args.LeaderCommit > n.commitIndex {
		lastNew := args.PrevLogIndex + uint64(len(args.Entries))
		if args.LeaderCommit < lastNew {
			n.commitIndex = args.LeaderCommit
		} else {
			n.commitIndex = lastNew
		}
		n.signalCommit()
	}

	reply.Success = true
	return reply
}

func (n *Node) signalReset() {
	select {
	case n.resetCh <- struct{}{}:
	default:
	}
}

// abortWaitersFrom signals ErrProposalFailed to all waiters at index >= from
// and removes them from the map. Must be called with n.mu held.
//
// Safety: channels are created as make(chan error, 1) in flushBatch and are
// never sent to except here (applyLoop only closes them). Since both paths
// hold n.mu, the buffer is always empty at this point — the send is non-blocking.
func (n *Node) abortWaitersFrom(from uint64) {
	for idx, ch := range n.waiters {
		if idx >= from {
			select {
			case ch <- ErrProposalFailed:
			default:
			}
			delete(n.waiters, idx)
		}
	}
}

// signalCommit notifies the applyLoop that commitIndex has advanced.
func (n *Node) signalCommit() {
	select {
	case n.commitCh <- struct{}{}:
	default:
	}
}

func (n *Node) lastLogInfo() (uint64, uint64) {
	if len(n.log) == 0 {
		if n.firstIndex > 1 {
			return n.firstIndex - 1, 0 // compacted: last known index is just before firstIndex
		}
		return 0, 0
	}
	last := n.log[len(n.log)-1]
	return last.Index, last.Term
}

func (n *Node) lastLogIdx() uint64 {
	if len(n.log) == 0 {
		if n.firstIndex > 1 {
			return n.firstIndex - 1
		}
		return 0
	}
	return n.log[len(n.log)-1].Index
}

// toSliceIdx converts a Raft log index to a slice index.
// Returns -1 if the index is below the compacted region.
func (n *Node) toSliceIdx(raftIdx uint64) int {
	if raftIdx < n.firstIndex {
		return -1
	}
	return int(raftIdx - n.firstIndex)
}

// hasLogEntry returns true if the given Raft index is in the in-memory log.
func (n *Node) hasLogEntry(raftIdx uint64) bool {
	si := n.toSliceIdx(raftIdx)
	return si >= 0 && si < len(n.log)
}

// CompactLog removes log entries before snapshotIndex according to TrailingLogs.
// If TrailingLogs > 0, the last TrailingLogs entries before snapshotIndex+1 are kept
// in memory so slightly-lagged followers can catch up without InstallSnapshot.
// After compaction, firstIndex = max(current firstIndex, snapshotIndex+1-TrailingLogs).
func (n *Node) CompactLog(snapshotIndex uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.snapshotIndex = snapshotIndex

	trailing := n.config.TrailingLogs
	var keepFrom uint64
	if trailing == 0 {
		// Original behavior: remove all entries up to and including snapshotIndex.
		keepFrom = snapshotIndex + 1
	} else if snapshotIndex+1 > trailing {
		keepFrom = snapshotIndex + 1 - trailing
	} else {
		keepFrom = n.firstIndex // TrailingLogs > snapshotIndex; keep everything
	}
	if keepFrom < n.firstIndex {
		keepFrom = n.firstIndex // can't go back further than in-memory
	}

	si := n.toSliceIdx(keepFrom)
	if si < 0 {
		// keepFrom is already past what we have; set firstIndex to snapshotIndex+1
		n.log = nil
		n.firstIndex = snapshotIndex + 1
		return
	}

	if si >= len(n.log) {
		n.log = nil
		n.firstIndex = snapshotIndex + 1
		return
	}

	if si > 0 {
		remaining := make([]LogEntry, len(n.log)-si)
		copy(remaining, n.log[si:])
		n.log = remaining
	}
	n.firstIndex = keepFrom
}

func (n *Node) isLogUpToDate(lastLogIndex, lastLogTerm uint64) bool {
	myLastIndex, myLastTerm := n.lastLogInfo()

	if lastLogTerm != myLastTerm {
		return lastLogTerm > myLastTerm
	}
	return lastLogIndex >= myLastIndex
}

// InstallSnapshotArgs is sent by the leader to bring a slow follower up to date.
type InstallSnapshotArgs struct {
	Term              uint64
	LeaderID          string
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
	Servers           []Server // cluster config at snapshot point; restores config.Peers on follower
}

// InstallSnapshotReply is the response to an InstallSnapshot RPC.
type InstallSnapshotReply struct {
	Term uint64
}

// HandleInstallSnapshot processes an incoming snapshot from the leader.
// The follower replaces its entire log and state with the snapshot.
func (n *Node) HandleInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := &InstallSnapshotReply{Term: n.currentTerm}

	if args.Term < n.currentTerm {
		return reply
	}

	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.votedFor = ""
		n.persistState()
	}
	n.state = Follower
	n.leaderID = args.LeaderID
	n.lastLeaderContact = time.Now()
	n.signalReset()

	reply.Term = n.currentTerm

	// Save snapshot to store
	if n.store != nil {
		if err := n.store.SaveSnapshot(Snapshot{
			Index:   args.LastIncludedIndex,
			Term:    args.LastIncludedTerm,
			Data:    args.Data,
			Servers: args.Servers,
		}); err != nil {
			return reply
		}
	}

	// Discard entire log and reset to post-snapshot state
	n.abortWaitersFrom(0)
	n.log = nil
	n.firstIndex = args.LastIncludedIndex + 1
	n.lastApplied = args.LastIncludedIndex
	n.commitIndex = args.LastIncludedIndex
	n.pendingConfChangeIndex = 0
	n.signalCommit()

	// Restore cluster config from snapshot using Suffrage-aware helper.
	if len(args.Servers) > 0 {
		peers, learners := restoreConfigFromServers(args.Servers, n.id)
		n.config.Peers = peers
		n.initialPeers = peers
		n.learnerIDs = learners
		// Derive removedFromCluster from snapshot Servers (mirror of cold-start
		// path in NewNode). InstallSnapshot from a leader represents authoritative
		// cluster state; if self is absent, we are an orphan.
		n.removedFromCluster = !containsServer(args.Servers, n.id)
	}

	// Deliver snapshot data via applyCh so the FSM can restore
	select {
	case n.applyCh <- LogEntry{
		Term:    args.LastIncludedTerm,
		Index:   args.LastIncludedIndex,
		Command: args.Data,
	}:
	default:
	}

	return reply
}

// TransferLeadership transfers leadership to the most up-to-date peer.
// Sends a TimeoutNow message to the best peer (highest matchIndex) to
// trigger an immediate election, then steps down. Returns ErrNotLeader
// if not the current leader, or ErrNoPeers if there are no peers.
func (n *Node) TransferLeadership() error {
	n.mu.Lock()

	if n.state != Leader {
		n.mu.Unlock()
		return ErrNotLeader
	}

	if len(n.config.Peers) == 0 {
		n.mu.Unlock()
		return ErrNoPeers
	}

	// Pick the peer with the highest matchIndex
	bestPeer := ""
	bestMatch := uint64(0)
	for _, peer := range n.config.Peers {
		if n.matchIndex[peer] >= bestMatch {
			bestMatch = n.matchIndex[peer]
			bestPeer = peer
		}
	}

	sendTimeout := n.sendTimeoutNow
	n.mu.Unlock()

	// Send TimeoutNow to trigger immediate election on the best peer
	if sendTimeout != nil && bestPeer != "" {
		_ = sendTimeout(bestPeer)
	}

	// Step down regardless of whether TimeoutNow succeeded
	n.mu.Lock()
	n.state = Follower
	n.leaderID = ""
	n.signalReset()
	n.mu.Unlock()

	return nil
}

// HandleTimeoutNow causes this node to immediately start an election.
// Sent by the leader during leadership transfer to the chosen successor.
func (n *Node) HandleTimeoutNow() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Follower {
		return
	}

	// Immediately become candidate, bypassing pre-vote and stickiness on peers.
	n.leaderTransfer = true
	n.state = Candidate
	n.signalReset()
}

// persistState saves currentTerm and votedFor to durable storage.
// Must be called with n.mu held. No-op if store is nil.
// Panics on error: a node that fails to persist its vote could violate Raft safety.
func (n *Node) persistState() {
	if n.store == nil {
		return
	}
	if err := n.store.SaveState(n.currentTerm, n.votedFor); err != nil {
		panic(fmt.Sprintf("raft: persist state failed: %v", err))
	}
}

// persistLogEntries saves log entries to durable storage.
// Must be called with n.mu held. No-op if store is nil.
// Panics on error: lost log entries break Raft durability guarantees.
// batcherLoop collects proposals from proposalCh and flushes them in batches.
// It adapts batch size and timeout based on EWMA request rate.
func (n *Node) batcherLoop() {
	for {
		// metrics has its own mutex — no n.mu needed here
		timeout := n.metrics.batchTimeout()
		maxBatch := n.metrics.maxBatch()

		timer := time.NewTimer(timeout)
		var pending []proposal

	collect:
		for {
			select {
			case <-n.stopCh:
				timer.Stop()
				for _, p := range pending {
					select {
					case p.doneCh <- proposalResult{err: ErrProposalFailed}:
					default:
					}
				}
				for {
					select {
					case p := <-n.proposalCh:
						select {
						case p.doneCh <- proposalResult{err: ErrProposalFailed}:
						default:
						}
					default:
						return
					}
				}
			case p := <-n.proposalCh:
				pending = append(pending, p)
				if len(pending) >= maxBatch {
					timer.Stop()
					break collect
				}
			case <-timer.C:
				break collect
			}
		}

		if len(pending) > 0 {
			n.flushBatch(pending)
			for i := range pending {
				pending[i] = proposal{}
			}
			pending = pending[:0]
		}
	}
}

// flushBatch persists all pending proposals as a single batch, advances commit,
// and spawns relay goroutines that deliver results to each caller.
func (n *Node) flushBatch(pending []proposal) {
	n.mu.Lock()

	if n.state != Leader {
		n.mu.Unlock()
		for _, p := range pending {
			select {
			case p.doneCh <- proposalResult{err: ErrNotLeader}:
			default:
			}
		}
		return
	}

	// §4.3/§4.4 guard: at most one membership change may be in-flight at a time.
	// Joint and single-server changes cannot interleave — the §4.3 invariant is
	// jointPhase=None ↔ no §4.4 entry pending. Reject extras synchronously so
	// doneCh callers unblock immediately.
	{
		var confChangeSeen bool
		var jointSeen bool
		filtered := pending[:0]
		for _, p := range pending {
			switch p.entryType {
			case LogEntryConfChange:
				if n.pendingConfChangeIndex != 0 || confChangeSeen || n.jointPhase != JointNone {
					select {
					case p.doneCh <- proposalResult{err: ErrConfChangeInProgress}:
					default:
					}
					continue
				}
				confChangeSeen = true

			case LogEntryJointConfChange:
				if jointSeen {
					select {
					case p.doneCh <- proposalResult{err: ErrConfChangeInProgress}:
					default:
					}
					continue
				}
				jc := decodeJointConfChange(p.command)
				switch jc.Op {
				case JointOpEnter:
					if n.jointPhase != JointNone || n.pendingConfChangeIndex != 0 {
						select {
						case p.doneCh <- proposalResult{err: ErrConfChangeInProgress}:
						default:
						}
						continue
					}
				case JointOpLeave:
					if n.jointPhase != JointEntering {
						select {
						case p.doneCh <- proposalResult{err: ErrConfChangeInProgress}:
						default:
						}
						continue
					}
				}
				jointSeen = true
			}
			filtered = append(filtered, p)
		}
		pending = filtered
	}
	if len(pending) == 0 {
		n.mu.Unlock()
		return
	}

	flushStart := time.Now()
	entries := make([]LogEntry, len(pending))
	base := n.lastLogIdx() + 1
	for i, p := range pending {
		entries[i] = LogEntry{
			Term:    n.currentTerm,
			Index:   base + uint64(i),
			Command: p.command,
			Type:    p.entryType,
		}
	}
	n.log = append(n.log, entries...)
	persistStart := time.Now()
	n.persistLogEntries(entries)
	persistDur := time.Since(persistStart)
	n.matchIndex[n.id] = entries[len(entries)-1].Index

	// §4.4 / §4.3: apply membership entries immediately on append (leader path).
	// Joint entries activate the dual-quorum phase even before commit so that
	// subsequent advanceCommitIndex uses the correct voter sets.
	for i := range entries {
		switch entries[i].Type {
		case LogEntryConfChange, LogEntryJointConfChange:
			n.applyConfigChangeLocked(entries[i])
		}
	}

	commitChs := make([]chan error, len(pending))
	for i, entry := range entries {
		ch := make(chan error, 1)
		n.waiters[entry.Index] = ch
		commitChs[i] = ch
	}
	n.advanceCommitIndex()
	n.mu.Unlock()

	n.metrics.update(len(pending))

	select {
	case n.replicationCh <- struct{}{}:
	default:
	}

	for i, p := range pending {
		go func(p proposal, ch chan error, entry LogEntry) {
			select {
			case err := <-ch:
				if raftTraceEnabled && !p.submitAt.IsZero() {
					log.Debug().
						Dur("batch_wait", flushStart.Sub(p.submitAt)).
						Dur("persist_log", persistDur).
						Dur("flush_total", time.Since(flushStart)).
						Msg("raftFlush trace")
				}
				if err != nil {
					select {
					case p.doneCh <- proposalResult{err: err}:
					default:
					}
				} else {
					select {
					case p.doneCh <- proposalResult{index: entry.Index}:
					default:
					}
				}
			case <-n.stopCh:
				select {
				case p.doneCh <- proposalResult{err: ErrProposalFailed}:
				default:
				}
			case <-p.ctx.Done():
			}
		}(p, commitChs[i], entries[i])
	}
}

func (n *Node) persistLogEntries(entries []LogEntry) {
	if n.store == nil || len(entries) == 0 {
		return
	}
	if err := n.store.AppendEntries(entries); err != nil {
		// Suppress write errors that occur after Stop() — the store may be
		// closing concurrently and the error is expected.
		select {
		case <-n.stopCh:
			return
		default:
		}
		panic(fmt.Sprintf("raft: persist log entries failed: %v", err))
	}
}
