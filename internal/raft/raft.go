package raft

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
)

var (
	ErrNotLeader      = errors.New("not the leader")
	ErrProposalFailed = errors.New("proposal failed: node stepped down")
	ErrNoPeers        = errors.New("no peers available for leadership transfer")
)

// Special command prefixes for configuration changes (membership)
var (
	configAddPrefix    = []byte("__raft_config_add:")
	configRemovePrefix = []byte("__raft_config_remove:")
)

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
}

// Config holds Raft node configuration.
// proposal carries a single command through the batcher pipeline.
type proposal struct {
	command []byte
	doneCh  chan proposalResult
	ctx     context.Context
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
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig(id string, peers []string) Config {
	return Config{
		ID:               id,
		Peers:            peers,
		ElectionTimeout:  150 * time.Millisecond,
		HeartbeatTimeout: 50 * time.Millisecond,
	}
}

// RPC message types for Raft protocol.

// RequestVoteArgs is sent by candidates to gather votes.
type RequestVoteArgs struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
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
	Term    uint64
	Success bool
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
	state       NodeState
	commitIndex uint64
	lastApplied uint64

	// leader volatile state
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// config
	config Config

	// channels
	applyCh  chan LogEntry
	stopCh   chan struct{}
	resetCh  chan struct{} // signals election timer reset
	commitCh chan struct{} // signals applyLoop when commitIndex advances
	stopped  bool

	// transport callback for sending RPCs
	sendRequestVote     func(peer string, args *RequestVoteArgs) (*RequestVoteReply, error)
	sendAppendEntries   func(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error)
	sendInstallSnapshot func(peer string, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
	sendTimeoutNow      func(peer string) error

	// durable storage (optional; when nil, state is in-memory only)
	store LogStore

	// leader tracking (observable from outside)
	leaderID string

	// proposal waiters: log index -> channel signaled when committed (nil = success, error = failure)
	waiters map[uint64]chan error

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
}

// NewNode creates a new Raft node. Call Start() to begin operation.
// If store is non-nil, it restores persisted state on creation.
func NewNode(config Config, store ...LogStore) *Node {
	n := &Node{
		id:            config.ID,
		state:         Follower,
		config:        config,
		log:           make([]LogEntry, 0),
		firstIndex:    1, // Raft indices start at 1
		nextIndex:     make(map[string]uint64),
		matchIndex:    make(map[string]uint64),
		applyCh:       make(chan LogEntry, 64),
		stopCh:        make(chan struct{}),
		resetCh:       make(chan struct{}, 1),
		commitCh:      make(chan struct{}, 1),
		waiters:       make(map[uint64]chan error),
		proposalCh:    make(chan proposal, 4096),
		replicationCh: make(chan struct{}, 1),
		metrics:       adaptiveMetrics{alpha: 0.3, lastFlushAt: time.Now()},
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

	// Restore log entries
	lastIdx, err := n.store.LastIndex()
	if err == nil && lastIdx > 0 {
		// Find the first available index in the store
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
}

// SetNoOpCommand sets the encoded command that will be proposed when this node
// becomes leader. The command must be recognised and ignored by the FSM (no-op).
// Call before Start().
func (n *Node) SetNoOpCommand(cmd []byte) {
	n.noOpCmd = cmd
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
	go n.run()
	go n.applyLoop()
	go n.batcherLoop()
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
	case n.proposalCh <- proposal{command: command, doneCh: doneCh, ctx: ctx}:
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

		// Process config changes internally (membership adds/removes)
		if IsConfigChange(entry.Command) {
			n.mu.Unlock()
			n.applyConfigChange(entry.Command)
			n.mu.Lock()
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

// ID returns the node's ID.
func (n *Node) ID() string {
	return n.id
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

func (n *Node) run() {
	for {
		select {
		case <-n.stopCh:
			return
		default:
		}

		n.mu.Lock()
		state := n.state
		n.mu.Unlock()

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
		n.mu.Lock()
		n.state = Candidate
		n.mu.Unlock()
	}
}

func (n *Node) runCandidate() {
	n.mu.Lock()
	n.currentTerm++
	n.votedFor = n.id
	n.persistState()
	term := n.currentTerm
	lastLogIndex, lastLogTerm := n.lastLogInfo()
	peers := n.config.Peers
	n.mu.Unlock()

	votes := 1              // vote for self
	total := len(peers) + 1 // include self
	majority := total/2 + 1

	// Single-peer node: already has majority with self-vote
	if votes >= majority {
		n.mu.Lock()
		n.state = Leader
		n.leaderID = n.id
		n.initLeaderState()
		n.mu.Unlock()
		return
	}

	voteCh := make(chan bool, len(peers))

	for _, peer := range peers {
		go func(p string) {
			args := &RequestVoteArgs{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply, err := n.sendRequestVote(p, args)
			if err != nil {
				voteCh <- false
				return
			}

			n.mu.Lock()
			if reply.Term > n.currentTerm {
				n.currentTerm = reply.Term
				n.state = Follower
				n.votedFor = ""
				n.persistState()
				n.mu.Unlock()
				voteCh <- false
				return
			}
			n.mu.Unlock()
			voteCh <- reply.VoteGranted
		}(peer)
	}

	timeout := n.randomElectionTimeout()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for range len(peers) {
		select {
		case <-n.stopCh:
			return
		case <-timer.C:
			return
		case <-n.resetCh:
			return
		case granted := <-voteCh:
			if granted {
				votes++
			}
			if votes >= majority {
				n.mu.Lock()
				if n.state == Candidate && n.currentTerm == term {
					n.state = Leader
					n.leaderID = n.id
					n.initLeaderState()
				}
				n.mu.Unlock()
				return
			}
		}
	}
}

func (n *Node) initLeaderState() {
	nextIdx := n.lastLogIdx() + 1
	for _, peer := range n.config.Peers {
		n.nextIndex[peer] = nextIdx
		n.matchIndex[peer] = 0
	}
	// Track self's matchIndex
	n.matchIndex[n.id] = n.lastLogIdx()
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
			n.replicateToAll()
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
	n.mu.Unlock()

	for _, peer := range peers {
		go n.replicateTo(peer)
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
		snapIdx, snapTerm, snapData, err := n.store.LoadSnapshot()
		if err == nil && snapData != nil {
			args := &InstallSnapshotArgs{
				Term:              term,
				LeaderID:          leaderID,
				LastIncludedIndex: snapIdx,
				LastIncludedTerm:  snapTerm,
				Data:              snapData,
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
			n.nextIndex[peer] = snapIdx + 1
			n.matchIndex[peer] = snapIdx
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

	if reply.Success {
		n.nextIndex[peer] = nextIdx + uint64(len(entries))
		n.matchIndex[peer] = n.nextIndex[peer] - 1
		n.advanceCommitIndex()
	} else {
		// Decrement nextIndex and retry on next heartbeat
		if n.nextIndex[peer] > 1 {
			n.nextIndex[peer]--
		}
	}
}

func (n *Node) advanceCommitIndex() {
	// Find the highest N such that a majority of matchIndex[i] >= N
	// and log[N].term == currentTerm
	for idx := n.lastLogIdx(); idx > n.commitIndex; idx-- {
		if !n.hasLogEntry(idx) {
			continue
		}
		if n.log[n.toSliceIdx(idx)].Term != n.currentTerm {
			continue
		}

		count := 0
		total := len(n.config.Peers) + 1 // include self
		for _, peer := range n.config.Peers {
			if n.matchIndex[peer] >= idx {
				count++
			}
		}
		// Count self
		if n.matchIndex[n.id] >= idx {
			count++
		}

		if count > total/2 {
			n.commitIndex = idx
			n.signalCommit()
			return
		}
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
	vals := make([]uint64, 0, len(n.config.Peers)+1)
	for _, peer := range n.config.Peers {
		vals = append(vals, n.matchIndex[peer])
	}
	vals = append(vals, n.matchIndex[n.id])
	sort.Slice(vals, func(i, j int) bool { return vals[i] > vals[j] })
	return vals[len(vals)/2]
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
	snapIdx, _, _, err := n.store.LoadSnapshot()
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

	reply := &RequestVoteReply{Term: n.currentTerm}

	// Reply false if term < currentTerm
	if args.Term < n.currentTerm {
		return reply
	}

	// If RPC request's term > currentTerm, update and convert to follower
	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.state = Follower
		n.votedFor = ""
		n.leaderID = ""
		n.persistState()
	}
	reply.Term = n.currentTerm

	// Grant vote if we haven't voted or already voted for this candidate,
	// and candidate's log is at least as up-to-date as ours
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
	n.signalReset()

	reply.Term = n.currentTerm

	// Log consistency check
	if args.PrevLogIndex > 0 {
		if !n.hasLogEntry(args.PrevLogIndex) {
			return reply // we don't have the entry (compacted or not yet received)
		}
		if n.log[n.toSliceIdx(args.PrevLogIndex)].Term != args.PrevLogTerm {
			// Conflict: delete this entry and all that follow
			n.abortWaitersFrom(args.PrevLogIndex)
			n.log = n.log[:n.toSliceIdx(args.PrevLogIndex)]
			return reply
		}
	}

	// Append new entries (skip already present)
	var newEntries []LogEntry
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + uint64(i) + 1
		if n.hasLogEntry(idx) {
			if n.log[n.toSliceIdx(idx)].Term != entry.Term {
				n.abortWaitersFrom(idx)
				n.log = n.log[:n.toSliceIdx(idx)]
				newEntries = args.Entries[i:]
				n.log = append(n.log, newEntries...)
				break
			}
		} else {
			newEntries = args.Entries[i:]
			n.log = append(n.log, newEntries...)
			break
		}
	}
	n.persistLogEntries(newEntries)

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

// CompactLog removes all entries up to and including snapshotIndex from the in-memory log.
// After compaction, firstIndex = snapshotIndex + 1.
func (n *Node) CompactLog(snapshotIndex uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()

	si := n.toSliceIdx(snapshotIndex)
	if si < 0 {
		return // already compacted past this point
	}

	keepFrom := si + 1
	if keepFrom >= len(n.log) {
		n.log = nil
	} else {
		remaining := make([]LogEntry, len(n.log)-keepFrom)
		copy(remaining, n.log[keepFrom:])
		n.log = remaining
	}
	n.firstIndex = snapshotIndex + 1
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
	n.signalReset()

	reply.Term = n.currentTerm

	// Save snapshot to store
	if n.store != nil {
		if err := n.store.SaveSnapshot(args.LastIncludedIndex, args.LastIncludedTerm, args.Data); err != nil {
			return reply
		}
	}

	// Discard entire log and reset to post-snapshot state
	n.abortWaitersFrom(0)
	n.log = nil
	n.firstIndex = args.LastIncludedIndex + 1
	n.lastApplied = args.LastIncludedIndex
	n.commitIndex = args.LastIncludedIndex
	n.signalCommit()

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

// AddPeer proposes adding a new peer to the cluster. The change takes effect
// when the config-change log entry is committed and applied on all nodes.
func (n *Node) AddPeer(peerID string) error {
	cmd := append(configAddPrefix, []byte(peerID)...)
	return n.Propose(cmd)
}

// RemovePeer proposes removing a peer from the cluster.
func (n *Node) RemovePeer(peerID string) error {
	cmd := append(configRemovePrefix, []byte(peerID)...)
	return n.Propose(cmd)
}

// applyConfigChange processes membership change commands in the apply loop.
func (n *Node) applyConfigChange(command []byte) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if bytes.HasPrefix(command, configAddPrefix) {
		peerID := string(command[len(configAddPrefix):])
		// Add only if not already present
		for _, p := range n.config.Peers {
			if p == peerID {
				return
			}
		}
		n.config.Peers = append(n.config.Peers, peerID)
		// Initialize leader state for new peer if we're the leader
		if n.state == Leader {
			n.nextIndex[peerID] = n.lastLogIdx() + 1
			n.matchIndex[peerID] = 0
		}
	} else if bytes.HasPrefix(command, configRemovePrefix) {
		peerID := string(command[len(configRemovePrefix):])
		peers := make([]string, 0, len(n.config.Peers))
		for _, p := range n.config.Peers {
			if p != peerID {
				peers = append(peers, p)
			}
		}
		n.config.Peers = peers
		delete(n.nextIndex, peerID)
		delete(n.matchIndex, peerID)
	}
}

// IsConfigChange returns true if the command is a membership change.
func IsConfigChange(command []byte) bool {
	return bytes.HasPrefix(command, configAddPrefix) || bytes.HasPrefix(command, configRemovePrefix)
}

// HandleTimeoutNow causes this node to immediately start an election.
// Sent by the leader during leadership transfer to the chosen successor.
func (n *Node) HandleTimeoutNow() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Follower {
		return
	}

	// Immediately become candidate
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

	entries := make([]LogEntry, len(pending))
	base := n.lastLogIdx() + 1
	for i, p := range pending {
		entries[i] = LogEntry{
			Term:    n.currentTerm,
			Index:   base + uint64(i),
			Command: p.command,
		}
	}
	n.log = append(n.log, entries...)
	n.persistLogEntries(entries)
	n.matchIndex[n.id] = entries[len(entries)-1].Index

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
