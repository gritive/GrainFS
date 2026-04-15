package raft

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

var (
	ErrNotLeader      = errors.New("not the leader")
	ErrProposalFailed = errors.New("proposal failed: node stepped down")
	ErrNoPeers        = errors.New("no peers available for leadership transfer")
)

// NodeState represents the current role of a Raft node.
type NodeState int

const (
	Follower  NodeState = iota
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
type Config struct {
	ID               string
	Peers            []string      // addresses of other nodes
	ElectionTimeout  time.Duration // base election timeout
	HeartbeatTimeout time.Duration
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
	stopped  bool

	// transport callback for sending RPCs
	sendRequestVote   func(peer string, args *RequestVoteArgs) (*RequestVoteReply, error)
	sendAppendEntries func(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error)

	// durable storage (optional; when nil, state is in-memory only)
	store LogStore

	// leader tracking (observable from outside)
	leaderID string

	// proposal waiters: log index -> channel signaled when committed
	waiters map[uint64]chan struct{}
}

// NewNode creates a new Raft node. Call Start() to begin operation.
// If store is non-nil, it restores persisted state on creation.
func NewNode(config Config, store ...LogStore) *Node {
	n := &Node{
		id:         config.ID,
		state:      Follower,
		config:     config,
		log:        make([]LogEntry, 0),
		nextIndex:  make(map[string]uint64),
		matchIndex: make(map[string]uint64),
		applyCh:    make(chan LogEntry, 64),
		stopCh:     make(chan struct{}),
		resetCh:    make(chan struct{}, 1),
		waiters:    make(map[uint64]chan struct{}),
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
		entries, err := n.store.GetEntries(1, lastIdx)
		if err == nil {
			n.log = entries
		}
	}
}

// SetTransport sets the RPC callbacks for sending messages to peers.
func (n *Node) SetTransport(
	sendVote func(peer string, args *RequestVoteArgs) (*RequestVoteReply, error),
	sendAppend func(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error),
) {
	n.sendRequestVote = sendVote
	n.sendAppendEntries = sendAppend
}

// Start begins the Raft node's main loop.
func (n *Node) Start() {
	go n.run()
	go n.applyLoop()
}

// Propose appends a command to the leader's log for replication.
// Returns ErrNotLeader if this node is not the leader.
func (n *Node) Propose(command []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader {
		return ErrNotLeader
	}

	entry := LogEntry{
		Term:    n.currentTerm,
		Index:   n.lastLogIdx() + 1,
		Command: command,
	}
	n.log = append(n.log, entry)
	n.persistLogEntries([]LogEntry{entry})
	// Update matchIndex for self
	n.matchIndex[n.id] = entry.Index
	n.advanceCommitIndex()

	return nil
}

// ProposeWait appends a command and blocks until it is committed or the context is cancelled.
// Returns the log index of the committed entry.
func (n *Node) ProposeWait(ctx context.Context, command []byte) (uint64, error) {
	n.mu.Lock()

	if n.state != Leader {
		n.mu.Unlock()
		return 0, ErrNotLeader
	}

	entry := LogEntry{
		Term:    n.currentTerm,
		Index:   n.lastLogIdx() + 1,
		Command: command,
	}
	n.log = append(n.log, entry)
	n.persistLogEntries([]LogEntry{entry})
	n.matchIndex[n.id] = entry.Index

	ch := make(chan struct{}, 1)
	n.waiters[entry.Index] = ch
	n.advanceCommitIndex()
	n.mu.Unlock()

	select {
	case <-ctx.Done():
		n.mu.Lock()
		delete(n.waiters, entry.Index)
		n.mu.Unlock()
		return 0, ctx.Err()
	case <-n.stopCh:
		return 0, ErrProposalFailed
	case <-ch:
		return entry.Index, nil
	}
}

func (n *Node) applyLoop() {
	for {
		select {
		case <-n.stopCh:
			return
		default:
		}

		n.mu.Lock()
		if n.commitIndex > n.lastApplied && int(n.lastApplied) < len(n.log) {
			n.lastApplied++
			entry := n.log[n.lastApplied-1]
			idx := entry.Index

			// Signal any waiter for this index
			if ch, ok := n.waiters[idx]; ok {
				close(ch)
				delete(n.waiters, idx)
			}
			n.mu.Unlock()

			select {
			case n.applyCh <- entry:
			case <-n.stopCh:
				return
			}
		} else {
			n.mu.Unlock()
			time.Sleep(5 * time.Millisecond) // avoid busy loop
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

	votes := 1 // vote for self
	total := len(peers) + 1 // include self
	majority := total/2 + 1

	// Solo node: already has majority with self-vote
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
	// Send initial AppendEntries (heartbeat/replication)
	n.replicateToAll()

	ticker := time.NewTicker(n.config.HeartbeatTimeout)
	defer ticker.Stop()

	select {
	case <-n.stopCh:
		return
	case <-ticker.C:
		n.replicateToAll()
	case <-n.resetCh:
		// Stepped down due to higher term
		return
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
	prevLogIndex := nextIdx - 1
	prevLogTerm := uint64(0)
	if prevLogIndex > 0 && int(prevLogIndex) <= len(n.log) {
		prevLogTerm = n.log[prevLogIndex-1].Term
	}

	// Collect entries to send
	var entries []LogEntry
	if int(nextIdx)-1 < len(n.log) {
		entries = make([]LogEntry, len(n.log)-int(nextIdx)+1)
		copy(entries, n.log[nextIdx-1:])
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
		if int(idx) > len(n.log) {
			continue
		}
		if n.log[idx-1].Term != n.currentTerm {
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
			return
		}
	}
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
		if int(args.PrevLogIndex) > len(n.log) {
			return reply // we don't have the entry
		}
		if n.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			// Conflict: delete this entry and all that follow
			n.log = n.log[:args.PrevLogIndex-1]
			return reply
		}
	}

	// Append new entries (skip already present)
	var newEntries []LogEntry
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + uint64(i) + 1
		if int(idx) <= len(n.log) {
			if n.log[idx-1].Term != entry.Term {
				n.log = n.log[:idx-1]
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

func (n *Node) lastLogInfo() (uint64, uint64) {
	if len(n.log) == 0 {
		return 0, 0
	}
	last := n.log[len(n.log)-1]
	return last.Index, last.Term
}

func (n *Node) lastLogIdx() uint64 {
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Index
}

func (n *Node) isLogUpToDate(lastLogIndex, lastLogTerm uint64) bool {
	myLastIndex, myLastTerm := n.lastLogInfo()

	if lastLogTerm != myLastTerm {
		return lastLogTerm > myLastTerm
	}
	return lastLogIndex >= myLastIndex
}

// TransferLeadership voluntarily steps down as leader, allowing a follower
// to win the next election. Returns ErrNotLeader if not the current leader,
// or ErrNoPeers if there are no peers to transfer to.
func (n *Node) TransferLeadership() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader {
		return ErrNotLeader
	}

	if len(n.config.Peers) == 0 {
		return ErrNoPeers
	}

	// Step down to follower — this causes the election timer to start on
	// followers, and one of them will become the new leader.
	n.state = Follower
	n.leaderID = ""
	n.signalReset()

	return nil
}

// persistState saves currentTerm and votedFor to durable storage.
// Must be called with n.mu held. No-op if store is nil.
func (n *Node) persistState() {
	if n.store == nil {
		return
	}
	_ = n.store.SaveState(n.currentTerm, n.votedFor)
}

// persistLogEntries saves log entries to durable storage.
// Must be called with n.mu held. No-op if store is nil.
func (n *Node) persistLogEntries(entries []LogEntry) {
	if n.store == nil || len(entries) == 0 {
		return
	}
	_ = n.store.AppendEntries(entries)
}
