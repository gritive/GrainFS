package raft

import (
	"sync"
	"time"
)

type inflightRange struct {
	from       uint64
	to         uint64
	bytes      int
	generation uint64
	acked      bool
}

type inflightTracker struct {
	maxCount     int
	maxBytes     int
	usedBytes    int
	generation   uint64
	ranges       []inflightRange
	ackedThrough uint64
}

func newInflightTracker(maxCount, maxBytes int) *inflightTracker {
	if maxCount <= 0 {
		maxCount = 1
	}
	return &inflightTracker{maxCount: maxCount, maxBytes: maxBytes}
}

func (t *inflightTracker) canSend(bytes int) bool {
	if len(t.ranges) >= t.maxCount {
		return false
	}
	if t.maxBytes > 0 && t.usedBytes+bytes > t.maxBytes {
		return len(t.ranges) == 0 && t.usedBytes == 0
	}
	return true
}

func (t *inflightTracker) sent(from, to uint64, bytes int) uint64 {
	t.ranges = append(t.ranges, inflightRange{from: from, to: to, bytes: bytes, generation: t.generation})
	t.usedBytes += bytes
	return t.generation
}

func (t *inflightTracker) ack(from, to uint64) (bool, uint64) {
	return t.ackGeneration(t.generation, from, to)
}

func (t *inflightTracker) ackGeneration(generation, from, to uint64) (bool, uint64) {
	found := false
	for i := range t.ranges {
		if t.ranges[i].generation == generation && t.ranges[i].from == from && t.ranges[i].to == to {
			t.ranges[i].acked = true
			found = true
			break
		}
	}
	if !found {
		return false, 0
	}

	advanced := false
	for len(t.ranges) > 0 && t.ranges[0].acked {
		r := t.ranges[0]
		t.ackedThrough = r.to
		t.usedBytes -= r.bytes
		copy(t.ranges, t.ranges[1:])
		t.ranges[len(t.ranges)-1] = inflightRange{}
		t.ranges = t.ranges[:len(t.ranges)-1]
		advanced = true
	}
	return advanced, t.ackedThrough
}

func (t *inflightTracker) reset(ackedThrough uint64) {
	t.generation++
	t.ranges = nil
	t.usedBytes = 0
	t.ackedThrough = ackedThrough
}

type peerReplicator struct {
	node *Node
	peer string

	wakeCh chan struct{}
	stopCh chan struct{}
	respCh chan replicationResult
	once   sync.Once

	tracker       *inflightTracker
	sendNextIndex uint64
	snapshotting  bool
}

type replicationResult struct {
	term          uint64
	success       bool
	conflictTerm  uint64
	conflictIndex uint64
	from          uint64
	to            uint64
	bytes         int
	generation    uint64
	heartbeat     bool
	snapshot      bool
	snapshotIndex uint64
	err           error
}

func newPeerReplicator(n *Node, peer string) *peerReplicator {
	maxInflight := n.config.MaxAppendEntriesInflight
	if maxInflight <= 0 {
		maxInflight = 2
	}
	return &peerReplicator{
		node:    n,
		peer:    peer,
		wakeCh:  make(chan struct{}, 1),
		stopCh:  make(chan struct{}),
		respCh:  make(chan replicationResult, maxInflight+2),
		tracker: newInflightTracker(maxInflight, n.config.MaxAppendEntriesInflightBytes),
	}
}

func (r *peerReplicator) wake() {
	select {
	case r.wakeCh <- struct{}{}:
	default:
	}
}

func (r *peerReplicator) stop() {
	r.once.Do(func() { close(r.stopCh) })
}

func (r *peerReplicator) run() {
	r.wake()
	for {
		select {
		case <-r.stopCh:
			return
		case <-r.wakeCh:
			r.sendAvailable()
		case res := <-r.respCh:
			if r.applyResult(res) {
				r.sendAvailable()
			}
		}
	}
}

func (r *peerReplicator) sendAvailable() {
	for {
		select {
		case <-r.stopCh:
			return
		default:
		}
		if !r.sendOne() {
			return
		}
	}
}

func (r *peerReplicator) sendOne() bool {
	n := r.node
	n.mu.Lock()
	if n.state != Leader || n.sendAppendEntries == nil {
		n.mu.Unlock()
		return false
	}
	term := n.currentTerm
	leaderID := n.id
	commitIndex := n.commitIndex
	if r.sendNextIndex == 0 || len(r.tracker.ranges) == 0 && r.sendNextIndex < n.nextIndex[r.peer] {
		r.sendNextIndex = n.nextIndex[r.peer]
	}
	nextIdx := r.sendNextIndex

	if nextIdx < n.firstIndex {
		if r.snapshotting || len(r.tracker.ranges) > 0 || n.sendInstallSnapshot == nil || n.store == nil {
			n.mu.Unlock()
			return false
		}
		snap, err := n.store.LoadSnapshot()
		if err != nil || snap.Data == nil {
			n.mu.Unlock()
			return false
		}
		r.snapshotting = true
		raftAESnapshotExclusiveTotal.Inc()
		args := &InstallSnapshotArgs{
			Term:              term,
			LeaderID:          leaderID,
			LastIncludedIndex: snap.Index,
			LastIncludedTerm:  snap.Term,
			Data:              snap.Data,
			Servers:           n.currentConfigServers(),
		}
		n.mu.Unlock()
		r.sendSnapshot(args)
		return false
	}

	prevLogIndex := nextIdx - 1
	prevLogTerm := uint64(0)
	if prevLogIndex > 0 && n.hasLogEntry(prevLogIndex) {
		prevLogTerm = n.log[n.toSliceIdx(prevLogIndex)].Term
	}

	entries, payloadBytes := n.entriesForAppendEntriesLocked(nextIdx, r.tracker.maxBytes-r.tracker.usedBytes)
	if len(entries) == 0 {
		if len(r.tracker.ranges) > 0 {
			n.mu.Unlock()
			return false
		}
		args := &AppendEntriesArgs{
			Term:         term,
			LeaderID:     leaderID,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			LeaderCommit: commitIndex,
		}
		n.mu.Unlock()
		r.sendAppend(args, replicationResult{term: term, heartbeat: true, from: nextIdx})
		return false
	}
	if !r.tracker.canSend(payloadBytes) {
		raftAEWindowFullTotal.Inc()
		n.mu.Unlock()
		return false
	}
	from := entries[0].Index
	to := entries[len(entries)-1].Index
	generation := r.tracker.sent(from, to, payloadBytes)
	r.sendNextIndex = to + 1
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderID:     leaderID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: commitIndex,
	}
	n.mu.Unlock()
	r.sendAppend(args, replicationResult{
		term:       term,
		from:       from,
		to:         to,
		bytes:      payloadBytes,
		generation: generation,
	})
	return true
}

func (r *peerReplicator) sendAppend(args *AppendEntriesArgs, base replicationResult) {
	n := r.node
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		reply, err := n.sendAppendEntries(r.peer, args)
		res := base
		res.err = err
		if reply != nil {
			res.term = reply.Term
			res.success = reply.Success
			res.conflictTerm = reply.ConflictTerm
			res.conflictIndex = reply.ConflictIndex
		}
		select {
		case r.respCh <- res:
		case <-r.stopCh:
		case <-n.stopCh:
		}
	}()
}

func (r *peerReplicator) sendSnapshot(args *InstallSnapshotArgs) {
	n := r.node
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		reply, err := n.sendInstallSnapshot(r.peer, args)
		res := replicationResult{term: args.Term, snapshot: true, snapshotIndex: args.LastIncludedIndex, err: err}
		if reply != nil {
			res.term = reply.Term
		}
		select {
		case r.respCh <- res:
		case <-r.stopCh:
		case <-n.stopCh:
		}
	}()
}

func (r *peerReplicator) applyResult(res replicationResult) bool {
	n := r.node
	if res.err != nil {
		n.notifyObservers(Event{Type: EventFailedHeartbeat, PeerID: r.peer})
		if res.snapshot {
			r.snapshotting = false
		} else if !res.heartbeat {
			r.tracker.reset(r.tracker.ackedThrough)
			r.sendNextIndex = 0
		}
		return false
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if res.term > n.currentTerm {
		n.currentTerm = res.term
		n.state = Follower
		n.votedFor = ""
		n.leaderID = ""
		n.persistState()
		n.signalReset()
		return false
	}
	if n.state != Leader || n.currentTerm != res.term {
		return false
	}

	n.checkQuorumAcks[r.peer] = time.Now()
	if res.snapshot {
		r.snapshotting = false
		n.nextIndex[r.peer] = res.snapshotIndex + 1
		n.matchIndex[r.peer] = res.snapshotIndex
		r.tracker.reset(res.snapshotIndex)
		r.sendNextIndex = res.snapshotIndex + 1
		n.advanceCommitIndex()
		return true
	}
	if res.heartbeat {
		if res.success {
			n.tickReadIndexAcks(r.peer)
			return false
		}
		raftAEConflictResetsTotal.Inc()
		r.tracker.reset(n.matchIndex[r.peer])
		n.applyConflictHint(r.peer, &AppendEntriesReply{
			Term:          res.term,
			Success:       false,
			ConflictTerm:  res.conflictTerm,
			ConflictIndex: res.conflictIndex,
		}, res.from, nil)
		r.sendNextIndex = n.nextIndex[r.peer]
		return true
	}
	if res.success {
		advanced, match := r.tracker.ackGeneration(res.generation, res.from, res.to)
		n.tickReadIndexAcks(r.peer)
		if !advanced {
			raftAEStaleReplyTotal.Inc()
			return true
		}
		if match > n.matchIndex[r.peer] {
			n.matchIndex[r.peer] = match
			n.nextIndex[r.peer] = match + 1
			n.advanceCommitIndex()
		}
		return true
	}

	raftAEConflictResetsTotal.Inc()
	r.tracker.reset(n.matchIndex[r.peer])
	n.applyConflictHint(r.peer, &AppendEntriesReply{
		Term:          res.term,
		Success:       false,
		ConflictTerm:  res.conflictTerm,
		ConflictIndex: res.conflictIndex,
	}, res.from, nil)
	r.sendNextIndex = n.nextIndex[r.peer]
	return true
}

func (n *Node) entriesForAppendEntriesLocked(nextIdx uint64, byteBudget int) ([]LogEntry, int) {
	if !n.hasLogEntry(nextIdx) {
		return nil, 0
	}
	si := n.toSliceIdx(nextIdx)
	maxEntries := len(n.log) - si
	if n.config.MaxEntriesPerAE > 0 && uint64(maxEntries) > n.config.MaxEntriesPerAE {
		maxEntries = int(n.config.MaxEntriesPerAE)
		raftAESplitCountTotal.Inc()
	}
	entries := make([]LogEntry, 0, maxEntries)
	total := 0
	for i := 0; i < maxEntries; i++ {
		entry := n.log[si+i]
		size := logEntryPayloadBytes(entry)
		if byteBudget > 0 && len(entries) > 0 && total+size > byteBudget {
			break
		}
		entries = append(entries, entry)
		total += size
		if byteBudget > 0 && total >= byteBudget {
			break
		}
	}
	return entries, total
}

func logEntryPayloadBytes(entry LogEntry) int {
	return len(entry.Command) + 32
}

func (n *Node) reconcilePeerReplicatorsLocked(view *membershipView) {
	if n.state != Leader {
		return
	}
	if n.peerReplicators == nil {
		n.peerReplicators = make(map[string]*peerReplicator)
	}
	desired := make(map[string]struct{})
	for _, peer := range view.allVotersExcept(n.id) {
		desired[peer] = struct{}{}
	}
	for _, peer := range view.learnersByID {
		desired[peer] = struct{}{}
	}
	for peer, r := range n.peerReplicators {
		if _, ok := desired[peer]; !ok {
			r.stop()
			delete(n.peerReplicators, peer)
		}
	}
	for peer := range desired {
		if _, ok := n.peerReplicators[peer]; ok {
			continue
		}
		r := newPeerReplicator(n, peer)
		n.peerReplicators[peer] = r
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			r.run()
		}()
	}
}

func (n *Node) stopPeerReplicatorsLocked() {
	for peer, r := range n.peerReplicators {
		r.stop()
		delete(n.peerReplicators, peer)
	}
}
