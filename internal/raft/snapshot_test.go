package raft

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/gritive/GrainFS/internal/badgerutil"
	"github.com/stretchr/testify/require"
)

// TestSnapshot_CreateAndCompact: single-voter node, propose 10 entries,
// CreateSnapshot at index 5, verify SnapshotStore has it, log compacted,
// entries 1-5 inaccessible, 6-10 still readable, TermAt(5) returns the
// snapshot's term.
func TestSnapshot_CreateAndCompact(t *testing.T) {
	n := startSingleVoter(t, "n1")

	// Propose 10 entries. Single-voter commits inline.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 1; i <= 10; i++ {
		_, err := n.ProposeWait(ctx, []byte(fmt.Sprintf("cmd-%d", i)))
		require.NoError(t, err)
	}
	require.NoError(t, waitFor(2*time.Second, func() bool {
		return n.CommittedIndex() >= 10
	}))

	// CreateSnapshot at index 5.
	require.NoError(t, n.CreateSnapshot(5, []byte("fsm-state-at-5")))

	// SnapshotStore should hold the snapshot.
	snap, err := n.LatestSnapshot()
	require.NoError(t, err)
	require.NotNil(t, snap)
	require.Equal(t, uint64(5), snap.LastIncludedIndex)
	require.Equal(t, []byte("fsm-state-at-5"), snap.Data)
	require.Equal(t, []string{"n1"}, snap.Configuration)

	// Stop so we can read st.log under quiescence.
	n.Stop()

	// FirstIndex == 6.
	require.Equal(t, uint64(6), n.st.log.FirstIndex())

	// Entries 1-5 are gone (ErrLogIndexOutOfRange).
	for i := uint64(1); i <= 5; i++ {
		_, err := n.st.log.Entry(i)
		require.ErrorIs(t, err, ErrLogIndexOutOfRange, "Entry(%d) must be compacted", i)
	}

	// Entries 6-10 still present.
	for i := uint64(6); i <= 10; i++ {
		e, err := n.st.log.Entry(i)
		require.NoError(t, err, "Entry(%d) must still be readable", i)
		require.Equal(t, i, e.Index)
	}

	// TermAt(5) returns the snapshot's term (boundary fast path).
	term, err := n.st.log.TermAt(5)
	require.NoError(t, err)
	require.Equal(t, snap.LastIncludedTerm, term, "TermAt(boundary) must equal snapshot's LastIncludedTerm")
}

func TestSnapshot_CreatePreservesLearners(t *testing.T) {
	n, err := NewNode(Config{ID: "n1"})
	require.NoError(t, err)
	require.NoError(t, n.st.log.Append([]LogEntry{{Term: 1, Index: 1, Command: []byte("x")}}))
	n.st.commitIndex = 1
	n.st.currentConfig = effectiveConfig{
		voters:   []string{"n1"},
		learners: map[string]string{"n2": "addr2"},
	}

	reply := make(chan error, 1)
	n.handleCreateSnapshot(command{kind: cmdCreateSnapshot, csIndex: 1, csData: []byte("fsm"), csReply: reply})
	require.NoError(t, <-reply)

	snap, err := n.LatestSnapshot()
	require.NoError(t, err)
	require.Equal(t, map[string]string{"n2": "addr2"}, snap.Learners)
}

func TestBadgerSnapshotStore_RoundTripsCanonicalMetadata(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithLogger(nil))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	store, err := NewBadgerSnapshotStore(db, []byte("raft/v2/snap/"))
	require.NoError(t, err)
	in := &Snapshot{
		Index:                12,
		Term:                 3,
		Servers:              []Server{{ID: "old-a", Suffrage: Voter}, {ID: "learner-a", Suffrage: NonVoter}},
		FormatVersion:        99,
		JointPhase:           JointEntering,
		JointOldVoters:       []string{"old-a", "old-b"},
		JointNewVoters:       []string{"old-a", "old-c"},
		JointEnterIndex:      11,
		JointManagedLearners: []string{"learner-a"},
		Data:                 []byte("fsm"),
	}

	require.NoError(t, store.Save(in))
	out, err := store.Latest()
	require.NoError(t, err)
	require.NotNil(t, out)
	require.Equal(t, uint64(12), out.Index)
	require.Equal(t, uint64(3), out.Term)
	require.Equal(t, []Server{{ID: "old-a", Suffrage: Voter}, {ID: "learner-a", Suffrage: NonVoter}}, out.Servers)
	require.Equal(t, uint8(99), out.FormatVersion)
	require.Equal(t, JointEntering, out.JointPhase)
	require.Equal(t, []string{"old-a", "old-b"}, out.JointOldVoters)
	require.Equal(t, []string{"old-a", "old-c"}, out.JointNewVoters)
	require.Equal(t, uint64(11), out.JointEnterIndex)
	require.Equal(t, []string{"learner-a"}, out.JointManagedLearners)
	require.Equal(t, []byte("fsm"), out.Data)
}

func TestBadgerSnapshotStore_RoundTripsLargeSnapshotData(t *testing.T) {
	db, err := badger.Open(badgerutil.SmallOptions(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	store, err := NewBadgerSnapshotStore(db, []byte("raft/v2/snap/"))
	require.NoError(t, err)

	data := make([]byte, 17<<20)
	for i := range data {
		data[i] = byte(i)
	}
	in := &Snapshot{
		LastIncludedIndex: 12,
		LastIncludedTerm:  3,
		Configuration:     []string{"n1"},
		Data:              data,
	}

	require.NoError(t, store.Save(in))
	out, err := store.Latest()
	require.NoError(t, err)
	require.NotNil(t, out)
	require.Equal(t, in.LastIncludedIndex, out.LastIncludedIndex)
	require.Equal(t, in.LastIncludedTerm, out.LastIncludedTerm)
	require.Equal(t, in.Configuration, out.Configuration)
	require.Equal(t, data, out.Data)
}

func TestBadgerSnapshotStore_ReplacingLargeSnapshotRemovesOldChunks(t *testing.T) {
	db, err := badger.Open(badgerutil.SmallOptions(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	store, err := newBadgerSnapshotStore(db, []byte("raft/v2/snap/"))
	require.NoError(t, err)

	large := &Snapshot{
		LastIncludedIndex: 12,
		LastIncludedTerm:  3,
		Configuration:     []string{"n1"},
		Data:              make([]byte, 17<<20),
	}
	require.NoError(t, store.Save(large))
	oldChunkKey := store.chunkKey(snapshotChunkID(large), 0)

	err = db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(oldChunkKey)
		return err
	})
	require.NoError(t, err)

	small := &Snapshot{
		LastIncludedIndex: 13,
		LastIncludedTerm:  3,
		Configuration:     []string{"n1"},
		Data:              []byte("small"),
	}
	require.NoError(t, store.Save(small))

	err = db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(oldChunkKey)
		return err
	})
	require.ErrorIs(t, err, badger.ErrKeyNotFound)
}

func TestInstallSnapshot_PreservesLearners(t *testing.T) {
	n, err := NewNode(Config{ID: "n1", Peers: []string{"n2"}})
	require.NoError(t, err)
	n.Start()
	t.Cleanup(n.Stop)
	go func() {
		for range n.ApplyCh() {
		}
	}()

	reply := n.HandleInstallSnapshot(&InstallSnapshotArgs{
		Term:              1,
		LeaderID:          "leader",
		LastIncludedIndex: 5,
		LastIncludedTerm:  1,
		Configuration:     []string{"n1"},
		Learners:          map[string]string{"n3": "addr3"},
		Data:              []byte("snapshot"),
	})
	require.Equal(t, uint64(1), reply.Term)

	require.Eventually(t, func() bool {
		cfg := n.rs.Load().config
		return cfg.isLearner("n3") && cfg.learners["n3"] == "addr3"
	}, time.Second, 10*time.Millisecond)
}

// TestSnapshot_RejectUncommitted: CreateSnapshot at index > commitIndex
// returns an error and leaves state unchanged.
func TestSnapshot_RejectUncommitted(t *testing.T) {
	n := startSingleVoter(t, "n1")

	// Propose 3 entries; commitIndex == 3 (single-voter inline).
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for i := 1; i <= 3; i++ {
		_, err := n.ProposeWait(ctx, []byte(fmt.Sprintf("cmd-%d", i)))
		require.NoError(t, err)
	}
	require.NoError(t, waitFor(time.Second, func() bool {
		return n.CommittedIndex() >= 3
	}))

	// Snapshot beyond commitIndex must error.
	err := n.CreateSnapshot(99, []byte("uncommitted"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "commitIndex")

	// State unchanged: no snapshot, no compaction.
	snap, _ := n.LatestSnapshot()
	require.Nil(t, snap, "no snapshot must be persisted on rejection")
	n.Stop()
	require.Equal(t, uint64(1), n.st.log.FirstIndex(), "FirstIndex must still be 1")
}

// TestSnapshot_RejectAlreadyCompacted: CreateSnapshot twice at the same index
// — the second call must error (boundary already covered).
func TestSnapshot_RejectAlreadyCompacted(t *testing.T) {
	n := startSingleVoter(t, "n1")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for i := 1; i <= 5; i++ {
		_, err := n.ProposeWait(ctx, []byte(fmt.Sprintf("cmd-%d", i)))
		require.NoError(t, err)
	}
	require.NoError(t, waitFor(time.Second, func() bool {
		return n.CommittedIndex() >= 5
	}))

	require.NoError(t, n.CreateSnapshot(3, []byte("first")))
	// Second snapshot at the same index: index < FirstIndex (now 4), so errors.
	err := n.CreateSnapshot(3, []byte("second"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "already snapshotted")
}

// TestSnapshot_RecoveryFromBadger: persist snapshot via badgerSnapshotStore +
// badgerLogStore, restart, verify durable state survives. Per advisor: do
// not assert ApplyCh re-delivery for compacted entries; assert durable state.
func TestSnapshot_RecoveryFromBadger(t *testing.T) {
	dir := t.TempDir()

	var snapshotIdx uint64
	var snapshotTerm uint64
	{
		db, closeDB := openTestDB(t, dir)

		logStore, err := newBadgerLogStore(db, []byte("raft/v2/log/"))
		require.NoError(t, err)
		stable, err := newBadgerStableStore(db, []byte("raft/v2/hardstate/"))
		require.NoError(t, err)
		snapStore, err := newBadgerSnapshotStore(db, []byte("raft/v2/snap/"))
		require.NoError(t, err)

		n, err := NewNode(Config{
			ID:            "n1",
			LogStore:      logStore,
			StableStore:   stable,
			SnapshotStore: snapStore,
		})
		require.NoError(t, err)
		n.Start()
		go func() {
			for range n.ApplyCh() {
			}
		}()
		require.NoError(t, waitFor(time.Second, func() bool { return n.IsLeader() }))

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for i := 1; i <= 10; i++ {
			_, err := n.ProposeWait(ctx, []byte(fmt.Sprintf("cmd-%d", i)))
			require.NoError(t, err)
		}
		require.NoError(t, waitFor(2*time.Second, func() bool {
			return n.CommittedIndex() >= 10
		}))

		require.NoError(t, n.CreateSnapshot(5, []byte("fsm-at-5")))
		// Capture for phase-2 assertions.
		snap, err := n.LatestSnapshot()
		require.NoError(t, err)
		snapshotIdx = snap.LastIncludedIndex
		snapshotTerm = snap.LastIncludedTerm

		n.Stop()
		closeDB()
	}

	// Phase 2: reopen with same dirs.
	db, closeDB := openTestDB(t, dir)
	defer closeDB()
	logStore, err := newBadgerLogStore(db, []byte("raft/v2/log/"))
	require.NoError(t, err)
	stable, err := newBadgerStableStore(db, []byte("raft/v2/hardstate/"))
	require.NoError(t, err)
	snapStore, err := newBadgerSnapshotStore(db, []byte("raft/v2/snap/"))
	require.NoError(t, err)

	n, err := NewNode(Config{
		ID:            "n1",
		LogStore:      logStore,
		StableStore:   stable,
		SnapshotStore: snapStore,
	})
	require.NoError(t, err)

	// Pre-Start assertions: durable state survives.
	snap, err := n.LatestSnapshot()
	require.NoError(t, err)
	require.NotNil(t, snap)
	require.Equal(t, snapshotIdx, snap.LastIncludedIndex)
	require.Equal(t, snapshotTerm, snap.LastIncludedTerm)
	require.Equal(t, []byte("fsm-at-5"), snap.Data)

	require.Equal(t, uint64(6), n.st.log.FirstIndex(), "log FirstIndex must persist")
	require.Equal(t, uint64(10), n.st.log.LastIndex(), "log LastIndex must persist")
	// commitIndex starts at the snapshot floor.
	require.Equal(t, uint64(5), n.st.commitIndex, "commitIndex must start at snapshot's LastIncludedIndex")
	// TermAt(5) (the boundary) must return the snapshot's term.
	term, err := n.st.log.TermAt(5)
	require.NoError(t, err)
	require.Equal(t, snapshotTerm, term, "TermAt(boundary) must equal snapshot's LastIncludedTerm after restart")
}

// TestInstallSnapshot_FollowerInstalls: drive HandleInstallSnapshot directly
// on a Follower-mode node; verify the snapshot is saved, the log is reset,
// and applyCh delivers a LogEntrySnapshot signal first.
func TestInstallSnapshot_FollowerInstalls(t *testing.T) {
	n, err := NewNode(Config{
		ID:               "n1",
		Peers:            []string{"p1", "p2"},
		ElectionTimeout:  time.Hour, // park election timer
		HeartbeatTimeout: testHeartbeat,
	})
	require.NoError(t, err)

	// Pre-seed a small log so we can verify TruncateAfter(0) clears it.
	seedLogEntries(n, []LogEntry{
		{Term: 1, Index: 1, Command: []byte("old-A")},
		{Term: 1, Index: 2, Command: []byte("old-B")},
	})
	n.st.currentTerm = 1
	n.rs.Store(n.st.snapshot())

	n.Start()
	t.Cleanup(n.Stop)

	// Capture applyCh into a channel-driven slice.
	applied := make(chan LogEntry, 16)
	go func() {
		for e := range n.ApplyCh() {
			applied <- e
		}
	}()

	args := &InstallSnapshotArgs{
		Term:              5,
		LeaderID:          "leader",
		LastIncludedIndex: 100,
		LastIncludedTerm:  4,
		Configuration:     []string{"n1", "p1", "p2"},
		Data:              []byte("snapshot-blob"),
	}
	reply := n.HandleInstallSnapshot(args)
	require.Equal(t, uint64(5), reply.Term, "reply.Term must reflect new term after step-up")

	// Wait for the LogEntrySnapshot to land on applyCh.
	select {
	case e := <-applied:
		require.Equal(t, LogEntrySnapshot, e.Type)
		require.Equal(t, uint64(100), e.Index)
		require.Equal(t, uint64(4), e.Term)
		require.Equal(t, []byte("snapshot-blob"), e.Command)
	case <-time.After(2 * time.Second):
		t.Fatal("LogEntrySnapshot not delivered on applyCh")
	}

	// Stop, then verify durable state.
	n.Stop()
	require.Equal(t, uint64(101), n.st.log.FirstIndex(), "FirstIndex must == LastIncludedIndex+1")
	require.Equal(t, uint64(100), n.st.log.LastIndex(), "log empty above boundary; LastIndex == FirstIndex-1")
	require.Equal(t, uint64(100), n.st.commitIndex, "commitIndex == LastIncludedIndex")
	// TermAt(boundary) returns LastIncludedTerm.
	term, err := n.st.log.TermAt(100)
	require.NoError(t, err)
	require.Equal(t, uint64(4), term)

	// Snapshot is durable in the SnapshotStore.
	snap, err := n.LatestSnapshot()
	require.NoError(t, err)
	require.NotNil(t, snap)
	require.Equal(t, uint64(100), snap.LastIncludedIndex)
}

// TestInstallSnapshot_StaleTermRejected: a follower at term 5 receives an
// InstallSnapshot at term 4. Reply must report term 5; state must not change.
func TestInstallSnapshot_StaleTermRejected(t *testing.T) {
	n, err := NewNode(Config{
		ID:               "n1",
		Peers:            []string{"p1", "p2"},
		ElectionTimeout:  time.Hour,
		HeartbeatTimeout: testHeartbeat,
	})
	require.NoError(t, err)
	seedLogEntries(n, []LogEntry{{Term: 1, Index: 1, Command: []byte("A")}})
	n.st.currentTerm = 5
	n.rs.Store(n.st.snapshot())

	n.Start()
	t.Cleanup(n.Stop)
	go func() {
		for range n.ApplyCh() {
		}
	}()

	reply := n.HandleInstallSnapshot(&InstallSnapshotArgs{
		Term:              4, // stale
		LeaderID:          "old-leader",
		LastIncludedIndex: 100,
		LastIncludedTerm:  3,
		Data:              []byte("stale"),
	})
	require.Equal(t, uint64(5), reply.Term, "reply.Term must report current (higher) term")

	// State unchanged.
	require.Equal(t, uint64(5), n.Term())
	snap, _ := n.LatestSnapshot()
	require.Nil(t, snap, "no snapshot saved on stale-term reject")

	n.Stop()
	require.Equal(t, uint64(1), n.st.log.FirstIndex(), "log FirstIndex unchanged")
	require.Equal(t, uint64(1), n.st.log.LastIndex(), "log LastIndex unchanged")
}

// snapshotCountingTransport wraps a Transport and counts SendInstallSnapshot
// calls per peer. AE/RV pass through transparently. Used to assert the
// leader actually invoked the snapshot RPC (not merely the AE path).
type snapshotCountingTransport struct {
	inner Transport
	mu    sync.Mutex
	count map[string]int
}

func (s *snapshotCountingTransport) SendRequestVote(peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	return s.inner.SendRequestVote(peer, args)
}

func (s *snapshotCountingTransport) SendAppendEntries(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	return s.inner.SendAppendEntries(peer, args)
}

func (s *snapshotCountingTransport) SendInstallSnapshot(peer string, args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	s.mu.Lock()
	if s.count == nil {
		s.count = make(map[string]int)
	}
	s.count[peer]++
	s.mu.Unlock()
	return s.inner.SendInstallSnapshot(peer, args)
}

func (s *snapshotCountingTransport) SendTimeoutNow(peer string, args *TimeoutNowArgs) (*TimeoutNowReply, error) {
	return s.inner.SendTimeoutNow(peer, args)
}

// TestInstallSnapshot_LeaderSendsWhenFollowerBehind: 3-voter cluster. n1 is
// leader; n2 starts late and is not registered in memNetwork until it starts,
// matching a truly offline peer. After n1 builds up a log and compacts past
// the early indices, n2 joins empty. When the leader heartbeats it, n1's
// nextIndex[n2] (initialised to last+1) falls below FirstIndex after the next
// AE-reject cycle — at which point dispatchOne picks InstallSnapshot.
//
// Asserts:
//   - SendInstallSnapshot fired to n2 (load-bearing — proves dispatchOne's
//     snapshot branch).
//   - n2's applyCh delivers a LogEntrySnapshot signal.
//   - The cluster continues to commit subsequent proposes.
func TestInstallSnapshot_LeaderSendsWhenFollowerBehind(t *testing.T) {
	net := newMemNetwork()

	// Build n1, n2, n3 with usual asymmetric election timeouts.
	n1, err := NewNode(Config{
		ID:               "n1",
		Peers:            []string{"n2", "n3"},
		ElectionTimeout:  fastElectionTimeout,
		HeartbeatTimeout: testHeartbeat,
	})
	require.NoError(t, err)
	n2, err := NewNode(Config{
		ID:               "n2",
		Peers:            []string{"n1", "n3"},
		ElectionTimeout:  slowElectionTimeout,
		HeartbeatTimeout: testHeartbeat,
	})
	require.NoError(t, err)
	n3, err := NewNode(Config{
		ID:               "n3",
		Peers:            []string{"n1", "n2"},
		ElectionTimeout:  slowElectionTimeout,
		HeartbeatTimeout: testHeartbeat,
	})
	require.NoError(t, err)

	// Wire transports for the online nodes. n1's outbound goes through a
	// counting wrapper so we can detect SendInstallSnapshot calls. n2 is
	// intentionally not registered yet: registering before Start allows
	// memTransport to enqueue pre-compaction AppendEntries into n2.cmdCh,
	// which is not how a real offline peer behaves.
	snapTr := &snapshotCountingTransport{
		inner: net.Register("n1", n1),
		count: make(map[string]int),
	}
	n1.SetTransport(snapTr)
	n3.SetTransport(net.Register("n3", n3))

	// Start n1 and n3 first so n1 can win the election and replicate to n3
	// (a 2-of-3 majority is enough). n2 stays offline so it CANNOT receive
	// any of the early entries via AE.
	for _, n := range []*Node{n1, n3} {
		n.Start()
		t.Cleanup(n.Stop)
		go func(n *Node) {
			for range n.ApplyCh() {
			}
		}(n)
	}
	require.NoError(t, waitFor(2*time.Second, func() bool { return n1.IsLeader() }))

	// Propose 10 entries; commits via {n1, n3} majority. n2 has no log.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < 10; i++ {
		_, err := n1.ProposeWait(ctx, []byte(fmt.Sprintf("cmd-%d", i)))
		require.NoError(t, err)
	}
	require.NoError(t, waitFor(2*time.Second, func() bool {
		return n1.CommittedIndex() >= 11 && n3.CommittedIndex() >= 11
	}))

	// Leader compacts to FirstIndex=9. Now ANY follower whose nextIndex
	// falls below 9 must be served via InstallSnapshot.
	require.NoError(t, n1.CreateSnapshot(8, []byte("fsm@8")))

	// Now start n2. Its log is empty (FirstIndex=1, LastIndex=0). On the
	// first AE from n1, n2 will reject (PrevLogIndex > 0 but its log too
	// short OR — depending on what n1 sent — its conflict hint pushes
	// nextIndex below 9). The leader's next dispatch picks InstallSnapshot
	// because nextIndex[n2] < FirstIndex (9).
	n2applied := make(chan LogEntry, 64)
	go func() {
		for e := range n2.ApplyCh() {
			n2applied <- e
		}
	}()
	n2.SetTransport(net.Register("n2", n2))
	n2.Start()
	t.Cleanup(n2.Stop)

	// Wait for SendInstallSnapshot to fire to n2.
	require.NoError(t, waitFor(3*time.Second, func() bool {
		snapTr.mu.Lock()
		defer snapTr.mu.Unlock()
		return snapTr.count["n2"] > 0
	}), "leader did not send InstallSnapshot to n2")

	// n2's applyCh must deliver a LogEntrySnapshot.
	deadline := time.After(2 * time.Second)
	for {
		select {
		case e := <-n2applied:
			if e.Type == LogEntrySnapshot {
				require.Equal(t, uint64(8), e.Index)
				goto post
			}
			// AE may also deliver entries before/after; keep draining.
		case <-deadline:
			t.Fatal("n2 did not receive LogEntrySnapshot on applyCh")
		}
	}
post:
	// n2's matchIndex on the leader must catch up past the snapshot.
	require.NoError(t, waitFor(2*time.Second, func() bool {
		// matchIndex is leader-only state — read after the actor has
		// processed the install reply. We piggyback on the apply path:
		// once the cluster commits a fresh propose, n2 must have been
		// caught up enough to participate in quorum.
		ctx2, cancel2 := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel2()
		_, err := n1.ProposeWait(ctx2, []byte("post-snapshot"))
		return err == nil && n2.CommittedIndex() >= n1.CommittedIndex()-1
	}), "n2 did not catch up post-InstallSnapshot")
}

// TestInstallSnapshot_SecondInstallOverPriorSnapshot: a follower that already
// has a snapshot at index 50 receives a fresher InstallSnapshot at index 100.
// Must NOT panic on the entry-side TruncateAfter (regression: prior bug used
// TruncateAfter(0), which violates the FirstIndex-1 floor guard once the
// follower had a snapshot).
func TestInstallSnapshot_SecondInstallOverPriorSnapshot(t *testing.T) {
	n, err := NewNode(Config{
		ID:               "n1",
		Peers:            []string{"p1", "p2"},
		ElectionTimeout:  time.Hour,
		HeartbeatTimeout: testHeartbeat,
	})
	require.NoError(t, err)
	n.st.currentTerm = 1
	n.rs.Store(n.st.snapshot())

	n.Start()
	t.Cleanup(n.Stop)
	go func() {
		for range n.ApplyCh() {
		}
	}()

	// First install: snapshot at index 50 → FirstIndex=51 on the follower.
	first := n.HandleInstallSnapshot(&InstallSnapshotArgs{
		Term:              1,
		LeaderID:          "leader",
		LastIncludedIndex: 50,
		LastIncludedTerm:  1,
		Data:              []byte("snap@50"),
	})
	require.Equal(t, uint64(1), first.Term)

	// Second install at higher index — must succeed without panic.
	second := n.HandleInstallSnapshot(&InstallSnapshotArgs{
		Term:              1,
		LeaderID:          "leader",
		LastIncludedIndex: 100,
		LastIncludedTerm:  2,
		Data:              []byte("snap@100"),
	})
	require.Equal(t, uint64(1), second.Term)

	n.Stop()
	require.Equal(t, uint64(101), n.st.log.FirstIndex(), "FirstIndex must advance to second snapshot's boundary+1")
	require.Equal(t, uint64(100), n.st.commitIndex)
}

// TestInstallSnapshot_StaleSnapshotIgnored: an InstallSnapshot whose
// LastIncludedIndex is BELOW our current FirstIndex (we already have a
// fresher snapshot) replies success but does not regress our state.
func TestInstallSnapshot_StaleSnapshotIgnored(t *testing.T) {
	n, err := NewNode(Config{
		ID:               "n1",
		Peers:            []string{"p1", "p2"},
		ElectionTimeout:  time.Hour,
		HeartbeatTimeout: testHeartbeat,
	})
	require.NoError(t, err)
	n.st.currentTerm = 1
	n.rs.Store(n.st.snapshot())
	n.Start()
	t.Cleanup(n.Stop)
	go func() {
		for range n.ApplyCh() {
		}
	}()

	// Install at index 100 first.
	n.HandleInstallSnapshot(&InstallSnapshotArgs{
		Term:              1,
		LeaderID:          "leader",
		LastIncludedIndex: 100,
		LastIncludedTerm:  2,
		Data:              []byte("fresh"),
	})

	// Stale install at index 50 — must be ignored without panic.
	reply := n.HandleInstallSnapshot(&InstallSnapshotArgs{
		Term:              1,
		LeaderID:          "leader",
		LastIncludedIndex: 50,
		LastIncludedTerm:  1,
		Data:              []byte("stale"),
	})
	require.Equal(t, uint64(1), reply.Term)

	n.Stop()
	require.Equal(t, uint64(101), n.st.log.FirstIndex(), "stale install must not regress FirstIndex")
	require.Equal(t, uint64(100), n.st.commitIndex, "stale install must not regress commitIndex")
}

// TestInstallSnapshot_SkipWhenAlreadyCaughtUp: if the follower's log already
// has an entry matching (LastIncludedIndex, LastIncludedTerm), the snapshot
// is redundant — reply success but do not truncate.
func TestInstallSnapshot_SkipWhenAlreadyCaughtUp(t *testing.T) {
	n, err := NewNode(Config{
		ID:               "n1",
		Peers:            []string{"p1", "p2"},
		ElectionTimeout:  time.Hour,
		HeartbeatTimeout: testHeartbeat,
	})
	require.NoError(t, err)
	seedLogEntries(n, []LogEntry{
		{Term: 1, Index: 1, Command: []byte("A")},
		{Term: 1, Index: 2, Command: []byte("B")},
		{Term: 1, Index: 3, Command: []byte("C")},
	})
	n.st.currentTerm = 1
	n.rs.Store(n.st.snapshot())

	n.Start()
	t.Cleanup(n.Stop)
	go func() {
		for range n.ApplyCh() {
		}
	}()

	// Snapshot at index 2, term 1 — matches our existing entry.
	reply := n.HandleInstallSnapshot(&InstallSnapshotArgs{
		Term:              1,
		LeaderID:          "leader",
		LastIncludedIndex: 2,
		LastIncludedTerm:  1,
		Data:              []byte("redundant"),
	})
	require.Equal(t, uint64(1), reply.Term)

	// Log untouched: FirstIndex still 1, LastIndex still 3.
	n.Stop()
	require.Equal(t, uint64(1), n.st.log.FirstIndex(), "log not truncated when snapshot is redundant")
	require.Equal(t, uint64(3), n.st.log.LastIndex(), "log entries preserved")

	// No snapshot was saved (skip path) — Latest returns nil.
	snap, _ := n.LatestSnapshot()
	require.Nil(t, snap, "no snapshot saved on redundancy path")
}
