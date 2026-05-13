package raft

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// openTestDB opens a Badger DB at dir with no logger. Returns the DB and a
// close function. The caller must arrange for close to be called before any
// subsequent open on the same dir (Badger is single-writer).
func openTestDB(t *testing.T, dir string) (*badger.DB, func()) {
	t.Helper()
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	require.NoError(t, err)
	return db, func() { _ = db.Close() }
}

// newPersistentNode creates a Node backed by badger-based LogStore and
// StableStore at dir. The returned Node is NOT started; call n.Start() after
// any additional pre-Start seeding.
//
// Cleanup ordering: Badger must outlive the Node actor (which drains the log
// on Stop). t.Cleanup is LIFO: we register closeDB first, then n.Stop, so
// the actor's Stop runs first and Badger Close runs last.
func newPersistentNode(t *testing.T, dir, id string, peers []string, electionTimeout time.Duration) *Node {
	t.Helper()
	db, closeDB := openTestDB(t, dir)

	logStore, err := newBadgerLogStore(db, []byte("raft/v2/log/"))
	require.NoError(t, err)
	stable, err := newBadgerStableStore(db, []byte("raft/v2/hardstate/"))
	require.NoError(t, err)

	cfg := Config{
		ID:               id,
		Peers:            peers,
		LogStore:         logStore,
		StableStore:      stable,
		ElectionTimeout:  electionTimeout,
		HeartbeatTimeout: testHeartbeat,
	}
	n, err := NewNode(cfg)
	require.NoError(t, err)

	// Register cleanup: Stop first (LIFO), then Close DB.
	// t.Cleanup is LIFO: register closeDB first so it runs last.
	t.Cleanup(closeDB)
	t.Cleanup(n.Stop)

	return n
}

// TestRecovery_FreshStartNoLog: a Node with empty badger stores behaves
// exactly like the in-memory case — currentTerm=0, votedFor="", log empty.
func TestRecovery_FreshStartNoLog(t *testing.T) {
	dir := t.TempDir()
	n := newPersistentNode(t, dir, "n1", nil, 0)
	n.Start()

	// Single-voter: must auto-promote to Leader at term 1.
	require.NoError(t, waitFor(time.Second, func() bool { return n.IsLeader() }))
	require.Equal(t, uint64(1), n.Term())
	require.Equal(t, Leader, n.State())
}

// TestRecovery_LogReplayedFromBadger: bring up a single-voter node with badger
// stores, propose 3 commands, stop, reopen. Verify LastIndex and currentTerm
// are preserved across the restart.
//
// Term progression: fresh start → term 1 (single-voter auto-promote, persisted).
// After stop/reopen: currentTerm is restored from StableStore (= 1). The
// single-voter bootstrap path only advances currentTerm to 1 if it is currently
// 0, so on restart currentTerm stays at 1 (preserved, not incremented).
func TestRecovery_LogReplayedFromBadger(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: fresh start, propose 3 entries, then stop cleanly.
	var afterFirstRun uint64
	{
		db, closeDB := openTestDB(t, dir)
		logStore, err := newBadgerLogStore(db, []byte("raft/v2/log/"))
		require.NoError(t, err)
		stable, err := newBadgerStableStore(db, []byte("raft/v2/hardstate/"))
		require.NoError(t, err)

		n, err := NewNode(Config{
			ID:          "n1",
			LogStore:    logStore,
			StableStore: stable,
		})
		require.NoError(t, err)
		n.Start()

		// Drain ApplyCh so actor never blocks.
		go func() {
			for range n.ApplyCh() {
			}
		}()

		require.NoError(t, waitFor(time.Second, func() bool { return n.IsLeader() }))

		// Single-voter: propose 3 user commands. The single-voter bootstrap path
		// does NOT append a no-op (unlike multi-voter becomeLeader); user entries
		// land at indices 1, 2, 3.
		for i := 0; i < 3; i++ {
			require.NoError(t, n.Propose([]byte("cmd")))
		}
		// Wait for all 3 entries to commit.
		require.NoError(t, waitFor(2*time.Second, func() bool {
			return n.CommittedIndex() >= 3
		}))

		afterFirstRun = n.st.log.LastIndex()
		require.Equal(t, uint64(3), afterFirstRun)

		n.Stop()
		closeDB()
	}

	// Phase 2: reopen with same dir, verify log and HardState are restored.
	{
		db, closeDB := openTestDB(t, dir)
		defer closeDB()

		logStore, err := newBadgerLogStore(db, []byte("raft/v2/log/"))
		require.NoError(t, err)
		stable, err := newBadgerStableStore(db, []byte("raft/v2/hardstate/"))
		require.NoError(t, err)

		n, err := NewNode(Config{
			ID:          "n1",
			LogStore:    logStore,
			StableStore: stable,
		})
		require.NoError(t, err)

		// Before Start: verify restored state.
		require.Equal(t, uint64(3), n.st.log.LastIndex(),
			"log must have 3 entries after restart (3 user commands, no no-op for single-voter)")
		// currentTerm is restored from StableStore (= 1); NOT incremented on restart.
		require.Equal(t, uint64(1), n.st.currentTerm,
			"currentTerm must be preserved from stable store")

		n.Start()
		defer n.Stop()

		go func() {
			for range n.ApplyCh() {
			}
		}()

		// Single-voter auto-promotes again. Term stays at 1 (already persisted).
		require.NoError(t, waitFor(time.Second, func() bool { return n.IsLeader() }))
		require.Equal(t, uint64(1), n.Term(), "term must remain 1 after restart")
	}
}

// TestRecovery_HardStatePersistedAcrossElectionVote: a 3-voter cluster drives
// an election so node X votes for Y at term T. X is stopped and reopened.
// After restart, X must have HardState{CurrentTerm:T, VotedFor:Y} and must
// NOT grant its vote to a different candidate Z at the same term T.
func TestRecovery_HardStatePersistedAcrossElectionVote(t *testing.T) {
	// Build a 3-node cluster with n2 and n3 sharing an in-memory transport.
	// n1 gets a fast election timeout to win the election, forcing n2 to grant
	// its vote. Then we stop n2, reopen it, and verify votedFor is preserved.
	net := newMemNetwork()

	// n2 dir: we want persistent storage for n2 to test recovery.
	n2dir := t.TempDir()

	// Create n1 (fast timer, in-memory, no persistence needed).
	n1, err := NewNode(Config{
		ID:               "n1",
		Peers:            []string{"n2", "n3"},
		ElectionTimeout:  fastElectionTimeout,
		HeartbeatTimeout: testHeartbeat,
	})
	require.NoError(t, err)

	// Create n2 with BOTH persistent stable AND log stores.
	// Pairing a persistent StableStore with an in-memory LogStore violates
	// Raft §5.4.1 — the test must exercise the supported production config.
	n2db, closeN2db := openTestDB(t, n2dir)
	n2stable, err := newBadgerStableStore(n2db, []byte("raft/v2/hardstate/"))
	require.NoError(t, err)
	n2log, err := newBadgerLogStore(n2db, []byte("raft/v2/log/"))
	require.NoError(t, err)
	n2, err := NewNode(Config{
		ID:               "n2",
		Peers:            []string{"n1", "n3"},
		ElectionTimeout:  slowElectionTimeout,
		HeartbeatTimeout: testHeartbeat,
		LogStore:         n2log,
		StableStore:      n2stable,
	})
	require.NoError(t, err)

	// Create n3 (in-memory, slow timer).
	n3, err := NewNode(Config{
		ID:               "n3",
		Peers:            []string{"n1", "n2"},
		ElectionTimeout:  slowElectionTimeout,
		HeartbeatTimeout: testHeartbeat,
	})
	require.NoError(t, err)

	// Wire transports and start.
	for _, n := range []*Node{n1, n2, n3} {
		n.SetTransport(net.Register(n.cfg.ID, n))
	}
	for _, n := range []*Node{n1, n2, n3} {
		n.Start()
		go func(n *Node) {
			for range n.ApplyCh() {
			}
		}(n)
	}

	// Wait for n1 to win the election — this forces n2 to grant its vote.
	require.NoError(t, waitFor(2*time.Second, func() bool { return n1.IsLeader() }),
		"n1 did not win election")
	electedTerm := n1.Term()

	// Wait for n2 to catch up to the elected term (vote may be in flight).
	require.NoError(t, waitFor(time.Second, func() bool {
		rs := n2.rs.Load()
		return rs.term == electedTerm
	}), "n2 must catch up to elected term")

	// Verify n2 voted for n1 in the elected term.
	// n2 might have already been pushed to Follower by a heartbeat from n1
	// at the same term (which doesn't clear votedFor). Read directly from
	// the stable store which is authoritative.
	hs2, hsErr := n2stable.HardState()
	require.NoError(t, hsErr)
	require.Equal(t, electedTerm, hs2.CurrentTerm, "n2 stable store must be at elected term")
	require.Equal(t, "n1", hs2.VotedFor, "n2 must have voted for n1 in stable store")

	// Stop n2 cleanly. n1 and n3 keep running so the cluster stays alive.
	n2.Stop()
	closeN2db()
	n1.Stop()
	n3.Stop()

	// Phase 2: reopen n2 with a fresh DB connection; verify HardState.
	n2db2, closeN2db2 := openTestDB(t, n2dir)
	defer closeN2db2()
	n2stable2, err := newBadgerStableStore(n2db2, []byte("raft/v2/hardstate/"))
	require.NoError(t, err)
	n2log2, err := newBadgerLogStore(n2db2, []byte("raft/v2/log/"))
	require.NoError(t, err)

	hs, err := n2stable2.HardState()
	require.NoError(t, err)
	require.Equal(t, electedTerm, hs.CurrentTerm,
		"restarted n2 must have currentTerm = elected term from stable store")
	require.Equal(t, "n1", hs.VotedFor,
		"restarted n2 must remember it voted for n1 (§5.4.1)")

	// Reopen n2 as a Node and verify it restores the vote, then denies Z at same term.
	n2recovered, err := NewNode(Config{
		ID:               "n2",
		Peers:            []string{"n1", "n3"},
		ElectionTimeout:  slowElectionTimeout,
		HeartbeatTimeout: testHeartbeat,
		LogStore:         n2log2,
		StableStore:      n2stable2,
	})
	require.NoError(t, err)
	n2recovered.Start()
	defer n2recovered.Stop()
	go func() {
		for range n2recovered.ApplyCh() {
		}
	}()

	// n2 has restored currentTerm=electedTerm and votedFor="n1".
	// A vote request from "z-candidate" at the SAME term must be denied.
	done := make(chan *RequestVoteReply, 1)
	go func() {
		done <- n2recovered.HandleRequestVote(&RequestVoteArgs{
			Term:         electedTerm,
			CandidateID:  "z-candidate",
			LastLogIndex: 1000, // higher log — log-uptodate check not the bottleneck
			LastLogTerm:  1000,
		})
	}()
	select {
	case reply := <-done:
		require.False(t, reply.VoteGranted,
			"restarted n2 must deny vote to z-candidate at same term T (already voted for n1 in T)")
	case <-time.After(2 * time.Second):
		t.Fatal("HandleRequestVote timed out")
	}
}

// dropJointTransport is a test-only Transport wrapper that drops any
// AppendEntries RPC that carries a LogEntryJointConfChange entry when armed.
// Used to simulate leader crash after Stage-1 commits but before Stage-2
// (the joint entry) replicates to quorum.
type dropJointTransport struct {
	inner   Transport
	enabled atomic.Bool
}

func newDropJointTransport(inner Transport) *dropJointTransport {
	return &dropJointTransport{inner: inner}
}

func (d *dropJointTransport) enable()  { d.enabled.Store(true) }
func (d *dropJointTransport) disable() { d.enabled.Store(false) }

func (d *dropJointTransport) SendAppendEntries(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	if d.enabled.Load() {
		for _, e := range args.Entries {
			if e.Type == LogEntryJointConfChange {
				return nil, errors.New("joint entry dropped by test")
			}
		}
	}
	return d.inner.SendAppendEntries(peer, args)
}

func (d *dropJointTransport) SendRequestVote(peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	return d.inner.SendRequestVote(peer, args)
}

func (d *dropJointTransport) SendInstallSnapshot(peer string, args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	return d.inner.SendInstallSnapshot(peer, args)
}

func (d *dropJointTransport) SendTimeoutNow(peer string, args *TimeoutNowArgs) (*TimeoutNowReply, error) {
	return d.inner.SendTimeoutNow(peer, args)
}

// TestPromoteToVoter_OrphanRecovery pins the M6.0 follow-up: when a leader
// crashes after Stage-1 (ConfChangePromoteStage1) commits but before Stage-2
// (the joint AddVoter entry) replicates, the newly elected leader must detect
// the orphaned target and complete the promotion automatically.
//
// Protocol:
//  1. n1(leader), n2, n3 form a 3-voter cluster; n4 joins as learner.
//  2. n2/n3 transports are wrapped with dropJointTransport so Stage-2 can
//     never reach quorum while the filter is armed.
//  3. PromoteToVoter(n4) fires; Stage-1 commits (n4 leaves learners map)
//     but Stage-2 hangs waiting for replication.
//  4. n1 stops (leader crash).
//  5. Joint filter is disabled; n2 or n3 is elected leader.
//  6. The new leader's recoverOrphanedPromote fires and dispatches Stage-2.
//  7. n4 must appear as Voter in the converged configuration.
func TestPromoteToVoter_OrphanRecovery(t *testing.T) {
	net := newMemNetwork()

	makeNode := func(id string, peers []string, et time.Duration) *Node {
		n, err := NewNode(Config{
			ID:               id,
			Peers:            peers,
			ElectionTimeout:  et,
			HeartbeatTimeout: testHeartbeat,
		})
		require.NoError(t, err)
		n.SetTransport(net.Register(id, n))
		n.Start()
		t.Cleanup(n.Stop)
		go func() {
			for range n.ApplyCh() {
			}
		}()
		return n
	}

	n1 := makeNode("n1", []string{"n2", "n3"}, fastElectionTimeout)
	n2 := makeNode("n2", []string{"n1", "n3"}, slowElectionTimeout)
	n3 := makeNode("n3", []string{"n1", "n2"}, slowElectionTimeout)

	require.NoError(t, waitFor(2*time.Second, func() bool { return n1.IsLeader() }),
		"n1 must become leader")

	// n4 joins as learner and catches up.
	n4 := makeNode("n4", []string{"n1"}, slowElectionTimeout)
	require.NoError(t, n1.AddLearner("n4", "n4-addr"))
	require.NoError(t, waitFor(3*time.Second, func() bool {
		return n1.peerMatchIndexForTest("n4") >= n1.CommittedIndex()
	}), "n4 must catch up before promote")
	// Record commitIndex after catch-up; Stage-1 will land at +1.
	catchupCommit := n1.CommittedIndex()

	// Arm a joint-drop filter on n1 (the leader's sender transport) so
	// Stage-2 (LogEntryJointConfChange) can never replicate to n2/n3.
	// memNetwork routes AEs through the SENDER's transport, so wrapping n1
	// is what prevents Stage-2 from reaching the followers.
	drop1 := newDropJointTransport(n1.loadTransport())
	drop1.enable()
	n1.SetTransport(drop1)

	// Start PromoteToVoter; it will block waiting for Stage-2 to commit.
	promoteDone := make(chan error, 1)
	go func() { promoteDone <- n1.PromoteToVoter("n4") }()

	// Wait for Stage-1 to commit: CommittedIndex advances past catchupCommit.
	// Stage-1 (ConfChangePromoteStage1) is a plain LogEntryConfChange, not a
	// joint entry, so drop1 lets it through and n2/n3 ACK it.
	require.NoError(t, waitFor(3*time.Second, func() bool {
		return n1.CommittedIndex() > catchupCommit
	}), "Stage-1 must commit on n1 (n2/n3 ACK the non-joint AE)")

	// Crash n1. PromoteToVoter returns an error (leader lost); ignore it.
	n1.Stop()
	select {
	case <-promoteDone:
	case <-time.After(2 * time.Second):
		// may not have returned yet if n1 stop races with promote goroutine
	}

	// Disarm the filter — the new leader's transport is the underlying memTransport,
	// so Stage-2 can now replicate freely once recovery fires it.
	drop1.disable()

	// One of n2/n3 becomes leader and must recover the orphaned promote.
	newLeader := func() *Node {
		for _, n := range []*Node{n2, n3} {
			if n.IsLeader() {
				return n
			}
		}
		return nil
	}
	require.NoError(t, waitFor(5*time.Second, func() bool { return newLeader() != nil }),
		"n2 or n3 must elect a new leader")

	// n4 must become Voter in the converged configuration on all surviving nodes.
	require.NoError(t, waitFor(5*time.Second, func() bool {
		for _, n := range []*Node{n2, n3, n4} {
			found := false
			for _, s := range n.Configuration().Servers {
				if s.ID == "n4" && s.Suffrage == Voter {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true
	}), "n4 must appear as Voter after orphan recovery")
}

// TestRecovery_RestartedNodeIsFollower: a single-voter node that was Leader
// restarts as Follower and then auto-promotes again.
func TestRecovery_RestartedNodeIsFollower(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: start, become leader, stop.
	{
		db, closeDB := openTestDB(t, dir)
		stable, err := newBadgerStableStore(db, []byte("raft/v2/hardstate/"))
		require.NoError(t, err)
		n, err := NewNode(Config{ID: "n1", StableStore: stable})
		require.NoError(t, err)
		n.Start()
		go func() {
			for range n.ApplyCh() {
			}
		}()
		require.NoError(t, waitFor(time.Second, func() bool { return n.IsLeader() }))
		n.Stop()
		closeDB()
	}

	// Phase 2: reopen — Node must start as Follower and then auto-promote.
	{
		db, closeDB := openTestDB(t, dir)
		defer closeDB()
		stable, err := newBadgerStableStore(db, []byte("raft/v2/hardstate/"))
		require.NoError(t, err)
		n, err := NewNode(Config{ID: "n1", StableStore: stable})
		require.NoError(t, err)

		// Before Start: state is Follower (initial readState from NewNode).
		require.Equal(t, Follower, n.State(),
			"Node must start as Follower before actor runs")

		n.Start()
		defer n.Stop()
		go func() {
			for range n.ApplyCh() {
			}
		}()

		// Single-voter auto-promotes back to Leader.
		require.NoError(t, waitFor(time.Second, func() bool { return n.IsLeader() }),
			"restarted single-voter must auto-promote to Leader")
		require.Equal(t, uint64(1), n.Term(),
			"term must remain 1 (persisted, not advanced on restart)")
	}
}
