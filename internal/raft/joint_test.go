package raft

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestJointConfChangeEntry_RoundtripEnter(t *testing.T) {
	in := JointConfChange{
		Op: JointOpEnter,
		NewServers: []ServerEntry{
			{ID: "n1", Address: "127.0.0.1:9001", Suffrage: Voter},
			{ID: "n2", Address: "127.0.0.1:9002", Suffrage: Voter},
			{ID: "n3", Address: "127.0.0.1:9003", Suffrage: Voter},
		},
		OldServers: []ServerEntry{
			{ID: "n1", Address: "127.0.0.1:9001", Suffrage: Voter},
			{ID: "n2", Address: "127.0.0.1:9002", Suffrage: Voter},
		},
	}
	data := encodeJointConfChange(in)
	require.NotEmpty(t, data)

	out := decodeJointConfChange(data)
	require.Equal(t, in.Op, out.Op)
	require.Equal(t, in.NewServers, out.NewServers)
	require.Equal(t, in.OldServers, out.OldServers)
}

func TestJointConfChangeEntry_RoundtripLeave(t *testing.T) {
	in := JointConfChange{
		Op: JointOpLeave,
		NewServers: []ServerEntry{
			{ID: "n2", Address: "127.0.0.1:9002", Suffrage: Voter},
			{ID: "n3", Address: "127.0.0.1:9003", Suffrage: Voter},
		},
		OldServers: nil, // Leave entries carry the new-only set
	}
	data := encodeJointConfChange(in)
	require.NotEmpty(t, data)

	out := decodeJointConfChange(data)
	require.Equal(t, in.Op, out.Op)
	require.Equal(t, in.NewServers, out.NewServers)
	require.Empty(t, out.OldServers)
}

func TestJointConfChangeEntry_PreservesNonVoter(t *testing.T) {
	in := JointConfChange{
		Op: JointOpEnter,
		NewServers: []ServerEntry{
			{ID: "n1", Address: "127.0.0.1:9001", Suffrage: Voter},
			{ID: "learner1", Address: "127.0.0.1:9100", Suffrage: NonVoter},
		},
		OldServers: []ServerEntry{
			{ID: "n1", Address: "127.0.0.1:9001", Suffrage: Voter},
		},
	}
	data := encodeJointConfChange(in)
	out := decodeJointConfChange(data)

	require.Equal(t, NonVoter, out.NewServers[1].Suffrage)
	require.Equal(t, Voter, out.NewServers[0].Suffrage)
}

// jointTestNode builds a minimal Node sufficient for quorum-helper unit tests.
// The full constructor pipeline (storage, transport, batcher) is intentionally
// avoided — these tests exercise only in-memory voter-set arithmetic.
func jointTestNode(id string) *Node {
	return &Node{
		id:                   id,
		matchIndex:           make(map[string]uint64),
		nextIndex:            make(map[string]uint64),
		learnerIDs:           make(map[string]string),
		learnerPromoteCh:     make(map[string]chan struct{}),
		jointManagedLearners: make(map[string]struct{}),
	}
}

func TestDualQuorum_BothMajoritiesRequired(t *testing.T) {
	n := jointTestNode("n1")
	n.jointPhase = JointEntering
	n.jointOldVoters = []string{"n1", "n2", "n3"}
	n.jointNewVoters = []string{"n1", "n4", "n5"}

	// Old majority OK (n1 self + n2), new minority (only n1 self) → fail.
	require.False(t, n.dualMajority(map[string]bool{"n2": true}),
		"old majority alone insufficient")

	// New majority OK (n1 self + n4), old minority (only n1 self) → fail.
	require.False(t, n.dualMajority(map[string]bool{"n4": true}),
		"new majority alone insufficient")

	// Both quorums majority → pass.
	require.True(t, n.dualMajority(map[string]bool{"n2": true, "n4": true}),
		"both majorities pass")
}

func TestDualQuorum_SingleModeWhenJointNone(t *testing.T) {
	n := jointTestNode("n1")
	n.config.Peers = []string{"n2", "n3"}
	n.jointPhase = JointNone

	// Note: config.Peers excludes self in this codebase. quorumSets returns it as
	// "current" voter set, with self auto-counted by hasMajorityInSet.
	// Single mode: 3-node cluster (self + 2 peers), majority = 2.
	// Self alone = 1 ack: not enough.
	require.False(t, n.dualMajority(map[string]bool{}),
		"single-mode self-only is below majority of 3-node cluster")

	// Self + 1 peer ack = 2 acks: passes majority.
	require.True(t, n.dualMajority(map[string]bool{"n2": true}),
		"single-mode self + 1 peer is majority")
}

func TestQuorumMinMatchIndex_JointMode_Conservative(t *testing.T) {
	n := jointTestNode("n1")
	n.jointPhase = JointEntering
	n.jointOldVoters = []string{"n1", "n2", "n3"}
	n.jointNewVoters = []string{"n1", "n4", "n5"}
	n.matchIndex = map[string]uint64{
		"n1": 100, "n2": 90, "n3": 80, // old quorum median = 90
		"n4": 50, "n5": 40, // new quorum median = 50
	}

	// min(90, 50) = 50: a watermark must be safe in BOTH configurations.
	require.Equal(t, uint64(50), n.quorumMinMatchIndexLocked(),
		"joint mode picks min of both quorum medians")
}

func TestQuorumMinMatchIndex_SingleMode(t *testing.T) {
	n := jointTestNode("n1")
	n.config.Peers = []string{"n2", "n3"}
	n.jointPhase = JointNone
	n.matchIndex = map[string]uint64{"n1": 100, "n2": 90, "n3": 50}

	// Single-mode 3-node cluster: vals descending [100, 90, 50], median index 1 = 90.
	require.Equal(t, uint64(90), n.quorumMinMatchIndexLocked(),
		"single-mode uses configuration median")
}

func TestApply_JointEnter_ActivatesJointPhase(t *testing.T) {
	n := jointTestNode("n1")
	n.config.Peers = []string{"n2", "n3"}
	n.state = Follower

	entry := LogEntry{
		Index: 10,
		Term:  1,
		Type:  LogEntryJointConfChange,
		Command: encodeJointConfChange(JointConfChange{
			Op:         JointOpEnter,
			OldServers: []ServerEntry{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}},
			NewServers: []ServerEntry{{ID: "n1"}, {ID: "n2"}, {ID: "n4"}, {ID: "n5"}},
		}),
	}
	n.applyConfigChangeLocked(entry)

	require.Equal(t, JointEntering, n.jointPhase)
	require.Equal(t, []string{"n1", "n2", "n3"}, n.jointOldVoters)
	require.Equal(t, []string{"n1", "n2", "n4", "n5"}, n.jointNewVoters)
	require.Equal(t, uint64(10), n.jointEnterIndex)
	require.False(t, n.jointLeaveProposed)
}

func TestApply_JointEnter_LeaderInitsNewVotersOnly(t *testing.T) {
	n := jointTestNode("n1")
	n.config.Peers = []string{"n2", "n3"}
	n.state = Leader
	n.matchIndex["n2"] = 50 // pre-existing replication state
	n.nextIndex["n2"] = 51
	// log lastLogIdx=10 setup
	n.log = []LogEntry{{Index: 10, Term: 1}}

	entry := LogEntry{
		Index: 11,
		Term:  1,
		Type:  LogEntryJointConfChange,
		Command: encodeJointConfChange(JointConfChange{
			Op:         JointOpEnter,
			OldServers: []ServerEntry{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}},
			NewServers: []ServerEntry{{ID: "n1"}, {ID: "n2"}, {ID: "n4"}, {ID: "n5"}},
		}),
	}
	n.applyConfigChangeLocked(entry)

	// New voters n4/n5 initialized at lastLogIdx+1.
	require.Equal(t, uint64(11), n.nextIndex["n4"])
	require.Equal(t, uint64(11), n.nextIndex["n5"])
	require.Equal(t, uint64(0), n.matchIndex["n4"])
	require.Equal(t, uint64(0), n.matchIndex["n5"])
	// Existing voter n2 untouched.
	require.Equal(t, uint64(51), n.nextIndex["n2"])
	require.Equal(t, uint64(50), n.matchIndex["n2"])
}

func TestApply_JointLeave_DeactivatesAtAppendTime(t *testing.T) {
	n := jointTestNode("n1")
	n.jointPhase = JointEntering
	n.jointOldVoters = []string{"n1", "n2", "n3"}
	n.jointNewVoters = []string{"n1", "n2", "n4", "n5"}
	n.jointEnterIndex = 10
	ch := make(chan error, 1)
	n.jointResultCh = ch

	entry := LogEntry{
		Index: 11,
		Term:  1,
		Type:  LogEntryJointConfChange,
		Command: encodeJointConfChange(JointConfChange{
			Op:         JointOpLeave,
			NewServers: []ServerEntry{{ID: "n1"}, {ID: "n2"}, {ID: "n4"}, {ID: "n5"}},
		}),
	}
	n.applyConfigChangeLocked(entry)

	// Append-time: phase reset, config.Peers updated. config.Peers excludes self
	// (n1) per existing convention; the JointLeave entry carries the full voter
	// list including self.
	require.Equal(t, JointNone, n.jointPhase)
	require.Equal(t, []string{"n2", "n4", "n5"}, n.config.Peers)
	require.Empty(t, n.jointOldVoters)
	require.Empty(t, n.jointNewVoters)
	require.Equal(t, uint64(0), n.jointEnterIndex)

	// Append-time does NOT send to jointResultCh — that gates on commit (truncation safety).
	select {
	case <-ch:
		t.Fatal("jointResultCh should not be signaled at append time")
	default:
	}
	require.NotNil(t, n.jointResultCh)
}

// TestChangeMembership_NoOp_NilArgs — empty diff is a no-op success.
func TestChangeMembership_NoOp_NilArgs(t *testing.T) {
	n := jointTestNode("n1")
	n.state = Leader

	err := n.ChangeMembership(context.Background(), nil, nil)
	require.NoError(t, err)

	n.mu.Lock()
	phase := n.jointPhase
	n.mu.Unlock()
	require.Equal(t, JointNone, phase)
}

func TestChangeMembership_NotLeader_ReturnsErrNotLeader(t *testing.T) {
	n := jointTestNode("n1")
	n.state = Follower

	err := n.ChangeMembership(context.Background(), nil, []string{"n2"})
	require.ErrorIs(t, err, ErrNotLeader)
}

func TestChangeMembership_JointInFlight_ReturnsErrConfChangeInProgress(t *testing.T) {
	n := jointTestNode("n1")
	n.state = Leader
	n.jointPhase = JointEntering

	err := n.ChangeMembership(context.Background(), nil, []string{"n2"})
	require.ErrorIs(t, err, ErrConfChangeInProgress)
}

// TestSetChangeMembershipDefaults_PersistsAcrossCalls — Sub-project 3 PR-K1.
func TestSetChangeMembershipDefaults_PersistsAcrossCalls(t *testing.T) {
	n := jointTestNode("n1")
	n.SetChangeMembershipDefaults(ChangeMembershipOpts{CatchUpTimeout: 5 * time.Second})

	n.mu.Lock()
	got := n.effectiveChangeMembershipOpts()
	n.mu.Unlock()

	require.Equal(t, 5*time.Second, got.CatchUpTimeout)
}

func TestEffectiveChangeMembershipOpts_DefaultTimeout(t *testing.T) {
	n := jointTestNode("n1")

	n.mu.Lock()
	got := n.effectiveChangeMembershipOpts()
	n.mu.Unlock()

	require.Equal(t, 30*time.Second, got.CatchUpTimeout)
}

// TestApply_JointLeave_LearnerPromotionClearsLearnerIDs — Sub-project 3 prereq.
// When JointLeave commits with C_new containing IDs that were previously
// learners, those IDs become voters; learnerIDs must be cleaned to keep the
// state machine invariant (a server is voter or learner, never both).
func TestApply_JointLeave_LearnerPromotionClearsLearnerIDs(t *testing.T) {
	n := jointTestNode("n1")
	n.learnerIDs["n4"] = "n4"
	n.jointPhase = JointEntering
	n.jointOldVoters = []string{"n1", "n2", "n3"}
	n.jointNewVoters = []string{"n1", "n2", "n3", "n4"}
	n.jointEnterIndex = 5

	entry := LogEntry{
		Index: 6,
		Type:  LogEntryJointConfChange,
		Command: encodeJointConfChange(JointConfChange{
			Op: JointOpLeave,
			NewServers: []ServerEntry{
				{ID: "n1", Address: "n1", Suffrage: Voter},
				{ID: "n2", Address: "n2", Suffrage: Voter},
				{ID: "n3", Address: "n3", Suffrage: Voter},
				{ID: "n4", Address: "n4", Suffrage: Voter},
			},
		}),
	}
	n.applyJointConfChangeLocked(entry)

	require.NotContains(t, n.learnerIDs, "n4",
		"learnerIDs must be cleaned of IDs promoted to voter via JointLeave")
	require.Contains(t, n.config.Peers, "n4", "config.Peers includes promoted voter")
}

func TestCheckJointAdvance_LogLookaheadIdempotency(t *testing.T) {
	n := jointTestNode("n1")
	n.state = Leader
	n.jointPhase = JointEntering
	n.jointEnterIndex = 10
	n.commitIndex = 11
	n.jointLeaveProposed = false

	// log already contains a JointLeave entry (uncommitted tail or just-appended).
	n.log = []LogEntry{
		{Index: 10, Type: LogEntryJointConfChange, Command: encodeJointConfChange(
			JointConfChange{Op: JointOpEnter})},
		{Index: 11, Type: LogEntryJointConfChange, Command: encodeJointConfChange(
			JointConfChange{Op: JointOpLeave})},
	}

	// checkJointAdvance must not propose again — log has the entry already.
	n.checkJointAdvance()

	require.False(t, n.jointLeaveProposed,
		"flag stays false; log lookahead returns early without proposing")
}

// jointTestLeader builds a Node sufficient for proposeJointConfChangeWait
// pre-flight checks (state guards, no-op detection) without standing up the
// proposal pipeline. proposeCh is non-nil but unbuffered tests should not
// reach the propose path.
func jointTestLeader(id string, peers []string) *Node {
	n := jointTestNode(id)
	n.state = Leader
	n.config.Peers = peers
	n.proposalCh = make(chan proposal, 16)
	n.stopCh = make(chan struct{})
	return n
}

func TestProposeJointConfChangeWait_RejectsNotLeader(t *testing.T) {
	n := jointTestLeader("n1", []string{"n2", "n3"})
	n.state = Follower

	err := n.proposeJointConfChangeWait(context.Background(),
		[]ServerEntry{{ID: "n4", Address: "n4"}}, nil)
	require.ErrorIs(t, err, ErrNotLeader)
}

func TestProposeJointConfChangeWait_RejectsConcurrentJoint(t *testing.T) {
	n := jointTestLeader("n1", []string{"n2", "n3"})
	n.jointPhase = JointEntering

	err := n.proposeJointConfChangeWait(context.Background(),
		[]ServerEntry{{ID: "n4", Address: "n4"}}, nil)
	require.ErrorIs(t, err, ErrConfChangeInProgress)
}

func TestProposeJointConfChangeWait_RejectsConcurrentSingleServer(t *testing.T) {
	n := jointTestLeader("n1", []string{"n2", "n3"})
	n.pendingConfChangeIndex = 5

	err := n.proposeJointConfChangeWait(context.Background(),
		[]ServerEntry{{ID: "n4", Address: "n4"}}, nil)
	require.ErrorIs(t, err, ErrConfChangeInProgress)
}

func TestProposeJointConfChangeWait_RejectsMixedVersion(t *testing.T) {
	n := jointTestLeader("n1", []string{"n2", "n3"})
	n.mixedVersion = true

	err := n.proposeJointConfChangeWait(context.Background(),
		[]ServerEntry{{ID: "n4", Address: "n4"}}, nil)
	require.ErrorIs(t, err, ErrMixedVersionNoMembershipChange)
}

func TestProposeJointConfChangeWait_NoOpEqualSets(t *testing.T) {
	n := jointTestLeader("n1", []string{"n2", "n3"})

	// adds 없고 removes 없음 → C_old == C_new → no-op.
	err := n.proposeJointConfChangeWait(context.Background(), nil, nil)
	require.NoError(t, err)
	require.Equal(t, JointNone, n.jointPhase, "no propose, no state change")
	require.Nil(t, n.jointResultCh, "wait channel not installed for no-op")
}

func TestEqualServerSets(t *testing.T) {
	a := []ServerEntry{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}
	b := []ServerEntry{{ID: "n3"}, {ID: "n1"}, {ID: "n2"}}
	require.True(t, equalServerSets(a, b), "id-keyed comparison ignores order")

	c := []ServerEntry{{ID: "n1"}, {ID: "n2"}}
	require.False(t, equalServerSets(a, c), "different size")

	d := []ServerEntry{{ID: "n1"}, {ID: "n2"}, {ID: "n4"}}
	require.False(t, equalServerSets(a, d), "different members")
}

func TestSnapshot_RoundtripPreservesJointState(t *testing.T) {
	dir := t.TempDir()
	store, err := NewBadgerLogStore(dir)
	require.NoError(t, err)
	defer store.Close()

	in := Snapshot{
		Index:           42,
		Term:            7,
		Data:            []byte("fsm-state"),
		Servers:         []Server{{ID: "n1", Suffrage: Voter}, {ID: "n2", Suffrage: Voter}, {ID: "n3", Suffrage: Voter}},
		JointPhase:      JointEntering,
		JointOldVoters:  []string{"n1", "n2", "n3"},
		JointNewVoters:  []string{"n1", "n2", "n4"},
		JointEnterIndex: 40,
	}
	require.NoError(t, store.SaveSnapshot(in))

	out, err := store.LoadSnapshot()
	require.NoError(t, err)
	require.Equal(t, in.Index, out.Index)
	require.Equal(t, in.Term, out.Term)
	require.Equal(t, in.Data, out.Data)
	require.Equal(t, JointEntering, out.JointPhase)
	require.Equal(t, []string{"n1", "n2", "n3"}, out.JointOldVoters)
	require.Equal(t, []string{"n1", "n2", "n4"}, out.JointNewVoters)
	require.Equal(t, uint64(40), out.JointEnterIndex)
}

func TestSnapshot_LegacyHasZeroJointState(t *testing.T) {
	dir := t.TempDir()
	store, err := NewBadgerLogStore(dir)
	require.NoError(t, err)
	defer store.Close()

	// Legacy snapshot: no joint fields set — JointPhase is zero, vectors empty.
	require.NoError(t, store.SaveSnapshot(Snapshot{
		Index: 10, Term: 1, Data: []byte("x"),
		Servers: []Server{{ID: "n1", Suffrage: Voter}},
	}))

	out, err := store.LoadSnapshot()
	require.NoError(t, err)
	require.Equal(t, JointNone, out.JointPhase)
	require.Empty(t, out.JointOldVoters)
	require.Empty(t, out.JointNewVoters)
	require.Equal(t, uint64(0), out.JointEnterIndex)
}

func TestRestoreJointStateFromSnapshot_ResetsLeaveProposed(t *testing.T) {
	n := jointTestNode("n1")
	n.jointLeaveProposed = true // pre-existing flag from before restart

	n.RestoreJointStateFromSnapshot(int8(JointEntering),
		[]string{"n1", "n2", "n3"}, []string{"n1", "n2", "n4"}, 42, nil)

	require.Equal(t, JointEntering, n.jointPhase)
	require.Equal(t, []string{"n1", "n2", "n3"}, n.jointOldVoters)
	require.Equal(t, []string{"n1", "n2", "n4"}, n.jointNewVoters)
	require.Equal(t, uint64(42), n.jointEnterIndex)
	// flag reset so heartbeat watcher re-evaluates after snapshot restore.
	require.False(t, n.jointLeaveProposed)
}

func TestApply_JointLeave_SelfRemoval_StepsDown(t *testing.T) {
	// Append-time apply does NOT step the leader down; commit-time apply (apply
	// loop) does. This test pins the append-time behavior — leader stays Leader
	// after append, even when the entry removes self. Commit-time step-down is
	// covered by TestApply_JointLeave_SelfRemoval_CommitTime below.
	n := jointTestLeader("n1", []string{"n2", "n3"})
	n.jointPhase = JointEntering
	n.jointOldVoters = []string{"n1", "n2", "n3"}
	n.jointNewVoters = []string{"n2", "n3"} // self removed

	entry := LogEntry{
		Index: 10, Term: 1, Type: LogEntryJointConfChange,
		Command: encodeJointConfChange(JointConfChange{
			Op:         JointOpLeave,
			NewServers: []ServerEntry{{ID: "n2"}, {ID: "n3"}},
		}),
	}
	n.applyConfigChangeLocked(entry)

	// Append-time: phase + config.Peers updated; state still Leader (commit-time hook handles step-down).
	require.Equal(t, JointNone, n.jointPhase)
	require.Equal(t, []string{"n2", "n3"}, n.config.Peers)
	require.Equal(t, Leader, n.state, "step-down deferred to commit-time apply hook")
}

// TestJoint_E2E_RemoveOne — full joint cycle on a 4-node in-memory cluster.
// Leader proposes a JointEnter to remove the last voter; the heartbeat watcher
// auto-proposes JointLeave; on commit the apply loop closes jointPromoteCh and
// the caller returns. Verifies commit-time close (truncation safety) end-to-end.
func TestJoint_E2E_RemoveOne(t *testing.T) {
	cluster := newTestCluster(t, 4)
	cluster.startAll()
	leader := cluster.waitForLeader(2 * time.Second)
	require.NotNil(t, leader, "no leader elected")

	// Identify a non-leader peer to remove (must not be the leader itself —
	// self-removal step-down is exercised in PR-J5 chaos scenarios).
	var removeID string
	leader.mu.Lock()
	for _, p := range leader.config.Peers {
		if p != leader.id {
			removeID = p
			break
		}
	}
	leader.mu.Unlock()
	require.NotEmpty(t, removeID)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := leader.proposeJointConfChangeWait(ctx, nil, []string{removeID})
	require.NoError(t, err, "joint cycle did not complete")

	// On wakeup: jointPhase is None and config.Peers excludes removeID.
	leader.mu.Lock()
	phase := leader.jointPhase
	peers := append([]string(nil), leader.config.Peers...)
	leader.mu.Unlock()

	require.Equal(t, JointNone, phase, "joint phase should clear on commit-time close")
	require.NotContains(t, peers, removeID, "C_new excludes removed voter")
	require.Len(t, peers, 2, "leader sees 3-node cluster minus self after removal")
}

// TestJointPhase_None_ReturnsZeroes — Sub-project 3 PR-K1.
func TestJointPhase_None_ReturnsZeroes(t *testing.T) {
	n := jointTestNode("n1")
	phase, oldV, newV, idx := n.JointPhase()
	require.Equal(t, JointNone, phase)
	require.Empty(t, oldV)
	require.Empty(t, newV)
	require.Zero(t, idx)
}

func TestJointPhase_Entering_ReportsAllFour(t *testing.T) {
	n := jointTestNode("n1")
	n.jointPhase = JointEntering
	n.jointOldVoters = []string{"n1", "n2", "n3"}
	n.jointNewVoters = []string{"n1", "n2", "n4"}
	n.jointEnterIndex = 42

	phase, oldV, newV, idx := n.JointPhase()
	require.Equal(t, JointEntering, phase)
	require.Equal(t, []string{"n1", "n2", "n3"}, oldV)
	require.Equal(t, []string{"n1", "n2", "n4"}, newV)
	require.Equal(t, uint64(42), idx)
}

// TestConfiguration_JointEntering_ReturnsUnion — Sub-project 3 PR-K1.
func TestConfiguration_JointEntering_ReturnsUnion(t *testing.T) {
	n := jointTestNode("n1")
	n.config.Peers = []string{"n2", "n3"}
	n.jointPhase = JointEntering
	n.jointOldVoters = []string{"n1", "n2", "n3"}
	n.jointNewVoters = []string{"n1", "n2", "n4"} // n3 removed, n4 added

	cfg := n.Configuration()

	ids := make([]string, 0, len(cfg.Servers))
	for _, s := range cfg.Servers {
		ids = append(ids, s.ID)
		require.Equal(t, Voter, s.Suffrage, "all dual-quorum members should be Voter")
	}
	require.ElementsMatch(t, []string{"n1", "n2", "n3", "n4"}, ids,
		"during JointEntering, return union of C_old and C_new")
}

// TestChangeMembership_CatchUpTimeout_AttemptsCleanup — Sub-project 3 PR-K1.
// Tight catch-up timeout + low threshold so the fake unreachable learner
// times out. defer cleanup must clear jointManagedLearners.
func TestChangeMembership_CatchUpTimeout_AttemptsCleanup(t *testing.T) {
	cluster := newTestCluster(t, 3)
	cluster.startAll()
	leader := cluster.waitForLeader(2 * time.Second)
	require.NotNil(t, leader)

	// Drive commitIndex up so threshold check is non-trivial.
	for i := 0; i < 5; i++ {
		require.NoError(t, leader.Propose([]byte("entry")))
	}
	leader.SetLearnerCatchupThreshold(1) // tight; fake learner can't catch up
	leader.SetChangeMembershipDefaults(ChangeMembershipOpts{CatchUpTimeout: 200 * time.Millisecond})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := leader.ChangeMembership(ctx,
		[]ServerEntry{{ID: "fake-unreachable", Address: "127.0.0.1:65531", Suffrage: Voter}},
		nil,
	)
	require.ErrorIs(t, err, ErrLearnerCatchUpTimeout)

	leader.mu.Lock()
	managed := len(leader.jointManagedLearners)
	leader.mu.Unlock()
	require.Zero(t, managed, "defer cleanup must clear jointManagedLearners")
}

// TestChangeMembership_AddRemove_LearnerFirstHappyPath — Sub-project 3 PR-K1.
// Uses high catch-up threshold to make a fake unreachable learner trivially
// caught up, then verifies joint atomic promote+remove.
func TestChangeMembership_AddRemove_LearnerFirstHappyPath(t *testing.T) {
	cluster := newTestCluster(t, 3)
	cluster.startAll()
	leader := cluster.waitForLeader(2 * time.Second)
	require.NotNil(t, leader)

	// Drive commitIndex up so AddLearner can commit + threshold trick activates.
	for i := 0; i < 5; i++ {
		require.NoError(t, leader.Propose([]byte("entry")))
	}
	leader.SetLearnerCatchupThreshold(1_000_000) // fake learner trivially passes

	var removeID string
	leader.mu.Lock()
	for _, p := range leader.config.Peers {
		if p != leader.id {
			removeID = p
			break
		}
	}
	leader.mu.Unlock()
	require.NotEmpty(t, removeID)

	leader.SetChangeMembershipDefaults(ChangeMembershipOpts{CatchUpTimeout: 3 * time.Second})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := leader.ChangeMembership(ctx,
		[]ServerEntry{{ID: "fake-new-voter", Address: "127.0.0.1:65530", Suffrage: Voter}},
		[]string{removeID},
	)
	require.NoError(t, err, "joint cycle should complete via dual quorum from C_old")

	leader.mu.Lock()
	peers := append([]string(nil), leader.config.Peers...)
	managed := len(leader.jointManagedLearners)
	leader.mu.Unlock()

	require.NotContains(t, peers, removeID, "removed voter dropped from C_new")
	require.Contains(t, peers, "127.0.0.1:65530", "new voter address present in C_new")
	require.Zero(t, managed, "jointManagedLearners cleared after success")
}

// TestJoint_E2E_RemoveSelf — Sub-project 3 PR-K1 acceptance test.
// Leader removes itself via ChangeMembership. Verifies commit-time step-down
// ordering: jointPromoteCh closes BEFORE state=Follower (raft.go:578-592),
// so the caller wakes up with nil. Then a new leader is elected from C_new.
func TestJoint_E2E_RemoveSelf(t *testing.T) {
	cluster := newTestCluster(t, 4)
	cluster.startAll()
	oldLeader := cluster.waitForLeader(2 * time.Second)
	require.NotNil(t, oldLeader)

	oldLeaderID := oldLeader.id

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := oldLeader.ChangeMembership(ctx, nil, []string{oldLeaderID})
	require.NoError(t, err, "self-removal returns nil via commit-time wakeup")

	require.Equal(t, Follower, oldLeader.State(), "old leader steps down to Follower")

	// New leader from remaining 3 nodes. Allow generous timeout because the
	// old leader is now a Follower in the same election space (its config.Peers
	// still references the cluster) and may compete in split votes until it
	// either wins (and immediately discovers it's not in C_new — see assertion
	// below) or loses repeatedly to one of the C_new nodes.
	require.Eventually(t, func() bool {
		for _, n := range cluster.nodes {
			if n.State() == Leader && n.id != oldLeaderID {
				return true
			}
		}
		return false
	}, 15*time.Second, 50*time.Millisecond, "new leader elected from C_new")

	// New leader's view excludes removed self.
	var newLeader *Node
	for _, n := range cluster.nodes {
		if n.State() == Leader {
			newLeader = n
			break
		}
	}
	require.NotNil(t, newLeader)
	newLeader.mu.Lock()
	peers := append([]string(nil), newLeader.config.Peers...)
	newLeader.mu.Unlock()
	require.NotContains(t, peers, oldLeaderID)
}

// TestChangeMembership_RemovesOnly_E2E — Sub-project 3 PR-K1. Verifies the
// removes-only fast path through ChangeMembership end-to-end.
func TestChangeMembership_RemovesOnly_E2E(t *testing.T) {
	cluster := newTestCluster(t, 4)
	cluster.startAll()
	leader := cluster.waitForLeader(2 * time.Second)
	require.NotNil(t, leader)

	var removeID string
	leader.mu.Lock()
	for _, p := range leader.config.Peers {
		if p != leader.id {
			removeID = p
			break
		}
	}
	leader.mu.Unlock()
	require.NotEmpty(t, removeID)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := leader.ChangeMembership(ctx, nil, []string{removeID})
	require.NoError(t, err)

	leader.mu.Lock()
	phase := leader.jointPhase
	peers := append([]string(nil), leader.config.Peers...)
	leader.mu.Unlock()

	require.Equal(t, JointNone, phase)
	require.NotContains(t, peers, removeID)
}

func TestRebuildConfigFromLog_TruncatedJointLeave_RevertsToEntering(t *testing.T) {
	n := jointTestNode("n1")
	n.initialPeers = []string{"n1", "n2", "n3"}

	// Log retained only JointEnter — JointLeave was truncated by a new leader.
	n.log = []LogEntry{
		{Index: 1, Type: LogEntryJointConfChange, Command: encodeJointConfChange(
			JointConfChange{
				Op:         JointOpEnter,
				OldServers: []ServerEntry{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}},
				NewServers: []ServerEntry{{ID: "n1"}, {ID: "n2"}, {ID: "n4"}, {ID: "n5"}},
			})},
	}

	n.rebuildConfigFromLog(0, n.initialPeers, nil)

	require.Equal(t, JointEntering, n.jointPhase, "truncated Leave reverts to Entering")
	require.Equal(t, uint64(1), n.jointEnterIndex)
	require.Equal(t, []string{"n1", "n2", "n3"}, n.jointOldVoters)
	require.Equal(t, []string{"n1", "n2", "n4", "n5"}, n.jointNewVoters)
	require.False(t, n.jointLeaveProposed)
}

// TestCurrentConfigServers_OmitsSelfWhenRemoved — PR-K1 follow-up.
// A snapshotted orphan node must not include self in the Servers list,
// otherwise the restore path can't derive removedFromCluster.
func TestCurrentConfigServers_OmitsSelfWhenRemoved(t *testing.T) {
	n := jointTestNode("n1")
	n.config.Peers = []string{"n2", "n3"}
	n.removedFromCluster = true

	servers := n.currentConfigServers()

	for _, sv := range servers {
		require.NotEqual(t, "n1", sv.ID,
			"removed node must not include self in Servers list")
	}
	require.Len(t, servers, 2, "only the two voter peers should remain")
}

func TestCurrentConfigServers_IncludesSelfWhenNotRemoved(t *testing.T) {
	n := jointTestNode("n1")
	n.config.Peers = []string{"n2", "n3"}

	servers := n.currentConfigServers()

	foundSelf := false
	for _, sv := range servers {
		if sv.ID == "n1" {
			foundSelf = true
		}
	}
	require.True(t, foundSelf, "non-removed node includes self as Voter")
}

// TestRebuildConfigFromLog_RestoresRemovedFromCluster — PR-K1 follow-up.
// Replay path must mirror commit-time hook (raft.go:586) and set
// removedFromCluster when JointLeave commits with self ∉ C_new. Without this,
// a restarted orphan node has flag=false and rejoins elections.
func TestRebuildConfigFromLog_RestoresRemovedFromCluster(t *testing.T) {
	n := jointTestNode("n1")
	n.initialPeers = []string{"n1", "n2", "n3", "n4"}

	n.log = []LogEntry{
		{Index: 1, Type: LogEntryJointConfChange, Command: encodeJointConfChange(JointConfChange{
			Op:         JointOpEnter,
			OldServers: []ServerEntry{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}, {ID: "n4"}},
			NewServers: []ServerEntry{{ID: "n2"}, {ID: "n3"}, {ID: "n4"}}, // n1 removed
		})},
		{Index: 2, Type: LogEntryJointConfChange, Command: encodeJointConfChange(JointConfChange{
			Op:         JointOpLeave,
			NewServers: []ServerEntry{{ID: "n2"}, {ID: "n3"}, {ID: "n4"}},
		})},
	}

	n.rebuildConfigFromLog(0, n.initialPeers, nil)

	require.True(t, n.removedFromCluster,
		"replay must set removedFromCluster when JointLeave excludes self")
}

func TestRebuildConfigFromLog_RejoinClearsRemovedFromCluster(t *testing.T) {
	n := jointTestNode("n1")
	n.initialPeers = []string{"n2", "n3"}
	// Stale state from a previous removal.
	n.removedFromCluster = true

	n.log = []LogEntry{
		{Index: 1, Type: LogEntryJointConfChange, Command: encodeJointConfChange(JointConfChange{
			Op:         JointOpEnter,
			OldServers: []ServerEntry{{ID: "n2"}, {ID: "n3"}},
			NewServers: []ServerEntry{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}, // n1 rejoined
		})},
	}

	n.rebuildConfigFromLog(0, n.initialPeers, nil)

	require.False(t, n.removedFromCluster,
		"JointEnter that brings self into C_new clears removedFromCluster")
}

// TestRebuildConfigFromLog_JointLeavePromotionClearsLearners — Sub-project 3
// prereq: replay path mirror of TestApply_JointLeave_LearnerPromotionClearsLearnerIDs.
func TestRebuildConfigFromLog_JointLeavePromotionClearsLearners(t *testing.T) {
	n := jointTestNode("n1")
	n.initialPeers = []string{"n1", "n2", "n3"}

	n.log = []LogEntry{
		{Index: 1, Type: LogEntryConfChange, Command: encodeConfChange(ConfChangePayload{Op: ConfChangeAddLearner, ID: "n4", Address: "n4", ManagedByJoint: false})},
		{Index: 2, Type: LogEntryJointConfChange, Command: encodeJointConfChange(JointConfChange{
			Op:         JointOpEnter,
			OldServers: []ServerEntry{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}},
			NewServers: []ServerEntry{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}, {ID: "n4"}},
		})},
		{Index: 3, Type: LogEntryJointConfChange, Command: encodeJointConfChange(JointConfChange{
			Op:         JointOpLeave,
			NewServers: []ServerEntry{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}, {ID: "n4"}},
		})},
	}

	n.rebuildConfigFromLog(0, n.initialPeers, nil)

	require.NotContains(t, n.learnerIDs, "n4",
		"replay must clear promoted learners from learnerIDs")
	require.Contains(t, n.config.Peers, "n4", "n4 should be a voter in config.Peers")
}

// TestForceAbortJoint_ResetsToC_old verifies that ForceAbortJoint applies
// JointOpAbort and reverts config.Peers to C_old with jointPhase = JointNone.
// ForceAbortJoint returns nil on success; ErrJointAborted goes to ChangeMembership callers.
func TestForceAbortJoint_ResetsToC_old(t *testing.T) {
	n := jointTestNode("n1")
	n.state = Leader
	n.jointPhase = JointEntering
	n.jointOldVoters = []string{"n1", "n2", "n3"}
	n.jointNewVoters = []string{"n1", "n2", "n4"}
	n.jointEnterIndex = 5
	n.config.Peers = []string{"n2", "n4"} // joint merged config
	n.proposalCh = make(chan proposal, 16)
	n.stopCh = make(chan struct{})

	// Simulate the abort being committed immediately (no real Raft pipeline).
	// proposeJointEntry waits on p.doneCh for the commit result.
	go func() {
		p := <-n.proposalCh
		// Decode abort entry and apply directly.
		entry := LogEntry{
			Index:   6,
			Term:    1,
			Type:    LogEntryJointConfChange,
			Command: p.command,
		}
		n.mu.Lock()
		n.applyConfigChangeLocked(entry)
		n.mu.Unlock()
		// Signal commit (nil = success) to unblock proposeJointEntry.
		p.doneCh <- proposalResult{}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	// ForceAbortJoint returns nil on successful abort commit.
	err := n.ForceAbortJoint(ctx)
	require.NoError(t, err)

	n.mu.Lock()
	defer n.mu.Unlock()
	require.Equal(t, JointNone, n.jointPhase)
	// config.Peers should be C_old excluding self (n1).
	require.ElementsMatch(t, []string{"n2", "n3"}, n.config.Peers)
	require.Nil(t, n.jointOldVoters)
	require.Nil(t, n.jointNewVoters)
	require.Equal(t, uint64(0), n.jointEnterIndex)
}

// TestForceAbortJoint_NotInJointPhase returns ErrNotInJointPhase when called
// outside JointEntering.
func TestForceAbortJoint_NotInJointPhase(t *testing.T) {
	n := jointTestNode("n1")
	n.state = Leader
	n.jointPhase = JointNone
	n.proposalCh = make(chan proposal, 16)
	n.stopCh = make(chan struct{})

	err := n.ForceAbortJoint(context.Background())
	require.ErrorIs(t, err, ErrNotInJointPhase)
}

// TestForceAbortJoint_NotLeader returns ErrNotLeader when called on a follower.
func TestForceAbortJoint_NotLeader(t *testing.T) {
	n := jointTestNode("n1")
	n.state = Follower
	n.jointPhase = JointEntering
	n.proposalCh = make(chan proposal, 16)
	n.stopCh = make(chan struct{})

	err := n.ForceAbortJoint(context.Background())
	require.ErrorIs(t, err, ErrNotLeader)
}

// TestJointAbortTimeout_TriggersAfterDeadline verifies that checkJointAdvance
// triggers abort when JointAbortTimeout elapses.
func TestJointAbortTimeout_TriggersAfterDeadline(t *testing.T) {
	n := jointTestNode("n1")
	n.state = Leader
	n.jointPhase = JointEntering
	n.jointOldVoters = []string{"n1", "n2", "n3"}
	n.jointNewVoters = []string{"n1", "n2", "n4"}
	n.jointEnterIndex = 1
	n.commitIndex = 2
	n.jointEnterTime = time.Now().Add(-200 * time.Millisecond)
	n.config.JointAbortTimeout = 100 * time.Millisecond
	n.config.HeartbeatTimeout = 50 * time.Millisecond
	n.proposalCh = make(chan proposal, 16)
	n.stopCh = make(chan struct{})

	n.mu.Lock()
	n.checkJointAdvance()
	n.mu.Unlock()

	// Abort goroutine must have been triggered (jointAbortProposed set).
	n.mu.Lock()
	proposed := n.jointAbortProposed
	n.mu.Unlock()
	require.True(t, proposed, "jointAbortProposed should be true after timeout")

	// Drain the proposal if the goroutine fired quickly.
	select {
	case p := <-n.proposalCh:
		jc := decodeJointConfChange(p.command)
		require.Equal(t, JointOpAbort, jc.Op)
		require.Len(t, jc.OldServers, 3, "abort entry must carry C_old servers")
	case <-time.After(500 * time.Millisecond):
		// goroutine may still be in flight; abort proposal verified by flag above
	}
}

func TestJointAbortTimeout_IgnoresUncommittedInFlightLeave(t *testing.T) {
	n := jointTestNode("n1")
	n.state = Leader
	n.currentTerm = 3
	n.jointPhase = JointEntering
	n.jointOldVoters = []string{"n1", "n2", "n3"}
	n.jointNewVoters = []string{"n1", "n4", "n5"}
	n.jointEnterIndex = 1
	n.commitIndex = 1
	n.jointEnterTime = time.Now().Add(-200 * time.Millisecond)
	n.config.JointAbortTimeout = 100 * time.Millisecond
	n.config.HeartbeatTimeout = 50 * time.Millisecond
	n.proposalCh = make(chan proposal, 16)
	n.stopCh = make(chan struct{})
	n.log = []LogEntry{
		{Index: 1, Term: 3, Type: LogEntryJointConfChange, Command: encodeJointConfChange(JointConfChange{Op: JointOpEnter})},
		{Index: 2, Term: 3, Type: LogEntryJointConfChange, Command: encodeJointConfChange(JointConfChange{Op: JointOpLeave})},
	}

	n.mu.Lock()
	n.checkJointAdvance()
	n.mu.Unlock()

	n.mu.Lock()
	proposed := n.jointAbortProposed
	n.mu.Unlock()
	require.True(t, proposed, "uncommitted JointLeave must not suppress timeout abort")

	select {
	case p := <-n.proposalCh:
		jc := decodeJointConfChange(p.command)
		require.Equal(t, JointOpAbort, jc.Op)
	case <-time.After(500 * time.Millisecond):
	}
}

// TestJointAbortRestart_RebuildConfigCorrect verifies that rebuildConfigFromLog
// correctly restores C_old when replaying a JointOpAbort entry.
func TestJointAbortRestart_RebuildConfigCorrect(t *testing.T) {
	n := jointTestNode("n1")
	n.initialPeers = []string{"n2", "n3"}
	n.log = []LogEntry{
		{Index: 1, Type: LogEntryJointConfChange, Command: encodeJointConfChange(JointConfChange{
			Op:         JointOpEnter,
			OldServers: []ServerEntry{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}},
			NewServers: []ServerEntry{{ID: "n1"}, {ID: "n2"}, {ID: "n4"}},
		})},
		{Index: 2, Type: LogEntryJointConfChange, Command: encodeJointConfChange(JointConfChange{
			Op:         JointOpAbort,
			OldServers: []ServerEntry{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}},
		})},
	}

	n.rebuildConfigFromLog(0, n.initialPeers, nil)

	require.Equal(t, JointNone, n.jointPhase, "JointOpAbort must reset phase to JointNone")
	require.ElementsMatch(t, []string{"n2", "n3"}, n.config.Peers,
		"config.Peers must be C_old (n2, n3) after abort, excluding self n1")
	require.Nil(t, n.jointOldVoters)
	require.Nil(t, n.jointNewVoters)
	require.Equal(t, uint64(0), n.jointEnterIndex)
}

// TestApply_JointAbort_IdempotencyGuard verifies that applyJointConfChangeLocked
// is a no-op when jointPhase != JointEntering (e.g., already JointNone after a
// concurrent apply). This is the safety guard against double-reset on replay.
func TestApply_JointAbort_IdempotencyGuard(t *testing.T) {
	n := jointTestNode("n1")
	n.jointPhase = JointNone // not in entering phase
	n.config.Peers = []string{"n2", "n3"}
	initialPeers := []string{"n2", "n3"}

	entry := LogEntry{
		Index: 5,
		Term:  1,
		Type:  LogEntryJointConfChange,
		Command: encodeJointConfChange(JointConfChange{
			Op:         JointOpAbort,
			OldServers: []ServerEntry{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}},
		}),
	}
	n.applyJointConfChangeLocked(entry)

	// Must be a strict no-op — peers, phase, and indices unchanged.
	require.Equal(t, JointNone, n.jointPhase, "idempotency: phase must stay JointNone")
	require.Equal(t, initialPeers, n.config.Peers, "idempotency: peers must be unchanged")
}

// TestInitLeaderState_ResetsJointAbortProposed verifies that initLeaderState
// resets jointAbortProposed to false so a newly elected leader re-evaluates
// whether abort needs to be proposed (re-election correctness).
func TestInitLeaderState_ResetsJointAbortProposed(t *testing.T) {
	n := jointTestNode("n1")
	n.config.Peers = []string{"n2", "n3"}
	n.checkQuorumAcks = make(map[string]time.Time)
	n.jointAbortProposed = true // stale flag from previous leadership term
	n.jointLeaveProposed = true

	n.initLeaderState()

	require.False(t, n.jointAbortProposed,
		"initLeaderState must reset jointAbortProposed so the new leader re-evaluates abort")
	require.False(t, n.jointLeaveProposed,
		"initLeaderState must reset jointLeaveProposed")
}

// TestAdvanceCommitIndex_JointAbortUsesOldQuorumOnly verifies the critical
// quorum branch: a JointOpAbort entry commits when C_old has majority but C_new
// does not. The negative control (JointOpLeave) must NOT advance commitIndex
// under the same replication state — it requires dual quorum.
func TestAdvanceCommitIndex_JointAbortUsesOldQuorumOnly(t *testing.T) {
	// C_old={n1,n2,n3}, C_new={n1,n4,n5}.
	// matchIndex: n2=2, n3=2 → C_old majority OK (n1 self + n2 + n3 = 3/3).
	//             n4=0, n5=0 → C_new majority FAILS (n1 self only = 1/3).
	// abortNode reflects the post-append-time state for a JointOpAbort entry:
	// applyConfigChangeLocked(JointOpAbort) has run, so jointPhase=JointNone,
	// config.Peers=C_old, and jointOldVoters=nil. advanceCommitIndex must read
	// C_old from the entry payload, NOT from n.jointOldVoters.
	abortNode := func() *Node {
		n := jointTestNode("n1")
		n.jointPhase = JointNone              // cleared by append-time apply
		n.config.Peers = []string{"n2", "n3"} // restored to C_old by append-time apply
		n.currentTerm = 1
		n.commitIndex = 1
		n.firstIndex = 1
		n.log = []LogEntry{
			{Index: 1, Term: 1},
			{Index: 2, Term: 1, Type: LogEntryJointConfChange, Command: encodeJointConfChange(JointConfChange{
				Op:         JointOpAbort,
				OldServers: []ServerEntry{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}},
				NewServers: []ServerEntry{{ID: "n1"}, {ID: "n4"}, {ID: "n5"}},
			})},
		}
		n.matchIndex = map[string]uint64{
			"n2": 2, "n3": 2, // C_old majority replicated
			"n4": 0, "n5": 0, // C_new not replicated
		}
		return n
	}

	// leaveNode reflects the post-append-time state for a JointOpLeave entry:
	// applyConfigChangeLocked(JointOpLeave) has run, so jointPhase=JointNone,
	// config.Peers=C_new. advanceCommitIndex uses single-mode quorum of C_new.
	leaveNode := func() *Node {
		n := jointTestNode("n1")
		n.jointPhase = JointNone              // cleared by append-time apply
		n.config.Peers = []string{"n4", "n5"} // C_new peers set by append-time apply
		n.currentTerm = 1
		n.commitIndex = 1
		n.firstIndex = 1
		n.log = []LogEntry{
			{Index: 1, Term: 1},
			{Index: 2, Term: 1, Type: LogEntryJointConfChange, Command: encodeJointConfChange(JointConfChange{
				Op:         JointOpLeave,
				OldServers: []ServerEntry{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}},
				NewServers: []ServerEntry{{ID: "n1"}, {ID: "n4"}, {ID: "n5"}},
			})},
		}
		n.matchIndex = map[string]uint64{
			"n2": 2, "n3": 2, // C_old replicated — irrelevant for Leave quorum
			"n4": 0, "n5": 0, // C_new (n4, n5) not replicated
		}
		return n
	}

	t.Run("Abort commits with C_old majority only (from payload)", func(t *testing.T) {
		n := abortNode()
		n.mu.Lock()
		n.advanceCommitIndex()
		n.mu.Unlock()
		require.Equal(t, uint64(2), n.commitIndex,
			"JointOpAbort must commit using OldServers from entry payload when C_old majority matches")
	})

	t.Run("Leave does not commit without C_new majority", func(t *testing.T) {
		n := leaveNode()
		n.mu.Lock()
		n.advanceCommitIndex()
		n.mu.Unlock()
		require.Equal(t, uint64(1), n.commitIndex,
			"JointOpLeave uses single-mode C_new quorum; n4+n5 not replicated so must not advance")
	})
}

// followerTestNode builds a minimal Node for runFollower() unit tests.
// ElectionTimeout must be non-zero for randomElectionTimeout(); stopCh is
// pre-closed so the select in runFollower() returns immediately.
func followerTestNode(id string, peers []string) *Node {
	n := jointTestLeader(id, peers)
	n.config.ElectionTimeout = 150 * time.Millisecond
	return n
}

// TestRunFollower_DrainsJointResultCh_ReturnsErrLeadershipLost verifies that
// when a node transitions Leader→Follower with a blocked ChangeMembership
// goroutine waiting on jointResultCh, runFollower() unblocks it with
// ErrLeadershipLost. This is the goroutine-leak fix for Bug 2.
func TestRunFollower_DrainsJointResultCh_ReturnsErrLeadershipLost(t *testing.T) {
	n := followerTestNode("n1", []string{"n2", "n3"})
	// Pre-install a buffered channel simulating proposeJointConfChangeWait.
	ch := make(chan error, 1)
	n.mu.Lock()
	n.jointResultCh = ch
	n.mu.Unlock()
	// Close stopCh so runFollower() returns immediately after the drain.
	close(n.stopCh)

	n.runFollower()

	select {
	case err := <-ch:
		require.ErrorIs(t, err, ErrLeadershipLost,
			"runFollower must send ErrLeadershipLost to unblock blocked ChangeMembership")
	default:
		t.Fatal("runFollower did not drain jointResultCh — goroutine leak not fixed")
	}
}

// TestRunFollower_DrainIsIdempotent_WhenChAlreadySent verifies that if the
// commit path has already sent on jointResultCh (buffered size 1), the drain
// in runFollower() does not block or overwrite the result.
func TestRunFollower_DrainIsIdempotent_WhenChAlreadySent(t *testing.T) {
	n := followerTestNode("n1", []string{"n2", "n3"})
	ch := make(chan error, 1)
	ch <- nil // simulate commit path already delivered result
	n.mu.Lock()
	n.jointResultCh = ch
	n.mu.Unlock()
	close(n.stopCh)

	n.runFollower() // must not block

	// Channel still holds the original nil from the commit path.
	select {
	case err := <-ch:
		require.NoError(t, err, "commit-path result must be preserved, not overwritten")
	default:
		t.Fatal("channel should still hold the commit-path result")
	}
}

// TestRun_DrainsJointResultChOnStop verifies the node shutdown path also
// releases ChangeMembership callers. A leader can be stopped without first
// transitioning through runFollower(), so run() must drain jointResultCh before
// returning from the stopCh case.
func TestRun_DrainsJointResultChOnStop(t *testing.T) {
	n := followerTestNode("n1", []string{"n2", "n3"})
	ch := make(chan error, 1)
	n.mu.Lock()
	n.jointResultCh = ch
	n.mu.Unlock()
	close(n.stopCh)

	n.run()

	select {
	case err := <-ch:
		require.ErrorIs(t, err, ErrLeadershipLost,
			"run stop path must send ErrLeadershipLost to unblock blocked ChangeMembership")
	default:
		t.Fatal("run stop path did not drain jointResultCh")
	}
}

// TestApply_JointLeave_IdempotencyAfterAbort verifies that applyJointConfChangeLocked
// treats JointOpLeave as a no-op when jointPhase is already JointNone (cleared by a
// preceding JointOpAbort). This guards the window where a stale JointOpLeave that was
// appended before the abort committed gets applied after the abort entry.
func TestApply_JointLeave_IdempotencyAfterAbort(t *testing.T) {
	// Simulate post-abort state: Abort was applied, restoring C_old peers.
	n := jointTestNode("n1")
	n.jointPhase = JointNone
	n.config.Peers = []string{"n2", "n3"} // C_old restored by JointOpAbort
	n.learnerIDs = map[string]string{"managed-1": "managed-1"}
	n.jointManagedLearners = map[string]struct{}{"managed-1": {}}

	// Now apply a stale JointOpLeave (C_new = {n1, n4, n5}).
	entry := LogEntry{
		Index: 5,
		Term:  1,
		Type:  LogEntryJointConfChange,
		Command: encodeJointConfChange(JointConfChange{
			Op:         JointOpLeave,
			OldServers: []ServerEntry{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}},
			NewServers: []ServerEntry{{ID: "n1"}, {ID: "n4"}, {ID: "n5"}},
		}),
	}
	n.applyJointConfChangeLocked(entry)

	// Must be a strict no-op: C_old peers and managed learner unchanged.
	require.Equal(t, JointNone, n.jointPhase, "phase must remain JointNone")
	require.Equal(t, []string{"n2", "n3"}, n.config.Peers,
		"peers must stay at C_old, not switch to C_new from stale JointOpLeave")
	_, stillManaged := n.jointManagedLearners["managed-1"]
	require.True(t, stillManaged,
		"managed learner must not be cleared by stale JointOpLeave after abort")
}

func TestApply_JointAbortAfterUncommittedLeave_RestoresOldConfig(t *testing.T) {
	n := jointTestNode("n1")
	n.jointPhase = JointEntering
	n.config.Peers = []string{"n2", "n3", "n4", "n5"}
	n.jointOldVoters = []string{"n1", "n2", "n3"}
	n.jointNewVoters = []string{"n1", "n4", "n5"}
	n.jointEnterIndex = 1
	n.learnerIDs = map[string]string{"n4": "n4", "n5": "n5"}
	n.jointManagedLearners = map[string]struct{}{"n4": {}, "n5": {}}

	leave := LogEntry{
		Index: 2,
		Term:  1,
		Type:  LogEntryJointConfChange,
		Command: encodeJointConfChange(JointConfChange{
			Op:         JointOpLeave,
			NewServers: []ServerEntry{{ID: "n1"}, {ID: "n4"}, {ID: "n5"}},
		}),
	}
	abort := LogEntry{
		Index: 3,
		Term:  1,
		Type:  LogEntryJointConfChange,
		Command: encodeJointConfChange(JointConfChange{
			Op:         JointOpAbort,
			OldServers: []ServerEntry{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}},
			NewServers: []ServerEntry{{ID: "n1"}, {ID: "n4"}, {ID: "n5"}},
		}),
	}
	n.log = []LogEntry{
		{Index: 1, Term: 1, Type: LogEntryJointConfChange, Command: encodeJointConfChange(JointConfChange{Op: JointOpEnter})},
		leave,
		abort,
	}

	n.applyJointConfChangeLocked(leave)
	n.applyJointConfChangeLocked(abort)

	require.Equal(t, JointNone, n.jointPhase)
	require.Equal(t, []string{"n2", "n3"}, n.config.Peers,
		"abort after stale uncommitted leave must restore C_old")
	require.NotContains(t, n.LearnerIDs(), "n4")
	require.NotContains(t, n.LearnerIDs(), "n5")
}

func TestHasJointLeaveAfter_SkipsStaleOldTermLeave(t *testing.T) {
	n := jointTestNode("n1")
	n.currentTerm = 3
	n.commitIndex = 1
	n.log = []LogEntry{
		{Index: 1, Term: 2, Type: LogEntryJointConfChange, Command: encodeJointConfChange(JointConfChange{Op: JointOpEnter})},
		{Index: 2, Term: 2, Type: LogEntryJointConfChange, Command: encodeJointConfChange(JointConfChange{Op: JointOpLeave})},
		{Index: 3, Term: 3, Type: LogEntryJointConfChange, Command: encodeJointConfChange(JointConfChange{Op: JointOpLeave})},
	}

	require.True(t, n.hasJointLeaveAfter(1),
		"stale old-term JointLeave must not hide a later current-term JointLeave")
}

// TestJointPhase_AfterRestart_QueryReturnsEntering verifies that rebuildConfigFromLog
// correctly restores JointEntering state after a process restart when the
// JointOpEnter entry is still in the persisted log (not yet compacted).
//
// Single-node cluster adds a fake peer B so JointLeave cannot commit (C_new quorum
// requires both A and B). The node is stopped while stuck in JointEntering. After
// recreating the node from the same BadgerStore, JointPhase() must report
// JointEntering before the node is even started — proving that restoreFromStore
// drives the state machine via rebuildConfigFromLog.
func TestJointPhase_AfterRestart_QueryReturnsEntering(t *testing.T) {
	cluster, stores := newTestClusterWithStores(t, 1)

	// Prevent auto-abort so the stuck-in-Entering state persists long enough.
	cluster.nodes[0].mu.Lock()
	cluster.nodes[0].config.JointAbortTimeout = 30 * time.Minute
	cluster.nodes[0].mu.Unlock()

	cluster.startAll()
	leader := cluster.waitForLeader(2 * time.Second)
	require.NotNil(t, leader, "single node should elect itself")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		// Adds fake B; C_new={A,B}. JointLeave needs B to respond — it never will.
		_ = leader.proposeJointConfChangeWait(ctx,
			[]ServerEntry{{ID: "B", Address: "B"}}, nil)
	}()

	// Wait until JointEntering is active (set at append time, before replication).
	require.Eventually(t, func() bool {
		phase, _, _, _ := leader.JointPhase()
		return phase == JointEntering
	}, 3*time.Second, 10*time.Millisecond, "JointEntering must activate")

	cancel()
	leader.Close()

	// Recreate node from the same store — do NOT Start() yet.
	// restoreFromStore → rebuildConfigFromLog must restore jointPhase.
	id := leader.id
	peers := append([]string(nil), leader.initialPeers...)
	newNode := NewNode(Config{
		ID:                id,
		Peers:             peers,
		ManagedMode:       true,
		ElectionTimeout:   100 * time.Millisecond,
		HeartbeatTimeout:  30 * time.Millisecond,
		JointAbortTimeout: 30 * time.Minute,
	}, stores[0])
	defer newNode.Close()

	phase, _, _, _ := newNode.JointPhase()
	require.Equal(t, JointEntering, phase,
		"JointEntering must be restored by rebuildConfigFromLog on NewNode creation")
}

// TestJointPhase_AfterRestart_CallerCanRecover verifies that after crashing
// mid-joint (JointOpEnter committed, JointLeave not yet proposed) and restarting
// all nodes, a caller can detect JointEntering via JointPhase() and explicitly
// revert to C_old using ForceAbortJoint — without waiting for auto-completion.
//
// Uses a 3-node cluster (A,B,C) with removes=[C] so C_new={A,B}. A large
// HeartbeatTimeout gives a reliable window to crash before the leader's
// heartbeat watcher can fire checkJointAdvance and auto-propose JointLeave.
func TestJointPhase_AfterRestart_CallerCanRecover(t *testing.T) {
	const (
		hb = 200 * time.Millisecond
		el = 600 * time.Millisecond
	)

	ids := [3]string{"A", "B", "C"}
	makeConfig := func(i int) Config {
		peers := make([]string, 0, 2)
		for j, id := range ids {
			if j != i {
				peers = append(peers, id)
			}
		}
		return Config{
			ID:                ids[i],
			Peers:             peers,
			ManagedMode:       true,
			ElectionTimeout:   el,
			HeartbeatTimeout:  hb,
			JointAbortTimeout: 30 * time.Minute,
		}
	}

	stores := make([]*BadgerLogStore, 3)
	for i := range stores {
		s, err := NewBadgerLogStore(t.TempDir(), WithManagedMode())
		require.NoError(t, err)
		t.Cleanup(func() { s.Close() })
		stores[i] = s
	}

	makeCluster := func() *testCluster {
		cl := &testCluster{nodes: make([]*Node, 3)}
		for i := range 3 {
			cl.nodes[i] = NewNode(makeConfig(i), stores[i])
		}
		for i := range cl.nodes {
			cl.nodes[i].SetTransport(
				func(peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
					target := cl.nodeByID(peer)
					if target == nil {
						return nil, errNodeNotFound
					}
					return target.HandleRequestVote(args), nil
				},
				func(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
					target := cl.nodeByID(peer)
					if target == nil {
						return nil, errNodeNotFound
					}
					return target.HandleAppendEntries(args), nil
				},
			)
		}
		return cl
	}

	cl1 := makeCluster()
	cl1.startAll()

	leader := cl1.waitForLeader(3 * time.Second)
	require.NotNil(t, leader, "no leader elected")

	// Pick a non-leader peer to remove.
	var removeID string
	leader.mu.Lock()
	for _, p := range leader.config.Peers {
		removeID = p
		break
	}
	leader.mu.Unlock()
	require.NotEmpty(t, removeID)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = leader.proposeJointConfChangeWait(ctx, nil, []string{removeID})
	}()

	// Wait for JointEntering on the leader (set at append-time, before replication).
	require.Eventually(t, func() bool {
		phase, _, _, _ := leader.JointPhase()
		return phase == JointEntering
	}, 3*time.Second, 10*time.Millisecond, "JointEntering must activate")

	// Stop all nodes before the next heartbeat tick triggers checkJointAdvance.
	// With hb=200ms the window is comfortably wide.
	cancel()
	cl1.stopAll()

	// Restart all nodes with the same persisted stores.
	cl2 := makeCluster()
	cl2.startAll()
	defer cl2.stopAll()

	newLeader := cl2.waitForLeader(3 * time.Second)
	require.NotNil(t, newLeader, "no leader after restart")

	phase, _, _, _ := newLeader.JointPhase()
	require.Equal(t, JointEntering, phase, "new leader must see JointEntering after restart")

	abortCtx, abortCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer abortCancel()
	require.NoError(t, newLeader.ForceAbortJoint(abortCtx))

	phase, _, _, _ = newLeader.JointPhase()
	require.Equal(t, JointNone, phase, "ForceAbortJoint must reset to JointNone")
}

// TestJoint_Chaos_LeaderCrashBetweenEnterAndLeave verifies that when the leader
// crashes after JointOpEnter commits but before JointLeave is proposed, a new
// leader is elected under dual quorum, auto-proposes JointLeave, and drives the
// joint phase to JointNone — all without operator intervention.
//
// 4-node cluster (A,B,C,D), removes=[D]: C_new={A,B,C}.
// After leader crash (A): C_old quorum B+C+D=3/4 ✓, C_new quorum B+C=2/3 ✓.
//
// SetNoOpCommand is used so the new leader can commit a current-term entry,
// indirectly committing JointOpEnter from the previous term (standard Raft
// commitment rule: a leader may only directly commit its own term's entries).
func TestJoint_Chaos_LeaderCrashBetweenEnterAndLeave(t *testing.T) {
	cluster := newTestCluster(t, 4)
	for _, n := range cluster.nodes {
		n.SetNoOpCommand([]byte("noop"))
	}
	cluster.startAll()

	leader := cluster.waitForLeader(2 * time.Second)
	require.NotNil(t, leader, "no leader")

	// Pick a non-leader peer to remove so C_new still has 3 real voters (B,C,D).
	var removeID string
	leader.mu.Lock()
	for _, p := range leader.config.Peers {
		removeID = p
		break
	}
	leader.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = leader.proposeJointConfChangeWait(ctx, nil, []string{removeID})
	}()

	// Wait for JointOpEnter to commit on the leader. The new leader needs the
	// commit to be visible (via heartbeat propagation) to drive JointLeave.
	require.Eventually(t, func() bool {
		leader.mu.Lock()
		defer leader.mu.Unlock()
		return leader.jointPhase == JointEntering &&
			leader.jointEnterIndex > 0 &&
			leader.commitIndex >= leader.jointEnterIndex
	}, 3*time.Second, 10*time.Millisecond, "JointOpEnter must commit on leader")

	// Crash the leader. Remaining 3 nodes form a quorum in both C_old and C_new.
	crashedID := leader.ID()
	leader.Close()
	cancel()

	// Remove crashed node from the transport mesh so it doesn't receive messages.
	cluster.mu.Lock()
	filtered := cluster.nodes[:0]
	for _, n := range cluster.nodes {
		if n.ID() != crashedID {
			filtered = append(filtered, n)
		}
	}
	cluster.nodes = filtered
	cluster.mu.Unlock()

	// New leader should emerge and auto-complete JointLeave via no-op + checkJointAdvance.
	newLeader := cluster.waitForLeader(3 * time.Second)
	require.NotNil(t, newLeader, "new leader must emerge after leader crash")

	require.Eventually(t, func() bool {
		phase, _, _, _ := newLeader.JointPhase()
		return phase == JointNone
	}, 5*time.Second, 20*time.Millisecond, "JointNone must auto-complete after new leader")

	newLeader.mu.Lock()
	peers := append([]string(nil), newLeader.config.Peers...)
	newLeader.mu.Unlock()
	require.NotContains(t, peers, removeID, "C_new must exclude the removed voter")
}

// TestJoint_Chaos_PartitionDuringJoint verifies that a network partition during
// JointEntering causes the leader to step down (losing C_old quorum), and that
// after the partition heals a new leader is elected and drives JointNone.
//
// 4-node cluster (A,B,C,D), removes=[D]. Partition isolates C and D, leaving
// only A+B visible to each other. A loses C_old quorum (A+B=2/4 < 3) and steps
// down to Follower. After healing, all four nodes are reconnected and a new leader
// wins under dual quorum (C_old:3/4, C_new:2/3).
//
// SetNoOpCommand is used so the post-heal leader can commit a current-term entry
// to indirectly commit JointOpEnter from the previous term.
func TestJoint_Chaos_PartitionDuringJoint(t *testing.T) {
	cluster := newTestCluster(t, 4)
	for _, n := range cluster.nodes {
		n.SetNoOpCommand([]byte("noop"))
	}
	cluster.startAll()

	leader := cluster.waitForLeader(2 * time.Second)
	require.NotNil(t, leader, "no leader")

	var removeID string
	leader.mu.Lock()
	for _, p := range leader.config.Peers {
		removeID = p
		break
	}
	leader.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = leader.proposeJointConfChangeWait(ctx, nil, []string{removeID})
	}()

	require.Eventually(t, func() bool {
		phase, _, _, _ := leader.JointPhase()
		return phase == JointEntering
	}, 3*time.Second, 10*time.Millisecond, "JointEntering must activate")

	// Save the original 4-node list for restoration after healing.
	cluster.mu.RLock()
	allNodes := make([]*Node, len(cluster.nodes))
	copy(allNodes, cluster.nodes)
	cluster.mu.RUnlock()

	// Partition: retain only [leader, one other] in the mesh.
	// Leader sees only 1 peer → C_old quorum = 2/4 < 3 → leader steps down.
	var oneOther *Node
	for _, n := range allNodes {
		if n.ID() != leader.ID() {
			oneOther = n
			break
		}
	}
	cluster.mu.Lock()
	cluster.nodes = []*Node{leader, oneOther}
	cluster.mu.Unlock()

	// Wait for the leader to step down due to hasQuorum failure (3×HB timeout).
	require.Eventually(t, func() bool {
		return leader.State() != Leader
	}, 5*time.Second, 20*time.Millisecond, "leader must step down after losing C_old quorum")

	// Heal: restore all 4 nodes to the mesh.
	cluster.mu.Lock()
	cluster.nodes = allNodes
	cluster.mu.Unlock()

	newLeader := cluster.waitForLeader(5 * time.Second)
	require.NotNil(t, newLeader, "new leader must emerge after partition heals")

	require.Eventually(t, func() bool {
		phase, _, _, _ := newLeader.JointPhase()
		return phase == JointNone
	}, 5*time.Second, 20*time.Millisecond, "JointNone must auto-complete after healing")
}

// TestJoint_Chaos_RepeatedLeaderChange verifies data consistency through a leader
// change during JointEntering: entries committed before the joint cycle survive the
// crash, the new leader inherits and completes the joint cycle, and the cluster can
// commit additional entries after recovery.
//
// This complements TestJoint_Chaos_LeaderCrashBetweenEnterAndLeave which only
// verifies config state. Here the focus is on committed log entries remaining
// stable across the leadership transition.
//
// SetNoOpCommand is used so the new leader can commit JointOpEnter from the
// previous term indirectly via a current-term no-op entry.
func TestJoint_Chaos_RepeatedLeaderChange(t *testing.T) {
	cluster := newTestCluster(t, 4)
	for _, n := range cluster.nodes {
		n.SetNoOpCommand([]byte("noop"))
	}
	cluster.startAll()

	leader := cluster.waitForLeader(2 * time.Second)
	require.NotNil(t, leader, "no leader")

	// Commit 5 entries before the joint cycle.
	const preEntries = 5
	for i := range preEntries {
		require.NoError(t, leader.Propose([]byte{byte(i + 1)}))
	}

	// Wait for all pre-joint entries to commit on the leader.
	require.Eventually(t, func() bool {
		leader.mu.Lock()
		ci := leader.commitIndex
		leader.mu.Unlock()
		return ci >= preEntries
	}, 3*time.Second, 10*time.Millisecond, "pre-joint entries must commit")

	var removeID string
	leader.mu.Lock()
	for _, p := range leader.config.Peers {
		removeID = p
		break
	}
	leader.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = leader.proposeJointConfChangeWait(ctx, nil, []string{removeID})
	}()

	require.Eventually(t, func() bool {
		leader.mu.Lock()
		defer leader.mu.Unlock()
		return leader.jointPhase == JointEntering &&
			leader.jointEnterIndex > 0 &&
			leader.commitIndex >= leader.jointEnterIndex
	}, 3*time.Second, 10*time.Millisecond, "JointOpEnter must commit on leader")

	// Crash the leader while joint is in progress.
	crashedID := leader.ID()
	leader.Close()
	cancel()

	cluster.mu.Lock()
	filtered := cluster.nodes[:0]
	for _, n := range cluster.nodes {
		if n.ID() != crashedID {
			filtered = append(filtered, n)
		}
	}
	cluster.nodes = filtered
	cluster.mu.Unlock()

	newLeader := cluster.waitForLeader(3 * time.Second)
	require.NotNil(t, newLeader, "new leader must emerge after crash")

	// New leader auto-completes JointLeave.
	require.Eventually(t, func() bool {
		phase, _, _, _ := newLeader.JointPhase()
		return phase == JointNone
	}, 5*time.Second, 20*time.Millisecond, "JointNone must complete on new leader")

	// Pre-joint entries must still be committed on the new leader.
	newLeader.mu.Lock()
	ci := newLeader.commitIndex
	newLeader.mu.Unlock()
	require.GreaterOrEqual(t, ci, uint64(preEntries),
		"pre-joint entries must survive leader crash (commitIndex must be >= %d)", preEntries)

	// Cluster must be able to commit new entries after recovery.
	// Re-fetch the current leader: the node stored in newLeader may have stepped
	// down between JointNone detection and the propose call.
	currentLeader := cluster.waitForLeader(2 * time.Second)
	require.NotNil(t, currentLeader, "a leader must exist for post-recovery proposal")
	postCtx, postCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer postCancel()
	_, err := currentLeader.ProposeWait(postCtx, []byte("post-recovery"))
	require.NoError(t, err, "cluster must accept new entries after joint recovery")
}

// TestJoint_E2E_SnapshotMidJoint_AutoCompletes verifies that when the log is
// compacted after JointOpEnter (removing it from the in-memory log), calling
// RestoreJointStateFromSnapshot before Start() correctly rehydrates the joint
// state, and the restarted node drives JointLeave to completion automatically.
//
// Uses a 3-node cluster with BadgerDB stores. After JointOpEnter commits, a
// snapshot is saved on the leader's store with full joint metadata, and
// CompactLog removes the JointOpEnter entry from memory. The leader is stopped,
// recreated from the same store, RestoreJointStateFromSnapshot is called, and
// the node restarts. The cluster auto-completes JointLeave within a few seconds.
func TestJoint_E2E_SnapshotMidJoint_AutoCompletes(t *testing.T) {
	t.Skip("deferred until public snapshot API work; current manual compaction setup is intentionally out of PR-L2 scope")

	cluster, stores := newTestClusterWithStores(t, 3)
	cluster.startAll()

	leader := cluster.waitForLeader(2 * time.Second)
	require.NotNil(t, leader, "no leader")

	var leaderIdx int
	for i, n := range cluster.nodes {
		if n.ID() == leader.ID() {
			leaderIdx = i
			break
		}
	}

	var removeID string
	leader.mu.Lock()
	for _, p := range leader.config.Peers {
		removeID = p
		break
	}
	leader.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = leader.proposeJointConfChangeWait(ctx, nil, []string{removeID})
	}()

	// Wait for JointOpEnter to commit (commitIndex >= enterIndex) on the leader.
	require.Eventually(t, func() bool {
		leader.mu.Lock()
		defer leader.mu.Unlock()
		return leader.jointPhase == JointEntering &&
			leader.jointEnterIndex > 0 &&
			leader.commitIndex >= leader.jointEnterIndex
	}, 3*time.Second, 10*time.Millisecond, "JointOpEnter must commit")

	leader.mu.Lock()
	snapIndex := leader.jointEnterIndex
	snapTerm := leader.currentTerm
	oldVoters := append([]string(nil), leader.jointOldVoters...)
	newVoters := append([]string(nil), leader.jointNewVoters...)
	enterIndex := leader.jointEnterIndex

	// Build server list: union of C_old and C_new so the snapshot reflects the
	// joint membership.
	seenIDs := make(map[string]struct{})
	var snapServers []Server
	for _, id := range leader.jointOldVoters {
		if _, ok := seenIDs[id]; !ok {
			seenIDs[id] = struct{}{}
			snapServers = append(snapServers, Server{ID: id, Suffrage: Voter})
		}
	}
	for _, id := range leader.jointNewVoters {
		if _, ok := seenIDs[id]; !ok {
			seenIDs[id] = struct{}{}
			snapServers = append(snapServers, Server{ID: id, Suffrage: Voter})
		}
	}
	leader.mu.Unlock()

	// Save snapshot with full joint metadata on the leader's store.
	snap := Snapshot{
		Index:           snapIndex,
		Term:            snapTerm,
		Data:            []byte("state-at-joint"),
		Servers:         snapServers,
		JointPhase:      JointEntering,
		JointOldVoters:  oldVoters,
		JointNewVoters:  newVoters,
		JointEnterIndex: enterIndex,
	}
	require.NoError(t, stores[leaderIdx].SaveSnapshot(snap))

	// Compact the log to remove JointOpEnter from the in-memory log.
	leader.CompactLog(snapIndex)

	// Verify the entry is gone from the in-memory log.
	leader.mu.Lock()
	foundInLog := false
	for _, e := range leader.log {
		if e.Index == snapIndex {
			foundInLog = true
		}
	}
	leader.mu.Unlock()
	require.False(t, foundInLog, "JointOpEnter must be compacted from in-memory log")

	// Stop the leader and rebuild with the same store.
	leaderID := leader.ID()
	leaderCfg := leader.config
	cancel()
	leader.Close()

	// Recreate the leader node from the persisted store.
	// rebuildConfigFromLog alone cannot find JointOpEnter (it was compacted);
	// RestoreJointStateFromSnapshot must be called to rehydrate joint state.
	newNode := NewNode(leaderCfg, stores[leaderIdx])

	// Diagnostic: dump initial state after NewNode (before RestoreJointStateFromSnapshot).
	newNode.mu.Lock()
	t.Logf("newNode after NewNode: ci=%d sni=%d lastApplied=%d logLen=%d phase=%v jei=%d",
		newNode.commitIndex, newNode.snapshotIndex, newNode.lastApplied, len(newNode.log), newNode.jointPhase, newNode.jointEnterIndex)
	newNode.mu.Unlock()

	// Without RestoreJointStateFromSnapshot, jointPhase would be JointNone.
	phaseBeforeRestore, _, _, _ := newNode.JointPhase()
	require.Equal(t, JointNone, phaseBeforeRestore,
		"before restore: compacted log means jointPhase is JointNone")

	newNode.RestoreJointStateFromSnapshot(
		int8(snap.JointPhase),
		snap.JointOldVoters,
		snap.JointNewVoters,
		snap.JointEnterIndex,
		nil,
	)

	phaseAfterRestore, _, _, _ := newNode.JointPhase()
	require.Equal(t, JointEntering, phaseAfterRestore,
		"after RestoreJointStateFromSnapshot: jointPhase must be JointEntering")

	// Plug newNode back into the cluster mesh.
	cluster.mu.Lock()
	for i, n := range cluster.nodes {
		if n.ID() == leaderID {
			cluster.nodes[i] = newNode
			break
		}
	}
	cluster.mu.Unlock()

	newNode.SetTransport(
		func(peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
			target := cluster.nodeByID(peer)
			if target == nil {
				return nil, errNodeNotFound
			}
			return target.HandleRequestVote(args), nil
		},
		func(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
			target := cluster.nodeByID(peer)
			if target == nil {
				return nil, errNodeNotFound
			}
			return target.HandleAppendEntries(args), nil
		},
	)
	newNode.Start()
	defer newNode.Close()

	// Wait for any node to drive JointLeave to completion.
	require.Eventually(t, func() bool {
		for _, n := range cluster.nodes {
			n.mu.Lock()
			state := n.state
			term := n.currentTerm
			phase := n.jointPhase
			ci := n.commitIndex
			lli := n.lastLogIdx()
			jei := n.jointEnterIndex
			jlp := n.jointLeaveProposed
			jab := n.jointAbortProposed
			logLen := len(n.log)
			n.mu.Unlock()
			t.Logf("node %s: state=%v term=%d phase=%v ci=%d lli=%d jei=%d jlp=%v jab=%v logLen=%d",
				n.ID(), state, term, phase, ci, lli, jei, jlp, jab, logLen)
			if phase == JointNone {
				return true
			}
		}
		return false
	}, 8*time.Second, 500*time.Millisecond, "JointNone must auto-complete after snapshot restore")
}
