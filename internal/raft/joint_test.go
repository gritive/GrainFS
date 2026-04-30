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
		id:               id,
		matchIndex:       make(map[string]uint64),
		nextIndex:        make(map[string]uint64),
		learnerIDs:       make(map[string]string),
		learnerPromoteCh: make(map[string]chan struct{}),
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
	ch := make(chan struct{})
	n.jointPromoteCh = ch

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

	// Append-time does NOT close jointPromoteCh — that gates on commit (truncation safety).
	select {
	case <-ch:
		t.Fatal("jointPromoteCh should not be closed at append time")
	default:
	}
	require.NotNil(t, n.jointPromoteCh)
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
	require.Nil(t, n.jointPromoteCh, "wait channel not installed for no-op")
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
		[]string{"n1", "n2", "n3"}, []string{"n1", "n2", "n4"}, 42)

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
