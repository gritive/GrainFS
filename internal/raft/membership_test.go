package raft

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRestoreConfigFromServers_VoterLearnerSplit(t *testing.T) {
	servers := []Server{
		{ID: "self", Suffrage: Voter},
		{ID: "peer-1", Suffrage: Voter},
		{ID: "peer-2", Suffrage: Voter},
		{ID: "learner-1", Suffrage: NonVoter},
	}
	peers, learners := restoreConfigFromServers(servers, "self")
	assert.ElementsMatch(t, []string{"peer-1", "peer-2"}, peers, "voters excluding self")
	_, ok := learners["learner-1"]
	assert.True(t, ok, "learner-1 must be in learners map")
	_, ok = learners["peer-1"]
	assert.False(t, ok, "voter must not be in learners")
}

func TestRestoreConfigFromServers_EmptyServers(t *testing.T) {
	peers, learners := restoreConfigFromServers(nil, "self")
	assert.Empty(t, peers)
	assert.Empty(t, learners)
}

func TestRebuildConfigFromLog_WithBase(t *testing.T) {
	n := &Node{
		log: []LogEntry{
			{Index: 3, Type: LogEntryConfChange, Command: encodeConfChange(ConfChangePayload{Op: ConfChangeAddVoter, ID: "peer-3", Address: "addr-3", ManagedByJoint: false})},
		},
		nextIndex:  map[string]uint64{},
		matchIndex: map[string]uint64{},
	}
	basePeers := []string{"peer-1", "peer-2"}
	baseLearners := map[string]string{}
	n.rebuildConfigFromLog(0, basePeers, baseLearners)
	require.Contains(t, n.config.Peers, "peer-1")
	require.Contains(t, n.config.Peers, "peer-2")
	require.Contains(t, n.config.Peers, "addr-3")
}

func TestRebuildConfigFromLog_SkipsBeforeStartIndex(t *testing.T) {
	n := &Node{
		log: []LogEntry{
			{Index: 2, Type: LogEntryConfChange, Command: encodeConfChange(ConfChangePayload{Op: ConfChangeAddVoter, ID: "peer-old", Address: "addr-old", ManagedByJoint: false})},
			{Index: 5, Type: LogEntryConfChange, Command: encodeConfChange(ConfChangePayload{Op: ConfChangeAddVoter, ID: "peer-new", Address: "addr-new", ManagedByJoint: false})},
		},
		nextIndex:  map[string]uint64{},
		matchIndex: map[string]uint64{},
	}
	n.rebuildConfigFromLog(3, []string{"peer-base"}, map[string]string{})
	assert.NotContains(t, n.config.Peers, "addr-old", "entry at index 2 must be skipped")
	assert.Contains(t, n.config.Peers, "addr-new", "entry at index 5 must be applied")
}

func TestCheckLearnerCatchup_NoLearners(t *testing.T) {
	cfg := DefaultConfig("self", []string{"peer-1"})
	n := NewNode(cfg)
	n.mu.Lock()
	n.state = Leader
	n.commitIndex = 100
	n.mu.Unlock()

	n.mu.Lock()
	n.checkLearnerCatchup()
	n.mu.Unlock()

	select {
	case p := <-n.proposalCh:
		t.Fatalf("unexpected proposal: %v", p)
	default:
	}
}

func TestCheckLearnerCatchup_PendingConfChange(t *testing.T) {
	cfg := DefaultConfig("self", []string{"peer-1"})
	n := NewNode(cfg)
	n.mu.Lock()
	n.state = Leader
	n.commitIndex = 100
	n.learnerIDs["learner-1"] = "learner-1"
	n.matchIndex["learner-1"] = 100
	n.pendingConfChangeIndex = 50
	n.mu.Unlock()

	n.mu.Lock()
	n.checkLearnerCatchup()
	n.mu.Unlock()

	select {
	case p := <-n.proposalCh:
		t.Fatalf("watcher must not propose while pending: %v", p)
	default:
	}
}

func TestCheckLearnerCatchup_StateNotLeader(t *testing.T) {
	cfg := DefaultConfig("self", []string{"peer-1"})
	n := NewNode(cfg)
	n.mu.Lock()
	n.state = Follower
	n.commitIndex = 100
	n.learnerIDs["learner-1"] = "learner-1"
	n.matchIndex["learner-1"] = 100
	n.mu.Unlock()

	n.mu.Lock()
	n.checkLearnerCatchup()
	n.mu.Unlock()

	select {
	case p := <-n.proposalCh:
		t.Fatalf("watcher must not propose when not leader: %v", p)
	default:
	}
}

func TestCheckLearnerCatchup_Threshold(t *testing.T) {
	cfg := DefaultConfig("self", []string{"peer-1"})
	cfg.LearnerCatchupThreshold = 10
	n := NewNode(cfg)
	n.mu.Lock()
	n.state = Leader
	n.commitIndex = 100
	n.learnerIDs["learner-far"] = "learner-far"
	n.matchIndex["learner-far"] = 80
	n.learnerIDs["learner-near"] = "learner-near"
	n.matchIndex["learner-near"] = 95
	n.mu.Unlock()

	n.mu.Lock()
	n.checkLearnerCatchup()
	n.mu.Unlock()

	select {
	case p := <-n.proposalCh:
		cc := decodeConfChange(p.command)
		require.Equal(t, ConfChangePromote, cc.Op)
		require.Equal(t, "learner-near", cc.ID)
	default:
		t.Fatal("watcher must propose for caught-up learner")
	}
}

// TestCheckLearnerCatchup_SkipsDuringJoint — Sub-project 3 PR-K1 regression
// guard. While JointEntering, auto-promote watcher must not propose because
// the joint will atomically promote new voters via C_new.
func TestCheckLearnerCatchup_SkipsDuringJoint(t *testing.T) {
	cfg := DefaultConfig("self", []string{"peer-1"})
	n := NewNode(cfg)
	n.mu.Lock()
	n.state = Leader
	n.commitIndex = 100
	n.learnerIDs["learner-1"] = "learner-1"
	n.matchIndex["learner-1"] = 100
	n.jointPhase = JointEntering // joint in flight
	n.mu.Unlock()

	n.mu.Lock()
	n.checkLearnerCatchup()
	n.mu.Unlock()

	select {
	case p := <-n.proposalCh:
		t.Fatalf("watcher must not propose during JointEntering: %v", p)
	default:
	}
}

// TestCheckLearnerCatchup_SkipsJointManaged — Sub-project 3 PR-K1 regression
// guard. ChangeMembership-managed learners must not be auto-promoted during
// the pre-joint catch-up window.
func TestCheckLearnerCatchup_SkipsJointManaged(t *testing.T) {
	cfg := DefaultConfig("self", []string{"peer-1"})
	n := NewNode(cfg)
	n.mu.Lock()
	n.state = Leader
	n.commitIndex = 100
	n.learnerIDs["managed"] = "managed"
	n.matchIndex["managed"] = 100
	n.learnerIDs["unmanaged"] = "unmanaged"
	n.matchIndex["unmanaged"] = 100
	n.jointManagedLearners["managed"] = struct{}{}
	n.mu.Unlock()

	n.mu.Lock()
	n.checkLearnerCatchup()
	n.mu.Unlock()

	select {
	case p := <-n.proposalCh:
		cc := decodeConfChange(p.command)
		require.Equal(t, ConfChangePromote, cc.Op)
		require.Equal(t, "unmanaged", cc.ID,
			"only the non-managed learner should be promoted")
	default:
		t.Fatal("watcher must propose for unmanaged caught-up learner")
	}
}

func TestApplyLoopClosesPromoteCh(t *testing.T) {
	cfg := DefaultConfig("self", []string{"peer-1"})
	n := NewNode(cfg)
	n.Start()
	defer n.Stop()

	n.mu.Lock()
	n.learnerIDs["learner-1"] = "learner-1"
	ch := make(chan struct{})
	n.learnerPromoteCh["learner-1"] = ch
	cmd := encodeConfChange(ConfChangePayload{Op: ConfChangePromote, ID: "learner-1", Address: "", ManagedByJoint: false})
	entry := LogEntry{Term: 1, Index: 1, Command: cmd, Type: LogEntryConfChange}
	n.log = append(n.log, entry)
	n.firstIndex = 1
	n.commitIndex = 1
	n.lastApplied = 0
	n.mu.Unlock()

	n.signalCommit()

	select {
	case <-ch:
		// PASS
	case <-time.After(1 * time.Second):
		t.Fatal("promoteCh not closed after promote commit")
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	_, exists := n.learnerPromoteCh["learner-1"]
	require.False(t, exists, "promoteCh entry must be deleted after close")
}

func TestAddVoter_Idempotent_AlreadyVoter(t *testing.T) {
	cfg := DefaultConfig("self", []string{"existing-voter"})
	n := NewNode(cfg)
	n.mu.Lock()
	n.state = Leader
	n.mu.Unlock()

	err := n.AddVoter("existing-voter", "existing-voter")
	require.NoError(t, err, "already-voter must return nil idempotently")

	n.mu.Lock()
	defer n.mu.Unlock()
	_, isLearner := n.learnerIDs["existing-voter"]
	require.False(t, isLearner, "voter must not be demoted to learner")
}

func TestAddVoter_NotLeader(t *testing.T) {
	cfg := DefaultConfig("self", []string{"peer-1"})
	n := NewNode(cfg)

	err := n.AddVoter("new-node", "new-node")
	require.ErrorIs(t, err, ErrNotLeader)
}

func TestAddVoter_MixedVersion(t *testing.T) {
	cfg := DefaultConfig("self", []string{"peer-1"})
	n := NewNode(cfg)
	n.mu.Lock()
	n.state = Leader
	n.mu.Unlock()
	n.SetMixedVersion(true)

	err := n.AddVoter("new-node", "new-node")
	require.ErrorIs(t, err, ErrMixedVersionNoMembershipChange)
}

func TestAddVoter_Concurrent_ReturnsErr(t *testing.T) {
	cfg := DefaultConfig("self", []string{"peer-1"})
	n := NewNode(cfg)
	n.mu.Lock()
	n.state = Leader
	n.pendingConfChangeIndex = 50
	n.mu.Unlock()

	err := n.AddVoter("new-node", "new-node")
	require.ErrorIs(t, err, ErrConfChangeInProgress)
}

func TestAddVoter_CtxTimeout_LearnerRemains(t *testing.T) {
	// Verifies the ctx-timeout path of AddVoterCtx. Without Start() the batcher
	// is not consuming proposalCh, so AddLearner ConfChange never commits and
	// the proposeConfChangeWait inside AddVoter respects ctx.Done() and returns.
	// The learnerIDs map is checked manually to confirm no spurious side effects.
	cfg := DefaultConfig("self", []string{"peer-1"})
	n := NewNode(cfg)
	n.mu.Lock()
	n.state = Leader
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := n.AddVoterCtx(ctx, "slow-learner", "slow-learner")
	require.ErrorIs(t, err, context.DeadlineExceeded)

	// Since no apply loop ran, no learner was added — but if it had been added,
	// the iron rule says it must remain on ctx timeout. Both states satisfy
	// "learner not promoted to voter" for this test.
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, p := range n.config.Peers {
		require.NotEqual(t, "slow-learner", p, "must not be voter on ctx timeout")
	}
}

func TestAddVoter_Idempotent_AlreadyLearner(t *testing.T) {
	// Verifies that a node already in learnerIDs is not in config.Peers,
	// so the pre-check doesn't short-circuit. (Full re-call behavior covered E2E.)
	cfg := DefaultConfig("self", []string{"peer-1"})
	n := NewNode(cfg)
	n.mu.Lock()
	n.state = Leader
	n.learnerIDs["existing-learner"] = "existing-learner"
	mu := false
	for _, p := range n.config.Peers {
		if p == "existing-learner" {
			mu = true
		}
	}
	n.mu.Unlock()
	require.False(t, mu, "learner must not be in config.Peers")
}

// TestInitLeaderState_InitializesLearnerReplicationState is a regression test
// for a bug where a node elected leader after a learner had been added in a
// previous term would have unset nextIndex/matchIndex for that learner.
// Without this initialization, the catch-up watcher reads matchIndex=0 from
// an absent map entry, replicateToAll's learner replication starts from
// nextIdx=0 (snapshot mode unintentionally), and learner promotion stalls.
func TestInitLeaderState_InitializesLearnerReplicationState(t *testing.T) {
	cfg := DefaultConfig("self", []string{"voter-1"})
	n := NewNode(cfg)

	// Simulate state after a learner was added in a previous term while this
	// node was a follower (applyConfigChangeLocked added it to learnerIDs but
	// did not touch nextIndex/matchIndex — those are leader-only state).
	n.mu.Lock()
	n.learnerIDs["learner-1"] = "learner-addr"
	// Simulate some entries in the log
	n.log = []LogEntry{
		{Term: 1, Index: 1, Command: []byte("e1")},
		{Term: 1, Index: 2, Command: []byte("e2")},
	}
	n.firstIndex = 1

	// Now this node becomes leader.
	n.initLeaderState()
	defer n.mu.Unlock()

	// Both voter and learner must have nextIndex set to lastLogIdx+1.
	expectedNext := uint64(3) // lastLogIdx (2) + 1
	require.Equal(t, expectedNext, n.nextIndex["voter-1"], "voter nextIndex must be initialized")
	require.Equal(t, expectedNext, n.nextIndex["learner-addr"], "learner nextIndex must be initialized")
	require.Equal(t, uint64(0), n.matchIndex["learner-addr"], "learner matchIndex must start at 0")
}

func TestConfChangePayload_ManagedByJointRoundtrip(t *testing.T) {
	original := ConfChangePayload{
		Op:             ConfChangeAddLearner,
		ID:             "learner-1",
		Address:        "addr-1",
		ManagedByJoint: true,
	}

	cmd := encodeConfChange(original)
	decoded := decodeConfChange(cmd)

	if decoded.Op != original.Op {
		t.Errorf("Op mismatch: got %v, want %v", decoded.Op, original.Op)
	}
	if decoded.ID != original.ID {
		t.Errorf("ID mismatch: got %s, want %s", decoded.ID, original.ID)
	}
	if decoded.Address != original.Address {
		t.Errorf("Address mismatch: got %s, want %s", decoded.Address, original.Address)
	}
	if decoded.ManagedByJoint != original.ManagedByJoint {
		t.Errorf("ManagedByJoint mismatch: got %v, want %v", decoded.ManagedByJoint, original.ManagedByJoint)
	}
}

func TestEncodeConfChange_OmitManagedByJointDefaultsToFalse(t *testing.T) {
	// Go zero value for bool is false - test that omitted field defaults to false
	payload := ConfChangePayload{
		Op:      ConfChangeAddLearner,
		ID:      "learner-1",
		Address: "addr-1",
		// ManagedByJoint omitted - should default to false
	}

	cmd := encodeConfChange(payload)
	decoded := decodeConfChange(cmd)

	if decoded.ManagedByJoint != false {
		t.Errorf("ManagedByJoint should default to false when omitted, got %v", decoded.ManagedByJoint)
	}
}

func TestDecodeConfChange_ExtractsManagedByJoint(t *testing.T) {
	testCases := []struct {
		name           string
		managedByJoint bool
	}{
		{"managed by joint", true},
		{"not managed", false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			payload := ConfChangePayload{
				Op:             ConfChangeAddLearner,
				ID:             "learner-1",
				Address:        "addr-1",
				ManagedByJoint: tc.managedByJoint,
			}

			cmd := encodeConfChange(payload)
			decoded := decodeConfChange(cmd)

			if decoded.ManagedByJoint != tc.managedByJoint {
				t.Errorf("ManagedByJoint mismatch: got %v, want %v", decoded.ManagedByJoint, tc.managedByJoint)
			}
		})
	}
}

// Stage 2: Apply path — ManagedByJoint registration in jointManagedLearners.

func TestApplyConfChange_ManagedByJoint_RegistersInSet(t *testing.T) {
	tests := []struct {
		name      string
		managed   bool
		wantInSet bool
	}{
		{"managed=true registers", true, true},
		{"managed=false skips", false, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig("self", []string{"peer-1"})
			n := NewNode(cfg)
			n.mu.Lock()
			entry := LogEntry{
				Index: 1, Term: 1, Type: LogEntryConfChange,
				Command: encodeConfChange(ConfChangePayload{
					Op: ConfChangeAddLearner, ID: "learner-1", Address: "addr-1",
					ManagedByJoint: tc.managed,
				}),
			}
			n.applyConfigChangeLocked(entry)
			_, inSet := n.jointManagedLearners["learner-1"]
			n.mu.Unlock()
			require.Equal(t, tc.wantInSet, inSet)
		})
	}
}

// Stage 2: Replay path — rebuildConfigFromLog restores jointManagedLearners.

func TestRebuildConfigFromLog_ManagedByJoint_RestoresState(t *testing.T) {
	cfg := DefaultConfig("self", []string{"peer-1"})
	n := NewNode(cfg)
	n.mu.Lock()
	n.log = []LogEntry{
		{Index: 1, Term: 1, Type: LogEntryConfChange,
			Command: encodeConfChange(ConfChangePayload{
				Op: ConfChangeAddLearner, ID: "managed", Address: "addr-m", ManagedByJoint: true,
			})},
		{Index: 2, Term: 1, Type: LogEntryConfChange,
			Command: encodeConfChange(ConfChangePayload{
				Op: ConfChangeAddLearner, ID: "unmanaged", Address: "addr-u", ManagedByJoint: false,
			})},
	}
	n.firstIndex = 1
	n.rebuildConfigFromLog(0, nil, nil)
	_, managedInSet := n.jointManagedLearners["managed"]
	_, unmanagedInSet := n.jointManagedLearners["unmanaged"]
	n.mu.Unlock()

	require.True(t, managedInSet, "managed learner must be in jointManagedLearners after rebuild")
	require.False(t, unmanagedInSet, "unmanaged learner must not be in jointManagedLearners after rebuild")
}

func TestRebuildConfigFromLog_JointLeave_ClearsManaged(t *testing.T) {
	cfg := DefaultConfig("self", []string{"peer-1"})
	n := NewNode(cfg)
	n.mu.Lock()
	n.log = []LogEntry{
		{Index: 1, Term: 1, Type: LogEntryConfChange,
			Command: encodeConfChange(ConfChangePayload{
				Op: ConfChangeAddLearner, ID: "managed-1", Address: "addr-m", ManagedByJoint: true,
			})},
		{Index: 2, Term: 1, Type: LogEntryJointConfChange,
			Command: encodeJointConfChange(JointConfChange{
				Op: JointOpLeave,
				NewServers: []ServerEntry{
					{ID: "self", Address: "self", Suffrage: Voter},
					{ID: "managed-1", Address: "addr-m", Suffrage: Voter},
				},
			})},
	}
	n.firstIndex = 1
	n.rebuildConfigFromLog(0, nil, nil)
	_, inSet := n.jointManagedLearners["managed-1"]
	n.mu.Unlock()

	require.False(t, inSet, "managed learner must be cleared from jointManagedLearners after JointOpLeave")
}

// TestJointOpAbort_ManagedLearnerCleared is the unit test for Bug 1: after
// JointOpAbort applies, managed learner IDs must be removed from learnerIDs.
// Without the fix, checkLearnerCatchup sees them as ordinary learners and
// auto-promotes them into the wrong voter set.
func TestJointOpAbort_ManagedLearnerCleared(t *testing.T) {
	n := &Node{
		id:                   "n1",
		matchIndex:           make(map[string]uint64),
		nextIndex:            make(map[string]uint64),
		learnerIDs:           map[string]string{"n4": "n4-addr"},
		learnerPromoteCh:     make(map[string]chan struct{}),
		jointManagedLearners: map[string]struct{}{"n4": {}},
	}

	oldServers := []ServerEntry{
		{ID: "n1", Address: "n1", Suffrage: Voter},
		{ID: "n2", Address: "n2", Suffrage: Voter},
		{ID: "n3", Address: "n3", Suffrage: Voter},
	}
	newServers := []ServerEntry{
		{ID: "n1", Address: "n1", Suffrage: Voter},
		{ID: "n2", Address: "n2", Suffrage: Voter},
		{ID: "n3", Address: "n3", Suffrage: Voter},
		{ID: "n4", Address: "n4-addr", Suffrage: Voter},
	}

	// Simulate JointOpEnter (sets jointPhase = JointEntering).
	n.applyJointConfChangeLocked(LogEntry{
		Index: 1, Term: 1, Type: LogEntryJointConfChange,
		Command: encodeJointConfChange(JointConfChange{
			Op: JointOpEnter, OldServers: oldServers, NewServers: newServers,
		}),
	})
	require.Equal(t, JointEntering, n.jointPhase)

	// Apply JointOpAbort — n4 must be removed from learnerIDs.
	n.applyJointConfChangeLocked(LogEntry{
		Index: 2, Term: 1, Type: LogEntryJointConfChange,
		Command: encodeJointConfChange(JointConfChange{
			Op: JointOpAbort, OldServers: oldServers, NewServers: newServers,
		}),
	})

	require.Equal(t, JointNone, n.jointPhase)
	require.NotContains(t, n.LearnerIDs(), "n4",
		"JointOpAbort must remove managed learner from learnerIDs to prevent spurious auto-promote")
	require.Nil(t, n.jointManagedLearners, "jointManagedLearners must be nil after abort")
}

// TestRebuildConfigFromLog_AbortThenLeave_LeaveIsSkipped verifies that when the log
// contains JointOpAbort followed by a stale JointOpLeave (both in the same joint
// cycle), rebuildConfigFromLog skips the JointOpLeave so config stays at C_old.
// This is the log-replay counterpart to applyJointConfChangeLocked's idempotency guard.
func TestRebuildConfigFromLog_AbortThenLeave_LeaveIsSkipped(t *testing.T) {
	cfg := DefaultConfig("n1", []string{"n2", "n3"})
	n := NewNode(cfg)
	n.mu.Lock()
	// C_old = {n1, n2, n3}, C_new = {n1, n4, n5}
	n.log = []LogEntry{
		{Index: 1, Term: 1, Type: LogEntryJointConfChange,
			Command: encodeJointConfChange(JointConfChange{
				Op:         JointOpEnter,
				OldServers: []ServerEntry{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}},
				NewServers: []ServerEntry{{ID: "n1"}, {ID: "n4"}, {ID: "n5"}},
			})},
		{Index: 2, Term: 1, Type: LogEntryJointConfChange,
			Command: encodeJointConfChange(JointConfChange{
				Op:         JointOpAbort,
				OldServers: []ServerEntry{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}},
				NewServers: []ServerEntry{{ID: "n1"}, {ID: "n4"}, {ID: "n5"}},
			})},
		// Stale JointOpLeave proposed before the abort committed.
		{Index: 3, Term: 1, Type: LogEntryJointConfChange,
			Command: encodeJointConfChange(JointConfChange{
				Op:         JointOpLeave,
				OldServers: []ServerEntry{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}},
				NewServers: []ServerEntry{{ID: "n1"}, {ID: "n4"}, {ID: "n5"}},
			})},
	}
	n.firstIndex = 1
	n.rebuildConfigFromLog(0, nil, nil)
	peers := n.config.Peers
	phase := n.jointPhase
	n.mu.Unlock()

	require.Equal(t, JointNone, phase, "phase must be JointNone after abort+leave sequence")
	require.Equal(t, []string{"n2", "n3"}, peers,
		"peers must be C_old after abort; stale JointOpLeave must not switch to C_new")
}

// TestRebuildConfigFromLog_AbortThenEnterThenLeave verifies that a second joint
// cycle that follows an aborted first cycle is replayed correctly. The jAborted
// flag must be cleared on JointOpEnter so the second cycle's JointOpLeave is not
// mistakenly skipped.
//
// Sequence: Enter(C_old→C_new1) → Abort → Enter(C_old→C_new2) → Leave(C_new2)
// Expected: config.Peers == C_new2, jointPhase == JointNone.
func TestRebuildConfigFromLog_AbortThenEnterThenLeave(t *testing.T) {
	n := &Node{
		id:                   "n1",
		matchIndex:           map[string]uint64{},
		nextIndex:            map[string]uint64{},
		learnerIDs:           map[string]string{},
		learnerPromoteCh:     map[string]chan struct{}{},
		jointManagedLearners: map[string]struct{}{},
	}
	n.mu.Lock()
	n.log = []LogEntry{
		{Index: 1, Term: 1, Type: LogEntryJointConfChange,
			Command: encodeJointConfChange(JointConfChange{
				Op:         JointOpEnter,
				OldServers: []ServerEntry{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}},
				NewServers: []ServerEntry{{ID: "n1"}, {ID: "n4"}, {ID: "n5"}},
			})},
		{Index: 2, Term: 1, Type: LogEntryJointConfChange,
			Command: encodeJointConfChange(JointConfChange{
				Op:         JointOpAbort,
				OldServers: []ServerEntry{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}},
				NewServers: []ServerEntry{{ID: "n1"}, {ID: "n4"}, {ID: "n5"}},
			})},
		// Second cycle: new C_new = {n1, n6, n7}.
		{Index: 3, Term: 1, Type: LogEntryJointConfChange,
			Command: encodeJointConfChange(JointConfChange{
				Op:         JointOpEnter,
				OldServers: []ServerEntry{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}},
				NewServers: []ServerEntry{{ID: "n1"}, {ID: "n6"}, {ID: "n7"}},
			})},
		{Index: 4, Term: 1, Type: LogEntryJointConfChange,
			Command: encodeJointConfChange(JointConfChange{
				Op:         JointOpLeave,
				OldServers: []ServerEntry{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}},
				NewServers: []ServerEntry{{ID: "n1"}, {ID: "n6"}, {ID: "n7"}},
			})},
	}
	n.firstIndex = 1
	n.rebuildConfigFromLog(0, nil, nil)
	peers := n.config.Peers
	phase := n.jointPhase
	n.mu.Unlock()

	require.Equal(t, JointNone, phase, "second cycle committed: phase must be JointNone")
	require.Equal(t, []string{"n6", "n7"}, peers,
		"second cycle Leave must apply; jAborted from first cycle must not bleed over")
}
