package raft

import (
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
			{Index: 3, Type: LogEntryConfChange, Command: encodeConfChange(ConfChangeAddVoter, "peer-3", "addr-3")},
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
			{Index: 2, Type: LogEntryConfChange, Command: encodeConfChange(ConfChangeAddVoter, "peer-old", "addr-old")},
			{Index: 5, Type: LogEntryConfChange, Command: encodeConfChange(ConfChangeAddVoter, "peer-new", "addr-new")},
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

func TestApplyLoopClosesPromoteCh(t *testing.T) {
	cfg := DefaultConfig("self", []string{"peer-1"})
	n := NewNode(cfg)
	n.Start()
	defer n.Stop()

	n.mu.Lock()
	n.learnerIDs["learner-1"] = "learner-1"
	ch := make(chan struct{})
	n.learnerPromoteCh["learner-1"] = ch
	cmd := encodeConfChange(ConfChangePromote, "learner-1", "")
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
