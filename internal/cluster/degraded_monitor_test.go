package cluster

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/alerts"
	"github.com/gritive/GrainFS/internal/raft"
)

// fakeRaftState is a stub RaftStateProvider for unit testing.
type fakeRaftState struct {
	state    raft.NodeState
	leaderID string
}

func (f *fakeRaftState) State() raft.NodeState { return f.state }
func (f *fakeRaftState) LeaderID() string      { return f.leaderID }

// fakeSender records every alert it is asked to send and can simulate errors.
type fakeSender struct {
	sent []alerts.Alert
	err  error
}

func (f *fakeSender) Send(a alerts.Alert) error {
	f.sent = append(f.sent, a)
	return f.err
}

func newQuorumMonitor(t *testing.T, st *fakeRaftState, sender *fakeSender) *DegradedMonitor {
	t.Helper()
	m := &DegradedMonitor{
		quorumAlertThreshold: 2,
	}
	m.WithQuorumCheck(st, sender)
	return m
}

func TestCheckQuorum_NoQuorumCheck_Wired(t *testing.T) {
	// When WithQuorumCheck is not called, checkQuorum is a no-op.
	m := &DegradedMonitor{quorumAlertThreshold: 2}
	m.checkQuorum() // must not panic
}

func TestCheckQuorum_LeaderPresent_NoAlert(t *testing.T) {
	st := &fakeRaftState{state: raft.Follower, leaderID: "node-1"}
	sender := &fakeSender{}
	m := newQuorumMonitor(t, st, sender)

	for i := 0; i < 5; i++ {
		m.checkQuorum()
	}
	assert.Empty(t, sender.sent, "no alert should fire when leader is known")
	assert.Equal(t, 0, m.quorumLostTicks)
}

func TestCheckQuorum_LeaderState_NoAlert(t *testing.T) {
	// This node IS the leader → cannot be quorum lost from its perspective.
	st := &fakeRaftState{state: raft.Leader, leaderID: "self"}
	sender := &fakeSender{}
	m := newQuorumMonitor(t, st, sender)

	for i := 0; i < 5; i++ {
		m.checkQuorum()
	}
	assert.Empty(t, sender.sent)
}

func TestCheckQuorum_FiresAfterThreshold(t *testing.T) {
	st := &fakeRaftState{state: raft.Follower, leaderID: ""}
	sender := &fakeSender{}
	m := newQuorumMonitor(t, st, sender)

	m.checkQuorum() // tick 1 — counter 1, no alert yet
	assert.Empty(t, sender.sent, "first tick must not alert")
	assert.Equal(t, 1, m.quorumLostTicks)

	m.checkQuorum() // tick 2 — counter 2 == threshold, alert fires
	require.Len(t, sender.sent, 1, "alert must fire on threshold tick")
	assert.Equal(t, "raft_quorum_lost", sender.sent[0].Type)
	assert.Equal(t, alerts.SeverityCritical, sender.sent[0].Severity)
	assert.Equal(t, "cluster", sender.sent[0].Resource)
	assert.Contains(t, sender.sent[0].Message, "quorum likely lost")

	// Subsequent ticks must not re-alert (would spam on every check).
	m.checkQuorum()
	m.checkQuorum()
	assert.Len(t, sender.sent, 1, "alert must not repeat")
}

func TestCheckQuorum_LeaderRecovers_CounterResets(t *testing.T) {
	st := &fakeRaftState{state: raft.Follower, leaderID: ""}
	sender := &fakeSender{}
	m := newQuorumMonitor(t, st, sender)

	m.checkQuorum() // 1
	assert.Equal(t, 1, m.quorumLostTicks)

	// Leader appears.
	st.leaderID = "node-1"
	m.checkQuorum()
	assert.Equal(t, 0, m.quorumLostTicks, "counter must reset when leader returns")
	assert.Empty(t, sender.sent)

	// Leader gone again — must restart counting from 0, not retain prior count.
	st.leaderID = ""
	m.checkQuorum() // 1
	assert.Empty(t, sender.sent, "must not alert on tick 1 after recovery")
	m.checkQuorum() // 2 → alert
	assert.Len(t, sender.sent, 1)
}

func TestCheckQuorum_SendError_DoesNotPanic(t *testing.T) {
	st := &fakeRaftState{state: raft.Follower, leaderID: ""}
	sender := &fakeSender{err: errors.New("webhook unreachable")}
	m := newQuorumMonitor(t, st, sender)

	m.checkQuorum()
	m.checkQuorum() // alert fires; sender returns error
	assert.Len(t, sender.sent, 1, "alert was attempted")
	// No panic, monitor continues.
}
