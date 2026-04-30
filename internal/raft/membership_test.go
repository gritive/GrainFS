package raft

import (
	"testing"

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
