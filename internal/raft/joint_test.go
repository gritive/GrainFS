package raft

import (
	"testing"

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
