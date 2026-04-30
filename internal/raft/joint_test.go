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
