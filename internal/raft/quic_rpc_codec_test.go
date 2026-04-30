package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecodeRequestVote(t *testing.T) {
	args := &RequestVoteArgs{
		Term:           5,
		CandidateID:    "node-1",
		LastLogIndex:   42,
		LastLogTerm:    3,
		PreVote:        true,
		LeaderTransfer: true,
	}

	data, err := encodeRPC(rpcTypeRequestVote, args)
	require.NoError(t, err, "encodeRPC should succeed")
	require.NotEmpty(t, data, "encoded data should not be empty")

	rpcType, payload, err := decodeRPC(data)
	require.NoError(t, err, "decodeRPC should succeed")
	assert.Equal(t, rpcTypeRequestVote, rpcType)

	decoded, err := decodeRequestVoteArgs(payload)
	require.NoError(t, err)
	assert.Equal(t, args.Term, decoded.Term)
	assert.Equal(t, args.CandidateID, decoded.CandidateID)
	assert.Equal(t, args.LastLogIndex, decoded.LastLogIndex)
	assert.Equal(t, args.LastLogTerm, decoded.LastLogTerm)
	assert.Equal(t, args.PreVote, decoded.PreVote, "PreVote must round-trip; lost flag silently regresses pre-vote protection")
	assert.Equal(t, args.LeaderTransfer, decoded.LeaderTransfer, "LeaderTransfer must round-trip; lost flag breaks leader transfer election")
}

func TestEncodeDecodeRequestVoteReply(t *testing.T) {
	reply := &RequestVoteReply{
		Term:        5,
		VoteGranted: true,
	}

	data, err := encodeRPC(rpcTypeRequestVoteReply, reply)
	require.NoError(t, err)

	rpcType, payload, err := decodeRPC(data)
	require.NoError(t, err)
	assert.Equal(t, rpcTypeRequestVoteReply, rpcType)

	decoded, err := decodeRequestVoteReply(payload)
	require.NoError(t, err)
	assert.Equal(t, reply.Term, decoded.Term)
	assert.Equal(t, reply.VoteGranted, decoded.VoteGranted)
}

func TestEncodeDecodeAppendEntries(t *testing.T) {
	args := &AppendEntriesArgs{
		Term:         10,
		LeaderID:     "leader-1",
		PrevLogIndex: 99,
		PrevLogTerm:  9,
		LeaderCommit: 95,
		Entries: []LogEntry{
			{Term: 10, Index: 100, Command: []byte("cmd-1")},
			{Term: 10, Index: 101, Command: []byte("cmd-2")},
		},
	}

	data, err := encodeRPC(rpcTypeAppendEntries, args)
	require.NoError(t, err)

	rpcType, payload, err := decodeRPC(data)
	require.NoError(t, err)
	assert.Equal(t, rpcTypeAppendEntries, rpcType)

	decoded, err := decodeAppendEntriesArgs(payload)
	require.NoError(t, err)
	assert.Equal(t, args.Term, decoded.Term)
	assert.Equal(t, args.LeaderID, decoded.LeaderID)
	assert.Equal(t, args.PrevLogIndex, decoded.PrevLogIndex)
	assert.Equal(t, args.PrevLogTerm, decoded.PrevLogTerm)
	assert.Equal(t, args.LeaderCommit, decoded.LeaderCommit)
	require.Len(t, decoded.Entries, 2)
	assert.Equal(t, []byte("cmd-1"), decoded.Entries[0].Command)
	assert.Equal(t, uint64(101), decoded.Entries[1].Index)
}

func TestEncodeDecodeAppendEntriesReply(t *testing.T) {
	reply := &AppendEntriesReply{
		Term:    10,
		Success: true,
	}

	data, err := encodeRPC(rpcTypeAppendEntriesReply, reply)
	require.NoError(t, err)

	rpcType, payload, err := decodeRPC(data)
	require.NoError(t, err)
	assert.Equal(t, rpcTypeAppendEntriesReply, rpcType)

	decoded, err := decodeAppendEntriesReply(payload)
	require.NoError(t, err)
	assert.Equal(t, reply.Term, decoded.Term)
	assert.Equal(t, reply.Success, decoded.Success)
}

func TestEncodeRPCRejectsUnknownType(t *testing.T) {
	_, err := encodeRPC("UnknownRPC", &RequestVoteArgs{})
	assert.Error(t, err, "unknown RPC type should return error")
}

func TestDecodeRPCInvalidData(t *testing.T) {
	_, _, err := decodeRPC([]byte("garbage"))
	assert.Error(t, err, "invalid data should fail")
}

func TestRPCDataDoesNotContainJSON(t *testing.T) {
	args := &RequestVoteArgs{
		Term:        1,
		CandidateID: "node-1",
	}

	data, err := encodeRPC(rpcTypeRequestVote, args)
	require.NoError(t, err)

	// Protobuf output should NOT contain JSON-like patterns
	str := string(data)
	assert.NotContains(t, str, `"term"`, "should not contain JSON field names")
	assert.NotContains(t, str, `"candidate_id"`, "should not contain JSON field names")
	assert.NotContains(t, str, `{`, "should not look like JSON")
}

func TestEncodeRPC_AllocsBounded(t *testing.T) {
	args := &RequestVoteArgs{Term: 1, CandidateID: "node-1", LastLogIndex: 10, LastLogTerm: 1}
	// warmup: populate pool
	_, _ = encodeRPC(rpcTypeRequestVote, args)

	allocs := testing.AllocsPerRun(100, func() {
		_, _ = encodeRPC(rpcTypeRequestVote, args)
	})
	assert.LessOrEqual(t, allocs, float64(4), "encodeRPC allocs should be ≤4 with pool reuse")
}

// TestRoundTrip_AllWireStructs uses reflect.DeepEqual on fully populated
// instances of every wire struct so that a future field added to the Go
// struct must also be wired into the FBS schema and codec, otherwise
// this test fails with a clear diff.
func TestRoundTrip_AllWireStructs(t *testing.T) {
	t.Run("RequestVoteArgs", func(t *testing.T) {
		orig := &RequestVoteArgs{
			Term: 7, CandidateID: "n1",
			LastLogIndex: 42, LastLogTerm: 6,
			PreVote: true, LeaderTransfer: true,
		}
		data, err := encodeRPCPayload(rpcTypeRequestVote, orig)
		require.NoError(t, err)
		got, err := decodeRequestVoteArgs(data)
		require.NoError(t, err)
		require.Equal(t, orig, got)
	})

	t.Run("RequestVoteReply", func(t *testing.T) {
		orig := &RequestVoteReply{Term: 9, VoteGranted: true}
		data, err := encodeRPCPayload(rpcTypeRequestVoteReply, orig)
		require.NoError(t, err)
		got, err := decodeRequestVoteReply(data)
		require.NoError(t, err)
		require.Equal(t, orig, got)
	})

	t.Run("AppendEntriesArgs", func(t *testing.T) {
		orig := &AppendEntriesArgs{
			Term: 11, LeaderID: "leader-7",
			PrevLogIndex: 99, PrevLogTerm: 10,
			Entries: []LogEntry{
				{Term: 11, Index: 100, Command: []byte("cmd-A"), Type: LogEntryCommand},
				{Term: 11, Index: 101, Command: []byte("cmd-B"), Type: LogEntryConfChange},
			},
			LeaderCommit: 99,
		}
		data, err := encodeRPCPayload(rpcTypeAppendEntries, orig)
		require.NoError(t, err)
		got, err := decodeAppendEntriesArgs(data)
		require.NoError(t, err)
		require.Equal(t, orig, got)
	})

	t.Run("AppendEntriesReply", func(t *testing.T) {
		orig := &AppendEntriesReply{
			Term: 13, Success: true,
			ConflictTerm: 12, ConflictIndex: 88,
		}
		data, err := encodeRPCPayload(rpcTypeAppendEntriesReply, orig)
		require.NoError(t, err)
		got, err := decodeAppendEntriesReply(data)
		require.NoError(t, err)
		require.Equal(t, orig, got)
	})

	t.Run("InstallSnapshotArgs", func(t *testing.T) {
		orig := &InstallSnapshotArgs{
			Term: 14, LeaderID: "leader-9",
			LastIncludedIndex: 1234, LastIncludedTerm: 13,
			Data: []byte("snapshot-bytes"),
			Servers: []Server{
				{ID: "n1", Suffrage: Voter},
				{ID: "n2", Suffrage: NonVoter},
			},
		}
		data, err := encodeRPCPayload(rpcTypeInstallSnapshot, orig)
		require.NoError(t, err)
		got, err := decodeInstallSnapshotArgs(data)
		require.NoError(t, err)
		require.Equal(t, orig, got)
	})

	t.Run("InstallSnapshotReply", func(t *testing.T) {
		orig := &InstallSnapshotReply{Term: 15}
		data, err := encodeRPCPayload(rpcTypeInstallSnapshotReply, orig)
		require.NoError(t, err)
		got, err := decodeInstallSnapshotReply(data)
		require.NoError(t, err)
		require.Equal(t, orig, got)
	})
}
