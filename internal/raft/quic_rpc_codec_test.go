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
