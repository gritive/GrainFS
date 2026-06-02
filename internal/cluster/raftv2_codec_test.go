package cluster

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
)

// TestV2EncodeRPC_ByteIdenticalToV1 verifies the v2 cluster-side wire codec
// produces byte-identical output to v1's internal/raft.encodeRPC. Golden hex
// was captured from v1 via a one-shot test (internal/raft/quic_rpc_wiredump_test.go)
// at PR 27 implementation time and deleted after capture. v1 is frozen for M5;
// these constants will become canonical when v1 is removed in PR 30.
//
// If this test ever fails after touching internal/raft/quic_rpc_codec.go OR
// internal/cluster/raftv2_quic_codec.go, you have a wire-format divergence
// between v1 and v2 — mixed-mode clusters will misbehave. Either:
//
//	(a) update both codecs together and regenerate the golden hex via the
//	    wiredump helper, or
//	(b) revert.
func TestV2EncodeRPC_ByteIdenticalToV1(t *testing.T) {
	cases := []struct {
		name    string
		rpcType string
		msg     any
		golden  string
	}{
		{
			name:    "RequestVote",
			rpcType: v2RPCTypeRequestVote,
			msg: &raft.RequestVoteArgs{
				Term:         7,
				CandidateID:  "node-A",
				LastLogIndex: 42,
				LastLogTerm:  6,
			},
			golden: "0c00000008000c00080004000800000008000000500000004800000014000000000000000c0028001c0018000c0004000c00000006000000000000002a000000000000000000000010000000070000000000000000000000060000006e6f64652d4100000b00000052657175657374566f746500",
		},
		{
			name:    "RequestVoteReply",
			rpcType: v2RPCTypeRequestVoteReply,
			msg:     &raft.RequestVoteReply{Term: 7, VoteGranted: true},
			golden:  "0c00000008000c00080004000800000008000000280000002000000010000000000000000800100008000700080000000000000107000000000000001000000052657175657374566f74655265706c7900000000",
		},
		{
			name:    "AppendEntries_empty",
			rpcType: v2RPCTypeAppendEntries,
			msg: &raft.AppendEntriesArgs{
				Term:         7,
				LeaderID:     "node-A",
				PrevLogIndex: 41,
				PrevLogTerm:  6,
				LeaderCommit: 41,
			},
			golden: "0c00000008000c00080004000800000008000000600000005800000014000000100034002c0028001c00140010000400100000002900000000000000000000003000000006000000000000002900000000000000000000000c0000000700000000000000060000006e6f64652d410000000000000d000000417070656e64456e7472696573000000",
		},
		{
			name:    "AppendEntries_one",
			rpcType: v2RPCTypeAppendEntries,
			msg: &raft.AppendEntriesArgs{
				Term:         7,
				LeaderID:     "node-A",
				PrevLogIndex: 41,
				PrevLogTerm:  6,
				Entries:      []raft.LogEntry{{Term: 7, Index: 42, Command: []byte("hello"), Type: raft.LogEntryCommand}},
				LeaderCommit: 41,
			},
			golden: "0c00000008000c00080004000800000008000000980000009000000014000000100034002c0028001c00140010000400100000002900000000000000000000003000000006000000000000002900000000000000000000000c0000000700000000000000060000006e6f64652d410000010000001000000000000a001c001000080004000a000000180000002a000000000000000700000000000000000000000500000068656c6c6f0000000d000000417070656e64456e7472696573000000",
		},
		{
			name:    "AppendEntriesReply",
			rpcType: v2RPCTypeAppendEntriesReply,
			msg:     &raft.AppendEntriesReply{Term: 7, Success: true},
			golden:  "0c00000008000c000800040008000000080000002800000020000000100000000000000008001000080007000800000000000001070000000000000012000000417070656e64456e74726965735265706c790000",
		},
		{
			name:    "InstallSnapshot",
			rpcType: v2RPCTypeInstallSnapshot,
			msg: &raft.InstallSnapshotArgs{
				Term:              7,
				LeaderID:          "node-A",
				LastIncludedIndex: 100,
				LastIncludedTerm:  6,
				Data:              []byte("snapshot-data"),
				Servers:           []raft.Server{{ID: "node-A", Suffrage: raft.Voter}, {ID: "node-B", Suffrage: raft.Voter}},
			},
			golden: "0c00000008000c00080004000800000008000000a8000000a000000014000000100030002400200014000c0008000400100000004c00000034000000060000000000000064000000000000000000000010000000070000000000000000000000060000006e6f64652d4100000d000000736e617073686f742d64617461000000020000000800000020000000eaffffff04000000060000006e6f64652d41000000000600080004000600000004000000060000006e6f64652d4200000f000000496e7374616c6c536e617073686f7400",
		},
		{
			name:    "InstallSnapshotReply",
			rpcType: v2RPCTypeInstallSnapshotReply,
			msg:     &raft.InstallSnapshotReply{Term: 7},
			golden:  "0c00000008000c0008000400080000000800000020000000180000000c000000000006000c00040006000000070000000000000014000000496e7374616c6c536e617073686f745265706c7900000000",
		},
		{
			name:    "TimeoutNow",
			rpcType: v2RPCTypeTimeoutNow,
			msg:     nil,
			golden:  "0c000000000006000800040006000000040000000a00000054696d656f75744e6f770000",
		},
		{
			name:    "TimeoutNowReply",
			rpcType: v2RPCTypeTimeoutNowReply,
			msg:     nil,
			golden:  "0c000000000006000800040006000000040000000f00000054696d656f75744e6f775265706c7900",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := v2EncodeRPC(tc.rpcType, tc.msg)
			require.NoError(t, err)
			require.Equal(t, tc.golden, hex.EncodeToString(got), "v2 wire bytes diverged from v1 golden")
		})
	}
}

// TestV2EncodeDecode_RoundTrip exercises encode→decode for each RPC type so
// the decode path is also covered (the byte-identical test above only covers
// encode).
func TestV2EncodeDecode_RoundTrip(t *testing.T) {
	t.Run("RequestVote", func(t *testing.T) {
		args := &raft.RequestVoteArgs{
			Term: 7, CandidateID: "node-A", LastLogIndex: 42, LastLogTerm: 6,
			PreVote: true, LeaderTransfer: true,
		}
		raw, err := v2EncodeRPC(v2RPCTypeRequestVote, args)
		require.NoError(t, err)
		rpcType, data, err := v2DecodeRPC(raw)
		require.NoError(t, err)
		require.Equal(t, v2RPCTypeRequestVote, rpcType)
		out, err := v2DecodeRequestVoteArgs(data)
		require.NoError(t, err)
		require.Equal(t, args, out)
	})
	t.Run("AppendEntries", func(t *testing.T) {
		args := &raft.AppendEntriesArgs{
			Term: 7, LeaderID: "node-A", PrevLogIndex: 41, PrevLogTerm: 6,
			Entries: []raft.LogEntry{
				{Term: 7, Index: 42, Command: []byte("hello"), Type: raft.LogEntryCommand},
				{Term: 7, Index: 43, Type: raft.LogEntryConfChange},
			},
			LeaderCommit: 41,
		}
		raw, err := v2EncodeRPC(v2RPCTypeAppendEntries, args)
		require.NoError(t, err)
		rpcType, data, err := v2DecodeRPC(raw)
		require.NoError(t, err)
		require.Equal(t, v2RPCTypeAppendEntries, rpcType)
		out, err := v2DecodeAppendEntriesArgs(data)
		require.NoError(t, err)
		require.Equal(t, args.Term, out.Term)
		require.Equal(t, args.LeaderID, out.LeaderID)
		require.Equal(t, len(args.Entries), len(out.Entries))
		require.Equal(t, args.Entries[0].Command, out.Entries[0].Command)
		require.Equal(t, args.Entries[1].Type, out.Entries[1].Type)
	})
	t.Run("InstallSnapshot", func(t *testing.T) {
		args := &raft.InstallSnapshotArgs{
			Term: 7, LeaderID: "node-A", LastIncludedIndex: 100, LastIncludedTerm: 6,
			Data:    []byte("snapshot-data"),
			Servers: []raft.Server{{ID: "node-A", Suffrage: raft.Voter}},
		}
		raw, err := v2EncodeRPC(v2RPCTypeInstallSnapshot, args)
		require.NoError(t, err)
		rpcType, data, err := v2DecodeRPC(raw)
		require.NoError(t, err)
		require.Equal(t, v2RPCTypeInstallSnapshot, rpcType)
		out, err := v2DecodeInstallSnapshotArgs(data)
		require.NoError(t, err)
		require.Equal(t, args.Term, out.Term)
		require.Equal(t, args.LeaderID, out.LeaderID)
		require.Equal(t, args.LastIncludedIndex, out.LastIncludedIndex)
		require.Equal(t, args.Data, out.Data)
		require.Equal(t, args.Servers, out.Servers)
	})
}
