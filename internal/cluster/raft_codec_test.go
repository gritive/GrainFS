package cluster

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
)

// TestV2EncodeRPC_WireGolden pins the cluster-side raft RPC wire format to its
// canonical bytes. The goldens were originally captured to prove byte-equality
// with the QUIC-era v1 codec; v1 is now deleted, so they stand on their own as
// the canonical wire format and a regression guard against accidental drift.
//
// If this test fails after touching raft_codec.go, you have changed the wire
// format. Since this is the sole codec (greenfield/flag-day, no v1 peer to stay
// compatible with), an intentional change is fine — regenerate the goldens from
// the new encoder output. An unintentional change is a bug — revert.
func TestV2EncodeRPC_WireGolden(t *testing.T) {
	cases := []struct {
		name    string
		rpcType string
		msg     any
		golden  string
	}{
		{
			name:    "RequestVote",
			rpcType: rpcTypeRequestVote,
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
			rpcType: rpcTypeRequestVoteReply,
			msg:     &raft.RequestVoteReply{Term: 7, VoteGranted: true},
			golden:  "0c00000008000c00080004000800000008000000280000002000000010000000000000000800100008000700080000000000000107000000000000001000000052657175657374566f74655265706c7900000000",
		},
		{
			name:    "AppendEntries_empty",
			rpcType: rpcTypeAppendEntries,
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
			rpcType: rpcTypeAppendEntries,
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
			rpcType: rpcTypeAppendEntriesReply,
			msg:     &raft.AppendEntriesReply{Term: 7, Success: true},
			golden:  "0c00000008000c000800040008000000080000002800000020000000100000000000000008001000080007000800000000000001070000000000000012000000417070656e64456e74726965735265706c790000",
		},
		{
			name:    "InstallSnapshot",
			rpcType: rpcTypeInstallSnapshot,
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
			rpcType: rpcTypeInstallSnapshotReply,
			msg:     &raft.InstallSnapshotReply{Term: 7},
			golden:  "0c00000008000c0008000400080000000800000020000000180000000c000000000006000c00040006000000070000000000000014000000496e7374616c6c536e617073686f745265706c7900000000",
		},
		{
			name:    "TimeoutNow",
			rpcType: rpcTypeTimeoutNow,
			msg:     nil,
			golden:  "0c000000000006000800040006000000040000000a00000054696d656f75744e6f770000",
		},
		{
			name:    "TimeoutNowReply",
			rpcType: rpcTypeTimeoutNowReply,
			msg:     nil,
			golden:  "0c000000000006000800040006000000040000000f00000054696d656f75744e6f775265706c7900",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := encodeRPC(tc.rpcType, tc.msg)
			require.NoError(t, err)
			require.Equal(t, tc.golden, hex.EncodeToString(got), "raft RPC wire bytes diverged from the canonical golden")
		})
	}
}

// TestV2EncodeDecode_RoundTrip exercises encode→decode for each RPC type so
// the decode path is also covered (the wire-golden test above only covers
// encode).
func TestV2EncodeDecode_RoundTrip(t *testing.T) {
	t.Run("RequestVote", func(t *testing.T) {
		args := &raft.RequestVoteArgs{
			Term: 7, CandidateID: "node-A", LastLogIndex: 42, LastLogTerm: 6,
			PreVote: true, LeaderTransfer: true,
		}
		raw, err := encodeRPC(rpcTypeRequestVote, args)
		require.NoError(t, err)
		rpcType, data, err := decodeRPC(raw)
		require.NoError(t, err)
		require.Equal(t, rpcTypeRequestVote, rpcType)
		out, err := decodeRequestVoteArgs(data)
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
		raw, err := encodeRPC(rpcTypeAppendEntries, args)
		require.NoError(t, err)
		rpcType, data, err := decodeRPC(raw)
		require.NoError(t, err)
		require.Equal(t, rpcTypeAppendEntries, rpcType)
		out, err := decodeAppendEntriesArgs(data)
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
		raw, err := encodeRPC(rpcTypeInstallSnapshot, args)
		require.NoError(t, err)
		rpcType, data, err := decodeRPC(raw)
		require.NoError(t, err)
		require.Equal(t, rpcTypeInstallSnapshot, rpcType)
		out, err := decodeInstallSnapshotArgs(data)
		require.NoError(t, err)
		require.Equal(t, args.Term, out.Term)
		require.Equal(t, args.LeaderID, out.LeaderID)
		require.Equal(t, args.LastIncludedIndex, out.LastIncludedIndex)
		require.Equal(t, args.Data, out.Data)
		require.Equal(t, args.Servers, out.Servers)
	})
}
