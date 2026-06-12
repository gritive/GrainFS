package cluster

import (
	"testing"

	"github.com/gritive/GrainFS/internal/raft"
)

func benchmarkAppendEntriesArgs() *raft.AppendEntriesArgs {
	return &raft.AppendEntriesArgs{
		Term:         7,
		LeaderID:     "node-A",
		PrevLogIndex: 41,
		PrevLogTerm:  6,
		Entries: []raft.LogEntry{
			{Term: 7, Index: 42, Command: []byte("hello"), Type: raft.LogEntryCommand},
			{Term: 7, Index: 43, Command: []byte("world"), Type: raft.LogEntryCommand},
			{Term: 7, Index: 44, Type: raft.LogEntryConfChange},
		},
		LeaderCommit: 41,
	}
}

func BenchmarkV2EncodeRPCAppendEntries(b *testing.B) {
	args := benchmarkAppendEntriesArgs()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		raw, err := encodeRPC(rpcTypeAppendEntries, args)
		if err != nil {
			b.Fatal(err)
		}
		if len(raw) == 0 {
			b.Fatal("empty AppendEntries RPC")
		}
	}
}

func BenchmarkV2DecodeRPCAppendEntries(b *testing.B) {
	raw, err := encodeRPC(rpcTypeAppendEntries, benchmarkAppendEntriesArgs())
	if err != nil {
		b.Fatal(err)
	}
	rpcType, data, err := decodeRPC(raw)
	if err != nil {
		b.Fatal(err)
	}
	if rpcType != rpcTypeAppendEntries {
		b.Fatalf("rpc type = %s, want %s", rpcType, rpcTypeAppendEntries)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		args, err := decodeAppendEntriesArgs(data)
		if err != nil {
			b.Fatal(err)
		}
		if len(args.Entries) != 3 {
			b.Fatalf("decoded %d entries, want 3", len(args.Entries))
		}
	}
}
