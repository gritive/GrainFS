package raft

import "testing"

func benchmarkHeartbeatItems(n int) []hbItem {
	items := make([]hbItem, n)
	for i := range items {
		items[i] = hbItem{
			groupID: "group-" + string(rune('A'+(i%26))),
			args: &AppendEntriesArgs{
				Term:         7,
				LeaderID:     "node-A",
				PrevLogIndex: uint64(40 + i),
				PrevLogTerm:  6,
				LeaderCommit: uint64(40 + i),
			},
		}
	}
	return items
}

func BenchmarkHeartbeatEncodeBatch(b *testing.B) {
	items := benchmarkHeartbeatItems(8)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		payload, err := encodeHeartbeatBatch(items)
		if err != nil {
			b.Fatal(err)
		}
		if len(payload) == 0 {
			b.Fatal("empty heartbeat batch payload")
		}
	}
}

func BenchmarkHeartbeatDecodeBatch(b *testing.B) {
	items := benchmarkHeartbeatItems(8)
	payload, err := encodeHeartbeatBatch(items)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decoded, err := decodeHeartbeatBatch(payload)
		if err != nil {
			b.Fatal(err)
		}
		if len(decoded) != len(items) {
			b.Fatalf("decoded %d items, want %d", len(decoded), len(items))
		}
	}
}
