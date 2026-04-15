package raft

import (
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

func newBenchStore(b *testing.B, syncWrites bool) *BadgerLogStore {
	b.Helper()
	dir := b.TempDir()
	opts := badger.DefaultOptions(dir).WithLogger(nil).WithSyncWrites(syncWrites)
	db, err := badger.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	store := &BadgerLogStore{db: db}
	b.Cleanup(func() { store.Close() })
	return store
}

func BenchmarkLogStore_AppendEntry(b *testing.B) {
	for _, sync := range []bool{false, true} {
		name := fmt.Sprintf("SyncWrites=%v", sync)
		b.Run(name, func(b *testing.B) {
			store := newBenchStore(b, sync)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				store.AppendEntries([]LogEntry{
					{Term: 1, Index: uint64(i + 1), Command: []byte("bench-cmd")},
				})
			}
		})
	}
}

func BenchmarkLogStore_SaveState(b *testing.B) {
	for _, sync := range []bool{false, true} {
		name := fmt.Sprintf("SyncWrites=%v", sync)
		b.Run(name, func(b *testing.B) {
			store := newBenchStore(b, sync)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				store.SaveState(uint64(i), "node-bench")
			}
		})
	}
}

func BenchmarkLogStore_AppendBatch10(b *testing.B) {
	for _, sync := range []bool{false, true} {
		name := fmt.Sprintf("SyncWrites=%v", sync)
		b.Run(name, func(b *testing.B) {
			store := newBenchStore(b, sync)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				entries := make([]LogEntry, 10)
				for j := 0; j < 10; j++ {
					entries[j] = LogEntry{Term: 1, Index: uint64(i*10 + j + 1), Command: []byte("batch-cmd")}
				}
				store.AppendEntries(entries)
			}
		})
	}
}
