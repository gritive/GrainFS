package cluster

import (
	"fmt"
	"os"
	"testing"

	"github.com/dgraph-io/badger/v4"

	"github.com/gritive/GrainFS/internal/raft"
)

// benchmarkApplyBatched measures applyActor.applyBatch throughput at a fixed
// batch size. batchSize=1 is the no-batching baseline; larger sizes amortize
// per-transaction commit overhead.
func benchmarkApplyBatched(b *testing.B, batchSize int) {
	dir, err := os.MkdirTemp("", "bench-apply-batch-*")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { os.RemoveAll(dir) })

	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })

	fsm := NewFSM(db, newStateKeyspaceEmpty())
	a := &applyActor{db: fsm.db, fsm: fsm}

	cmds := make([][]byte, b.N)
	for i := range cmds {
		raw, err := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
			Bucket:      "bench",
			Key:         fmt.Sprintf("obj-%d", i),
			Size:        1024,
			ContentType: "application/octet-stream",
			ETag:        "abc123",
			ModTime:     1,
		})
		if err != nil {
			b.Fatal(err)
		}
		cmds[i] = raw
	}

	batch := make([]raft.LogEntry, batchSize)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i += batchSize {
		n := batchSize
		if i+n > b.N {
			n = b.N - i
		}
		for j := 0; j < n; j++ {
			batch[j] = raft.LogEntry{
				Index:   uint64(i + j + 1),
				Term:    1,
				Type:    raft.LogEntryCommand,
				Command: cmds[i+j],
			}
		}
		_ = a.applyBatch(batch[:n])
	}
}

func BenchmarkApplyActor_Batch1(b *testing.B)  { benchmarkApplyBatched(b, 1) }
func BenchmarkApplyActor_Batch4(b *testing.B)  { benchmarkApplyBatched(b, 4) }
func BenchmarkApplyActor_Batch16(b *testing.B) { benchmarkApplyBatched(b, 16) }
func BenchmarkApplyActor_Batch64(b *testing.B) { benchmarkApplyBatched(b, 64) }
