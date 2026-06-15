package cluster

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/gritive/GrainFS/internal/badgermeta"
)

// ─── FSM IterObjectMetas ──────────────────────────────────────────────────────

func benchmarkIterObjectMetas(b *testing.B, count int) {
	b.Helper()
	dir, err := os.MkdirTemp("", "bench-fsm-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())
	for i := 0; i < count; i++ {
		raw, _ := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
			Bucket:      "bench",
			Key:         fmt.Sprintf("obj-%06d", i),
			Size:        1024,
			ContentType: "application/octet-stream",
			ETag:        fmt.Sprintf("etag-%d", i),
			ModTime:     time.Now().UnixNano(),
		})
		if err := fsm.Apply(raw); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		n := 0
		_ = fsm.IterObjectMetas(func(_ ObjectMetaRef) error {
			n++
			return nil
		})
		if n != count {
			b.Fatalf("expected %d objects, got %d", count, n)
		}
	}
}

func BenchmarkIterObjectMetas_100(b *testing.B) { benchmarkIterObjectMetas(b, 100) }
func BenchmarkIterObjectMetas_1k(b *testing.B)  { benchmarkIterObjectMetas(b, 1_000) }
func BenchmarkIterObjectMetas_10k(b *testing.B) { benchmarkIterObjectMetas(b, 10_000) }

// ─── FSM Apply (CmdPutObjectMeta throughput) ─────────────────────────────────

func BenchmarkFSM_Apply_PutObjectMeta(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-apply-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		raw, _ := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
			Bucket:      "bench",
			Key:         fmt.Sprintf("obj-%d", i),
			Size:        1024,
			ContentType: "application/octet-stream",
			ETag:        "abc123",
			ModTime:     1,
		})
		if err := fsm.Apply(raw); err != nil {
			b.Fatal(err)
		}
	}
}

// ─── ECSplit (baseline comparison) ───────────────────────────────────────────

func BenchmarkECSplit_1MB_4plus2(b *testing.B) {
	cfg := ECConfig{DataShards: 4, ParityShards: 2}
	data := make([]byte, 1<<20) // 1 MiB
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		shards, err := ECSplit(cfg, data)
		if err != nil {
			b.Fatal(err)
		}
		_ = shards
	}
}
