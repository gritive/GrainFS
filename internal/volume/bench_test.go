package volume

import (
	"bytes"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/volume/dedup"
)

// benchVolSize is large enough to hold all benchmark writes without wrapping.
const benchVolSize = 1 << 30 // 1 GiB

func benchBackend(b *testing.B) storage.Backend {
	b.Helper()
	dir := b.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	if err != nil {
		b.Fatal(err)
	}
	return backend
}

func benchBadger(b *testing.B) *badger.DB {
	b.Helper()
	opts := badger.DefaultOptions("").WithInMemory(true).WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })
	return db
}

// BenchmarkWriteAt_NoDedup measures baseline write throughput with no dedup.
func BenchmarkWriteAt_NoDedup(b *testing.B) {
	mgr := NewManager(benchBackend(b))
	if _, err := mgr.Create("vol", benchVolSize); err != nil {
		b.Fatal(err)
	}
	data := bytes.Repeat([]byte{0xAB}, DefaultBlockSize)

	b.SetBytes(int64(DefaultBlockSize))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		off := int64(i%256) * int64(DefaultBlockSize)
		if _, err := mgr.WriteAt("vol", data, off); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWriteAt_DedupMiss measures write throughput when every block is
// unique (worst case for dedup: SHA-256 + BadgerDB txn on every write).
func BenchmarkWriteAt_DedupMiss(b *testing.B) {
	idx := dedup.NewBadgerIndex(benchBadger(b))
	mgr := NewManagerWithOptions(benchBackend(b), ManagerOptions{DedupIndex: idx})
	if _, err := mgr.Create("vol", benchVolSize); err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(DefaultBlockSize))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Unique content per iteration → always a dedup miss
		data := bytes.Repeat([]byte{byte(i), byte(i >> 8)}, DefaultBlockSize/2)
		off := int64(i%256) * int64(DefaultBlockSize)
		if _, err := mgr.WriteAt("vol", data, off); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWriteAt_DedupHit measures write throughput when every block is
// identical (best case for dedup: SHA-256 + BadgerDB refcount increment, no PutObject).
func BenchmarkWriteAt_DedupHit(b *testing.B) {
	idx := dedup.NewBadgerIndex(benchBadger(b))
	mgr := NewManagerWithOptions(benchBackend(b), ManagerOptions{DedupIndex: idx})
	if _, err := mgr.Create("vol", benchVolSize); err != nil {
		b.Fatal(err)
	}
	data := bytes.Repeat([]byte{0xAB}, DefaultBlockSize)

	// Pre-register one block so subsequent writes are all hits.
	if _, err := mgr.WriteAt("vol", data, 0); err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(DefaultBlockSize))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		off := int64(1+i%255) * int64(DefaultBlockSize)
		if _, err := mgr.WriteAt("vol", data, off); err != nil {
			b.Fatal(err)
		}
	}
}
