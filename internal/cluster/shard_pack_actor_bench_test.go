package cluster

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"testing"

	"github.com/gritive/GrainFS/internal/storage/datawal"
)

// benchmarkShardPackActorConcurrent runs N concurrent goroutines each putting
// distinct keys. The WAL passed in determines whether we measure actor overhead
// alone (noopWAL) or end-to-end including fsync (realWAL).
func benchmarkShardPackActorConcurrent(b *testing.B, concurrency int, dwal DataWALAppender) {
	store, err := newShardPackStore(b.TempDir(), dwal)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = store.Close() })

	payload := make([]byte, 64<<10)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()

	var wg sync.WaitGroup
	jobs := make(chan int, concurrency*2)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				if err := store.put("b", fmt.Sprintf("obj-%08d", idx), 1, payload); err != nil {
					b.Error(err)
					return
				}
			}
		}()
	}
	for i := 0; i < b.N; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
}

// noopWAL satisfies DataWALAppender but never writes or syncs. Used to isolate
// actor overhead (channel + batching + map updates) from fsync cost.
type noopWAL struct{}

func (noopWAL) Append(context.Context, datawal.Record) (uint64, error) { return 0, nil }
func (noopWAL) AppendReader(context.Context, datawal.Record, io.Reader) (uint64, error) {
	return 0, nil
}
func (noopWAL) Flush() error { return nil }

func mockWAL() DataWALAppender { return noopWAL{} }

func realWAL(b *testing.B) DataWALAppender {
	w, err := datawal.Open(filepath.Join(b.TempDir(), "datawal"), nil)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = w.Close() })
	return w
}

func BenchmarkShardPackActor_MockWAL_Concurrent1(b *testing.B) {
	benchmarkShardPackActorConcurrent(b, 1, mockWAL())
}
func BenchmarkShardPackActor_MockWAL_Concurrent8(b *testing.B) {
	benchmarkShardPackActorConcurrent(b, 8, mockWAL())
}
func BenchmarkShardPackActor_MockWAL_Concurrent16(b *testing.B) {
	benchmarkShardPackActorConcurrent(b, 16, mockWAL())
}
func BenchmarkShardPackActor_MockWAL_Concurrent64(b *testing.B) {
	benchmarkShardPackActorConcurrent(b, 64, mockWAL())
}

func BenchmarkShardPackActor_RealWAL_Concurrent1(b *testing.B) {
	benchmarkShardPackActorConcurrent(b, 1, realWAL(b))
}
func BenchmarkShardPackActor_RealWAL_Concurrent8(b *testing.B) {
	benchmarkShardPackActorConcurrent(b, 8, realWAL(b))
}
func BenchmarkShardPackActor_RealWAL_Concurrent16(b *testing.B) {
	benchmarkShardPackActorConcurrent(b, 16, realWAL(b))
}
func BenchmarkShardPackActor_RealWAL_Concurrent64(b *testing.B) {
	benchmarkShardPackActorConcurrent(b, 64, realWAL(b))
}
