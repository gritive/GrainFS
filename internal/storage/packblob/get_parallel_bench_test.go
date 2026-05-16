package packblob

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
)

// BenchmarkParallelGetSmallObjects measures the RLock contention on
// PackedBackend.mu under read-only parallel load. This is the workload the
// lock-free audit names as the trigger condition ("if packed small object
// reads become a hot-path bottleneck, convert this to the same immutable
// snapshot pattern used by CachedBackend").
//
// Pre-populates N small objects, then runs b.RunParallel readers picking
// keys uniformly at random. Run with -mutexprofile to see whether the
// RLock shows measurable contention.
func BenchmarkParallelGetSmallObjects(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("entries=%d", n), func(b *testing.B) {
			pb, keys := setupPackedBackend(b, n)
			b.Cleanup(func() { _ = pb.Close() })

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb2 *testing.PB) {
				rng := rand.New(rand.NewSource(rand.Int63()))
				ctx := context.Background()
				for pb2.Next() {
					k := keys[rng.Intn(len(keys))]
					rc, _, err := pb.GetObject(ctx, "bench", k)
					if err != nil {
						b.Fatal(err)
					}
					_ = rc.Close()
				}
			})
		})
	}
}

// BenchmarkParallelGetWithWriter adds a single concurrent writer that
// inserts new small objects while many readers run in parallel. This is
// the workload where RWMutex writer-priority can starve readers — and
// where CoW would have the largest win.
func BenchmarkParallelGetWithWriter(b *testing.B) {
	for _, n := range []int{10_000, 100_000} {
		b.Run(fmt.Sprintf("entries=%d", n), func(b *testing.B) {
			pb, keys := setupPackedBackend(b, n)
			b.Cleanup(func() { _ = pb.Close() })

			ctx := context.Background()
			stop := make(chan struct{})
			defer close(stop)
			var writes atomic.Int64
			go func() {
				payload := bytes.Repeat([]byte("w"), 256)
				i := int64(n)
				for {
					select {
					case <-stop:
						return
					default:
					}
					_, err := pb.PutObject(ctx, "bench", fmt.Sprintf("wkey-%d", i), bytes.NewReader(payload), "application/octet-stream")
					if err != nil {
						return
					}
					i++
					writes.Add(1)
				}
			}()

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb2 *testing.PB) {
				rng := rand.New(rand.NewSource(rand.Int63()))
				for pb2.Next() {
					k := keys[rng.Intn(len(keys))]
					rc, _, err := pb.GetObject(ctx, "bench", k)
					if err != nil {
						b.Fatal(err)
					}
					_ = rc.Close()
				}
			})
			b.ReportMetric(float64(writes.Load()), "writes_during_bench")
		})
	}
}

func setupPackedBackend(b *testing.B, n int) (*PackedBackend, []string) {
	b.Helper()
	inner, err := storage.NewLocalBackend(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = inner.Close() })

	pb, err := NewPackedBackend(inner, b.TempDir(), 4*1024)
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	if err := pb.CreateBucket(ctx, "bench"); err != nil {
		b.Fatal(err)
	}
	payload := bytes.Repeat([]byte("x"), 256) // small, will pack
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key-%d", i)
		keys[i] = k
		if _, err := pb.PutObject(ctx, "bench", k, bytes.NewReader(payload), "application/octet-stream"); err != nil {
			b.Fatal(err)
		}
	}
	return pb, keys
}
