package pullthrough

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/iam"
)

// BenchmarkParallelResolve measures Resolve cache-hit hot path under read-only
// parallel load. This is the workload every pull-through-bucket S3 request
// hits; RWMutex.RLock acquire/release becomes the dominant overhead at scale.
func BenchmarkParallelResolve(b *testing.B) {
	store := iam.NewStore()
	const buckets = 100
	for i := 0; i < buckets; i++ {
		store.ApplyBucketUpstreamForTest(iam.BucketUpstream{
			Bucket:    bucketName(i),
			Endpoint:  fmt.Sprintf("http://up-%d.example:9000", i),
			AccessKey: fmt.Sprintf("AK%d", i),
			SecretKey: fmt.Sprintf("sk-%d", i),
			CreatedAt: time.Now().UTC(),
		})
	}
	r := NewIAMResolver(store)
	// warm caches
	for i := 0; i < buckets; i++ {
		_, _ = r.Resolve(bucketName(i))
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			r.Resolve(bucketName(i % buckets))
			i++
		}
	})
}

// BenchmarkParallelResolveWithRotation simulates a low-rate rotation (new
// upstream record published every ~100ms) while many readers run. RWMutex
// writer-priority can starve readers when rotations happen mid-traffic.
func BenchmarkParallelResolveWithRotation(b *testing.B) {
	store := iam.NewStore()
	const buckets = 100
	for i := 0; i < buckets; i++ {
		store.ApplyBucketUpstreamForTest(iam.BucketUpstream{
			Bucket:    bucketName(i),
			Endpoint:  fmt.Sprintf("http://up-%d.example:9000", i),
			AccessKey: fmt.Sprintf("AK%d", i),
			SecretKey: fmt.Sprintf("sk-%d", i),
			CreatedAt: time.Now().UTC(),
		})
	}
	r := NewIAMResolver(store)
	for i := 0; i < buckets; i++ {
		_, _ = r.Resolve(bucketName(i))
	}
	stop := make(chan struct{})
	defer close(stop)
	var rotations atomic.Int64
	go func() {
		t := time.NewTicker(100 * time.Millisecond)
		defer t.Stop()
		gen := 0
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				bkt := bucketName(gen % buckets)
				store.ApplyBucketUpstreamForTest(iam.BucketUpstream{
					Bucket:    bkt,
					Endpoint:  fmt.Sprintf("http://up-%d-v%d.example:9000", gen%buckets, gen),
					AccessKey: fmt.Sprintf("AK%d-v%d", gen%buckets, gen),
					SecretKey: fmt.Sprintf("sk-%d-v%d", gen%buckets, gen),
					CreatedAt: time.Now().UTC(),
				})
				rotations.Add(1)
				gen++
			}
		}
	}()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			r.Resolve(bucketName(i % buckets))
			i++
		}
	})
	b.ReportMetric(float64(rotations.Load()), "rotations_during_bench")
}

func bucketName(i int) string {
	return fmt.Sprintf("bench-%d", i)
}
