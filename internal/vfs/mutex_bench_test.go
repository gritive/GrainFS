package vfs

import (
	"fmt"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
)

// BenchmarkMutexContention measures mutex contention on GrainVFS cacheMu.
// Run with -mutexprofile to collect contention data:
//
//	go test -run=^$ -bench=BenchmarkMutexContention -benchtime=10s \
//	  -mutexprofile=/tmp/mu.out ./internal/vfs/
func BenchmarkMutexContention(b *testing.B) {
	dir := b.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	if err != nil {
		b.Fatal(err)
	}

	// Long TTL keeps entries alive for the whole benchmark — cache is always warm,
	// so cacheMu.RLock/Lock is exercised on every Stat call.
	fs, err := New(backend, "bench-vol",
		WithStatCacheTTL(10*time.Minute),
		WithDirCacheTTL(10*time.Minute),
	)
	if err != nil {
		b.Fatal(err)
	}

	// Pre-create files so Stat can hit the cache after the first miss
	const nFiles = 200
	for i := range nFiles {
		f, err := fs.Create(fmt.Sprintf("file%d.txt", i))
		if err != nil {
			b.Fatal(err)
		}
		f.Close()
	}

	// Warm the cache: one Stat per file before the timed loop
	for i := range nFiles {
		fs.Stat(fmt.Sprintf("file%d.txt", i)) //nolint:errcheck
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			// Mix: ~90% cache reads (RLock) + ~10% cache writes (Lock via invalidation)
			if i%10 == 0 {
				fs.invalidateStatCache(fmt.Sprintf("file%d.txt", i%nFiles))
			} else {
				fs.Stat(fmt.Sprintf("file%d.txt", i%nFiles)) //nolint:errcheck
			}
			i++
		}
	})
}
