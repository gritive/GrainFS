package nfs4server

import (
	"fmt"
	"testing"
)

// BenchmarkMutexContention measures mutex contention on StateManager hot-path methods.
// Run with -mutexprofile to collect contention data:
//
//	go test -run=^$ -bench=BenchmarkMutexContention -benchtime=10s \
//	  -mutexprofile=/tmp/mu.out ./internal/nfs4server/
func BenchmarkMutexContention(b *testing.B) {
	sm := NewStateManager()

	// Pre-populate with 1000 paths so ResolveFH has real work to do
	fhs := make([]FileHandle, 1000)
	for i := range fhs {
		fhs[i] = sm.GetOrCreateFH(fmt.Sprintf("/vol/dir%d/file%d", i/100, i))
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			// Mix: ~80% reads (ResolveFH) + ~20% writes (GetOrCreateFH)
			// reflects NFS COMPOUND pattern: many lookups, occasional creates
			if i%5 == 0 {
				sm.GetOrCreateFH(fmt.Sprintf("/vol/bench/%d", i))
			} else {
				sm.ResolveFH(fhs[i%len(fhs)])
			}
			i++
		}
	})
}
