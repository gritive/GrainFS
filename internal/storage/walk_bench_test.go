package storage

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const walkBenchN = 1000 // number of objects to populate

func prepareBenchBackend(b *testing.B, n int) (*LocalBackend, string) {
	b.Helper()
	dir := b.TempDir()
	backend, err := NewLocalBackend(dir)
	require.NoError(b, err)
	b.Cleanup(func() { backend.Close() })

	const bucket = "bench"
	require.NoError(b, backend.CreateBucket(bucket))
	for i := range n {
		key := fmt.Sprintf("obj/%06d.bin", i)
		_, err := backend.PutObject(bucket, key, strings.NewReader("data"), "application/octet-stream")
		require.NoError(b, err)
	}
	return backend, bucket
}

// BenchmarkWalkObjects measures streaming iteration via callback (O(1) memory).
func BenchmarkWalkObjects(b *testing.B) {
	backend, bucket := prepareBenchBackend(b, walkBenchN)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		count := 0
		_ = backend.WalkObjects(bucket, "", func(*Object) error {
			count++
			return nil
		})
		_ = count
	}
}

// BenchmarkListObjectsLoop measures the previous pattern: bulk-load into a
// slice then iterate (O(n) memory).
func BenchmarkListObjectsLoop(b *testing.B) {
	backend, bucket := prepareBenchBackend(b, walkBenchN)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		objs, _ := backend.ListObjects(bucket, "", walkBenchN)
		count := 0
		for range objs {
			count++
		}
		_ = count
	}
}
