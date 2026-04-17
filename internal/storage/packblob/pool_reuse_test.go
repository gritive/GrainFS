package packblob

import (
	"bytes"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCompress_PoolReuse_AllocsBounded verifies that the sync.Pool prevents
// repeated encoder/decoder allocation under sustained use.
// With pool reuse, only output buffers are allocated per call — not new encoders.
func TestCompress_PoolReuse_AllocsBounded(t *testing.T) {
	data := bytes.Repeat([]byte("pool reuse test "), 200)

	// Warmup: ensure encoder and decoder are in their pools before measuring
	c, err := compress(data)
	require.NoError(t, err)
	_, err = decompress(c)
	require.NoError(t, err)

	// Measure allocations per round-trip
	allocs := testing.AllocsPerRun(50, func() {
		compressed, _ := compress(data)
		decompress(compressed) //nolint:errcheck
	})

	// With pool: each call allocates only output buffers (~2 slices per round-trip).
	// Without pool: zstd.NewWriter allocates ~4MB encoder each time → many more allocs.
	const maxAllocsPerRoundTrip = 6.0
	require.LessOrEqualf(t, allocs, maxAllocsPerRoundTrip,
		"compress/decompress allocates %.1f per round-trip; pool should keep this ≤ %.0f", allocs, maxAllocsPerRoundTrip)
}

// TestCompress_PoolReuse_HeapGrowthBounded verifies that running 1000 sequential
// compress/decompress cycles does not cause unbounded heap growth.
func TestCompress_PoolReuse_HeapGrowthBounded(t *testing.T) {
	data := bytes.Repeat([]byte("heap growth test "), 500)

	// Warmup
	for range 5 {
		c, _ := compress(data)
		decompress(c) //nolint:errcheck
	}

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	const iterations = 1000
	for range iterations {
		c, err := compress(data)
		require.NoError(t, err)
		_, err = decompress(c)
		require.NoError(t, err)
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	heapAllocsPerOp := float64(after.Mallocs-before.Mallocs) / iterations

	// Each compress/decompress should allocate only output buffers.
	// zstd.EncodeAll allocates the output slice; DecodeAll allocates the output slice.
	// Pool prevents encoder/decoder object reallocation.
	const maxHeapAllocsPerOp = 10.0
	require.LessOrEqualf(t, heapAllocsPerOp, maxHeapAllocsPerOp,
		"%.1f heap allocs/op; pool should keep per-op allocation bounded", heapAllocsPerOp)
}

// BenchmarkCompress_Pool measures compress/decompress throughput with pool reuse.
// Run with: go test -bench=BenchmarkCompress_Pool -benchmem
func BenchmarkCompress_Pool(b *testing.B) {
	data := bytes.Repeat([]byte("benchmark pool "), 500)
	b.ResetTimer()
	for b.Loop() {
		c, _ := compress(data)
		decompress(c) //nolint:errcheck
	}
}
