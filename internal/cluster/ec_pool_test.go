package cluster

import (
	"bytes"
	"io"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func allocBytesPerRunForTest(t *testing.T, runs int, run func() error) uint64 {
	t.Helper()

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	for range runs {
		require.NoError(t, run())
	}
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	return (after.TotalAlloc - before.TotalAlloc) / uint64(runs)
}

func TestECSplit_AllocsBounded(t *testing.T) {
	cfg := ECConfig{DataShards: 4, ParityShards: 2}
	data := make([]byte, 4*1024)
	// warmup to populate encoder cache
	_, _ = ECSplit(cfg, data)
	allocs := testing.AllocsPerRun(100, func() {
		_, _ = ECSplit(cfg, data)
	})
	t.Logf("ECSplit allocs after pool: %.0f", allocs)
	if allocs > 15 {
		t.Errorf("ECSplit allocates %.0f times (want ≤15, baseline was 59)", allocs)
	}
}

func TestECReconstruct_AllocsBounded(t *testing.T) {
	cfg := ECConfig{DataShards: 4, ParityShards: 2}
	data := make([]byte, 4*1024)
	shards, _ := ECSplit(cfg, data)
	// warmup to populate encoder cache
	_, _ = ECReconstruct(cfg, shards)
	allocs := testing.AllocsPerRun(100, func() {
		_, _ = ECReconstruct(cfg, shards)
	})
	t.Logf("ECReconstruct allocs after pool: %.0f", allocs)
	if allocs > 12 {
		t.Errorf("ECReconstruct allocates %.0f times (want ≤12, baseline was 48)", allocs)
	}
}

func TestECReconstructStreamTo_AllocBytesBounded(t *testing.T) {
	cfg := ECConfig{DataShards: 4, ParityShards: 2}
	data := make([]byte, 4*1024*1024)
	shards, _ := ECSplit(cfg, data)

	run := func() error {
		readers := make([]io.Reader, len(shards))
		for i := 0; i < cfg.DataShards; i++ {
			readers[i] = bytes.NewReader(shards[i])
		}
		return ECReconstructStreamTo(io.Discard, cfg, readers)
	}

	require.NoError(t, run())
	allocedBytes := int64(allocBytesPerRunForTest(t, 3, run))
	t.Logf("ECReconstructStreamTo alloc bytes after pool: %d", allocedBytes)
	if allocedBytes > 256*1024 {
		t.Errorf("ECReconstructStreamTo allocates %d B/op (want ≤256KiB)", allocedBytes)
	}
}

func TestECReconstructStreamTo_MissingDataShardAllocBytesBounded(t *testing.T) {
	cfg := ECConfig{DataShards: 4, ParityShards: 2}
	data := make([]byte, 16*1024*1024)
	shards, err := ECSplit(cfg, data)
	require.NoError(t, err)

	run := func() error {
		readers := make([]io.Reader, len(shards))
		for i := range shards {
			if i == 0 {
				continue
			}
			readers[i] = bytes.NewReader(shards[i])
		}
		return ECReconstructStreamTo(io.Discard, cfg, readers)
	}

	require.NoError(t, run())
	allocedBytes := int64(allocBytesPerRunForTest(t, 3, run))
	t.Logf("ECReconstructStreamTo missing data shard alloc bytes: %d", allocedBytes)
	require.LessOrEqualf(t, allocedBytes, int64(12*1024*1024),
		"ECReconstructStreamTo missing data shard allocates %d B/op (want ≤12MiB)", allocedBytes)
}

// S1 routes large (>4MiB) reads through the streaming reconstruct path even
// when the shard cache could store them. This asserts that path returns
// BYTE-IDENTICAL data for a degraded large read (a data shard missing, forcing
// parity reconstruction) on the 2+2 profile used by the single-node 4-drive
// benchmark — the correctness the buffered-vs-streaming routing change relies
// on. Non-trivial content (not zeros) so reconstruction is actually exercised.
func TestECReconstructStreamTo_MissingDataShard_LargeObjectByteIdentical(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 2}
	const size = maxECPooledReadObjectSize + 1024*1024 // > 4MiB, multipart-sized
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i*31 + 7) // deterministic non-trivial pattern
	}
	shards, err := ECSplit(cfg, data)
	require.NoError(t, err)

	// Drop data shard 0 → reconstruct from data shard 1 + parity (2,3).
	readers := make([]io.Reader, len(shards))
	for i := range shards {
		if i == 0 {
			continue // missing data shard
		}
		readers[i] = bytes.NewReader(shards[i])
	}

	var out bytes.Buffer
	require.NoError(t, ECReconstructStreamTo(&out, cfg, readers))
	require.Equal(t, len(data), out.Len(), "reconstructed length must match original")
	require.True(t, bytes.Equal(data, out.Bytes()),
		"degraded large-object streaming reconstruct must be byte-identical to the original")
}
