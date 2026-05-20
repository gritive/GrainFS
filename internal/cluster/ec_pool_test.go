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
