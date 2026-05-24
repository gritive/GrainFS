package cluster

import (
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage/datawal"
)

// newTestWAL opens a real on-disk data WAL in a per-test temp dir.
func newTestWAL(t *testing.T) *datawal.WAL {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "datawal")
	w, err := datawal.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	return w
}

// dumpShardPackState returns a sorted snapshot of every (key, length)
// pair in the store's index. Blob offsets are intentionally omitted:
// concurrent runs commit records in non-deterministic order so offsets differ,
// but every key must be present with the correct data length.
func dumpShardPackState(t *testing.T, s *shardPackStore) []string {
	t.Helper()
	s.indexMu.RLock()
	defer s.indexMu.RUnlock()
	out := make([]string, 0, len(s.index))
	for k, v := range s.index {
		out = append(out, fmt.Sprintf("%s -> len=%d", k, v.length))
	}
	sort.Strings(out)
	return out
}

// TestShardPackActor_ConcurrentWritesMatchSerial — core regression guard.
// 64 goroutines each put a unique key. Final state must match a serial
// (batch=1, single goroutine) reference run.
func TestShardPackActor_ConcurrentWritesMatchSerial(t *testing.T) {
	const N = 64
	payload := []byte("hello world payload bytes")

	// Reference: batching disabled via env override -> commit-of-1, single goroutine.
	t.Setenv("GRAINFS_SHARDPACK_BATCH_MAX", "1")
	refStore, err := newShardPackStore(t.TempDir(), newTestWAL(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = refStore.Close() })
	for i := 0; i < N; i++ {
		require.NoError(t, refStore.put("bucket", fmt.Sprintf("obj-%04d", i), 1, payload))
	}
	refState := dumpShardPackState(t, refStore)

	// Concurrent: batching enabled (default), 64 goroutines in parallel.
	t.Setenv("GRAINFS_SHARDPACK_BATCH_MAX", "64")
	store, err := newShardPackStore(t.TempDir(), newTestWAL(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		i := i
		go func() {
			defer wg.Done()
			require.NoError(t, store.put("bucket", fmt.Sprintf("obj-%04d", i), 1, payload))
		}()
	}
	wg.Wait()
	state := dumpShardPackState(t, store)

	require.Equal(t, refState, state,
		"concurrent batched writes diverged from serial reference")

	// Verify each key is actually readable with the expected payload.
	for i := 0; i < N; i++ {
		got, ok, gerr := store.get("bucket", fmt.Sprintf("obj-%04d", i), 1)
		require.NoError(t, gerr)
		require.True(t, ok, "key obj-%04d missing", i)
		require.Equal(t, payload, got, "key obj-%04d data mismatch", i)
	}
}
