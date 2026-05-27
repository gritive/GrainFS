package cluster

import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage/datawal"
)

// newTestWAL opens a real on-disk data WAL in a per-test temp dir.
func newTestWAL(t *testing.T) *datawal.WAL {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "datawal")
	w, err := datawal.Open(dir, nil, "datawal")
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

func TestShardPackActor_ReadYourWritesAfterAck(t *testing.T) {
	store, err := newShardPackStore(t.TempDir(), newTestWAL(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	payload := []byte("xyz")
	require.NoError(t, store.put("b", "k", 1, payload))

	got, ok, err := store.get("b", "k", 1)
	require.NoError(t, err)
	require.True(t, ok, "key should be visible immediately after ack")
	require.Equal(t, payload, got)
}

func TestShardPackActor_BackpressureBlocksOnFullMailbox(t *testing.T) {
	store, err := newShardPackStore(t.TempDir(), newTestWAL(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	release := make(chan struct{})
	orig := commitShardPackTxn
	commitShardPackTxn = func(w DataWALAppender) error {
		<-release
		return orig(w)
	}
	defer func() { commitShardPackTxn = orig }()

	// Fill the mailbox (capacity 64) + 1 in-flight batch + 1 blocked sender.
	payload := []byte("p")
	var wg sync.WaitGroup
	for i := 0; i < maxShardPackBatch+2; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = store.put("b", fmt.Sprintf("k-%d", i), 1, payload)
		}()
	}

	// The next put should block on enqueue (cmdCh full).
	blocked := make(chan struct{})
	go func() {
		_ = store.put("b", "blocked-key", 1, payload)
		close(blocked)
	}()

	select {
	case <-blocked:
		t.Fatal("put should block while mailbox is saturated")
	case <-time.After(50 * time.Millisecond):
		// expected: still blocked
	}

	close(release)
	wg.Wait()
	<-blocked
}

func TestShardPackActor_WALFlushErrorAllBatchFails(t *testing.T) {
	store, err := newShardPackStore(t.TempDir(), newTestWAL(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	orig := commitShardPackTxn
	failOnce := true
	commitShardPackTxn = func(w DataWALAppender) error {
		if failOnce {
			failOnce = false
			return errors.New("injected flush failure")
		}
		return orig(w)
	}
	defer func() { commitShardPackTxn = orig }()

	// First put hits the injected failure.
	err = store.put("b", "k1", 1, []byte("v1"))
	require.Error(t, err, "injected flush failure must be reported to caller")

	// Subsequent put succeeds (flush seam restored).
	require.NoError(t, store.put("b", "k2", 1, []byte("v2")))

	_, ok, err := store.get("b", "k2", 1)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestShardPackActor_RotateAtOffsetCap(t *testing.T) {
	store, err := newShardPackStore(t.TempDir(), newTestWAL(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	// Shrink maxSize so a few records trigger rotate.
	store.maxSize = 1 << 10 // 1 KiB

	payload := make([]byte, 256)
	for i := 0; i < 8; i++ {
		require.NoError(t, store.put("b", fmt.Sprintf("k-%d", i), 1, payload),
			"put %d should succeed across rotates", i)
	}

	// All 8 keys present, possibly spread across multiple active blobs.
	for i := 0; i < 8; i++ {
		got, ok, gerr := store.get("b", fmt.Sprintf("k-%d", i), 1)
		require.NoError(t, gerr)
		require.True(t, ok, "key %d missing after rotates", i)
		require.Equal(t, payload, got)
	}
	// nextID must have advanced past 1 (rotate happened).
	require.Greater(t, store.activeID, uint64(0))
}

func TestShardPackActor_CloseDrainsInFlightAcks(t *testing.T) {
	store, err := newShardPackStore(t.TempDir(), newTestWAL(t))
	require.NoError(t, err)

	const N = 32
	payload := []byte("p")
	acks := make(chan error, N)
	for i := 0; i < N; i++ {
		i := i
		go func() {
			acks <- store.put("b", fmt.Sprintf("k-%d", i), 1, payload)
		}()
	}

	// Give callers a moment to enqueue, then Close.
	time.Sleep(10 * time.Millisecond)
	require.NoError(t, store.Close())

	// Every in-flight caller must receive an ack (nil or error — not blocked).
	for i := 0; i < N; i++ {
		select {
		case err := <-acks:
			// Either nil (committed before Close drain ended) or ErrShardPackClosed
			// (raced with Close). Both acceptable as long as caller is not stuck.
			_ = err
		case <-time.After(2 * time.Second):
			t.Fatalf("caller %d stuck waiting for ack after Close", i)
		}
	}

	// New puts after Close return ErrShardPackClosed immediately.
	err = store.put("b", "after-close", 1, payload)
	require.ErrorIs(t, err, ErrShardPackClosed)
}

func TestShardPackBatchMax(t *testing.T) {
	require.Equal(t, maxShardPackBatch, shardPackBatchMax(""))
	require.Equal(t, 1, shardPackBatchMax("1"))
	require.Equal(t, 1, shardPackBatchMax("0"))
	require.Equal(t, 8, shardPackBatchMax("8"))
	require.Equal(t, maxShardPackBatch, shardPackBatchMax("garbage"))
	require.Equal(t, maxShardPackBatch, shardPackBatchMax("999"))
}
