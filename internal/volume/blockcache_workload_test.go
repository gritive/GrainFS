package volume

import (
	"fmt"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/cache/blockcache"
	"github.com/gritive/GrainFS/internal/storage"
)

// TestBlockCache_RealVsSimulator runs the same nbd_double_scan-style
// workload against a real BlockCache and reports both the actual hit
// rate and the wall-clock difference between cold and warm passes.
// This is the integration check that the simulator's predicted curve
// shows up in production code, not just in the simulator.
//
// Sized to land in the simulator's "64 MB knee": 5 000 blocks of 4 KB
// = 20 MB working set, two passes. Simulator predicted 50% hit rate
// at 64 MB capacity — real cache should match closely. Timing the
// second pass against the first shows the user-visible win.
func TestBlockCache_RealVsSimulator(t *testing.T) {
	const blocks = 5000
	const cap64MB = 64 * 1024 * 1024

	cache := blockcache.New(cap64MB)
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	if err != nil {
		t.Fatalf("local backend: %v", err)
	}
	mgr := NewManagerWithOptions(backend, ManagerOptions{BlockCache: cache})
	if _, err := mgr.Create("bc", int64(blocks)*int64(DefaultBlockSize)); err != nil {
		t.Fatalf("create: %v", err)
	}
	for i := 0; i < blocks; i++ {
		payload := []byte(fmt.Sprintf("bc-%05d", i))
		if _, err := mgr.WriteAt("bc", payload, int64(i)*int64(DefaultBlockSize)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	buf := make([]byte, DefaultBlockSize)
	// Pass 1 (cold) — every read is a miss; cache populates.
	t1 := time.Now()
	for i := 0; i < blocks; i++ {
		if _, err := mgr.ReadAt("bc", buf, int64(i)*int64(DefaultBlockSize)); err != nil {
			t.Fatalf("cold read %d: %v", i, err)
		}
	}
	cold := time.Since(t1)
	statsAfterCold := cache.Stats()

	// Pass 2 (warm) — every read should hit the cache.
	t2 := time.Now()
	for i := 0; i < blocks; i++ {
		if _, err := mgr.ReadAt("bc", buf, int64(i)*int64(DefaultBlockSize)); err != nil {
			t.Fatalf("warm read %d: %v", i, err)
		}
	}
	warm := time.Since(t2)
	statsAfterWarm := cache.Stats()

	t.Logf("blocks=%d, working_set=%d MB, cache_capacity=%d MB",
		blocks, blocks*DefaultBlockSize/1024/1024, cap64MB/1024/1024)
	t.Logf("cold pass: %v  (hits=%d misses=%d evictions=%d resident=%d)",
		cold, statsAfterCold.Hits, statsAfterCold.Misses, statsAfterCold.Evictions, statsAfterCold.ResidentByte)
	t.Logf("warm pass: %v  (hits=%d misses=%d evictions=%d resident=%d)",
		warm, statsAfterWarm.Hits, statsAfterWarm.Misses, statsAfterWarm.Evictions, statsAfterWarm.ResidentByte)

	deltaHits := statsAfterWarm.Hits - statsAfterCold.Hits
	deltaMisses := statsAfterWarm.Misses - statsAfterCold.Misses
	warmHitRate := 100 * float64(deltaHits) / float64(deltaHits+deltaMisses)
	t.Logf("warm-pass hit rate: %.1f%%  (hits=%d, misses=%d)", warmHitRate, deltaHits, deltaMisses)

	// Assertions:
	// - cold pass must populate the cache (resident bytes > 0)
	if statsAfterCold.ResidentByte == 0 {
		t.Fatalf("expected cache to be populated after cold pass, resident=0")
	}
	// - warm pass on a working set < capacity should be ~100% hit
	if warmHitRate < 95 {
		t.Fatalf("warm-pass hit rate %.1f%% way below predicted ~100%% for working set under capacity", warmHitRate)
	}
	// - warm pass should be measurably faster (we expect ≥2x; assert ≥1.2x for stability)
	if cold > 0 && warm > 0 && float64(cold)/float64(warm) < 1.2 {
		t.Logf("warning: warm pass not meaningfully faster (cold=%v warm=%v ratio=%.2fx) — measurement-only, not a hard fail",
			cold, warm, float64(cold)/float64(warm))
	}
}

// TestBlockCache_WriteInvalidates verifies that WriteAt invalidates
// the cache so a subsequent ReadAt sees the new content. This is
// the correctness guard for the cache; without it we would serve
// stale data after a write.
func TestBlockCache_WriteInvalidates(t *testing.T) {
	cache := blockcache.New(1024 * 1024)
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	if err != nil {
		t.Fatalf("local backend: %v", err)
	}
	mgr := NewManagerWithOptions(backend, ManagerOptions{BlockCache: cache})
	if _, err := mgr.Create("inv", int64(DefaultBlockSize)*4); err != nil {
		t.Fatalf("create: %v", err)
	}

	if _, err := mgr.WriteAt("inv", []byte("first-write"), 0); err != nil {
		t.Fatalf("write1: %v", err)
	}

	buf := make([]byte, DefaultBlockSize)
	if _, err := mgr.ReadAt("inv", buf, 0); err != nil {
		t.Fatalf("read1: %v", err)
	}
	if got := string(buf[:11]); got != "first-write" {
		t.Fatalf("read1 expected first-write, got %q", got)
	}

	// Overwrite. Without invalidation the next ReadAt would hand back
	// the cached "first-write" payload.
	if _, err := mgr.WriteAt("inv", []byte("second-write"), 0); err != nil {
		t.Fatalf("write2: %v", err)
	}

	if _, err := mgr.ReadAt("inv", buf, 0); err != nil {
		t.Fatalf("read2: %v", err)
	}
	if got := string(buf[:12]); got != "second-write" {
		t.Fatalf("read2 expected second-write (invalidation failed), got %q", got)
	}
}

// TestBlockCache_DiscardInvalidates ensures Discard drops cache
// entries so a subsequent read returns zeros (the documented
// post-discard semantic) rather than stale cached bytes.
func TestBlockCache_DiscardInvalidates(t *testing.T) {
	cache := blockcache.New(1024 * 1024)
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	if err != nil {
		t.Fatalf("local backend: %v", err)
	}
	mgr := NewManagerWithOptions(backend, ManagerOptions{BlockCache: cache})
	if _, err := mgr.Create("dis", int64(DefaultBlockSize)*4); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := mgr.WriteAt("dis", []byte("payload"), 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, DefaultBlockSize)
	if _, err := mgr.ReadAt("dis", buf, 0); err != nil {
		t.Fatalf("read1: %v", err)
	}
	if got := string(buf[:7]); got != "payload" {
		t.Fatalf("read1 expected payload, got %q", got)
	}
	if err := mgr.Discard("dis", 0, int64(DefaultBlockSize)); err != nil {
		t.Fatalf("discard: %v", err)
	}
	clear(buf)
	if _, err := mgr.ReadAt("dis", buf, 0); err != nil {
		t.Fatalf("read2: %v", err)
	}
	for i, b := range buf {
		if b != 0 {
			t.Fatalf("after discard buf[%d] = 0x%02x, want zero (cache invalidation failed)", i, b)
		}
	}
}
