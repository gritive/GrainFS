package volume

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/gritive/GrainFS/internal/metrics/readamp"
)

// Phase 2 #3 evaluation. These are not regressions — they print the hit
// rate the unified-buffer-cache simulator produces under representative
// volume.ReadAt access patterns. Each subtest seeds a deterministic
// workload, runs ReadAt, and reads the simulator counters back out.
//
// The numbers below feed straight into the design doc: if the curve is
// flat-and-low across 16/64/256 MB, a UBC is wasted memory; if it
// rises sharply, the curve also tells us how much cache to budget.
//
// Run: go test -v -run 'TestReadAmpWorkload' ./internal/volume/
//
// We measure per workload, not aggregate: the test resets every tracker
// before each subtest so one workload's residency does not contaminate
// the next.
func TestReadAmpWorkload(t *testing.T) {
	type baseline struct{ hits16, misses16, hits64, misses64, hits256, misses256 uint64 }
	snapBaseline := func() baseline {
		var b baseline
		b.hits16, b.misses16, _ = readamp.VolumeBlock16MB.Snapshot()
		b.hits64, b.misses64, _ = readamp.VolumeBlock64MB.Snapshot()
		b.hits256, b.misses256, _ = readamp.VolumeBlock256MB.Snapshot()
		return b
	}
	resetTrackers := func() {
		// Reset clears LRU residency so each subtest sees a cold cache.
		// Counters stay monotonic per Prometheus contract — we report
		// deltas against a baseline snapshot below.
		readamp.VolumeBlock16MB.Reset()
		readamp.VolumeBlock64MB.Reset()
		readamp.VolumeBlock256MB.Reset()
	}
	delta := func(now, before uint64) uint64 {
		if now < before {
			return 0
		}
		return now - before
	}
	report := func(t *testing.T, label string, b baseline) {
		t.Helper()
		for _, tr := range []struct {
			name string
			t    *readamp.Tracker
			h0   uint64
			m0   uint64
		}{
			{"16MB", readamp.VolumeBlock16MB, b.hits16, b.misses16},
			{"64MB", readamp.VolumeBlock64MB, b.hits64, b.misses64},
			{"256MB", readamp.VolumeBlock256MB, b.hits256, b.misses256},
		} {
			h, m, _ := tr.t.Snapshot()
			hits := delta(h, tr.h0)
			misses := delta(m, tr.m0)
			total := hits + misses
			rate := 0.0
			if total > 0 {
				rate = 100 * float64(hits) / float64(total)
			}
			t.Logf("%-32s %5s: %5.1f%% hit (%d hit / %d miss)", label, tr.name, rate, hits, misses)
		}
	}

	readamp.Enable()
	defer readamp.Disable()

	// --- Workload A: sequential first read of N unique blocks ---
	// No re-reads. Worst case for any cache: 0% hit ceiling. Confirms
	// the simulator does not invent hits where there is no locality.
	t.Run("sequential_unique", func(t *testing.T) {
		resetTrackers()
		base := snapBaseline()
		mgr := setupManager(t)
		_, err := mgr.Create("seq", 4*1024*1024) // 4 MB → 1024 blocks of 4 KB
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		// Pre-populate every block so ReadAt actually fetches.
		for i := 0; i < 1024; i++ {
			payload := []byte(fmt.Sprintf("blk-%04d", i))
			if _, err := mgr.WriteAt("seq", payload, int64(i)*int64(DefaultBlockSize)); err != nil {
				t.Fatalf("write %d: %v", i, err)
			}
		}
		buf := make([]byte, DefaultBlockSize)
		for i := 0; i < 1024; i++ {
			if _, err := mgr.ReadAt("seq", buf, int64(i)*int64(DefaultBlockSize)); err != nil {
				t.Fatalf("read %d: %v", i, err)
			}
		}
		report(t, "sequential_unique (1024 blocks)", base)
	})

	// --- Workload B: repeated GET of the same hot block ---
	// 100 reads of a single block. Top-end of locality. With cap > 0
	// we should see ~99% hit (first read is the only miss).
	t.Run("hot_single_block", func(t *testing.T) {
		resetTrackers()
		base := snapBaseline()
		mgr := setupManager(t)
		_, err := mgr.Create("hot", 4*1024*1024)
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		if _, err := mgr.WriteAt("hot", []byte("hot-block-payload"), 0); err != nil {
			t.Fatalf("write: %v", err)
		}
		buf := make([]byte, DefaultBlockSize)
		for i := 0; i < 100; i++ {
			if _, err := mgr.ReadAt("hot", buf, 0); err != nil {
				t.Fatalf("read iter %d: %v", i, err)
			}
		}
		report(t, "hot_single_block (100×1 block)", base)
	})

	// --- Workload C: working-set fits 64 MB but not 16 MB ---
	// 10 000 unique blocks (40 MB), then re-read each once. 16 MB
	// (4096 blocks) cannot retain everything; 64 MB and 256 MB can.
	// This is the bend in the hit-rate curve a UBC would target.
	t.Run("working_set_40mb", func(t *testing.T) {
		resetTrackers()
		base := snapBaseline()
		mgr := setupManager(t)
		const blocks = 10000
		_, err := mgr.Create("ws", int64(blocks)*int64(DefaultBlockSize))
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		for i := 0; i < blocks; i++ {
			payload := []byte(fmt.Sprintf("ws-%05d", i))
			if _, err := mgr.WriteAt("ws", payload, int64(i)*int64(DefaultBlockSize)); err != nil {
				t.Fatalf("write %d: %v", i, err)
			}
		}
		buf := make([]byte, DefaultBlockSize)
		// Phase 1: read every block once (warmup → all miss).
		for i := 0; i < blocks; i++ {
			if _, err := mgr.ReadAt("ws", buf, int64(i)*int64(DefaultBlockSize)); err != nil {
				t.Fatalf("read %d: %v", i, err)
			}
		}
		// Phase 2: read every block again (where the curve appears).
		for i := 0; i < blocks; i++ {
			if _, err := mgr.ReadAt("ws", buf, int64(i)*int64(DefaultBlockSize)); err != nil {
				t.Fatalf("re-read %d: %v", i, err)
			}
		}
		report(t, "working_set_40mb (10k blocks ×2)", base)
	})

	// --- Workload D: pareto / hot+cold mix ---
	// 80% of accesses go to 20% of blocks. Classic real-app pattern
	// (e.g. metadata blocks read often, payload blocks read rarely).
	// Hit rate should plateau quickly — even tiny caches win.
	t.Run("pareto_80_20", func(t *testing.T) {
		resetTrackers()
		base := snapBaseline()
		mgr := setupManager(t)
		const blocks = 5000
		_, err := mgr.Create("par", int64(blocks)*int64(DefaultBlockSize))
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		for i := 0; i < blocks; i++ {
			payload := []byte(fmt.Sprintf("par-%05d", i))
			if _, err := mgr.WriteAt("par", payload, int64(i)*int64(DefaultBlockSize)); err != nil {
				t.Fatalf("write %d: %v", i, err)
			}
		}
		buf := make([]byte, DefaultBlockSize)
		rng := rand.New(rand.NewSource(42))
		const accesses = 50000
		hot := blocks / 5
		for i := 0; i < accesses; i++ {
			var blk int
			if rng.Intn(100) < 80 {
				blk = rng.Intn(hot) // hot 20% of blocks
			} else {
				blk = hot + rng.Intn(blocks-hot) // cold 80% of blocks
			}
			if _, err := mgr.ReadAt("par", buf, int64(blk)*int64(DefaultBlockSize)); err != nil {
				t.Fatalf("read iter %d blk %d: %v", i, blk, err)
			}
		}
		report(t, "pareto_80_20 (50k reads, 5k blocks)", base)
	})

	// --- Workload E: NBD-like sequential scan, twice ---
	// Filesystem mounted on NBD often does a metadata scan, then later
	// a similar scan when looking up entries. Most blocks are touched
	// twice within a short window.
	t.Run("nbd_double_scan", func(t *testing.T) {
		resetTrackers()
		base := snapBaseline()
		mgr := setupManager(t)
		const blocks = 5000
		_, err := mgr.Create("nbd", int64(blocks)*int64(DefaultBlockSize))
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		for i := 0; i < blocks; i++ {
			payload := []byte(fmt.Sprintf("nbd-%05d", i))
			if _, err := mgr.WriteAt("nbd", payload, int64(i)*int64(DefaultBlockSize)); err != nil {
				t.Fatalf("write %d: %v", i, err)
			}
		}
		buf := make([]byte, DefaultBlockSize)
		for pass := 0; pass < 2; pass++ {
			for i := 0; i < blocks; i++ {
				if _, err := mgr.ReadAt("nbd", buf, int64(i)*int64(DefaultBlockSize)); err != nil {
					t.Fatalf("pass %d read %d: %v", pass, i, err)
				}
			}
		}
		report(t, "nbd_double_scan (5k blocks ×2)", base)
	})

	// --- Workload F: 200 MB working set re-read ---
	// 50 000 unique blocks (≈200 MB), each touched twice. 16 MB and
	// 64 MB caches will thrash; 256 MB still cannot retain all of it.
	// Tells us what hit rate looks like when the working set exceeds
	// every reachable cache size — useful for the curve's right tail.
	t.Run("working_set_200mb", func(t *testing.T) {
		resetTrackers()
		base := snapBaseline()
		mgr := setupManager(t)
		const blocks = 50000
		_, err := mgr.Create("ws2", int64(blocks)*int64(DefaultBlockSize))
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		for i := 0; i < blocks; i++ {
			payload := []byte(fmt.Sprintf("ws2-%05d", i))
			if _, err := mgr.WriteAt("ws2", payload, int64(i)*int64(DefaultBlockSize)); err != nil {
				t.Fatalf("write %d: %v", i, err)
			}
		}
		buf := make([]byte, DefaultBlockSize)
		for pass := 0; pass < 2; pass++ {
			for i := 0; i < blocks; i++ {
				if _, err := mgr.ReadAt("ws2", buf, int64(i)*int64(DefaultBlockSize)); err != nil {
					t.Fatalf("pass %d read %d: %v", pass, i, err)
				}
			}
		}
		report(t, "working_set_200mb (50k blocks ×2)", base)
	})
}
