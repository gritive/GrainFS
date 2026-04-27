package storage

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/gritive/GrainFS/internal/metrics/readamp"
)

// Phase 2 #3 second baseline. We already characterized volume.ReadAt
// (block-shaped, 4 KB granularity). This test asks the same question
// at the OBJECT layer: under workloads that pass through CachedBackend
// (which is what S3 GET hits today), what hit-rate would a unified
// buffer cache catch BEYOND the existing object cache?
//
// The setup mirrors production: LocalBackend wrapped in CachedBackend
// (the same wiring serve.go uses by default). Every BackendObject*
// readamp record fires AFTER CachedBackend has missed — that is the
// boundary UBC would sit at, so the hit rates here are the marginal
// value-add UBC offers over what we already do.
//
// CachedBackend default size is 64 MB / 4 MB-per-object. Workloads
// below stay under those thresholds so CachedBackend itself acts as
// the first layer (its native counters tell us its own hit rate; we
// look for residual locality the simulator catches afterwards).
func TestReadAmpStorage_Workload(t *testing.T) {
	type baseline struct{ h16, m16, h64, m64, h256, m256 uint64 }
	snap := func() baseline {
		var b baseline
		b.h16, b.m16, _ = readamp.BackendObject16MB.Snapshot()
		b.h64, b.m64, _ = readamp.BackendObject64MB.Snapshot()
		b.h256, b.m256, _ = readamp.BackendObject256MB.Snapshot()
		return b
	}
	resetTrackers := func() {
		readamp.BackendObject16MB.Reset()
		readamp.BackendObject64MB.Reset()
		readamp.BackendObject256MB.Reset()
	}
	delta := func(a, b uint64) uint64 {
		if a < b {
			return 0
		}
		return a - b
	}
	report := func(t *testing.T, label string, base baseline) {
		t.Helper()
		for _, tr := range []struct {
			name string
			t    *readamp.Tracker
			h0   uint64
			m0   uint64
		}{
			{"16MB", readamp.BackendObject16MB, base.h16, base.m16},
			{"64MB", readamp.BackendObject64MB, base.h64, base.m64},
			{"256MB", readamp.BackendObject256MB, base.h256, base.m256},
		} {
			h, m, _ := tr.t.Snapshot()
			hits := delta(h, tr.h0)
			misses := delta(m, tr.m0)
			total := hits + misses
			rate := 0.0
			if total > 0 {
				rate = 100 * float64(hits) / float64(total)
			}
			t.Logf("%-36s %5s: %5.1f%% hit (%d hit / %d miss)", label, tr.name, rate, hits, misses)
		}
	}

	readamp.Enable()
	defer readamp.Disable()

	// Helper to set up a CachedBackend(LocalBackend) stack — what serve.go
	// configures by default for non-cluster runs. The cache size is the
	// production default so the simulator records only what slips through
	// the existing object cache.
	setupBackend := func(t *testing.T) Backend {
		t.Helper()
		dir := t.TempDir()
		local, err := NewLocalBackend(dir)
		if err != nil {
			t.Fatalf("local backend: %v", err)
		}
		return NewCachedBackend(local)
	}

	bucket := "ubc-bench"
	put := func(b Backend, key string, payload []byte) {
		t.Helper()
		if _, err := b.PutObject(bucket, key, bytes.NewReader(payload), "application/octet-stream"); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}
	mustGet := func(b Backend, key string) {
		t.Helper()
		rc, _, err := b.GetObject(bucket, key)
		if err != nil {
			t.Fatalf("get %s: %v", key, err)
		}
		_, _ = io.Copy(io.Discard, rc)
		rc.Close()
	}

	// --- A: cold sequential — 200 unique objects, each read once ---
	// Worst case: nothing recurs, simulator must report 0% (CachedBackend
	// doesn't help, and neither would UBC).
	t.Run("cold_sequential", func(t *testing.T) {
		resetTrackers()
		base := snap()
		b := setupBackend(t)
		if err := b.CreateBucket(bucket); err != nil {
			t.Fatalf("create bucket: %v", err)
		}
		payload := bytes.Repeat([]byte("x"), 4096)
		for i := 0; i < 200; i++ {
			put(b, fmt.Sprintf("seq-%04d", i), payload)
		}
		for i := 0; i < 200; i++ {
			mustGet(b, fmt.Sprintf("seq-%04d", i))
		}
		report(t, "cold_sequential (200 unique GETs)", base)
	})

	// --- B: hot key burst — 1 object, 100 GETs ---
	// CachedBackend will already absorb these (object stays cached).
	// The simulator should see ZERO disk-bound GETs after the first put.
	// This validates we don't double-count what the existing cache
	// already handles.
	t.Run("hot_key_burst", func(t *testing.T) {
		resetTrackers()
		base := snap()
		b := setupBackend(t)
		if err := b.CreateBucket(bucket); err != nil {
			t.Fatalf("create bucket: %v", err)
		}
		put(b, "hot.txt", []byte("hot payload"))
		for i := 0; i < 100; i++ {
			mustGet(b, "hot.txt")
		}
		report(t, "hot_key_burst (1 obj × 100 GETs)", base)
	})

	// --- C: working set under 64 MB, two passes ---
	// 800 small objects (~3 MB total), GET each twice. CachedBackend
	// at 64 MB easily fits everything — simulator should see the second
	// pass as 100% hit on the LARGE simulated cache, but 0% on small
	// caches sized below the working set.
	t.Run("working_set_under_objcache", func(t *testing.T) {
		resetTrackers()
		base := snap()
		b := setupBackend(t)
		if err := b.CreateBucket(bucket); err != nil {
			t.Fatalf("create bucket: %v", err)
		}
		const n = 800
		payload := bytes.Repeat([]byte("ws"), 4096) // 8 KB per object → 6.4 MB total
		for i := 0; i < n; i++ {
			put(b, fmt.Sprintf("ws-%05d", i), payload)
		}
		for pass := 0; pass < 2; pass++ {
			for i := 0; i < n; i++ {
				mustGet(b, fmt.Sprintf("ws-%05d", i))
			}
		}
		report(t, "working_set_6MB (800 objs × 2 GETs)", base)
	})

	// --- D: working set OVER object cache (64 MB), two passes ---
	// 200 large objects of 1 MB each = 200 MB total. CachedBackend at
	// 64 MB cannot retain them all → second-pass GETs miss the object
	// cache, fall through to LocalBackend → readamp records them. This
	// is the simulator's golden case: when object cache is too small,
	// would a UBC at this layer help?
	t.Run("working_set_over_objcache", func(t *testing.T) {
		resetTrackers()
		base := snap()
		b := setupBackend(t)
		if err := b.CreateBucket(bucket); err != nil {
			t.Fatalf("create bucket: %v", err)
		}
		const n = 200
		payload := bytes.Repeat([]byte{0xAB}, 1024*1024) // 1 MB per object
		for i := 0; i < n; i++ {
			put(b, fmt.Sprintf("big-%03d", i), payload)
		}
		for pass := 0; pass < 2; pass++ {
			for i := 0; i < n; i++ {
				mustGet(b, fmt.Sprintf("big-%03d", i))
			}
		}
		report(t, "working_set_200MB (200 obj×1MB ×2 GETs)", base)
	})

	// --- E: pareto access on 500 small objects ---
	// 80% of GETs hit 20% of objects. Most-asked layer in any web
	// application. CachedBackend should already absorb the hot
	// portion. Simulator counters here = the "post-CachedBackend
	// residual" — what UBC would catch beyond the object cache.
	t.Run("pareto_80_20_objects", func(t *testing.T) {
		resetTrackers()
		base := snap()
		b := setupBackend(t)
		if err := b.CreateBucket(bucket); err != nil {
			t.Fatalf("create bucket: %v", err)
		}
		const n = 500
		payload := bytes.Repeat([]byte("p"), 8192) // 8 KB per object
		for i := 0; i < n; i++ {
			put(b, fmt.Sprintf("p-%04d", i), payload)
		}
		rng := rand.New(rand.NewSource(7))
		const accesses = 5000
		hot := n / 5
		for i := 0; i < accesses; i++ {
			var k int
			if rng.Intn(100) < 80 {
				k = rng.Intn(hot)
			} else {
				k = hot + rng.Intn(n-hot)
			}
			mustGet(b, fmt.Sprintf("p-%04d", k))
		}
		report(t, "pareto_80_20 (5k GETs on 500 obj)", base)
	})
}
