package storage

import (
	"bytes"
	"context"
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
// The test scales CachedBackend down to 8 MB / 1 MB-per-object so CI does not
// spend seconds writing hundreds of MiB just to cross the same cache-ratio
// boundaries. Workloads below preserve the relationships that matter:
// smaller-than-cache, larger-than-cache, and hot/cold skew.
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

	const testCacheBytes = 8 * 1024 * 1024
	const testMaxObjectBytes = 1 * 1024 * 1024

	// Helper to set up a CachedBackend(LocalBackend) stack. The cache is scaled
	// down for test speed, but the object-cache hit/miss boundaries are the same
	// as production: objects at or below max-object size can cache, and
	// sequential working sets larger than total capacity churn.
	setupBackend := func(t *testing.T) Backend {
		t.Helper()
		dir := t.TempDir()
		local, err := NewLocalBackend(dir)
		if err != nil {
			t.Fatalf("local backend: %v", err)
		}
		return NewCachedBackend(
			local,
			WithMaxCacheBytes(testCacheBytes),
			WithMaxObjectCacheBytes(testMaxObjectBytes),
		)
	}

	bucket := "ubc-bench"
	put := func(b Backend, key string, payload []byte) {
		t.Helper()
		if _, err := b.PutObject(context.Background(), bucket, key, bytes.NewReader(payload), "application/octet-stream"); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}
	mustGet := func(b Backend, key string) {
		t.Helper()
		rc, _, err := b.GetObject(context.Background(), bucket, key)
		if err != nil {
			t.Fatalf("get %s: %v", key, err)
		}
		_, _ = io.Copy(io.Discard, rc)
		rc.Close()
	}

	// --- A: cold sequential — unique objects, each read once ---
	// Worst case: nothing recurs, simulator must report 0% (CachedBackend
	// doesn't help, and neither would UBC).
	t.Run("cold_sequential", func(t *testing.T) {
		resetTrackers()
		base := snap()
		b := setupBackend(t)
		if err := b.CreateBucket(context.Background(), bucket); err != nil {
			t.Fatalf("create bucket: %v", err)
		}
		payload := bytes.Repeat([]byte("x"), 4096)
		for i := 0; i < 64; i++ {
			put(b, fmt.Sprintf("seq-%04d", i), payload)
		}
		for i := 0; i < 64; i++ {
			mustGet(b, fmt.Sprintf("seq-%04d", i))
		}
		report(t, "cold_sequential (64 unique GETs)", base)
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
		if err := b.CreateBucket(context.Background(), bucket); err != nil {
			t.Fatalf("create bucket: %v", err)
		}
		put(b, "hot.txt", []byte("hot payload"))
		for i := 0; i < 100; i++ {
			mustGet(b, "hot.txt")
		}
		report(t, "hot_key_burst (1 obj × 100 GETs)", base)
	})

	// --- C: working set under object cache, two passes ---
	// 128 small objects (~1 MB total), GET each twice. The scaled 8 MB
	// CachedBackend fits everything, so the second pass stays in object cache.
	t.Run("working_set_under_objcache", func(t *testing.T) {
		resetTrackers()
		base := snap()
		b := setupBackend(t)
		if err := b.CreateBucket(context.Background(), bucket); err != nil {
			t.Fatalf("create bucket: %v", err)
		}
		const n = 128
		payload := bytes.Repeat([]byte("ws"), 4096) // 8 KB per object -> 1 MB total
		for i := 0; i < n; i++ {
			put(b, fmt.Sprintf("ws-%05d", i), payload)
		}
		for pass := 0; pass < 2; pass++ {
			for i := 0; i < n; i++ {
				mustGet(b, fmt.Sprintf("ws-%05d", i))
			}
		}
		report(t, "working_set_1MB (128 objs × 2 GETs)", base)
	})

	// --- D: working set OVER object cache, two passes ---
	// 12 objects of 1 MB each = 12 MB total. The scaled 8 MB CachedBackend cannot
	// retain the sequential working set, so second-pass GETs miss the object
	// cache and fall through to LocalBackend. This is the simulator's golden case:
	// when object cache is too small, would a UBC at this layer help?
	t.Run("working_set_over_objcache", func(t *testing.T) {
		resetTrackers()
		base := snap()
		b := setupBackend(t)
		if err := b.CreateBucket(context.Background(), bucket); err != nil {
			t.Fatalf("create bucket: %v", err)
		}
		const n = 12
		payload := bytes.Repeat([]byte{0xAB}, 1024*1024) // 1 MB per object
		for i := 0; i < n; i++ {
			put(b, fmt.Sprintf("big-%03d", i), payload)
		}
		for pass := 0; pass < 2; pass++ {
			for i := 0; i < n; i++ {
				mustGet(b, fmt.Sprintf("big-%03d", i))
			}
		}
		report(t, "working_set_12MB (12 obj×1MB ×2 GETs)", base)
	})

	// --- E: pareto access on small objects ---
	// 80% of GETs hit 20% of objects. Most-asked layer in any web
	// application. CachedBackend should already absorb the hot
	// portion. Simulator counters here = the "post-CachedBackend
	// residual" — what UBC would catch beyond the object cache.
	t.Run("pareto_80_20_objects", func(t *testing.T) {
		resetTrackers()
		base := snap()
		b := setupBackend(t)
		if err := b.CreateBucket(context.Background(), bucket); err != nil {
			t.Fatalf("create bucket: %v", err)
		}
		const n = 128
		payload := bytes.Repeat([]byte("p"), 8192) // 8 KB per object
		for i := 0; i < n; i++ {
			put(b, fmt.Sprintf("p-%04d", i), payload)
		}
		rng := rand.New(rand.NewSource(7))
		const accesses = 512
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
		report(t, "pareto_80_20 (512 GETs on 128 obj)", base)
	})
}
