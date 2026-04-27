package readamp

import (
	"fmt"
	"sync"
	"testing"
)

// resetGlobalState lets tests run in any order without leaking enabled
// state. Each test that depends on the global flag flips it explicitly.
func resetGlobalState() { Disable() }

func TestRecord_DisabledIsNoOp(t *testing.T) {
	defer resetGlobalState()
	Disable()

	tr := New(t.Name(), 4)
	if got := tr.Record("a"); got {
		t.Fatalf("disabled tracker should never report hits, got %v", got)
	}
	hits, misses, resident := tr.Snapshot()
	if hits != 0 || misses != 0 || resident != 0 {
		t.Fatalf("disabled tracker should not record anything, got hits=%d misses=%d resident=%d",
			hits, misses, resident)
	}
}

func TestRecord_FirstSeenIsMiss(t *testing.T) {
	defer resetGlobalState()
	Enable()

	tr := New(t.Name(), 4)
	if hit := tr.Record("a"); hit {
		t.Fatalf("first access of new key must be a miss, got hit=true")
	}
	hits, misses, resident := tr.Snapshot()
	if hits != 0 || misses != 1 || resident != 1 {
		t.Fatalf("hits=%d misses=%d resident=%d", hits, misses, resident)
	}
}

func TestRecord_RepeatWithinCapacityIsHit(t *testing.T) {
	defer resetGlobalState()
	Enable()

	tr := New(t.Name(), 4)
	tr.Record("a")
	if hit := tr.Record("a"); !hit {
		t.Fatalf("re-access within capacity must hit")
	}
	hits, misses, _ := tr.Snapshot()
	if hits != 1 || misses != 1 {
		t.Fatalf("hits=%d misses=%d", hits, misses)
	}
}

func TestRecord_LRUEvictsOldest(t *testing.T) {
	defer resetGlobalState()
	Enable()

	tr := New(t.Name(), 2)
	tr.Record("a") // resident: [a]
	tr.Record("b") // resident: [b a]
	tr.Record("c") // resident: [c b], a evicted
	// b is still resident, must hit before we touch other keys.
	if hit := tr.Record("b"); !hit {
		t.Fatalf("b within capacity should hit")
	}
	// resident: [b c]; record an unrelated key to evict c.
	tr.Record("d") // resident: [d b], c evicted
	if hit := tr.Record("a"); hit {
		t.Fatalf("evicted key should miss, got hit")
	}
}

// TestRecord_HitRateOnRepeatedWorkload validates the simulator answers
// the question we will actually ask in production telemetry: under a
// given capacity, what hit rate does this access pattern produce.
func TestRecord_HitRateOnRepeatedWorkload(t *testing.T) {
	defer resetGlobalState()
	Enable()

	tr := New(t.Name(), 16)

	// Workload: 100 unique keys, then re-access the last 16. Cache size
	// is exactly 16 — every replay must hit.
	for i := 0; i < 100; i++ {
		tr.Record(fmt.Sprintf("k%d", i))
	}
	for i := 84; i < 100; i++ {
		if hit := tr.Record(fmt.Sprintf("k%d", i)); !hit {
			t.Fatalf("key k%d should still be resident under capacity 16", i)
		}
	}
	hits, misses, _ := tr.Snapshot()
	// 100 initial misses + 16 hits on replay
	if hits != 16 || misses != 100 {
		t.Fatalf("hits=%d misses=%d (want 16/100)", hits, misses)
	}
}

func TestRecord_ConcurrentAccessIsSafe(t *testing.T) {
	defer resetGlobalState()
	Enable()

	tr := New(t.Name(), 64)

	// 8 goroutines hammer the same 32-key working set. Cache size 64
	// is large enough to retain everything, so steady-state hit rate
	// should be high — but we mostly assert the simulator does not
	// race or panic.
	const goroutines = 8
	const iters = 1000
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				tr.Record(fmt.Sprintf("k%d", i%32))
			}
		}()
	}
	wg.Wait()

	hits, misses, resident := tr.Snapshot()
	total := hits + misses
	if total != goroutines*iters {
		t.Fatalf("counter total=%d, want %d", total, goroutines*iters)
	}
	if resident > 64 {
		t.Fatalf("resident set %d exceeded capacity 64", resident)
	}
}

func TestRecord_CapacityZeroDoesNotPanic(t *testing.T) {
	defer resetGlobalState()
	Enable()
	// New clamps capacity to 1; verify instead of crashing on bad input.
	tr := New(t.Name(), 0)
	tr.Record("a")
	tr.Record("b") // a evicts under cap=1
	if hit := tr.Record("a"); hit {
		t.Fatalf("cap=1 should evict a after b")
	}
}
