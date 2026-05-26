package encrypt

import (
	"runtime"
	"sync"
	"testing"
)

func TestKEKLeaseTracker_AcquireRelease(t *testing.T) {
	tr := NewKEKLeaseTracker()
	rel1 := tr.Acquire(5)
	if got := tr.Count(5); got != 1 {
		t.Errorf("count after 1 acquire = %d, want 1", got)
	}
	rel2 := tr.Acquire(5)
	if got := tr.Count(5); got != 2 {
		t.Errorf("count after 2 acquires = %d, want 2", got)
	}
	rel1()
	if got := tr.Count(5); got != 1 {
		t.Errorf("count after release = %d, want 1", got)
	}
	rel2()
	if got := tr.Count(5); got != 0 {
		t.Errorf("count after 2 releases = %d, want 0", got)
	}
}

func TestKEKLeaseTracker_Concurrent_LockFree(t *testing.T) {
	tr := NewKEKLeaseTracker()
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rel := tr.Acquire(7)
			runtime.Gosched()
			rel()
		}()
	}
	wg.Wait()
	if got := tr.Count(7); got != 0 {
		t.Errorf("count after 1000 acquire/release = %d, want 0", got)
	}
}

func TestKEKLeaseTracker_Snapshot(t *testing.T) {
	tr := NewKEKLeaseTracker()
	tr.Acquire(1)
	tr.Acquire(1)
	tr.Acquire(2)
	snap := tr.Snapshot()
	if snap[1] != 2 || snap[2] != 1 {
		t.Errorf("snapshot = %v, want {1:2 2:1}", snap)
	}
}

func TestKEKLeaseTracker_DoubleReleasePanics(t *testing.T) {
	tr := NewKEKLeaseTracker()
	rel := tr.Acquire(3)
	rel()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic on double release")
		}
	}()
	rel()
}
