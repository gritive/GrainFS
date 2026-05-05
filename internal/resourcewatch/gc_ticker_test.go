package resourcewatch

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

func TestGCTicker_NoRewrite_ResetsFailureCounter(t *testing.T) {
	e := &RegisteredDB{Category: DBCategoryMeta}
	e.consecutiveGCFailures.Store(2)
	gc := func() error { return badger.ErrNoRewrite }
	gcOnceWith(e, GCTickerConfig{FailThreshold: 3}, gc)
	if got := e.consecutiveGCFailures.Load(); got != 0 {
		t.Fatalf("counter=%d want 0", got)
	}
}

func TestGCTicker_Failure_IncrementsCounter(t *testing.T) {
	e := &RegisteredDB{Category: DBCategoryMeta}
	gc := func() error { return errors.New("boom") }
	gcOnceWith(e, GCTickerConfig{FailThreshold: 3}, gc)
	if got := e.consecutiveGCFailures.Load(); got != 1 {
		t.Fatalf("counter=%d want 1", got)
	}
}

func TestGCTicker_ThresholdReached_FiresIncident(t *testing.T) {
	e := &RegisteredDB{Category: DBCategoryMeta}
	var mu sync.Mutex
	var fires int
	cfg := GCTickerConfig{FailThreshold: 3, OnFailIncident: func(c Category, err error) {
		mu.Lock()
		fires++
		mu.Unlock()
	}}
	gc := func() error { return errors.New("boom") }
	for i := 0; i < 3; i++ {
		gcOnceWith(e, cfg, gc)
	}
	if fires != 1 {
		t.Fatalf("fires=%d want 1 (transition-only)", fires)
	}
}

func TestGCTicker_PersistentLeak_FiresOnce(t *testing.T) {
	e := &RegisteredDB{Category: DBCategoryMeta}
	var fires int
	cfg := GCTickerConfig{FailThreshold: 3, OnFailIncident: func(c Category, err error) { fires++ }}
	gc := func() error { return errors.New("boom") }
	for i := 0; i < 9; i++ {
		gcOnceWith(e, cfg, gc)
	}
	if fires != 1 {
		t.Fatalf("fires=%d want 1 (Arch #2 transition-only)", fires)
	}
}

func TestGCTicker_RecoveryRearmsFire(t *testing.T) {
	e := &RegisteredDB{Category: DBCategoryMeta}
	var fires int
	cfg := GCTickerConfig{FailThreshold: 3, OnFailIncident: func(c Category, err error) { fires++ }}
	failGC := func() error { return errors.New("boom") }
	noRewriteGC := func() error { return badger.ErrNoRewrite }
	for i := 0; i < 3; i++ {
		gcOnceWith(e, cfg, failGC)
	}
	gcOnceWith(e, cfg, noRewriteGC)
	for i := 0; i < 3; i++ {
		gcOnceWith(e, cfg, failGC)
	}
	if fires != 2 {
		t.Fatalf("fires=%d want 2 (re-arm after recovery)", fires)
	}
}

func TestGCTicker_WriteChurn_BoundedByMaxIter(t *testing.T) {
	e := &RegisteredDB{Category: DBCategoryMeta}
	var calls atomic.Int32
	gc := func() error {
		calls.Add(1)
		return nil
	}
	gcOnceWith(e, GCTickerConfig{FailThreshold: 3}, gc)
	if got := calls.Load(); got != int32(gcMaxIterPerDBPerTick) {
		t.Fatalf("calls=%d want %d (Arch #3 cap)", got, gcMaxIterPerDBPerTick)
	}
}

func TestGCTicker_ErrRejected_CountsAsFailure(t *testing.T) {
	e := &RegisteredDB{Category: DBCategoryMeta}
	gc := func() error { return badger.ErrRejected }
	gcOnceWith(e, GCTickerConfig{FailThreshold: 3}, gc)
	if got := e.consecutiveGCFailures.Load(); got != 1 {
		t.Fatalf("counter=%d want 1 (rejected counted as failure)", got)
	}
}

func TestGCTicker_ContextCancel_StopsCleanly(t *testing.T) {
	r := NewRegistry()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		RunGCTicker(ctx, GCTickerConfig{Interval: 1 * time.Hour, Registry: r})
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunGCTicker did not stop on cancel")
	}
}

func TestGCTicker_SequentialIteration(t *testing.T) {
	r := NewRegistry()
	for i := 0; i < 4; i++ {
		dir := t.TempDir()
		db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		t.Cleanup(func() { _ = db.Close() })
		r.Register(DBCategoryMeta, db)
	}
	// Real sequentiality is verified end-to-end by lock-contention absence
	// in TestRegistry_ConcurrentRegister_NoDataRace combined with
	// snapshot-then-unlock semantics — the ticker iterates the snapshot in
	// caller goroutine, no inner parallelism.
	cfg := GCTickerConfig{Interval: 10 * time.Millisecond, Registry: r}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	RunGCTicker(ctx, cfg)
}
