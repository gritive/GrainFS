package cluster

import (
	"errors"
	"testing"
)

func TestDistributedBackendApplyErrorExportedCleanupOnRead(t *testing.T) {
	b := &DistributedBackend{} // minimal — applyErrs는 nil 시작, recordApplyResult가 lazy init
	b.recordApplyResult(42, errors.New("test-err"))

	err := b.ApplyError(42)
	if err == nil || err.Error() != "test-err" {
		t.Fatalf("first read: got %v", err)
	}
	if e := b.ApplyError(42); e != nil {
		t.Fatalf("second read: got %v, want nil (cleanup)", e)
	}
}

func TestDistributedBackendRecordApplyResultIgnoresNil(t *testing.T) {
	b := &DistributedBackend{}
	b.recordApplyResult(1, nil) // should be no-op
	if e := b.ApplyError(1); e != nil {
		t.Fatalf("nil should not be stored: %v", e)
	}
}

func TestDistributedBackendRecordApplyResult1024Cleanup(t *testing.T) {
	b := &DistributedBackend{}
	b.recordApplyResult(1, errors.New("old"))
	b.recordApplyResult(2000, errors.New("new"))
	if e := b.ApplyError(1); e != nil {
		t.Fatalf("old should be cleaned, got %v", e)
	}
	if e := b.ApplyError(2000); e == nil || e.Error() != "new" {
		t.Fatalf("new should remain: %v", e)
	}
}

// Red 11c: apply loop ordering — recordApplyResult must be set BEFORE
// lastApplied.Store so the propose loop, on observing lastApplied >= idx,
// always sees the apply error (race-free). This test simulates the apply
// loop's side effects on the backend in the documented order and then
// verifies the read-side observation.
func TestDistributedBackendProposeReceivesApplyError(t *testing.T) {
	b := &DistributedBackend{}
	sentinel := errors.New("test-sentinel")

	// Simulate apply loop ordering: recordApplyResult first, then lastApplied.Store.
	const idx uint64 = 42
	b.recordApplyResult(idx, sentinel)
	b.lastApplied.Store(50) // any value >= idx

	// propose's leader path polls until lastApplied.Load() >= idx then reads
	// ApplyError(idx). Verify that read consumes the sentinel.
	if !(b.lastApplied.Load() >= idx) {
		t.Fatalf("lastApplied should be >= %d", idx)
	}
	got := b.ApplyError(idx)
	if !errors.Is(got, sentinel) {
		t.Fatalf("expected propagated apply error, got %v", got)
	}
}
