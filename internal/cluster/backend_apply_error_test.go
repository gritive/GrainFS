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
