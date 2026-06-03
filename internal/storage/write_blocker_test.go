package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeWriteBlocker is a second backend decorator — distinct from
// *RecoveryWriteGate — that advertises the write-block capability. It exists to
// prove the 3 plan sites key off the capability marker, not the concrete
// RecoveryWriteGate type: a non-RecoveryWriteGate decorator that blocks writes
// must be treated identically.
//
// The fake is itself an ObjectVersionDeleter (mirroring *RecoveryWriteGate,
// which returns its block error from DeleteObjectVersion). Absent the capability
// skip, the plan loop would pick this erroring blocker as the undo / ACL-rollback
// deleter; the skip makes it fall through.
type fakeWriteBlocker struct {
	Backend
	inner Backend
	err   error
}

func (f *fakeWriteBlocker) Unwrap() Backend { return f.inner }

func (f *fakeWriteBlocker) writeBlockError() error { return f.err }

func (f *fakeWriteBlocker) DeleteObjectVersion(string, string, string) error { return f.err }

// sweepingBackend is the inner backend reached after the blocker is unwrapped.
// It implements OrphanMultipartSweeper so SweepOrphanMultiparts would reach it
// absent a gate, proving the blocker shadows it. It is deliberately NOT an
// ObjectVersionDeleter, so the undo / ACL-rollback plans yield nil once the
// blocker in front is skipped.
type sweepingBackend struct {
	Backend
}

func (d *sweepingBackend) SweepOrphanMultiparts(context.Context, time.Time) (OrphanMultipartSweepResult, error) {
	return OrphanMultipartSweepResult{Removed: 1}, nil
}

func TestWriteBlockerCapabilitySkipsUndoDeleter(t *testing.T) {
	sentinel := errors.New("fake block")
	inner := &sweepingBackend{}
	chain := &fakeWriteBlocker{inner: inner, err: sentinel}

	plan := buildOperationsPlan(chain)
	require.Nil(t, plan.deleteObjectVersionForUndo,
		"undo deleter must be skipped when a write-blocking decorator sits in front")
}

func TestWriteBlockerCapabilitySkipsACLRollbackDeleter(t *testing.T) {
	sentinel := errors.New("fake block")
	inner := &sweepingBackend{}
	chain := &fakeWriteBlocker{inner: inner, err: sentinel}

	plan := buildACLCapabilityPlan(chain)
	require.Nil(t, plan.rollback,
		"ACL rollback deleter must be skipped when a write-blocking decorator sits in front")
}

func TestWriteBlockerCapabilityReturnsErrFromSweep(t *testing.T) {
	sentinel := errors.New("fake block")
	inner := &sweepingBackend{}
	chain := &fakeWriteBlocker{inner: inner, err: sentinel}

	ops := &Operations{backend: chain}
	_, err := ops.SweepOrphanMultiparts(context.Background(), time.Now())
	require.ErrorIs(t, err, sentinel,
		"SweepOrphanMultiparts must surface the write-blocker's error verbatim")
}
