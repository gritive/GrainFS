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

// --- buildOperationsPlan capability-resolution characterization tests ---
//
// These pin the two distinct resolution rules buildOperationsPlan enforces:
//   - copy capability (copyObjectAccelerator + Copier) is OUTERMOST-ONLY, so the
//     fast copy path cannot bypass inner decorators (encryption etc.).
//   - the remaining capability probes are first-implementer-across-chain wins.

// copierBackend implements Copier only. It is a pass-through decorator so it can
// sit at either layer of a chain.
type copierBackend struct {
	Backend
	inner Backend
}

func (c *copierBackend) Unwrap() Backend { return c.inner }

func (c *copierBackend) CopyObject(string, string, string, string) (*Object, error) {
	return &Object{}, nil
}

// accelCopierBackend implements BOTH copyObjectAccelerator and Copier, to prove
// the accelerator is preferred when one layer offers both.
type accelCopierBackend struct {
	Backend
	inner Backend
}

func (a *accelCopierBackend) Unwrap() Backend { return a.inner }

func (a *accelCopierBackend) CopyObject(string, string, string, string) (*Object, error) {
	return &Object{}, nil
}

func (a *accelCopierBackend) CopyObjectWithRequest(context.Context, CopyObjectAccelerationRequest) (*Object, error) {
	return &Object{}, nil
}

// (versioningBackend, an existing BucketVersioner fake from
// operations_versioning_test.go, is reused below as the deep layer.)

// plainBackend is a pass-through decorator implementing none of the probed
// capabilities. The embedded Backend interface is nil at runtime, but the fakes
// never invoke base methods — only the decorator-walk type assertions matter.
type plainBackend struct {
	Backend
	inner Backend
}

func (p *plainBackend) Unwrap() Backend { return p.inner }

// Compile-time guards: these make the nil-plan assertions below load-bearing. If
// a fake silently failed to satisfy its capability interface (e.g. wrong method
// signature), the nil result would be vacuous and could never catch the
// naive walk-the-chain regression. Pinning satisfaction here keeps the tests
// discriminating.
var (
	_ Copier                = (*copierBackend)(nil)
	_ Copier                = (*accelCopierBackend)(nil)
	_ copyObjectAccelerator = (*accelCopierBackend)(nil)
)

// TestBuildOperationsPlanCopyIsOutermostOnly is the discriminating case: a
// non-copy outer decorator in front of a Copier inner layer must yield NO copy
// capability. Copy is restricted to the outermost backend; a first-wins walk
// would (incorrectly) source copier from the deep layer.
func TestBuildOperationsPlanCopyIsOutermostOnly(t *testing.T) {
	inner := &copierBackend{}
	chain := &plainBackend{inner: inner}

	plan := buildOperationsPlan(chain)
	require.Nil(t, plan.copier,
		"copier must be outermost-only: a deep-layer Copier behind a non-copy outer layer must not be sourced")
	require.Nil(t, plan.copyObjectAccelerator,
		"accelerator must be outermost-only")
}

// TestBuildOperationsPlanAcceleratorPreferredOverCopier pins that, on the
// outermost layer offering both, the accelerator wins and copier stays nil.
func TestBuildOperationsPlanAcceleratorPreferredOverCopier(t *testing.T) {
	chain := &accelCopierBackend{}

	plan := buildOperationsPlan(chain)
	require.NotNil(t, plan.copyObjectAccelerator,
		"accelerator must be selected when the outermost layer implements it")
	require.Nil(t, plan.copier,
		"copier must stay nil when the accelerator is selected on the same layer")
}

// TestBuildOperationsPlanCopierDiscoveredOnOutermost pins the normal path: the
// outermost layer implementing Copier only is discovered.
func TestBuildOperationsPlanCopierDiscoveredOnOutermost(t *testing.T) {
	chain := &copierBackend{}

	plan := buildOperationsPlan(chain)
	require.NotNil(t, plan.copier,
		"copier must be discovered when the outermost layer implements it")
	require.Nil(t, plan.copyObjectAccelerator,
		"accelerator must stay nil when only Copier is implemented")
}

// TestBuildOperationsPlanFirstWinsForDeepLayerProbe guards against
// over-correcting the copy fix into the order-independent probes: BucketVersioner
// (and the other 12 simple probes) must still be discovered on a DEEP layer.
func TestBuildOperationsPlanFirstWinsForDeepLayerProbe(t *testing.T) {
	var inner Backend = &versioningBackend{}
	chain := &plainBackend{inner: inner}

	plan := buildOperationsPlan(chain)
	require.NotNil(t, plan.bucketVersioner,
		"bucketVersioner must remain first-wins-across-chain: a deep-layer implementer must be discovered")
}
