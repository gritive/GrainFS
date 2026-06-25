package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/gritive/GrainFS/internal/policy"
)

// Operations is the S3-facing storage facade used by upper layers for
// meaningful storage actions. It owns optional capability lookup, safe fallback
// behavior, and wrapper ordering.
//
// Decision flow:
//
//	operation requested
//	    |
//	    +-- adapter exists on outer chain? ---- yes --> call adapter
//	    |
//	    no
//	    |
//	    +-- safe fallback exists? ------------ yes --> call fallback via Operations
//	    |
//	    no
//	    |
//	    +-- return UnsupportedOperation{Op, Reason}
//
// Capability plan caching: planForCall returns a cached operationsPlan
// validated against a single Generation() source in the wrapper chain. The
// only Backend that implements operationPlanGeneration is SwappableBackend
// (enforced at construction in NewOperations). Generation bumps on Swap
// invalidate the cache; cache reads are lock-free via atomic.Pointer.
type Operations struct {
	backend     Backend
	plan        atomic.Pointer[operationsPlan]
	planGen     atomic.Uint64
	aclPlan     atomic.Pointer[aclCapabilityPlan]
	aclPlanGen  atomic.Uint64           // independent of planGen; each cache invalidates itself
	genSource   operationPlanGeneration // nil = no SwappableBackend in chain → plan never invalidates
	policyStore *policy.CompiledPolicyStore
}

var _ Backend = (*Operations)(nil)

type operationsPlan struct {
	atomicACLPutter            AtomicACLPutter
	aclSetter                  ACLSetter
	copyObjectAccelerator      copyObjectAccelerator
	copier                     Copier
	bucketVersioner            BucketVersioner
	versionedGetter            VersionedGetter
	versionedHeader            VersionedHeader
	objectVersionLister        ObjectVersionLister
	objectVersionDeleter       ObjectVersionDeleter
	versionedSoftDeleter       VersionedSoftDeleter
	policyBackend              PolicyBackend
	deleteObjectVersionForUndo ObjectVersionDeleter
	tagsSetter                 ObjectTagsSetter
	tagsGetter                 ObjectTagsGetter
}

type BucketVersioner interface {
	SetBucketVersioning(bucket, state string) error
	GetBucketVersioning(bucket string) (string, error)
}

type VersionedGetter interface {
	GetObjectVersion(ctx context.Context, bucket, key, versionID string) (io.ReadCloser, *Object, error)
}

type VersionedHeader interface {
	HeadObjectVersion(ctx context.Context, bucket, key, versionID string) (*Object, error)
}

type ObjectVersionLister interface {
	ListObjectVersions(ctx context.Context, bucket, prefix string, maxKeys int) ([]*ObjectVersion, error)
}

type ObjectVersionDeleter interface {
	DeleteObjectVersion(bucket, key, versionID string) error
}

type VersionedSoftDeleter interface {
	DeleteObjectReturningMarker(bucket, key string) (string, error)
}

type PolicyBackend interface {
	GetBucketPolicy(bucket string) ([]byte, error)
	SetBucketPolicy(bucket string, policyJSON []byte) error
	DeleteBucketPolicy(bucket string) error
}

type OperationsOption func(*Operations)

func WithPolicyStore(store *policy.CompiledPolicyStore) OperationsOption {
	return func(o *Operations) {
		o.policyStore = store
	}
}

func NewOperations(backend Backend, opts ...OperationsOption) *Operations {
	o := &Operations{
		backend:   backend,
		genSource: findGenerationSource(backend),
	}
	plan := buildOperationsPlan(backend)
	o.plan.Store(&plan)
	if o.genSource != nil {
		o.planGen.Store(o.genSource.Generation())
	}
	for _, opt := range opts {
		opt(o)
	}
	if o.policyStore != nil {
		o.policyStore.SetLoader(o.loadCommittedBucketPolicy)
	}
	return o
}

// loadCommittedBucketPolicy reads a bucket's policy from the committed local
// replica for CompiledPolicyStore pull-on-miss. It bypasses the policy cache
// (reads the PolicyBackend directly) and normalizes outcomes so internal/policy
// stays free of any storage dependency:
//   - no policy backend, or ErrBucketNotFound → found=false, err=nil (allow, negative-cache)
//   - any other error (router/recovery/fault) → found=false, err set     (allow, uncached)
//   - bytes                                    → found=true             (compile)
func (o *Operations) loadCommittedBucketPolicy(bucket string) ([]byte, bool, error) {
	plan := o.planForCall()
	if plan.policyBackend == nil {
		return nil, false, nil
	}
	data, err := plan.policyBackend.GetBucketPolicy(bucket)
	if err != nil {
		if errors.Is(err, ErrBucketNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return data, true, nil
}

func (o *Operations) Backend() Backend {
	return o.backend
}

// buildOperationsPlan resolves the storage decorator chain's optional
// capabilities into an operationsPlan. Two distinct rules apply:
//   - The 13 capabilities in assignFirstWinsCapabilities are first-implementer-
//     across-the-whole-chain wins (order-independent).
//   - The copy pair (accelerator + copier) is OUTERMOST-ONLY; see
//     resolveCopyCapability.
func buildOperationsPlan(backend Backend) operationsPlan {
	var p operationsPlan
	for b := backend; b != nil; b = unwrapOperationBackend(b) {
		assignFirstWinsCapabilities(&p, b)
	}
	p.copyObjectAccelerator, p.copier = resolveCopyCapability(backend)
	return p
}

// assignFirstWinsCapabilities records, for each order-independent optional
// capability not yet found, the first implementing layer b. Called for every
// layer of the chain, so the outermost implementer of each capability wins.
func assignFirstWinsCapabilities(p *operationsPlan, b Backend) {
	if p.atomicACLPutter == nil {
		if v, ok := b.(AtomicACLPutter); ok {
			p.atomicACLPutter = v
		}
	}
	if p.aclSetter == nil {
		if v, ok := b.(ACLSetter); ok {
			p.aclSetter = v
		}
	}
	if p.bucketVersioner == nil {
		if v, ok := b.(BucketVersioner); ok {
			p.bucketVersioner = v
		}
	}
	if p.versionedGetter == nil {
		if v, ok := b.(VersionedGetter); ok {
			p.versionedGetter = v
		}
	}
	if p.versionedHeader == nil {
		if v, ok := b.(VersionedHeader); ok {
			p.versionedHeader = v
		}
	}
	if p.objectVersionLister == nil {
		if v, ok := b.(ObjectVersionLister); ok {
			p.objectVersionLister = v
		}
	}
	if p.objectVersionDeleter == nil {
		if v, ok := b.(ObjectVersionDeleter); ok {
			p.objectVersionDeleter = v
		}
	}
	if p.versionedSoftDeleter == nil {
		if v, ok := b.(VersionedSoftDeleter); ok {
			p.versionedSoftDeleter = v
		}
	}
	if p.policyBackend == nil {
		if v, ok := b.(PolicyBackend); ok {
			p.policyBackend = v
		}
	}
	// deleteObjectVersionForUndo probes the same ObjectVersionDeleter interface as
	// objectVersionDeleter but is a separate field: it skips any write-blocking
	// layer so an undo never routes through a decorator that rejects writes.
	if p.deleteObjectVersionForUndo == nil {
		if _, blocksWrites := b.(writeBlocker); !blocksWrites {
			if v, ok := b.(ObjectVersionDeleter); ok {
				p.deleteObjectVersionForUndo = v
			}
		}
	}
	if p.tagsSetter == nil {
		if v, ok := b.(ObjectTagsSetter); ok {
			p.tagsSetter = v
		}
	}
	if p.tagsGetter == nil {
		if v, ok := b.(ObjectTagsGetter); ok {
			p.tagsGetter = v
		}
	}
}

// resolveCopyCapability resolves the copy fast-path from the OUTERMOST backend
// only — deliberately not first-wins. The accelerated copy path bypasses inner
// decorators (encryption etc.), so it must only be taken when the topmost layer
// offers it; sourcing copier from a deeper layer would skip those decorators and
// corrupt data. The accelerator is preferred when the outermost layer implements
// both; at most one is non-nil.
//
// This is equivalent to the former inline copyMustUseOuterFallback flag: that
// flag was forced true at the end of the first iteration, so the old loop could
// only assign copy during iteration 1 (b == backend), accelerator first.
func resolveCopyCapability(backend Backend) (copyObjectAccelerator, Copier) {
	if v, ok := backend.(copyObjectAccelerator); ok {
		return v, nil
	}
	if v, ok := backend.(Copier); ok {
		return nil, v
	}
	return nil, nil
}

type operationPlanGeneration interface {
	Generation() uint64
}

// planForCall returns the cached storage decorator capability plan. Fast path
// is a single atomic load of the generation source and one atomic.Pointer
// load — no allocation, no lock.
func (o *Operations) planForCall() operationsPlan {
	current := o.currentGeneration()
	if cached := o.plan.Load(); cached != nil && o.planGen.Load() == current {
		return *cached
	}
	return o.rebuildPlan(current)
}

func (o *Operations) rebuildPlan(current uint64) operationsPlan {
	// Seqlock-style: bracket the rebuild with generation checks. If a swap
	// races us, retry. Bounded by the rate of Swap() which is rare. Only
	// touches its own (plan, planGen) pair — aclPlan tracks its own gen and
	// invalidates independently via aclPlanForCall.
	for {
		plan := buildOperationsPlan(o.backend)
		endGen := o.currentGeneration()
		if current == endGen {
			o.plan.Store(&plan)
			o.planGen.Store(current)
			return plan
		}
		current = endGen
	}
}

func (o *Operations) currentGeneration() uint64 {
	if o.genSource == nil {
		return 0
	}
	return o.genSource.Generation()
}

// findGenerationSource walks the chain and returns the single Generation()
// implementer. Panics if more than one is found — the cache invariant requires
// a single source so that bumps are observable as a single atomic.Uint64.
// Only SwappableBackend is expected to satisfy operationPlanGeneration.
func findGenerationSource(backend Backend) operationPlanGeneration {
	var found operationPlanGeneration
	for b := backend; b != nil; b = unwrapOperationBackend(b) {
		marker, ok := b.(operationPlanGeneration)
		if !ok {
			continue
		}
		if found != nil {
			panic(fmt.Sprintf("storage: more than one operationPlanGeneration source in chain (existing=%T, new=%T); only SwappableBackend may implement Generation()", found, marker))
		}
		found = marker
	}
	return found
}

func unwrapOperationBackend(backend Backend) Backend {
	type unwrapper interface {
		Unwrap() Backend
	}
	if u, ok := backend.(unwrapper); ok {
		return u.Unwrap()
	}
	return nil
}
