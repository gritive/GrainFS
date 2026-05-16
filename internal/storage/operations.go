package storage

import (
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
}

type BucketVersioner interface {
	SetBucketVersioning(bucket, state string) error
	GetBucketVersioning(bucket string) (string, error)
}

type VersionedGetter interface {
	GetObjectVersion(bucket, key, versionID string) (io.ReadCloser, *Object, error)
}

type VersionedHeader interface {
	HeadObjectVersion(bucket, key, versionID string) (*Object, error)
}

type ObjectVersionLister interface {
	ListObjectVersions(bucket, prefix string, maxKeys int) ([]*ObjectVersion, error)
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
	return o
}

func (o *Operations) Backend() Backend {
	return o.backend
}

func buildOperationsPlan(backend Backend) operationsPlan {
	var p operationsPlan
	copyMustUseOuterFallback := false
	for b := backend; b != nil; b = unwrapOperationBackend(b) {
		next := unwrapOperationBackend(b)
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
		if p.copyObjectAccelerator == nil && !copyMustUseOuterFallback {
			if v, ok := b.(copyObjectAccelerator); ok {
				p.copyObjectAccelerator = v
				copyMustUseOuterFallback = true
			}
		}
		if p.copier == nil && !copyMustUseOuterFallback {
			if v, ok := b.(Copier); ok {
				p.copier = v
				copyMustUseOuterFallback = true
			}
		}
		if _, ok := b.(*CachedBackend); ok {
			copyMustUseOuterFallback = true
		}
		if next != nil {
			copyMustUseOuterFallback = true
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
		if p.deleteObjectVersionForUndo == nil {
			if _, blocksWrites := b.(*RecoveryWriteGate); !blocksWrites {
				if v, ok := b.(ObjectVersionDeleter); ok {
					p.deleteObjectVersionForUndo = v
				}
			}
		}
	}
	return p
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
