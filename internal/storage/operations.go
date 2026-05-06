package storage

import (
	"io"
	"sync"

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
type Operations struct {
	backend     Backend
	plan        operationsPlan
	planMu      sync.RWMutex
	planGen     []uint64
	policyStore *policy.CompiledPolicyStore
}

var _ Backend = (*Operations)(nil)

type operationsPlan struct {
	atomicACLPutter            AtomicACLPutter
	aclSetter                  ACLSetter
	copyObjectAdapter          CopyObjectAdapter
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
		backend: backend,
		plan:    buildOperationsPlan(backend),
		planGen: collectOperationsPlanGeneration(backend),
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
		if p.copyObjectAdapter == nil && !copyMustUseOuterFallback {
			if v, ok := b.(CopyObjectAdapter); ok {
				p.copyObjectAdapter = v
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

func (o *Operations) planForCall() operationsPlan {
	current := collectOperationsPlanGeneration(o.backend)

	o.planMu.RLock()
	if equalGenerationSnapshot(o.planGen, current) {
		plan := o.plan
		o.planMu.RUnlock()
		return plan
	}
	o.planMu.RUnlock()

	o.planMu.Lock()
	defer o.planMu.Unlock()

	current = collectOperationsPlanGeneration(o.backend)
	if !equalGenerationSnapshot(o.planGen, current) {
		o.plan = buildOperationsPlan(o.backend)
		o.planGen = current
	}
	return o.plan
}

func collectOperationsPlanGeneration(backend Backend) []uint64 {
	var gen []uint64
	for b := backend; b != nil; b = unwrapOperationBackend(b) {
		if marker, ok := b.(operationPlanGeneration); ok {
			gen = append(gen, marker.Generation())
		}
	}
	return gen
}

func equalGenerationSnapshot(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
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
