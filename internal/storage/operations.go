package storage

import (
	"io"

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
	policyStore *policy.CompiledPolicyStore
}

type operationsPlan struct {
	atomicACLPutter            AtomicACLPutter
	aclSetter                  ACLSetter
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
		if p.copier == nil && !copyMustUseOuterFallback {
			if v, ok := b.(Copier); ok {
				p.copier = v
			}
		}
		if _, ok := b.(*CachedBackend); ok {
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

func unwrapOperationBackend(backend Backend) Backend {
	type unwrapper interface {
		Unwrap() Backend
	}
	if u, ok := backend.(unwrapper); ok {
		return u.Unwrap()
	}
	return nil
}
