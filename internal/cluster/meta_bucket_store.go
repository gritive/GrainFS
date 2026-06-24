package cluster

import (
	"context"
	"errors"

	"github.com/gritive/GrainFS/internal/raft"
)

// MetaBucketStore is the cluster-wide bucket metadata seam: it provides
// write operations (create/delete/versioning/policy) and read operations
// (local snapshot via Record, linearized via RecordLinearized) for the
// meta-Raft bucket namespace.
//
// Implemented by *ForwardingBucketAssigner, which already holds the local
// MetaRaft reference needed to propose commands and read FSM state.
type MetaBucketStore interface {
	// CreateBucket proposes a CreateBucket command to the meta-Raft cluster.
	CreateBucket(ctx context.Context, bucket, groupID string, bypassReserved bool) error
	// DeleteBucket proposes a DeleteBucket command to the meta-Raft cluster.
	DeleteBucket(ctx context.Context, bucket string) error
	// SetVersioning proposes a SetBucketVersioning command to the meta-Raft cluster.
	SetVersioning(ctx context.Context, bucket, state string) error
	// SetPolicy proposes a SetBucketPolicy command to the meta-Raft cluster.
	SetPolicy(ctx context.Context, bucket string, policy []byte) error
	// DeletePolicy proposes a DeleteBucketPolicy command to the meta-Raft cluster.
	DeletePolicy(ctx context.Context, bucket string) error

	// Record returns a local (possibly stale) snapshot of the bucket record from
	// the in-memory meta-FSM. Fast, non-blocking.
	Record(bucket string) (BucketRecord, bool)

	// RecordLinearized returns the bucket record after fencing against the
	// meta-Raft committed index, ensuring a linearizable read on a leader.
	// If the meta-Raft has no leader (raft.ErrNotLeader from ReadIndex), it
	// degrades to the local snapshot and returns nil error (availability over
	// strict consistency). Other ReadIndex errors are returned verbatim.
	RecordLinearized(ctx context.Context, bucket string) (BucketRecord, bool, error)
}

// metaRaftBucketBackend is the set of MetaRaft methods used by the
// MetaBucketStore implementation. *MetaRaft satisfies this interface; tests
// can substitute a lightweight fake.
type metaRaftBucketBackend interface {
	ProposeCreateBucket(ctx context.Context, bucket, groupID string, bypassReserved bool) error
	ProposeDeleteBucket(ctx context.Context, bucket string) error
	ProposeSetBucketVersioning(ctx context.Context, bucket, state string) error
	ProposeSetBucketPolicy(ctx context.Context, bucket string, policy []byte) error
	ProposeDeleteBucketPolicy(ctx context.Context, bucket string) error
	FSM() *MetaFSM
	ReadIndex(ctx context.Context) (uint64, error)
	WaitApplied(ctx context.Context, index uint64) error
}

// metaBucketStoreImpl implements MetaBucketStore against a metaRaftBucketBackend.
// Production: instantiated via ForwardingBucketAssigner.MetaBucketStore();
// tests: instantiated via newMetaBucketStoreFromIface with a fake backend.
type metaBucketStoreImpl struct {
	meta metaRaftBucketBackend
}

// newMetaBucketStoreFromIface constructs a MetaBucketStore from any
// metaRaftBucketBackend. Used in tests to substitute a lightweight fake for
// *MetaRaft.
func newMetaBucketStoreFromIface(meta metaRaftBucketBackend) MetaBucketStore {
	return &metaBucketStoreImpl{meta: meta}
}

func (s *metaBucketStoreImpl) CreateBucket(ctx context.Context, bucket, groupID string, bypassReserved bool) error {
	return s.meta.ProposeCreateBucket(ctx, bucket, groupID, bypassReserved)
}

func (s *metaBucketStoreImpl) DeleteBucket(ctx context.Context, bucket string) error {
	return s.meta.ProposeDeleteBucket(ctx, bucket)
}

func (s *metaBucketStoreImpl) SetVersioning(ctx context.Context, bucket, state string) error {
	return s.meta.ProposeSetBucketVersioning(ctx, bucket, state)
}

func (s *metaBucketStoreImpl) SetPolicy(ctx context.Context, bucket string, policy []byte) error {
	return s.meta.ProposeSetBucketPolicy(ctx, bucket, policy)
}

func (s *metaBucketStoreImpl) DeletePolicy(ctx context.Context, bucket string) error {
	return s.meta.ProposeDeleteBucketPolicy(ctx, bucket)
}

func (s *metaBucketStoreImpl) Record(bucket string) (BucketRecord, bool) {
	return s.meta.FSM().BucketRecord(bucket)
}

func (s *metaBucketStoreImpl) RecordLinearized(ctx context.Context, bucket string) (BucketRecord, bool, error) {
	idx, err := s.meta.ReadIndex(ctx)
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			// Leaderless: degrade to local snapshot, return nil error for availability.
			rec, ok := s.meta.FSM().BucketRecord(bucket)
			return rec, ok, nil
		}
		return BucketRecord{}, false, err
	}
	if err := s.meta.WaitApplied(ctx, idx); err != nil {
		return BucketRecord{}, false, err
	}
	rec, ok := s.meta.FSM().BucketRecord(bucket)
	return rec, ok, nil
}

// MetaBucketStore returns a MetaBucketStore view of this ForwardingBucketAssigner.
// The returned store routes all propose calls through the same local MetaRaft that
// backs ProposeBucketAssignment, and reads from its FSM.
func (a *ForwardingBucketAssigner) MetaBucketStore() MetaBucketStore {
	return newMetaBucketStoreFromIface(a.local)
}
