package lifecycle

import (
	"context"
	"time"
)

// Proposer is the seam between Service and the cluster meta-Raft proposer.
// internal/cluster provides the adapter.
type Proposer interface {
	ProposeLifecyclePut(ctx context.Context, bucket string, raw []byte) error
	ProposeLifecycleDelete(ctx context.Context, bucket string) error
}

// LeadershipSignal is the seam between Service and the Raft node. It does not
// expose raft.Event so lifecycle does not depend on the raft package.
//
// Subscribe returns a channel that is signalled on every leader-state change.
// The returned cancel func must be called to release the subscription.
type LeadershipSignal interface {
	IsLeader() bool
	Subscribe() (events <-chan struct{}, cancel func())
}

// Service is the deep module for the Bucket Lifecycle Policy domain. It owns
// validation, replication via Proposer, persistence in Store, and the leader-
// only executor. Callers do not reach past this interface.
type Service struct {
	store      *Store
	proposer   Proposer
	leadership LeadershipSignal
	backend    Scrubbable    // for executor; may be nil in unit tests
	deleter    ObjectDeleter // for executor; may be nil in unit tests
	interval   time.Duration
}

// NewService wires the service. backend/deleter may be nil for tests that do
// not exercise Run.
func NewService(store *Store, prop Proposer, lead LeadershipSignal, backend Scrubbable, deleter ObjectDeleter, interval time.Duration) *Service {
	return &Service{
		store:      store,
		proposer:   prop,
		leadership: lead,
		backend:    backend,
		deleter:    deleter,
		interval:   interval,
	}
}

// Enabled reports whether the lifecycle service is active. With the service
// wired, it is always true; the boot-disabled case is represented by a nil
// *Service in callers, which is the single source of truth.
func (s *Service) Enabled() bool { return s != nil }

// Get returns the lifecycle configuration for bucket, or (nil, nil) if not set.
func (s *Service) Get(bucket string) (*LifecycleConfiguration, error) {
	return s.store.Get(bucket)
}

// GetRaw returns the raw S3 wire XML bytes for bucket, or (nil, nil) if not
// set. Used by S3 GET handlers to preserve operator round-trip byte-for-byte
// (ADR 0011).
func (s *Service) GetRaw(bucket string) ([]byte, error) {
	return s.store.GetRaw(bucket)
}
