package serveruntime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
)

// ShouldCreateDefaultBucketOnStartup reports whether the singleton path
// should auto-create the "default" bucket. In cluster mode this is a
// cluster-wide metadata operation and must be driven by an explicit
// client/API action; recovery-read-only also disables it.
func ShouldCreateDefaultBucketOnStartup(peers []string, recoveryReadOnly bool) bool {
	return !recoveryReadOnly && len(peers) == 0
}

// MetaProposalTargets returns the leader-first ordered peer list for
// meta-raft proposals. Caller usually wants leader to receive the proposal
// first so non-leader followers can short-circuit on rejection.
func MetaProposalTargets(leader string, peers []string) []string {
	if leader == "" {
		return peers
	}
	targets := make([]string, 0, len(peers)+1)
	targets = append(targets, leader)
	for _, peer := range peers {
		if peer != leader {
			targets = append(targets, peer)
		}
	}
	return targets
}

// reservedBucketSeeder is an optional capability that backends may implement
// to allow seeding reserved bucket names from the bootstrap path.
// Public-API callers must use storage.Backend.CreateBucket which enforces
// the reserved-name guard in the FSM.
type reservedBucketSeeder interface {
	CreateBucketBypassReserved(ctx context.Context, bucket string) error
}

// backendUnwrapper is the optional capability that wrapping backends expose
// so the unwrap chain can be traversed to find a reservedBucketSeeder.
type backendUnwrapper interface {
	Unwrap() storage.Backend
}

// findReservedSeeder traverses the Unwrap chain of backend to locate the first
// layer that implements reservedBucketSeeder.
func findReservedSeeder(backend storage.Backend) (reservedBucketSeeder, bool) {
	for b := backend; b != nil; {
		if s, ok := b.(reservedBucketSeeder); ok {
			return s, true
		}
		if u, ok := b.(backendUnwrapper); ok {
			b = u.Unwrap()
		} else {
			break
		}
	}
	return nil, false
}

// seedReservedBucket creates a reserved bucket via the bypass path if the
// backend (or any layer in its Unwrap chain) supports it, falling back to
// the regular CreateBucket path for backends without a reserved-name guard
// (e.g. LocalBackend in unit-test fixtures).
// ErrBucketAlreadyExists is treated as success (idempotent).
func seedReservedBucket(ctx context.Context, backend storage.Backend, bucket string) error {
	if seeder, ok := findReservedSeeder(backend); ok {
		err := seeder.CreateBucketBypassReserved(ctx, bucket)
		if err == nil || errors.Is(err, storage.ErrBucketAlreadyExists) {
			return nil
		}
		return err
	}
	// Fallback: no layer in the chain implements bypass (e.g. LocalBackend which
	// has no reserved-name guard in its own FSM path).
	err := backend.CreateBucket(ctx, bucket)
	if err == nil || errors.Is(err, storage.ErrBucketAlreadyExists) {
		return nil
	}
	return err
}

// CreateDefaultBucketWithRetry attempts to seed the "default" and "_grainfs"
// reserved buckets with exponential backoff up to timeout.
// ErrBucketAlreadyExists is treated as success so concurrent startup of
// multiple peers does not race-fail.
func CreateDefaultBucketWithRetry(ctx context.Context, backend storage.Backend, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	backoff := 100 * time.Millisecond
	const maxBackoff = 2 * time.Second
	var lastErr error
	for {
		if err := seedReservedBucket(ctx, backend, "default"); err != nil {
			lastErr = err
		} else if err := seedReservedBucket(ctx, backend, "_grainfs"); err != nil {
			lastErr = err
		} else {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("reserved buckets not seeded after %s: %w", timeout, lastErr)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, maxBackoff)
	}
}
