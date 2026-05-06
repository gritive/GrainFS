package serveruntime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
)

// FilterEmpty drops empty entries produced by strings.Split. Used during
// runCluster bootstrap to clean up the comma-split --peers list so empty
// strings don't waste a gossip tick each.
func FilterEmpty(ss []string) []string {
	out := ss[:0]
	for _, s := range ss {
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

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

// CreateDefaultBucketWithRetry attempts CreateBucket with exponential
// backoff up to timeout. ErrBucketAlreadyExists is treated as success so
// concurrent startup of multiple peers does not race-fail.
func CreateDefaultBucketWithRetry(ctx context.Context, backend storage.Backend, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	backoff := 100 * time.Millisecond
	const maxBackoff = 2 * time.Second
	var lastErr error
	for {
		err := backend.CreateBucket(ctx, "default")
		if err == nil || errors.Is(err, storage.ErrBucketAlreadyExists) {
			return nil
		}
		lastErr = err
		if time.Now().After(deadline) {
			return fmt.Errorf("default bucket not created after %s: %w", timeout, lastErr)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, maxBackoff)
	}
}
