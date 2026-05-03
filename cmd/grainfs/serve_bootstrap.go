package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
)

// filterEmpty drops empty peer entries produced by strings.Split.
func filterEmpty(ss []string) []string {
	out := ss[:0]
	for _, s := range ss {
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

func shouldCreateDefaultBucketOnStartup(peers []string, recoveryReadOnly bool) bool {
	return !recoveryReadOnly && len(peers) == 0
}

func metaProposalTargets(leader string, peers []string) []string {
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

func createDefaultBucketWithRetry(ctx context.Context, backend storage.Backend, timeout time.Duration) error {
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
