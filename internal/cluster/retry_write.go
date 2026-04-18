package cluster

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"
)

// ErrPermanent signals a non-retryable write failure. retryWriteShard gives up immediately.
var ErrPermanent = errors.New("permanent write error")

// retryWriteShard calls fn up to maxAttempts times with exponential backoff + ±20% jitter.
// Returns nil on success. Returns ErrPermanent immediately if fn returns ErrPermanent.
// Returns ctx.Err() if the context is cancelled during a backoff sleep.
func retryWriteShard(ctx context.Context, fn func() error, maxAttempts int, baseDelay time.Duration) error {
	var lastErr error
	for attempt := range maxAttempts {
		lastErr = fn()
		if lastErr == nil {
			return nil
		}
		if errors.Is(lastErr, ErrPermanent) {
			return lastErr
		}
		if attempt == maxAttempts-1 {
			break
		}
		// exponential backoff: baseDelay * 2^attempt, capped at 30s
		delay := baseDelay * (1 << attempt)
		const maxDelay = 30 * time.Second
		if delay > maxDelay {
			delay = maxDelay
		}
		// ±20% jitter
		jitter := float64(delay) * 0.2 * (rand.Float64()*2 - 1)
		sleep := time.Duration(float64(delay) + jitter)
		if sleep <= 0 {
			sleep = time.Millisecond
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleep):
		}
	}
	return lastErr
}
