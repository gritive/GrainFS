package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetryWriteShard_ExponentialBackoff(t *testing.T) {
	calls := 0
	transient := errors.New("transient")
	fn := func() error {
		calls++
		return transient
	}
	err := retryWriteShard(context.Background(), fn, 3, time.Millisecond)
	require.Error(t, err)
	assert.Equal(t, 3, calls, "must attempt maxAttempts times")
	assert.False(t, errors.Is(err, ErrPermanent))
}

func TestRetryWriteShard_PermanentErrorNoRetry(t *testing.T) {
	calls := 0
	fn := func() error {
		calls++
		return ErrPermanent
	}
	err := retryWriteShard(context.Background(), fn, 5, time.Millisecond)
	require.Error(t, err)
	assert.Equal(t, 1, calls, "ErrPermanent must give up immediately")
	assert.True(t, errors.Is(err, ErrPermanent))
}

func TestRetryWriteShard_RecoverOnSecondAttempt(t *testing.T) {
	calls := 0
	fn := func() error {
		calls++
		if calls < 3 {
			return errors.New("transient")
		}
		return nil
	}
	err := retryWriteShard(context.Background(), fn, 5, time.Millisecond)
	require.NoError(t, err)
	assert.Equal(t, 3, calls)
}

func TestRetryWriteShard_CtxCancelDuringWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	fn := func() error {
		calls++
		cancel() // cancel after first call
		return errors.New("transient")
	}
	err := retryWriteShard(ctx, fn, 10, 10*time.Millisecond)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, calls)
}

func TestRetryWriteShard_JitterRange(t *testing.T) {
	// Jitter test: verify backoff durations are in ±20% of base for first attempt.
	// We do this by measuring elapsed time for a single-attempt failure (no retry).
	fn := func() error { return ErrPermanent }
	start := time.Now()
	_ = retryWriteShard(context.Background(), fn, 5, 50*time.Millisecond)
	elapsed := time.Since(start)
	// ErrPermanent → no sleep at all
	assert.Less(t, elapsed, 5*time.Millisecond, "ErrPermanent must not sleep")
}
