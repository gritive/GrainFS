package e2e

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWaitForWritableEndpoint_UsesPerAttemptTimeout(t *testing.T) {
	endpoints := []string{"node-a", "node-b", "node-c"}
	var calls int32

	start := time.Now()
	idx, err := waitForWritableEndpoint(
		context.Background(),
		endpoints,
		200*time.Millisecond,
		20*time.Millisecond,
		1*time.Millisecond,
		func(ctx context.Context, endpoint string) error {
			call := atomic.AddInt32(&calls, 1)
			if call < 3 {
				<-ctx.Done()
				return ctx.Err()
			}
			if endpoint != "node-c" {
				return fmt.Errorf("unexpected endpoint %q", endpoint)
			}
			return nil
		},
	)

	require.NoError(t, err)
	require.Equal(t, 2, idx)
	require.Equal(t, int32(3), atomic.LoadInt32(&calls))
	require.Less(t, time.Since(start), 500*time.Millisecond)
}

func TestWaitForWritableEndpoint_ReturnsErrorWhenAllEndpointsFail(t *testing.T) {
	endpoints := []string{"node-a", "node-b"}

	_, err := waitForWritableEndpoint(
		context.Background(),
		endpoints,
		25*time.Millisecond,
		5*time.Millisecond,
		1*time.Millisecond,
		func(ctx context.Context, endpoint string) error {
			<-ctx.Done()
			return errors.New("not leader")
		},
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "no writable endpoint found within")
}
