package e2e

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/stretchr/testify/require"
)

func registerWaitForWritableEndpointUsesPerAttemptTimeout() {
	ginkgo.It("uses the per-attempt timeout", func() {
		t := ginkgo.GinkgoTB()
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
	})
}

func registerWaitForWritableEndpointReturnsErrorWhenAllEndpointsFail() {
	ginkgo.It("returns an error when all endpoints fail", func() {
		t := ginkgo.GinkgoTB()
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
	})
}

var _ = ginkgo.Describe("Wait for writable endpoint", func() {
	ginkgo.Context("SingleNode", func() {
		registerWaitForWritableEndpointUsesPerAttemptTimeout()
		registerWaitForWritableEndpointReturnsErrorWhenAllEndpointsFail()
	})
})
