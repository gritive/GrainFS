package storage_test

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSwappableBackend_ForwardsToInner(t *testing.T) {
	inner := cluster.NewSingletonBackendForTest(t)
	sb := storage.NewSwappableBackend(inner)

	require.NoError(t, sb.CreateBucket(context.Background(), "test"))
	require.NoError(t, sb.HeadBucket(context.Background(), "test"))

	_, err := sb.PutObject(context.Background(), "test", "hello.txt", strings.NewReader("hello"), "text/plain")
	require.NoError(t, err)

	var gotData string
	var gotSize int64
	require.Eventually(t, func() bool {
		rc, obj, err := sb.GetObject(context.Background(), "test", "hello.txt")
		if err != nil {
			return false
		}
		defer rc.Close()
		data, _ := io.ReadAll(rc)
		gotData = string(data)
		gotSize = obj.Size
		return true
	}, 2*time.Second, 10*time.Millisecond)
	assert.Equal(t, "hello", gotData)
	assert.Equal(t, int64(5), gotSize)
}

func TestSwappableBackend_SwapChangesBackend(t *testing.T) {
	inner1 := cluster.NewSingletonBackendForTest(t)
	inner2 := cluster.NewSingletonBackendForTest(t)

	sb := storage.NewSwappableBackend(inner1)

	// Create bucket in first backend
	require.NoError(t, sb.CreateBucket(context.Background(), "first"))

	// Swap to second backend
	sb.Swap(inner2)

	// Old bucket should not be visible in new backend
	assert.ErrorIs(t, sb.HeadBucket(context.Background(), "first"), storage.ErrBucketNotFound)

	// Create in new backend
	require.NoError(t, sb.CreateBucket(context.Background(), "second"))
	require.NoError(t, sb.HeadBucket(context.Background(), "second"))
}

func TestSwappableBackend_ConcurrentSwapSafe(t *testing.T) {
	inner := cluster.NewSingletonBackendForTest(t)
	sb := storage.NewSwappableBackend(inner)
	require.NoError(t, sb.CreateBucket(context.Background(), "test"))

	// Pre-create 10 backends before launching goroutines: NewSingletonBackendForTest
	// calls t.Fatalf on error, which is not safe to call from a non-test goroutine.
	extras := make([]*cluster.DistributedBackend, 10)
	for i := range extras {
		extras[i] = cluster.NewSingletonBackendForTest(t)
	}

	var wg sync.WaitGroup
	// Concurrent reads while swapping
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = sb.HeadBucket(context.Background(), "test")
			}
		}()
	}

	// Concurrent swaps using the pre-created backends
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 10; j++ {
			sb.Swap(extras[j])
		}
	}()

	wg.Wait()
}
