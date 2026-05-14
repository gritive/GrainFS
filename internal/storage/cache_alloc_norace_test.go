//go:build !race

package storage

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// AllocsPerRun budgets run outside -race because race instrumentation changes
// heap escapes and allocation counts.
func TestCachedBackend_GetObjectCacheHitAllocBudget(t *testing.T) {
	cb, _ := newTestCachedBackend(t)

	require.NoError(t, cb.CreateBucket(context.Background(), "test"))
	_, err := cb.PutObject(context.Background(), "test", "key1", strings.NewReader("hello cache"), "text/plain")
	require.NoError(t, err)
	rc, _, err := cb.GetObject(context.Background(), "test", "key1")
	require.NoError(t, err)
	require.NoError(t, rc.Close())

	var closeErr error
	allocs := testing.AllocsPerRun(100, func() {
		rc, _, err := cb.GetObject(context.Background(), "test", "key1")
		if err != nil {
			panic(err)
		}
		closeErr = rc.Close()
	})

	require.NoError(t, closeErr)
	require.LessOrEqual(t, allocs, 2.0, "cache hits should not allocate a fresh reader or joined cache key")
}

func TestCachedBackend_HeadObjectCacheHitAllocBudget(t *testing.T) {
	cb, _ := newTestCachedBackend(t)

	require.NoError(t, cb.CreateBucket(context.Background(), "test"))
	_, err := cb.PutObject(context.Background(), "test", "key1", strings.NewReader("data"), "text/plain")
	require.NoError(t, err)
	rc, _, err := cb.GetObject(context.Background(), "test", "key1")
	require.NoError(t, err)
	require.NoError(t, rc.Close())

	allocs := testing.AllocsPerRun(100, func() {
		_, err := cb.HeadObject(context.Background(), "test", "key1")
		if err != nil {
			panic(err)
		}
	})

	require.LessOrEqual(t, allocs, 1.0, "metadata cache hits should avoid joined cache key allocation")
}
