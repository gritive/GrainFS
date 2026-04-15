package storage

import (
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSwappableBackend_ForwardsToInner(t *testing.T) {
	dir := t.TempDir()
	inner, err := NewLocalBackend(dir)
	require.NoError(t, err)

	sb := NewSwappableBackend(inner)

	require.NoError(t, sb.CreateBucket("test"))
	require.NoError(t, sb.HeadBucket("test"))

	_, err = sb.PutObject("test", "hello.txt", strings.NewReader("hello"), "text/plain")
	require.NoError(t, err)

	rc, obj, err := sb.GetObject("test", "hello.txt")
	require.NoError(t, err)
	defer rc.Close()

	data, _ := io.ReadAll(rc)
	assert.Equal(t, "hello", string(data))
	assert.Equal(t, int64(5), obj.Size)
}

func TestSwappableBackend_SwapChangesBackend(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()
	inner1, _ := NewLocalBackend(dir1)
	inner2, _ := NewLocalBackend(dir2)

	sb := NewSwappableBackend(inner1)

	// Create bucket in first backend
	require.NoError(t, sb.CreateBucket("first"))

	// Swap to second backend
	sb.Swap(inner2)

	// Old bucket should not be visible in new backend
	assert.ErrorIs(t, sb.HeadBucket("first"), ErrBucketNotFound)

	// Create in new backend
	require.NoError(t, sb.CreateBucket("second"))
	require.NoError(t, sb.HeadBucket("second"))
}

func TestSwappableBackend_ConcurrentSwapSafe(t *testing.T) {
	dir := t.TempDir()
	inner, _ := NewLocalBackend(dir)
	sb := NewSwappableBackend(inner)
	require.NoError(t, sb.CreateBucket("test"))

	var wg sync.WaitGroup
	// Concurrent reads while swapping
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = sb.HeadBucket("test")
			}
		}()
	}

	// Concurrent swaps
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 10; j++ {
			dir := t.TempDir()
			b, _ := NewLocalBackend(dir)
			sb.Swap(b)
		}
	}()

	wg.Wait()
}
