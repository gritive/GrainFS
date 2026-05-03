package cluster

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/resourcewatch"
)

func TestPutObjectStreamingNxPreservesETagAndVersions(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "b"))

	first := strings.Repeat("a", 1024)
	o1, err := b.PutObject(context.Background(), "b", "k", strings.NewReader(first), "text/plain")
	require.NoError(t, err)

	second := strings.Repeat("b", 2048)
	o2, err := b.PutObject(context.Background(), "b", "k", strings.NewReader(second), "text/plain")
	require.NoError(t, err)

	sum := md5.Sum([]byte(second))
	require.Equal(t, hex.EncodeToString(sum[:]), o2.ETag)
	require.NotEqual(t, o1.VersionID, o2.VersionID)

	rc, got, err := b.GetObject(context.Background(), "b", "k")
	require.NoError(t, err)
	defer rc.Close()
	data, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, second, string(data))
	require.Equal(t, o2.VersionID, got.VersionID)

	rc, got, err = b.GetObjectVersion("b", "k", o1.VersionID)
	require.NoError(t, err)
	defer rc.Close()
	data, err = io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, first, string(data))
	require.Equal(t, o1.VersionID, got.VersionID)
}

func TestPutObjectStreamingNxLargeBodyBoundedAllocation(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "b"))
	payload := bytes.Repeat([]byte("x"), 16<<20)

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := b.PutObject(context.Background(), "b", "large", bytes.NewReader(payload), "application/octet-stream")
	require.NoError(t, err)
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(8<<20))
}

func TestPutObjectStreamingNxDoesNotLeakFDs(t *testing.T) {
	provider := resourcewatch.NewFDProvider(resourcewatch.FDProviderOptions{})

	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "b"))
	before, err := provider.Snapshot(context.Background())
	if err != nil {
		t.Skipf("fd provider unavailable: %v", err)
	}

	payload := bytes.Repeat([]byte("x"), 1<<20)
	for i := 0; i < 8; i++ {
		_, err := b.PutObject(context.Background(), "b", fmt.Sprintf("large-%d", i), bytes.NewReader(payload), "application/octet-stream")
		require.NoError(t, err)
	}

	after, err := provider.Snapshot(context.Background())
	require.NoError(t, err)
	require.LessOrEqual(t, after.Open, before.Open+4, "spooling PUT path should not leave object or temp file descriptors open")
}
