package cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestStreamEncoderPool_ConcurrentRoundTrip is the concurrency-safety gate for
// sharing one cached reedsolomon stream encoder across PUTs. Each object is
// ≥4 MiB so its per-data-shard size exceeds defaultECStreamBlockSize and the
// stream block size clamps to the 1 MiB default — the cacheable band that
// spoolECShards reuses. N concurrent PUTs hammer the SAME shared encoder; every
// object must round-trip byte-for-byte (a data race on the shared encoder's
// scratch would corrupt a shard and fail the EC read-back). Must stay green
// under -race before AND after the pooling change.
func TestStreamEncoderPool_ConcurrentRoundTrip(t *testing.T) {
	bk := newECBenchmarkBackend(t)
	ctx := context.Background()
	require.NoError(t, bk.CreateBucket(ctx, "b"))

	const n = 16
	objs := make([][]byte, n)
	for i := range objs {
		data := make([]byte, 5<<20) // 5 MiB → 4 data shards → >1 MiB/shard → default clamp
		for j := range data {
			data[j] = byte(i*7 + j*131 + 3) // deterministic, distinct per object
		}
		objs[i] = data
	}

	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = bk.PutObject(ctx, "b", fmt.Sprintf("k%d", i), bytes.NewReader(objs[i]), "application/octet-stream")
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		require.NoError(t, err, "concurrent PUT %d", i)
	}

	for i := 0; i < n; i++ {
		rc, _, err := bk.GetObject(ctx, "b", fmt.Sprintf("k%d", i))
		require.NoError(t, err)
		got, err := io.ReadAll(rc)
		_ = rc.Close()
		require.NoError(t, err)
		require.True(t, bytes.Equal(objs[i], got), "object %d corrupted (shared stream-encoder race?)", i)
	}
}
