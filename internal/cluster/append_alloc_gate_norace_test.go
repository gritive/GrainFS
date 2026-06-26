//go:build !race

package cluster

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClusterAppendAllocPerAppendStaysFlat(t *testing.T) {
	bk := newECBenchmarkBackend(t)
	bk.SetCoalesceConfig(CoalesceConfig{
		SegmentCount:    1 << 30,
		SizeBytes:       1 << 60,
		IdleTimeout:     1 << 60,
		CleanupInterval: 0,
		SizeCapBytes:    5 << 40,
	})
	require.NoError(t, bk.CreateBucket(context.Background(), "bench"))

	got := measureClusterAppendAllocs(bk)
	assertClusterAppendAllocSlopeFlat(t, got)
}

func measureClusterAppendAllocs(bk *DistributedBackend) map[int]float64 {
	const chunk = 64 << 10
	ctx := context.Background()
	data := bytes.Repeat([]byte("x"), chunk)
	out := make(map[int]float64, 3)
	for _, appends := range []int{4, 8, 16} {
		run := 0
		out[appends] = testing.AllocsPerRun(3, func() {
			key := fmt.Sprintf("alloc-gate-%d-%d", appends, run)
			run++
			var off int64
			for i := 0; i < appends; i++ {
				obj, err := bk.AppendObject(ctx, "bench", key, off, bytes.NewReader(data))
				if err != nil {
					panic(err)
				}
				off = obj.Size
			}
		})
	}
	return out
}

func assertClusterAppendAllocSlopeFlat(t *testing.T, allocs map[int]float64) {
	t.Helper()

	perAppend4 := allocs[4] / 4
	ceiling := perAppend4*1.30 + 40
	for _, appends := range []int{8, 16} {
		perAppend := allocs[appends] / float64(appends)
		require.LessOrEqualf(t, perAppend, ceiling,
			"cluster AppendObject allocs/op must stay O(1) per append: allocs=%v perAppend4=%.1f perAppend%d=%.1f ceiling=%.1f",
			allocs, perAppend4, appends, perAppend, ceiling)
	}
}
