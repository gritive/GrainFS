//go:build !race

package storage

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/stretchr/testify/require"
)

func TestS3AppendAllocPerAppendStaysFlat(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(cryptoLabel(encrypted), func(t *testing.T) {
			ops := newAppendAllocGateOps(t, encrypted)
			got := measureStorageAppendAllocs(t, ops)
			assertAppendAllocSlopeFlat(t, got)
		})
	}
}

func newAppendAllocGateOps(t *testing.T, encrypted bool) *Operations {
	t.Helper()

	var (
		backend *LocalBackend
		err     error
	)
	if encrypted {
		cid := bytes.Repeat([]byte{0x88}, 16)
		keeper, kerr := encrypt.NewDEKKeeper(bytes.Repeat([]byte{0x88}, encrypt.KEKSize), cid)
		require.NoError(t, kerr)
		backend, err = NewLocalBackendWithDEKKeeper(t.TempDir(), keeper, cid)
	} else {
		backend, err = NewLocalBackend(t.TempDir())
	}
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, backend.Close()) })
	require.NoError(t, backend.CreateBucket(context.Background(), s3BenchBucket))
	return NewOperations(backend)
}

func measureStorageAppendAllocs(t *testing.T, ops *Operations) map[int]float64 {
	t.Helper()

	const chunk = 64 << 10
	ctx := context.Background()
	data := s3BenchPayload(chunk)
	out := make(map[int]float64, 3)
	for _, appends := range []int{4, 8, 16} {
		run := 0
		out[appends] = testing.AllocsPerRun(3, func() {
			key := fmt.Sprintf("alloc-gate-%d-%d", appends, run)
			run++
			var off int64
			for i := 0; i < appends; i++ {
				obj, err := ops.AppendObject(ctx, s3BenchBucket, key, off, bytes.NewReader(data))
				if err != nil {
					panic(err)
				}
				off = obj.Size
			}
		})
	}
	return out
}

func assertAppendAllocSlopeFlat(t *testing.T, allocs map[int]float64) {
	t.Helper()

	perAppend4 := allocs[4] / 4
	ceiling := perAppend4*1.30 + 40
	for _, appends := range []int{8, 16} {
		perAppend := allocs[appends] / float64(appends)
		require.LessOrEqualf(t, perAppend, ceiling,
			"AppendObject allocs/op must stay O(1) per append: allocs=%v perAppend4=%.1f perAppend%d=%.1f ceiling=%.1f",
			allocs, perAppend4, appends, perAppend, ceiling)
	}
}
