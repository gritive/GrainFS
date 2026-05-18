package cluster

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestAppendableReadAtAcrossCoalescedAndRaw exercises range reads against an
// appendable object that has BOTH a coalesced blob (EC distributed) AND raw
// tail segments. The test seeds 16 appends to drive coalesce, then 4 more
// raw appends, and asserts ReadAt returns the correct bytes for ranges that
// fall:
//   - within the coalesced section
//   - across the coalesced→raw boundary
//   - within the raw tail
//   - covering the full body
func TestAppendableReadAtAcrossCoalescedAndRaw(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "b"))

	b.SetECConfig(ECConfig{DataShards: 4, ParityShards: 2})
	svc := NewShardService(b.root, nil)
	b.SetShardService(svc, []string{b.selfAddr, b.selfAddr, b.selfAddr, b.selfAddr, b.selfAddr, b.selfAddr})

	bucket, key := "b", "k"
	var off int64
	var expected []byte
	// 16 chunks of 5 bytes → 80 bytes total — coalesce triggers.
	for i := 0; i < 16; i++ {
		chunk := []byte(fmt.Sprintf("c%02d-", i))
		_, err := b.AppendObject(ctx, bucket, key, off, bytes.NewReader(chunk))
		require.NoError(t, err)
		off += int64(len(chunk))
		expected = append(expected, chunk...)
	}
	require.Eventually(t, func() bool {
		obj, _ := b.HeadObject(ctx, bucket, key)
		return obj != nil && len(obj.Coalesced) == 1 && len(obj.Segments) == 0
	}, 5*time.Second, 20*time.Millisecond, "coalesce did not complete")

	// 4 more raw appends — these stay in Segments[].
	rangeDisableCfg := *b.coalesceCfg.Load()
	rangeDisableCfg.SegmentCount = 1 << 30 // disable count trigger
	rangeDisableCfg.SizeBytes = 1 << 30
	rangeDisableCfg.IdleTimeout = time.Hour
	b.SetCoalesceConfig(rangeDisableCfg)
	for i := 16; i < 20; i++ {
		chunk := []byte(fmt.Sprintf("r%02d-", i))
		_, err := b.AppendObject(ctx, bucket, key, off, bytes.NewReader(chunk))
		require.NoError(t, err)
		off += int64(len(chunk))
		expected = append(expected, chunk...)
	}

	obj, err := b.HeadObject(ctx, bucket, key)
	require.NoError(t, err)
	require.Len(t, obj.Coalesced, 1)
	require.Len(t, obj.Segments, 4)
	require.Equal(t, int64(len(expected)), obj.Size)

	coalescedSize := obj.Coalesced[0].Size
	cases := []struct {
		name        string
		offset, end int64
	}{
		{"within_coalesced", 5, 50},
		{"coalesced_to_raw_boundary", coalescedSize - 10, coalescedSize + 10},
		{"within_raw_tail", coalescedSize + 4, int64(len(expected))},
		{"full", 0, int64(len(expected))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := make([]byte, tc.end-tc.offset)
			n, err := b.ReadAt(ctx, bucket, key, tc.offset, buf)
			require.NoError(t, err)
			require.Equal(t, len(buf), n)
			require.Equal(t, expected[tc.offset:tc.end], buf, "range [%d,%d)", tc.offset, tc.end)
		})
	}
}
