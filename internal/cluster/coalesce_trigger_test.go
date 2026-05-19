package cluster

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

func TestEvaluateCoalesceTrigger(t *testing.T) {
	cfg := CoalesceConfig{SegmentCount: 16, SizeBytes: 64 * 1024 * 1024, IdleTimeout: 30 * time.Second}
	now := time.Unix(1_000_000, 0)

	seg := func(n int, sz int64) []storage.SegmentRef {
		out := make([]storage.SegmentRef, n)
		for i := range out {
			out[i] = storage.SegmentRef{BlobID: "s", Size: sz, Checksum: []byte("e")}
		}
		return out
	}

	cases := []struct {
		name       string
		segs       []storage.SegmentRef
		firstAt    time.Time
		wantTrigg  bool
		wantReason string
	}{
		{"none", seg(1, 1), now, false, ""},
		{"count", seg(16, 1), now, true, "count"},
		{"count_below", seg(15, 1), now, false, ""},
		{"size", seg(2, 64*1024*1024), now, true, "size"},
		{"size_below", seg(2, 16*1024*1024), now, false, ""},
		{"idle", seg(2, 1), now.Add(-31 * time.Second), true, "idle"},
		{"idle_below", seg(2, 1), now.Add(-29 * time.Second), false, ""},
		{"empty", nil, now, false, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			trig, reason := evaluateCoalesceTrigger(tc.segs, tc.firstAt, now, cfg)
			if trig != tc.wantTrigg {
				t.Fatalf("trig = %v, want %v", trig, tc.wantTrigg)
			}
			if trig && reason != tc.wantReason {
				t.Fatalf("reason = %q, want %q", reason, tc.wantReason)
			}
		})
	}
}

func TestAppendObjectTriggersCoalesceOnCount(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "b"))

	// Override threshold to 3 segments so the test stays fast.
	cfg := *b.coalesceCfg.Load()
	cfg.SegmentCount = 3
	b.SetCoalesceConfig(cfg)

	bucket, key := "b", "k"
	for i := 0; i < 3; i++ {
		off := currentSize(t, b, bucket, key)
		_, err := b.AppendObject(ctx, bucket, key, off, bytes.NewReader([]byte("a")))
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool {
		obj, _ := b.HeadObject(ctx, bucket, key)
		return obj != nil && len(obj.Segments) == 0 && len(obj.Coalesced) == 1
	}, 2*time.Second, 10*time.Millisecond, "coalesce never ran")
}
