package cluster

import (
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
)

func TestEvaluateCoalesceTrigger(t *testing.T) {
	cfg := CoalesceConfig{SegmentCount: 16, SizeBytes: 64 * 1024 * 1024, IdleTimeout: 30 * time.Second}
	now := time.Unix(1_000_000, 0)

	seg := func(n int, sz int64) []storage.SegmentRef {
		out := make([]storage.SegmentRef, n)
		for i := range out {
			out[i] = storage.SegmentRef{BlobID: "s", Size: sz, ETag: "e"}
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
		{"size_below", seg(2, 32*1024*1024), now, false, ""},
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
