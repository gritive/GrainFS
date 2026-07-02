package cluster

import (
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
)

func TestEvaluateCoalesceTrigger(t *testing.T) {
	cfg := CoalesceConfig{SegmentCount: 16, SizeBytes: 64 * 1024 * 1024, IdleTimeout: 30 * time.Second}
	now := time.Unix(1_000_000, 0)

	cases := []struct {
		name       string
		count      int
		size       int64
		firstAt    time.Time
		wantTrigg  bool
		wantReason string
	}{
		{"none", 1, 1, now, false, ""},
		{"count", 16, 16, now, true, "count"},
		{"count_below", 15, 15, now, false, ""},
		{"size", 2, 128 * 1024 * 1024, now, true, "size"},
		{"size_below", 2, 32 * 1024 * 1024, now, false, ""},
		{"idle", 2, 2, now.Add(-31 * time.Second), true, "idle"},
		{"idle_below", 2, 2, now.Add(-29 * time.Second), false, ""},
		{"zero_count", 0, 0, now, false, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan := planCoalesceTrigger(tc.count, tc.size, tc.firstAt, now, cfg)
			if plan.ShouldEnqueue != tc.wantTrigg {
				t.Fatalf("trig = %v, want %v", plan.ShouldEnqueue, tc.wantTrigg)
			}
			if plan.ShouldEnqueue && plan.Reason != tc.wantReason {
				t.Fatalf("reason = %q, want %q", plan.Reason, tc.wantReason)
			}
		})
	}
}

// Side-record appends keep the manifest's Segments empty (segments live in
// side records; the summary carries the tail counts). The append-site trigger
// inputs MUST come from the summary in side mode — reading cmd.Segments there
// left the trigger permanently dead for every side-mode appendable (the bug
// behind the red coalesce e2e specs).
func TestCoalesceTriggerInputsFromAppend(t *testing.T) {
	cmd := PutObjectMetaCmd{
		Segments: []SegmentMetaEntry{
			{BlobID: "s1", Size: 10},
			{BlobID: "s2", Size: 20},
		},
	}
	summary := storage.AppendSummary{Size: 160, SegmentCount: 16, CompactedPrefixCount: 4}

	t.Run("side_mode_uses_summary", func(t *testing.T) {
		count, size := coalesceTriggerInputsFromAppend(cmd, summary, true)
		if count != 16 || size != 160 {
			t.Fatalf("count,size = %d,%d, want 16,160 (summary tail counts)", count, size)
		}
	})
	t.Run("manifest_mode_uses_segments", func(t *testing.T) {
		count, size := coalesceTriggerInputsFromAppend(cmd, storage.AppendSummary{}, false)
		if count != 2 || size != 30 {
			t.Fatalf("count,size = %d,%d, want 2,30 (manifest segments)", count, size)
		}
	})
	t.Run("side_mode_empty_summary_no_trigger_inputs", func(t *testing.T) {
		count, size := coalesceTriggerInputsFromAppend(PutObjectMetaCmd{}, storage.AppendSummary{}, true)
		if count != 0 || size != 0 {
			t.Fatalf("count,size = %d,%d, want 0,0", count, size)
		}
	})
}
