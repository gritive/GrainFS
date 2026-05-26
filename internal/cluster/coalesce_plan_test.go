package cluster

import (
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
)

func TestPlanCoalesceTriggerPrecedence(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	segs := []storage.SegmentRef{
		{BlobID: "s1", Size: 10},
		{BlobID: "s2", Size: 10},
	}

	plan := planCoalesceTrigger(segs, now.Add(-time.Hour), now, CoalesceConfig{
		SegmentCount: 2,
		SizeBytes:    10,
		IdleTimeout:  time.Second,
	})
	if !plan.ShouldEnqueue || plan.Reason != "count" {
		t.Fatalf("plan = %+v, want count trigger", plan)
	}

	plan = planCoalesceTrigger(segs, now.Add(-time.Hour), now, CoalesceConfig{
		SegmentCount: 3,
		SizeBytes:    20,
		IdleTimeout:  time.Second,
	})
	if !plan.ShouldEnqueue || plan.Reason != "size" {
		t.Fatalf("plan = %+v, want size trigger", plan)
	}

	plan = planCoalesceTrigger(segs, now.Add(-time.Hour), now, CoalesceConfig{
		SegmentCount: 3,
		SizeBytes:    30,
		IdleTimeout:  time.Second,
	})
	if !plan.ShouldEnqueue || plan.Reason != "idle" {
		t.Fatalf("plan = %+v, want idle trigger", plan)
	}
}

func TestPlanCoalesceTriggerRejectsEmptySegments(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	plan := planCoalesceTrigger(nil, now.Add(-time.Hour), now, CoalesceConfig{
		SegmentCount: 1,
		SizeBytes:    1,
		IdleTimeout:  time.Second,
	})
	if plan.ShouldEnqueue || plan.Reason != "" {
		t.Fatalf("plan = %+v, want no trigger", plan)
	}
}

func TestPlanCoalesceSnapshotCopiesSegmentsAndBuildsCommandInputs(t *testing.T) {
	segs := []storage.SegmentRef{
		{BlobID: "s1", Size: 10, Checksum: []byte{1}},
		{BlobID: "s2", Size: 20, Checksum: []byte{2}},
	}

	plan := planCoalesceSnapshot("b", "k", segs, "c1")
	if plan.Bucket != "b" || plan.Key != "k" || plan.CoalescedID != "c1" || plan.ShardKey != "k/coalesced/c1" {
		t.Fatalf("plan target fields = %+v", plan)
	}
	if len(plan.Segments) != 2 || plan.Segments[0].BlobID != "s1" || plan.Segments[1].BlobID != "s2" {
		t.Fatalf("plan segments = %+v", plan.Segments)
	}
	if len(plan.ConsumedSegmentIDs) != 2 || plan.ConsumedSegmentIDs[0] != "s1" || plan.ConsumedSegmentIDs[1] != "s2" {
		t.Fatalf("plan consumed IDs = %+v", plan.ConsumedSegmentIDs)
	}

	segs[0].BlobID = "mutated"
	segs[0].Checksum[0] = 9
	if plan.Segments[0].BlobID != "s1" || plan.Segments[0].Checksum[0] != 1 {
		t.Fatalf("plan aliases input segments: %+v", plan.Segments[0])
	}
}
