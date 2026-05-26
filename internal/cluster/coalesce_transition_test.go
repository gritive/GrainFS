package cluster

import (
	"errors"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
)

func TestApplyCoalesceSegmentsTransitionPreservesRacedAppend(t *testing.T) {
	existing := objectMeta{
		Key: "k", Size: 30, IsAppendable: true,
		Segments: []storage.SegmentRef{
			{BlobID: "s1", Size: 10},
			{BlobID: "s2", Size: 10},
			{BlobID: "s3", Size: 10},
		},
	}

	updated, result, err := applyCoalesceSegmentsTransition(existing, CoalesceSegmentsCmd{
		CoalescedID:        "c1",
		ShardKey:           "k/coalesced/c1",
		Size:               20,
		ETag:               "etag-c1",
		ConsumedSegmentIDs: []string{"s1", "s2"},
		Placement:          []string{"n1", "n2", "n3"},
		ECData:             2,
		ECParity:           1,
	})
	if err != nil {
		t.Fatalf("applyCoalesceSegmentsTransition: %v", err)
	}
	if result.Noop || result.CoalescedEntriesAtCap {
		t.Fatalf("unexpected result flags: %+v", result)
	}
	if len(updated.Segments) != 1 || updated.Segments[0].BlobID != "s3" {
		t.Fatalf("segments = %+v", updated.Segments)
	}
	if len(updated.Coalesced) != 1 {
		t.Fatalf("coalesced = %+v", updated.Coalesced)
	}
	ref := updated.Coalesced[0]
	if ref.CoalescedID != "c1" || ref.ShardKey != "k/coalesced/c1" || ref.Size != 20 ||
		ref.ETag != "etag-c1" || ref.Version != 1 || ref.ECData != 2 || ref.ECParity != 1 {
		t.Fatalf("coalesced ref = %+v", ref)
	}
	if len(ref.NodeIDs) != 3 || ref.NodeIDs[0] != "n1" || ref.NodeIDs[2] != "n3" {
		t.Fatalf("coalesced NodeIDs = %+v", ref.NodeIDs)
	}
	if updated.Size != existing.Size {
		t.Fatalf("Size changed: %d want %d", updated.Size, existing.Size)
	}
}

func TestApplyCoalesceSegmentsTransitionReportsNoopAndCap(t *testing.T) {
	existing := objectMeta{
		Key: "k", Size: 30, IsAppendable: true,
		Segments:  []storage.SegmentRef{{BlobID: "s3", Size: 10}},
		Coalesced: []CoalescedShardRef{{CoalescedID: "c1"}},
	}

	_, result, err := applyCoalesceSegmentsTransition(existing, CoalesceSegmentsCmd{
		CoalescedID:        "c1",
		ShardKey:           "k/coalesced/c1",
		ConsumedSegmentIDs: []string{"s1", "s2"},
	})
	if err != nil {
		t.Fatalf("idempotent transition error: %v", err)
	}
	if !result.Noop {
		t.Fatalf("Noop=false for duplicate coalesced ID: %+v", result)
	}

	full := objectMeta{
		Key:       "k",
		Size:      30,
		Coalesced: make([]CoalescedShardRef, MaxCoalescedEntries),
	}
	_, result, err = applyCoalesceSegmentsTransition(full, CoalesceSegmentsCmd{
		CoalescedID: "overflow",
		ShardKey:    "k/coalesced/overflow",
	})
	if err == nil || !errors.Is(err, errCoalescedEntriesAtCap) {
		t.Fatalf("cap error=%v", err)
	}
	if !result.CoalescedEntriesAtCap {
		t.Fatalf("CoalescedEntriesAtCap=false: %+v", result)
	}
}
