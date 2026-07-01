package cluster

import (
	"fmt"

	"github.com/gritive/GrainFS/internal/storage"
)

var errCoalescedEntriesAtCap = fmt.Errorf("coalesce: max coalesced entries (%d) reached", MaxCoalescedEntries)

type coalesceSegmentsTransitionResult struct {
	Noop                  bool
	CoalescedEntriesAtCap bool
}

// coalescedRefFromPlan builds the CoalescedShardRef that a coalesce operation
// appends to the manifest.
func coalescedRefFromPlan(cmd CoalesceSegmentsPlan) CoalescedShardRef {
	return CoalescedShardRef{
		CoalescedID: cmd.CoalescedID,
		Size:        cmd.Size,
		ETag:        cmd.ETag,
		ShardKey:    cmd.ShardKey,
		Version:     1,
		ECData:      cmd.ECData,
		ECParity:    cmd.ECParity,
		NodeIDs:     append([]string(nil), cmd.Placement...),
	}
}

// planCoalesceBlobRMW lifts the idempotency (CoalescedID),
// MaxCoalescedEntries cap, and consumed-segment removal into a PutObjectMetaCmd
// RMW against the CURRENT base manifest blob:
//   - F8: consumed segment IDs are removed from the CURRENT base.Segments by
//     EXACT BlobID match, so a segment a concurrent append added AFTER the
//     coalesce job snapshotted its candidates survives (it is not in
//     ConsumedSegmentIDs, so it is kept).
//   - the published cmd is a CAS candidate: MetaSeqCAS=true, MetaSeq=base+1.
//     Size/ETag are unchanged (coalesce is a reorganization, not a content
//     change).
func planCoalesceBlobRMWWithSideSummary(base PutObjectMetaCmd, summary storage.AppendSummary, hasSummary bool, cmd CoalesceSegmentsPlan) (PutObjectMetaCmd, storage.AppendSummary, coalesceSegmentsTransitionResult, error) {
	for _, c := range base.Coalesced {
		if c.CoalescedID == cmd.CoalescedID {
			return PutObjectMetaCmd{}, storage.AppendSummary{}, coalesceSegmentsTransitionResult{Noop: true}, nil
		}
	}
	if len(base.Coalesced) >= MaxCoalescedEntries {
		return PutObjectMetaCmd{}, storage.AppendSummary{}, coalesceSegmentsTransitionResult{CoalescedEntriesAtCap: true}, errCoalescedEntriesAtCap
	}

	consumed := make(map[string]bool, len(cmd.ConsumedSegmentIDs))
	for _, id := range cmd.ConsumedSegmentIDs {
		consumed[id] = true
	}
	kept := make([]SegmentMetaEntry, 0, len(base.Segments))
	for _, s := range base.Segments {
		if !consumed[s.BlobID] {
			kept = append(kept, s)
		}
	}

	next := base
	next.Segments = kept
	next.Coalesced = append(append([]CoalescedShardRef(nil), base.Coalesced...), coalescedRefFromPlan(cmd))
	next.IsAppendable = true
	// Coalesce is a reorganization: Size/ETag/VersionID/PlacementGroupID/ModTime
	// are carried forward unchanged from the base. Only the CAS fence advances.
	next.MetaSeqCAS = true
	next.MetaSeq = base.MetaSeq + 1
	// A coalesce never carries forward a delete-marker / hard-delete / conditional
	// state (mirrors planAppendObjectBlobRMW).
	next.IsDeleteMarker = false
	next.IsHardDeleted = false
	next.ExpectedETag = ""
	next.PreserveLatest = false
	if !hasSummary {
		return next, storage.AppendSummary{}, coalesceSegmentsTransitionResult{}, nil
	}
	nextSummary, err := advanceAppendSummaryForCoalesce(summary, cmd)
	if err != nil {
		return PutObjectMetaCmd{}, storage.AppendSummary{}, coalesceSegmentsTransitionResult{}, err
	}
	return next, nextSummary, coalesceSegmentsTransitionResult{}, nil
}

func advanceAppendSummaryForCoalesce(summary storage.AppendSummary, cmd CoalesceSegmentsPlan) (storage.AppendSummary, error) {
	consumedCount := len(cmd.ConsumedSegmentIDs)
	if consumedCount > summary.SegmentCount {
		return storage.AppendSummary{}, fmt.Errorf("coalesce append summary: consumed %d exceeds tail segment count %d", consumedCount, summary.SegmentCount)
	}
	if cmd.Size > summary.Size {
		return storage.AppendSummary{}, fmt.Errorf("coalesce append summary: consumed size %d exceeds tail size %d", cmd.Size, summary.Size)
	}
	nextSummary := summary
	nextSummary.Size -= cmd.Size
	nextSummary.SegmentCount -= consumedCount
	nextSummary.CompactedPrefixCount += consumedCount
	return nextSummary, nil
}
