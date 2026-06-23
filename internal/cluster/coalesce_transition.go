package cluster

import (
	"fmt"
)

var errCoalescedEntriesAtCap = fmt.Errorf("coalesce: max coalesced entries (%d) reached", MaxCoalescedEntries)

type coalesceSegmentsTransitionResult struct {
	Noop                  bool
	CoalescedEntriesAtCap bool
}

// coalescedRefFromCmd builds the CoalescedShardRef that a coalesce operation
// appends to the manifest. Shared by the FSM apply path
// (applyCoalesceSegmentsTransition) and the off-raft blob RMW
// (planCoalesceBlobRMW) so the two produce byte-identical refs.
func coalescedRefFromCmd(cmd CoalesceSegmentsCmd) CoalescedShardRef {
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

// planCoalesceBlobRMW is the off-raft (quorum-meta blob) twin of
// applyCoalesceSegmentsTransition: it lifts the idempotency (CoalescedID),
// MaxCoalescedEntries cap, and consumed-segment removal into a PutObjectMetaCmd
// RMW against the CURRENT base manifest blob. It mirrors the FSM transition's
// semantics exactly, with two off-raft additions:
//   - F8: consumed segment IDs are removed from the CURRENT base.Segments by
//     EXACT BlobID match, so a segment a concurrent append added AFTER the
//     coalesce job snapshotted its candidates survives (it is not in
//     ConsumedSegmentIDs, so it is kept).
//   - the published cmd is a CAS candidate: MetaSeqCAS=true, MetaSeq=base+1.
//     Size/ETag are unchanged (coalesce is a reorganization, not a content
//     change). The result Noop/cap flags match the FSM transition.
func planCoalesceBlobRMW(base PutObjectMetaCmd, cmd CoalesceSegmentsCmd) (PutObjectMetaCmd, coalesceSegmentsTransitionResult, error) {
	for _, c := range base.Coalesced {
		if c.CoalescedID == cmd.CoalescedID {
			return PutObjectMetaCmd{}, coalesceSegmentsTransitionResult{Noop: true}, nil
		}
	}
	if len(base.Coalesced) >= MaxCoalescedEntries {
		return PutObjectMetaCmd{}, coalesceSegmentsTransitionResult{CoalescedEntriesAtCap: true}, errCoalescedEntriesAtCap
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
	next.Coalesced = append(append([]CoalescedShardRef(nil), base.Coalesced...), coalescedRefFromCmd(cmd))
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
	return next, coalesceSegmentsTransitionResult{}, nil
}
