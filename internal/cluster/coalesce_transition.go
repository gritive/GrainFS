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

func applyCoalesceSegmentsTransition(existing objectMeta, cmd CoalesceSegmentsCmd) (objectMeta, coalesceSegmentsTransitionResult, error) {
	for _, c := range existing.Coalesced {
		if c.CoalescedID == cmd.CoalescedID {
			return objectMeta{}, coalesceSegmentsTransitionResult{Noop: true}, nil
		}
	}

	if len(existing.Coalesced) >= MaxCoalescedEntries {
		return objectMeta{}, coalesceSegmentsTransitionResult{CoalescedEntriesAtCap: true}, errCoalescedEntriesAtCap
	}

	consumed := make(map[string]bool, len(cmd.ConsumedSegmentIDs))
	for _, id := range cmd.ConsumedSegmentIDs {
		consumed[id] = true
	}
	kept := make([]storage.SegmentRef, 0, len(existing.Segments))
	for _, s := range existing.Segments {
		if !consumed[s.BlobID] {
			kept = append(kept, s)
		}
	}
	existing.Segments = kept
	existing.Coalesced = append(append([]CoalescedShardRef(nil), existing.Coalesced...), CoalescedShardRef{
		CoalescedID: cmd.CoalescedID,
		Size:        cmd.Size,
		ETag:        cmd.ETag,
		ShardKey:    cmd.ShardKey,
		Version:     1,
		ECData:      cmd.ECData,
		ECParity:    cmd.ECParity,
		NodeIDs:     append([]string(nil), cmd.Placement...),
	})
	return existing, coalesceSegmentsTransitionResult{}, nil
}
