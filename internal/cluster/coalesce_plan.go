package cluster

import (
	"time"

	"github.com/gritive/GrainFS/internal/storage"
)

type coalesceTriggerPlan struct {
	ShouldEnqueue bool
	Reason        string
}

func planCoalesceTrigger(segs []storage.SegmentRef, firstCreatedAt, now time.Time, cfg CoalesceConfig) coalesceTriggerPlan {
	if len(segs) == 0 {
		return coalesceTriggerPlan{}
	}
	if cfg.SegmentCount > 0 && len(segs) >= cfg.SegmentCount {
		return coalesceTriggerPlan{ShouldEnqueue: true, Reason: "count"}
	}
	var total int64
	for _, s := range segs {
		total += s.Size
	}
	if cfg.SizeBytes > 0 && total >= cfg.SizeBytes {
		return coalesceTriggerPlan{ShouldEnqueue: true, Reason: "size"}
	}
	if cfg.IdleTimeout > 0 && now.Sub(firstCreatedAt) >= cfg.IdleTimeout {
		return coalesceTriggerPlan{ShouldEnqueue: true, Reason: "idle"}
	}
	return coalesceTriggerPlan{}
}

type coalesceSnapshotPlan struct {
	Bucket             string
	Key                string
	CoalescedID        string
	ShardKey           string
	Segments           []storage.SegmentRef
	ConsumedSegmentIDs []string
}

func planCoalesceSnapshot(bucket, key string, segs []storage.SegmentRef, coalescedID string) coalesceSnapshotPlan {
	snapshot := make([]storage.SegmentRef, len(segs))
	for i, s := range segs {
		snapshot[i] = cloneSegmentRef(s)
	}
	consumedIDs := make([]string, len(snapshot))
	for i, s := range snapshot {
		consumedIDs[i] = s.BlobID
	}
	return coalesceSnapshotPlan{
		Bucket:             bucket,
		Key:                key,
		CoalescedID:        coalescedID,
		ShardKey:           key + "/coalesced/" + coalescedID,
		Segments:           snapshot,
		ConsumedSegmentIDs: consumedIDs,
	}
}
