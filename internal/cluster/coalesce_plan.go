package cluster

import (
	"time"

	"github.com/gritive/GrainFS/internal/storage"
)

type coalesceTriggerPlan struct {
	ShouldEnqueue bool
	Reason        string
}

// planCoalesceTrigger evaluates the trigger thresholds against the RAW tail
// segment count and byte total. It deliberately takes counts, not segment
// refs: side-record appendables keep the manifest's Segments empty (the tail
// lives in side records, summarized by storage.AppendSummary), so callers on
// that path feed the summary's counts — a refs-based signature silently
// evaluated an empty list there and the trigger never fired.
func planCoalesceTrigger(segCount int, totalSize int64, firstCreatedAt, now time.Time, cfg CoalesceConfig) coalesceTriggerPlan {
	if segCount <= 0 {
		return coalesceTriggerPlan{}
	}
	if cfg.SegmentCount > 0 && segCount >= cfg.SegmentCount {
		return coalesceTriggerPlan{ShouldEnqueue: true, Reason: "count"}
	}
	if cfg.SizeBytes > 0 && totalSize >= cfg.SizeBytes {
		return coalesceTriggerPlan{ShouldEnqueue: true, Reason: "size"}
	}
	if cfg.IdleTimeout > 0 && now.Sub(firstCreatedAt) >= cfg.IdleTimeout {
		return coalesceTriggerPlan{ShouldEnqueue: true, Reason: "idle"}
	}
	return coalesceTriggerPlan{}
}

// coalesceTriggerInputsFromAppend derives the trigger inputs from a freshly
// committed append. Side mode (segments live in side records) reads the tail
// counts from the summary; manifest mode reads them from cmd.Segments.
func coalesceTriggerInputsFromAppend(cmd PutObjectMetaCmd, summary storage.AppendSummary, sideMode bool) (int, int64) {
	if sideMode {
		return summary.SegmentCount, summary.Size
	}
	var total int64
	for _, s := range cmd.Segments {
		total += s.Size
	}
	return len(cmd.Segments), total
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
