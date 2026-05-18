package cluster

import (
	"time"

	"github.com/gritive/GrainFS/internal/storage"
)

// CoalesceSegmentsCmd is the Raft payload that records a single coalesce
// operation: take a prefix of objectMeta.Segments (identified by blobIDs in
// ConsumedSegmentIDs) and replace them with one CoalescedShardRef.
//
// Apply MUST be idempotent: replay after partial application is safe because
// the apply path only removes segments whose BlobID still appears in
// objectMeta.Segments. See design 2026-05-18-append-segment-coalesce-ec-design.md
// § "Race handling".
type CoalesceSegmentsCmd struct {
	Bucket             string
	Key                string
	CoalescedID        string   // UUIDv7
	ShardKey           string   // EC shardKey = "<key>/coalesced/<coalescedID>"
	Size               int64    // coalesced data total bytes
	ETag               string   // coalesced body MD5
	ConsumedSegmentIDs []string // segment blob IDs consumed by this operation
}

// MaxCoalescedEntries caps how many CoalescedShardRef entries a single
// appendable object may accumulate. Prevents unbounded chain when raw
// segments keep arriving after each coalesce. Reaching this cap stalls
// coalesce until the object is rotated/closed.
const MaxCoalescedEntries = 1024

// evaluateCoalesceTrigger returns (trigger, reason) for the given segment
// snapshot. Pure function — no side effects.
//
// firstCreatedAt is the timestamp of segments[0] (or the first observed
// time). Caller passes the wall clock for idle comparison (testable).
//
// Precedence: count → size → idle. The first satisfied condition wins.
func evaluateCoalesceTrigger(segs []storage.SegmentRef, firstCreatedAt, now time.Time, cfg CoalesceConfig) (bool, string) {
	if len(segs) == 0 {
		return false, ""
	}
	if cfg.SegmentCount > 0 && len(segs) >= cfg.SegmentCount {
		return true, "count"
	}
	var total int64
	for _, s := range segs {
		total += s.Size
	}
	if cfg.SizeBytes > 0 && total >= cfg.SizeBytes {
		return true, "size"
	}
	if cfg.IdleTimeout > 0 && now.Sub(firstCreatedAt) >= cfg.IdleTimeout {
		return true, "idle"
	}
	return false, ""
}
