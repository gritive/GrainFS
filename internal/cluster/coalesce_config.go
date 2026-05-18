package cluster

import "time"

// CoalesceConfig governs when raw append-segments are merged into a single
// coalesced blob. See design 2026-05-18-append-segment-coalesce-ec-design.md
// § Configuration.
type CoalesceConfig struct {
	// SegmentCount: trigger coalesce when raw segments >= this count.
	SegmentCount int
	// SizeBytes: trigger when sum(segment sizes) >= this byte count.
	SizeBytes int64
	// IdleTimeout: trigger when now - firstSeen >= this duration.
	// Required for low-volume appendable objects that never hit count/size.
	IdleTimeout time.Duration
	// CleanupInterval: periodic backstop scan period that re-enqueues objects
	// whose in-process trigger was lost (e.g. process restart).
	CleanupInterval time.Duration
	// SizeCapBytes is the per-object hard total-size cap. AppendObject
	// requests that would exceed this are rejected with
	// storage.ErrAppendObjectTooLarge (HTTP 400 EntityTooLarge). Default
	// 5 TiB matches the S3 PutObject single-object cap.
	SizeCapBytes int64
}

// DefaultCoalesceConfig returns the Phase B2 default trigger thresholds.
func DefaultCoalesceConfig() CoalesceConfig {
	return CoalesceConfig{
		SegmentCount:    16,
		SizeBytes:       64 * 1024 * 1024,
		IdleTimeout:     30 * time.Second,
		CleanupInterval: 60 * time.Second,
		SizeCapBytes:    5 * 1024 * 1024 * 1024 * 1024, // 5 TiB
	}
}
