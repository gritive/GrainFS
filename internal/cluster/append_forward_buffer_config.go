package cluster

// AppendForwardBufferConfig governs the byte-based reservation pool used by
// ClusterCoordinator when buffering AppendObject bodies for non-owner →
// owner forwarding. Reservation is actual-size based (not slot-based); a
// 1 MiB chunk only consumes 1 MiB of the pool.
//
// Wired from CLI flags --cluster-append-forward-buffer-total-bytes /
// --cluster-append-forward-buffer-max-per-request. See design
// 2026-05-19-appendobject-hardening-design.md § Follow-up 3.
type AppendForwardBufferConfig struct {
	// TotalBytes caps the sum of in-flight forward-body reservations across
	// all concurrent AppendObject forwards. Default 512 MiB.
	TotalBytes int64
	// MaxPerRequest is the per-request hard cap; oversized bodies reject
	// immediately (matches HTTP layer appendBodyMaxBytes = 64 MiB).
	MaxPerRequest int64
}

func DefaultAppendForwardBufferConfig() AppendForwardBufferConfig {
	return AppendForwardBufferConfig{
		TotalBytes:    512 * 1024 * 1024,
		MaxPerRequest: 64 * 1024 * 1024,
	}
}
