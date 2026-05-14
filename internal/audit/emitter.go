// internal/audit/emitter.go
package audit

import "github.com/rs/zerolog/log"

// Emitter writes audit events to stdout (always) and the ring buffer (async, for Iceberg).
// All methods are goroutine-safe and never block.
type Emitter struct {
	nodeID string
	ring   *Ring
}

// NewEmitter creates an Emitter with a fresh ring buffer.
func NewEmitter(nodeID string) *Emitter {
	return &Emitter{nodeID: nodeID, ring: NewRing()}
}

// EmitS3 logs an S3 event to stdout and enqueues it in the ring.
// Events for the audit bucket or the system:audit SA are silently dropped to prevent recursion.
func (e *Emitter) EmitS3(ev S3Event) {
	if ev.Bucket == BucketName || ev.SAID == SystemSA {
		return
	}
	log.Info().
		Str("event", "audit.s3").
		Str("method", ev.Method).
		Str("bucket", ev.Bucket).
		Str("key", ev.Key).
		Int32("status", ev.Status).
		Int32("latency_ms", ev.LatencyMs).
		Msg("audit")
	ev.NodeID = e.nodeID
	e.ring.Put(ev)
}

// Ring returns the ring buffer for the committer to drain.
func (e *Emitter) Ring() *Ring { return e.ring }
