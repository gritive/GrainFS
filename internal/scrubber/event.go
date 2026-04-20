package scrubber

import (
	"time"

	"github.com/google/uuid"
)

// HealPhase identifies a step within a healing session.
type HealPhase string

const (
	PhaseDetect      HealPhase = "detect"      // CRC/orphan detection
	PhaseReconstruct HealPhase = "reconstruct" // Reed-Solomon decode
	PhaseWrite       HealPhase = "write"       // repaired shard written
	PhaseVerify      HealPhase = "verify"      // post-repair verification
	PhaseStartup     HealPhase = "startup"     // restart-time cleanup
	PhaseDegraded    HealPhase = "degraded"    // isolation / recovery
)

// HealOutcome describes the result of a healing step.
type HealOutcome string

const (
	OutcomeSuccess  HealOutcome = "success"
	OutcomeFailed   HealOutcome = "failed"
	OutcomeSkipped  HealOutcome = "skipped"
	OutcomeIsolated HealOutcome = "isolated"
)

// HealEvent is the unit record emitted for every healing action.
// Receipts (Phase 16 Week 5) aggregate a CorrelationID-grouped set of these
// into a signed audit artifact; for now only the raw event stream exists.
type HealEvent struct {
	ID            string      `json:"id"`
	Timestamp     time.Time   `json:"timestamp"`
	Phase         HealPhase   `json:"phase"`
	Bucket        string      `json:"bucket,omitempty"`
	Key           string      `json:"key,omitempty"`
	VersionID     string      `json:"version_id,omitempty"`
	ShardID       int32       `json:"shard_id"` // -1 when not applicable (e.g. startup cleanup)
	PeerID        string      `json:"peer_id,omitempty"`
	BytesRepaired int64       `json:"bytes_repaired,omitempty"`
	DurationMs    uint32      `json:"duration_ms,omitempty"`
	Outcome       HealOutcome `json:"outcome"`
	ErrCode       string      `json:"err_code,omitempty"`       // only when Outcome == failed
	CorrelationID string      `json:"correlation_id,omitempty"` // groups events from one repair session
}

// NewEvent constructs a HealEvent with ID and Timestamp populated.
// Callers fill in the remaining fields as appropriate for the phase.
func NewEvent(phase HealPhase, outcome HealOutcome) HealEvent {
	return HealEvent{
		ID:        uuid.Must(uuid.NewV7()).String(),
		Timestamp: time.Now().UTC(),
		Phase:     phase,
		ShardID:   -1,
		Outcome:   outcome,
	}
}

// Emitter delivers HealEvents somewhere (SSE hub, eventstore, in-memory buffer).
// The interface is intentionally narrow so callers don't need to know where
// events go. Implementations must be safe for concurrent use.
type Emitter interface {
	Emit(HealEvent)
}

// NoopEmitter discards all events. Used by tests and code paths that run
// before the production emitter is wired up.
type NoopEmitter struct{}

// Emit implements Emitter by doing nothing.
func (NoopEmitter) Emit(HealEvent) {}

// newCorrelationID returns a fresh UUIDv7 string usable as a correlation ID.
// Centralised so RepairEngine and BackgroundScrubber stay in sync on format.
func newCorrelationID() string {
	return uuid.Must(uuid.NewV7()).String()
}
