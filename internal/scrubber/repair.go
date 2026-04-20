package scrubber

import (
	"fmt"
	"time"

	"github.com/klauspost/reedsolomon"
)

// RepairEngine reconstructs and rewrites degraded EC shards.
type RepairEngine struct {
	backend Scrubbable
	emitter Emitter
}

// RepairOption configures a RepairEngine.
type RepairOption func(*RepairEngine)

// WithRepairEmitter wires the engine to a HealEvent sink. Defaults to NoopEmitter.
func WithRepairEmitter(e Emitter) RepairOption {
	return func(r *RepairEngine) {
		if e != nil {
			r.emitter = e
		}
	}
}

// NewRepairEngine creates a RepairEngine.
func NewRepairEngine(b Scrubbable, opts ...RepairOption) *RepairEngine {
	r := &RepairEngine{backend: b, emitter: NoopEmitter{}}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Repair reconstructs missing/corrupt shards using Reed-Solomon.
// Returns an error if more shards are damaged than parity allows.
//
// Emits HealEvents for the reconstruct, write, and verify phases. The caller
// (BackgroundScrubber) emits the preceding detect events and supplies the
// correlationID via the engine's RepairWithCorrelation entry point if grouping
// is needed; standalone callers (tests) get an auto-generated correlation per
// invocation.
func (r *RepairEngine) Repair(rec ObjectRecord, status ShardStatus) error {
	return r.RepairWithCorrelation(rec, status, "")
}

// RepairWithCorrelation runs Repair and tags every emitted HealEvent with the
// supplied correlation ID. An empty ID becomes a freshly minted UUIDv7 so that
// receipts (Phase 16 Week 5) can group all events from one repair session.
func (r *RepairEngine) RepairWithCorrelation(rec ObjectRecord, status ShardStatus, correlationID string) error {
	if correlationID == "" {
		correlationID = newCorrelationID()
	}

	damaged := len(status.Missing) + len(status.Corrupt)
	if damaged > rec.ParityShards {
		return fmt.Errorf("unrepairable: %d shards lost, only %d parity", damaged, rec.ParityShards)
	}

	total := rec.DataShards + rec.ParityShards
	paths := r.backend.ShardPaths(rec.Bucket, rec.Key, rec.VersionID, total)

	shards := make([][]byte, total)
	for i, path := range paths {
		data, err := r.backend.ReadShard(rec.Bucket, rec.Key, path)
		if err != nil {
			shards[i] = nil // nil → reedsolomon treats as missing
			continue
		}
		shards[i] = data
	}

	enc, err := reedsolomon.New(rec.DataShards, rec.ParityShards)
	if err != nil {
		return fmt.Errorf("create encoder: %w", err)
	}

	reconstructStart := time.Now()
	if err := enc.Reconstruct(shards); err != nil {
		ev := newRepairEvent(PhaseReconstruct, OutcomeFailed, rec, correlationID)
		ev.DurationMs = uint32(time.Since(reconstructStart).Milliseconds())
		ev.ErrCode = "reconstruct_failed"
		r.emitter.Emit(ev)
		return fmt.Errorf("reconstruct: %w", err)
	}
	reconstructEv := newRepairEvent(PhaseReconstruct, OutcomeSuccess, rec, correlationID)
	reconstructEv.DurationMs = uint32(time.Since(reconstructStart).Milliseconds())
	r.emitter.Emit(reconstructEv)

	// Rewrite only damaged shards
	toRepair := append(append([]int{}, status.Missing...), status.Corrupt...)
	for _, idx := range toRepair {
		writeStart := time.Now()
		if err := r.backend.WriteShard(rec.Bucket, rec.Key, paths[idx], shards[idx]); err != nil {
			ev := newRepairEvent(PhaseWrite, OutcomeFailed, rec, correlationID)
			ev.ShardID = int32(idx)
			ev.DurationMs = uint32(time.Since(writeStart).Milliseconds())
			ev.ErrCode = "write_failed"
			r.emitter.Emit(ev)
			return fmt.Errorf("write shard %d: %w", idx, err)
		}
		ev := newRepairEvent(PhaseWrite, OutcomeSuccess, rec, correlationID)
		ev.ShardID = int32(idx)
		ev.BytesRepaired = int64(len(shards[idx]))
		ev.DurationMs = uint32(time.Since(writeStart).Milliseconds())
		r.emitter.Emit(ev)
	}

	verifyEv := newRepairEvent(PhaseVerify, OutcomeSuccess, rec, correlationID)
	r.emitter.Emit(verifyEv)
	return nil
}

func newRepairEvent(phase HealPhase, outcome HealOutcome, rec ObjectRecord, correlationID string) HealEvent {
	ev := NewEvent(phase, outcome)
	ev.Bucket = rec.Bucket
	ev.Key = rec.Key
	ev.VersionID = rec.VersionID
	ev.CorrelationID = correlationID
	return ev
}
