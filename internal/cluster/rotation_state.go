package cluster

import (
	"errors"
	"fmt"
	"sync"
)

// Phase enumeration for cluster-key rotation. Steady is also the post-rotation
// state — phase 4 (DROPPED) collapses back into Steady once the drop apply
// completes (the new key becomes the new "old"/active).
const (
	PhaseSteady   = 1
	PhaseBegun    = 2
	PhaseSwitched = 3
)

// CapRotationV1 is the bit that gates rotation across mixed-version clusters.
// FSM Apply rejects RotateKeyBegin without this bit (rotation-spec D9 revised:
// security state cannot diverge silently).
const CapRotationV1 uint32 = 1 << 0

// RotationState is the deterministic in-memory state of an in-flight rotation.
// Lives in the meta-raft FSM. No I/O — side effects belong in the apply worker
// (rotation_worker.go) per spec D16.
type RotationState struct {
	RotationID [16]byte // 0 in Steady
	Phase      int      // PhaseSteady / Begun / Switched
	OldSPKI    [32]byte
	NewSPKI    [32]byte // 0 in Steady
	StartedBy  string
	GraceUntil int64                          // unix nanos; for previous.key deletion timer
	AppliedBy  map[string]RotationApplyRecord // peer node ID → applied phase
}

// RotationApplyRecord is what each peer reports via heartbeat after applying
// (or failing to apply) a phase transition. The leader aggregates these to
// drive phase progression.
type RotationApplyRecord struct {
	RotationID [16]byte
	Phase      int    // -1 = error
	Error      string // empty on success
}

// RotateKeyBegin is the FSM command that opens a rotation. Carries SPKI
// hashes only — never the raw PSK (rotation-spec D2 revised + D14=B).
type RotateKeyBegin struct {
	RotationID       [16]byte
	ExpectedNewSPKI  [32]byte // peers verify their local keys.d/next.key matches
	AuditNewSPKIHash [32]byte // SHA256(SPKI), audit-only (D20: not SHA256(PSK))
	StartedBy        string   // raft node ID that initiated (informational)
	Capabilities     uint32   // gate; rotation requires CapRotationV1
}

// RotateKeySwitch transitions phase 2 → 3. Once committed, peers swap the
// active TLS cert from OLD → NEW while keeping both in the accept set.
type RotateKeySwitch struct {
	RotationID [16]byte // must match active rotation
}

// RotateKeyDrop transitions phase 3 → steady-on-NEW. Old SPKI removed from
// accept set. previous.key scheduled for deletion at GraceUntil.
type RotateKeyDrop struct {
	RotationID [16]byte
	GraceUntil int64 // unix nanos
}

// RotateKeyAbort returns a phase 2 rotation to phase 1 (revert). For phase 3
// abort, the FSM forward-rolls to steady-on-NEW (D18) because some peers
// already present NEW and reverting would strand them.
type RotateKeyAbort struct {
	RotationID [16]byte
	Reason     string // "operator" — auto-abort to OLD removed (D13)
}

// RotationFSM is the deterministic state machine. No I/O. The apply worker
// (rotation_worker.go) observes phase changes and performs side effects.
//
// Concurrency: protected by RWMutex. State() may be called from many readers
// (e.g., heartbeat encoder, status RPC); Apply() is called serially from the
// raft FSM goroutine.
type RotationFSM struct {
	mu    sync.RWMutex
	state RotationState
}

func NewRotationFSM() *RotationFSM {
	return &RotationFSM{
		state: RotationState{
			Phase:     PhaseSteady,
			AppliedBy: make(map[string]RotationApplyRecord),
		},
	}
}

// SetSteady seeds the FSM with the active SPKI at startup, before any
// rotation has occurred on this node. Called by meta-raft init.
func (f *RotationFSM) SetSteady(activeSPKI [32]byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.state.OldSPKI = activeSPKI
	f.state.Phase = PhaseSteady
	f.state.RotationID = [16]byte{}
	f.state.NewSPKI = [32]byte{}
}

// State returns a snapshot copy of the current state. Safe to call from any
// goroutine; the returned struct is independent of the FSM's internal state.
func (f *RotationFSM) State() RotationState {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := f.state
	// Copy the AppliedBy map so callers can't mutate FSM internals.
	out.AppliedBy = make(map[string]RotationApplyRecord, len(f.state.AppliedBy))
	for k, v := range f.state.AppliedBy {
		out.AppliedBy[k] = v
	}
	return out
}

// RecordAck merges a peer's applied-phase report into AppliedBy. Called from
// the heartbeat-receive path on the leader. Does not change phase — the
// leader's auto-progress logic reads AppliedBy and decides when to commit
// the next phase command.
func (f *RotationFSM) RecordAck(peerNodeID string, rec RotationApplyRecord) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.state.AppliedBy == nil {
		f.state.AppliedBy = make(map[string]RotationApplyRecord)
	}
	f.state.AppliedBy[peerNodeID] = rec
}

// Apply mutates state per the command type. Returns error on protocol
// violation; idempotent on duplicate same-RotationID Begin.
func (f *RotationFSM) Apply(cmd interface{}) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch c := cmd.(type) {
	case RotateKeyBegin:
		return f.applyBegin(c)
	case RotateKeySwitch:
		return f.applySwitch(c)
	case RotateKeyDrop:
		return f.applyDrop(c)
	case RotateKeyAbort:
		return f.applyAbort(c)
	default:
		return fmt.Errorf("unknown rotation command: %T", cmd)
	}
}

func (f *RotationFSM) applyBegin(c RotateKeyBegin) error {
	if c.Capabilities&CapRotationV1 == 0 {
		return errors.New("RotateKeyBegin missing CapRotationV1 capability")
	}
	if c.RotationID == ([16]byte{}) {
		return errors.New("RotateKeyBegin requires non-zero RotationID")
	}
	switch f.state.Phase {
	case PhaseSteady:
		f.state.RotationID = c.RotationID
		f.state.Phase = PhaseBegun
		f.state.NewSPKI = c.ExpectedNewSPKI
		f.state.StartedBy = c.StartedBy
		// Reset AppliedBy for the new rotation (D15: per-rotation tracking).
		f.state.AppliedBy = make(map[string]RotationApplyRecord)
		return nil
	case PhaseBegun, PhaseSwitched:
		if f.state.RotationID == c.RotationID {
			return nil // idempotent — duplicate apply with same RotationID
		}
		return fmt.Errorf("rotation already in progress (RotationID=%x); abort first", f.state.RotationID)
	}
	return fmt.Errorf("invalid phase: %d", f.state.Phase)
}

func (f *RotationFSM) applySwitch(c RotateKeySwitch) error {
	if f.state.Phase != PhaseBegun {
		return fmt.Errorf("Switch requires PhaseBegun, currently %d", f.state.Phase)
	}
	if f.state.RotationID != c.RotationID {
		return errors.New("Switch RotationID mismatch")
	}
	f.state.Phase = PhaseSwitched
	return nil
}

func (f *RotationFSM) applyDrop(c RotateKeyDrop) error {
	if f.state.Phase != PhaseSwitched {
		return fmt.Errorf("Drop requires PhaseSwitched, currently %d", f.state.Phase)
	}
	if f.state.RotationID != c.RotationID {
		return errors.New("Drop RotationID mismatch")
	}
	// Phase 4 → collapse to PhaseSteady on NEW key. NEW becomes the new OLD.
	f.state.OldSPKI = f.state.NewSPKI
	f.state.NewSPKI = [32]byte{}
	f.state.Phase = PhaseSteady
	f.state.RotationID = [16]byte{}
	f.state.GraceUntil = c.GraceUntil
	return nil
}

func (f *RotationFSM) applyAbort(c RotateKeyAbort) error {
	if f.state.Phase == PhaseSteady {
		return errors.New("no rotation in progress")
	}
	if f.state.RotationID != c.RotationID {
		return errors.New("Abort RotationID mismatch")
	}
	switch f.state.Phase {
	case PhaseBegun:
		// Backward-roll: drop NEW, return to OLD.
		f.state.NewSPKI = [32]byte{}
		f.state.Phase = PhaseSteady
		f.state.RotationID = [16]byte{}
		return nil
	case PhaseSwitched:
		// D18: forward-roll. Some peers already present NEW; can't safely
		// revert. Treat as Drop.
		f.state.OldSPKI = f.state.NewSPKI
		f.state.NewSPKI = [32]byte{}
		f.state.Phase = PhaseSteady
		f.state.RotationID = [16]byte{}
		return nil
	}
	return fmt.Errorf("invalid phase for abort: %d", f.state.Phase)
}
