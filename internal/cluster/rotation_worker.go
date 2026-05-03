package cluster

import (
	"fmt"

	"github.com/gritive/GrainFS/internal/transport"
)

// IdentitySwapper is the subset of *transport.QUICTransport the worker needs.
// Defined here as an interface so tests can use a fake.
type IdentitySwapper interface {
	SwapIdentity(snap *transport.IdentitySnapshot)
}

// RotationWorker performs the side effects of FSM phase changes: disk reads
// (next.key SPKI verification), transport identity swap, key file moves.
//
// Per spec D16, the FSM apply path is deterministic and runs only state
// mutation. The worker observes phase changes (via the RotationFSM event
// channel set up by the meta-raft layer) and runs all I/O. Heartbeat code
// reads the worker's most recent ack and posts it on outbound heartbeats
// only after the worker has completed its side effects (including fsync).
type RotationWorker struct {
	ks     *transport.Keystore
	tr     IdentitySwapper
	nodeID string
}

// NewRotationWorker constructs a worker bound to a specific node's keystore
// and transport.
func NewRotationWorker(ks *transport.Keystore, tr IdentitySwapper, nodeID string) *RotationWorker {
	return &RotationWorker{ks: ks, tr: tr, nodeID: nodeID}
}

// OnPhaseChange is called from meta-raft's apply path AFTER the FSM has
// committed the phase. The worker reads disk, computes SPKIs, swaps the
// transport identity, and returns the ack. Returning RotationApplyRecord
// with Phase = -1 indicates an error; the meta-raft layer should propagate
// it via heartbeat so the leader stops auto-progressing.
func (w *RotationWorker) OnPhaseChange(st RotationState) RotationApplyRecord {
	switch st.Phase {
	case PhaseBegun:
		return w.applyBegun(st)
	case PhaseSwitched:
		return w.applySwitched(st)
	case PhaseSteady:
		return w.applySteady(st)
	}
	return RotationApplyRecord{
		RotationID: st.RotationID,
		Phase:      -1,
		Error:      fmt.Sprintf("unknown phase %d", st.Phase),
	}
}

// applyBegun: read keys.d/next.key, verify SPKI matches FSM-committed
// expected, install snapshot {accept: [OLD, NEW], present: OLD}.
func (w *RotationWorker) applyBegun(st RotationState) RotationApplyRecord {
	nextKey, err := w.ks.ReadNext()
	if err != nil {
		return RotationApplyRecord{RotationID: st.RotationID, Phase: -1,
			Error: fmt.Sprintf("read next.key: %v", err)}
	}
	_, gotSPKI, err := transport.DeriveClusterIdentity(nextKey)
	if err != nil {
		return RotationApplyRecord{RotationID: st.RotationID, Phase: -1,
			Error: fmt.Sprintf("derive next identity: %v", err)}
	}
	if gotSPKI != st.NewSPKI {
		return RotationApplyRecord{RotationID: st.RotationID, Phase: -1,
			Error: fmt.Sprintf("next.key SPKI mismatch: got %x want %x", gotSPKI, st.NewSPKI)}
	}
	currentKey, err := w.ks.ReadCurrent()
	if err != nil {
		return RotationApplyRecord{RotationID: st.RotationID, Phase: -1,
			Error: fmt.Sprintf("read current.key: %v", err)}
	}
	curCert, _, err := transport.DeriveClusterIdentity(currentKey)
	if err != nil {
		return RotationApplyRecord{RotationID: st.RotationID, Phase: -1,
			Error: fmt.Sprintf("derive current identity: %v", err)}
	}
	w.tr.SwapIdentity(&transport.IdentitySnapshot{
		AcceptSPKIs: [][32]byte{st.OldSPKI, st.NewSPKI},
		PresentCert: curCert, // still present OLD in phase 2
		PresentSPKI: st.OldSPKI,
	})
	return RotationApplyRecord{RotationID: st.RotationID, Phase: PhaseBegun}
}

// applySwitched: install snapshot {accept: [OLD, NEW], present: NEW}.
func (w *RotationWorker) applySwitched(st RotationState) RotationApplyRecord {
	nextKey, err := w.ks.ReadNext()
	if err != nil {
		return RotationApplyRecord{RotationID: st.RotationID, Phase: -1,
			Error: fmt.Sprintf("read next.key (switch): %v", err)}
	}
	newCert, _, err := transport.DeriveClusterIdentity(nextKey)
	if err != nil {
		return RotationApplyRecord{RotationID: st.RotationID, Phase: -1,
			Error: fmt.Sprintf("derive new identity (switch): %v", err)}
	}
	w.tr.SwapIdentity(&transport.IdentitySnapshot{
		AcceptSPKIs: [][32]byte{st.OldSPKI, st.NewSPKI},
		PresentCert: newCert, // now PRESENT NEW
		PresentSPKI: st.NewSPKI,
	})
	return RotationApplyRecord{RotationID: st.RotationID, Phase: PhaseSwitched}
}

// applySteady is reached either after Drop (NEW becomes active, OLD goes to
// previous.key for grace) or after Abort (revert side effects). The current
// state's OldSPKI is the active SPKI per FSM. Compute current.key's SPKI:
//   - If matches: nothing to do on disk; rebuild snapshot with single SPKI.
//   - If differs: promote next.key → current.key (Drop or phase-3 forward
//     abort), then rebuild snapshot.
//
// Either way we delete next.key (no-op if already gone — phase 2 abort path).
func (w *RotationWorker) applySteady(st RotationState) RotationApplyRecord {
	curKey, err := w.ks.ReadCurrent()
	if err != nil {
		return RotationApplyRecord{Phase: -1, Error: fmt.Sprintf("read current.key: %v", err)}
	}
	curCert, curSPKI, err := transport.DeriveClusterIdentity(curKey)
	if err != nil {
		return RotationApplyRecord{Phase: -1, Error: fmt.Sprintf("derive current identity: %v", err)}
	}
	if curSPKI != st.OldSPKI {
		if err := w.ks.MoveNextToCurrent(); err != nil {
			return RotationApplyRecord{Phase: -1, Error: fmt.Sprintf("move next→current: %v", err)}
		}
		curKey, err = w.ks.ReadCurrent()
		if err != nil {
			return RotationApplyRecord{Phase: -1, Error: fmt.Sprintf("re-read current.key: %v", err)}
		}
		curCert, curSPKI, err = transport.DeriveClusterIdentity(curKey)
		if err != nil {
			return RotationApplyRecord{Phase: -1, Error: fmt.Sprintf("re-derive current identity: %v", err)}
		}
		if curSPKI != st.OldSPKI {
			return RotationApplyRecord{Phase: -1, Error: fmt.Sprintf("after promotion, current.key SPKI %x != active %x", curSPKI, st.OldSPKI)}
		}
	}
	// Phase-2 abort path: next.key still exists and must be deleted.
	_ = w.ks.DeleteNext()
	w.tr.SwapIdentity(&transport.IdentitySnapshot{
		AcceptSPKIs: [][32]byte{curSPKI},
		PresentCert: curCert,
		PresentSPKI: curSPKI,
	})
	// previous.key grace timer is scheduled by the meta-raft layer (which has
	// access to a real clock and goroutine pool); the worker just leaves the
	// file in place.
	return RotationApplyRecord{RotationID: st.RotationID, Phase: PhaseSteady}
}
