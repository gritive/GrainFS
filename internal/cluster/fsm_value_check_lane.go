package cluster

import (
	"context"
	"fmt"

	"github.com/gritive/GrainFS/internal/encrypt"
)

// FSMValueCheckLane is a read-only encrypt.RewrapLane that errors if this node
// still holds any policy: or obj: FSM-value below keeper-current generation.
//
// It is registered in the RewrapController alongside the EC and packblob lanes.
// Because RewrapController.Kick returns the FIRST lane error, a stale
// FSMValueCheckLane suppresses the completion report until this node's
// FSM-values are drained. Once all policy:/obj: values are at keeper-current,
// RewrapByGen returns nil and Kick can report completion.
//
// Design: this lane does NO mutation — it is a predicate. The actual resealing
// is driven by CmdResealFSMValues batches proposed by the group leader (S7-1a).
// The marker CmdFSMValueResealDone (proposed after drain) re-Kicks the
// controller on every node after all reseal batches are applied.
type FSMValueCheckLane struct {
	groups func() []*GroupBackend
}

var _ encrypt.RewrapLane = (*FSMValueCheckLane)(nil)

// NewFSMValueCheckLane constructs the check lane. groups enumerates the node's
// data-group backends at call time (lazy, to tolerate late boot wiring).
func NewFSMValueCheckLane(groups func() []*GroupBackend) *FSMValueCheckLane {
	return &FSMValueCheckLane{groups: groups}
}

// Name identifies the lane in rewrap orchestration.
func (l *FSMValueCheckLane) Name() string { return "fsmvalue-check" }

// RewrapByGen does NO mutation. It returns an error if this node still holds
// any policy:/obj: value below keeper-current generation — making
// RewrapController.Kick suppress the completion report until the
// marker-driven reseal has converged on this node.
//
// activeGen (from the controller's snapshot of the keeper) is used for the
// stale check; we re-read keeper-current per group so the predicate matches
// what the drain converges toward (per-node keeper-current may differ from
// the controller's view in lagged-follower scenarios).
func (l *FSMValueCheckLane) RewrapByGen(ctx context.Context, _, _ uint32) error {
	for _, gb := range l.groups() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		current, ok := gb.fsm.activeDEKGen()
		if !ok {
			continue // encryption disabled for this group
		}
		stale, err := gb.CollectStaleFSMValueKeys(current, 1, fsmValueRewrapMaxBytes)
		if err != nil {
			return fmt.Errorf("fsmvalue-check: group %s: collect stale keys: %w", gb.ID(), err)
		}
		if len(stale) > 0 {
			return fmt.Errorf("fsmvalue-check: group %s has FSM-values below gen %d (not yet drained)", gb.ID(), current)
		}
	}
	return nil
}
