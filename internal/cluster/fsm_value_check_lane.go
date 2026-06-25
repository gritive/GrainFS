package cluster

import (
	"context"
	"fmt"

	"github.com/gritive/GrainFS/internal/encrypt"
)

// FSMValueCheckLane is an encrypt.RewrapLane that reseals this node's policy:
// and obj: FSM values onto keeper-current generation.
//
// It is registered in the RewrapController alongside the EC and packblob lanes.
// Because RewrapController.Kick reports completion only after every lane
// returns nil, decode/open/write errors in this lane fail closed and suppress
// the epoch report. Stale values are not an error by themselves: this lane
// drains them locally without data-group raft.
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

// RewrapByGen drains policy:/obj: values below keeper-current generation. The
// controller passes activeGen from its keeper snapshot, but the local rewrite
// re-reads keeper-current per group so back-to-back rotations converge on the
// latest locally installed DEK generation.
func (l *FSMValueCheckLane) RewrapByGen(ctx context.Context, _, _ uint32) error {
	for _, gb := range l.groups() {
		for {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			n, err := RewrapLocalFSMValues(ctx, gb, 0)
			if err != nil {
				return fmt.Errorf("fsmvalue-rewrap: group %s: %w", gb.ID(), err)
			}
			if n == 0 {
				break
			}
		}
	}
	return nil
}
