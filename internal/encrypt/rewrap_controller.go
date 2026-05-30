package encrypt

import (
	"context"
	"fmt"
	"sync/atomic"
)

// RewrapLane migrates every record THIS NODE owns from oldGen to activeGen.
// Implementations must be idempotent and safe under concurrent live writes.
// Lane implementations live in the storage/cluster layers (S6b) and are
// intentionally absent in S6a.
type RewrapLane interface {
	Name() string
	RewrapByGen(ctx context.Context, oldGen, activeGen uint32) error
}

// RewrapController fans a DEK-generation rotation out to its registered lanes.
// On Kick it resolves the active generation from the keeper and asks each lane
// to migrate records off oldGen.
//
// S6a NOTE: the controller deliberately does NOT report completion to the raft
// ledger. Completion reporting — and its prune-safety meaning — lands in S6d,
// once real lanes exist. Reporting "done" with zero lanes registered would let
// a future Prune trust a generation that still has un-migrated data.
type RewrapController struct {
	keeper *DEKKeeper
	// lanes is a copy-on-write slice. The controller is published to the
	// scrubberKick closure early in boot (before lanes exist), but lanes are
	// registered LATER once the backends are built — so RegisterLane (boot
	// goroutine) can race a Kick triggered by the apply loop replaying a
	// committed rotation on restart. atomic COW makes registration lock-free
	// and the Kick read race-free.
	lanes atomic.Pointer[[]RewrapLane]
}

// NewRewrapController returns a controller bound to keeper with no lanes.
func NewRewrapController(keeper *DEKKeeper) *RewrapController {
	return &RewrapController{keeper: keeper}
}

// RegisterLane adds a lane. Lanes are registered at wiring time (S6b+); S6a
// registers none. Safe to call concurrently with Kick (atomic COW).
func (c *RewrapController) RegisterLane(l RewrapLane) {
	for {
		old := c.lanes.Load()
		var cur []RewrapLane
		if old != nil {
			cur = *old
		}
		next := make([]RewrapLane, len(cur), len(cur)+1)
		copy(next, cur)
		next = append(next, l)
		if c.lanes.CompareAndSwap(old, &next) {
			return
		}
	}
}

// Kick migrates every registered lane's records off oldGen onto the active
// generation. It returns the first lane error encountered; nil means every
// registered lane reported success (the precondition a future S6d producer
// will gate its completion report on). With zero lanes it is a nil no-op.
func (c *RewrapController) Kick(ctx context.Context, oldGen uint32) error {
	if c.keeper == nil {
		return fmt.Errorf("rewrap: controller has no DEK keeper")
	}
	activeGen := c.keeper.ActiveDEKGeneration()
	if oldGen >= activeGen {
		return nil // nothing older than active to migrate
	}
	lp := c.lanes.Load()
	if lp == nil {
		return nil // no lanes registered yet
	}
	for _, l := range *lp {
		if err := l.RewrapByGen(ctx, oldGen, activeGen); err != nil {
			return fmt.Errorf("rewrap: lane %s gen %d→%d: %w", l.Name(), oldGen, activeGen, err)
		}
	}
	return nil
}
