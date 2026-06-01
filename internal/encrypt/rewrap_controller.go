package encrypt

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync/atomic"
)

// errLanesNotReady is returned by Kick when MarkReady has not yet been called.
// This closes the restart-replay window where a zero/partial-lane Kick would
// return nil and let the producer emit a false completion.
var errLanesNotReady = errors.New("rewrap: lanes not registered yet")

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
	// ready is set by MarkReady at the end of lane wiring. Kick refuses
	// (errLanesNotReady) until ready is true, preventing the restart-replay
	// race where Kick fires before all lanes are registered.
	ready atomic.Bool
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

// MarkReady signals that all lanes have been registered. Kick will refuse
// with errLanesNotReady until MarkReady is called. Call this at the end of
// wireRewrapLanes (or equivalent wiring) to close the premature-clean window.
func (c *RewrapController) MarkReady() { c.ready.Store(true) }

// Kick migrates every registered lane's records off oldGen onto the active
// generation. It returns the active generation it swept to plus an error. It
// errors with errLanesNotReady if MarkReady has not been called. All lanes are
// run regardless of individual errors; the first error is returned so that no
// lane is starved by a sibling's incomplete sweep. A nil error means every
// registered lane reported success (the precondition the S6d producer gates its
// completion report on). With zero lanes it is a nil no-op (once ready).
//
// The returned activeGen is the SAME value used for the sweep; the producer
// MUST derive its reported gen-set from it (via RetiredGensBelow) rather than a
// fresh keeper read — otherwise a rotation racing between the sweep and the
// report would let the producer report a generation it just swept onto (now
// below the newer active) as fully rewrapped.
func (c *RewrapController) Kick(ctx context.Context, oldGen uint32) (uint32, error) {
	if c.keeper == nil {
		return 0, fmt.Errorf("rewrap: controller has no DEK keeper")
	}
	if !c.ready.Load() {
		return 0, errLanesNotReady
	}
	activeGen := c.keeper.ActiveDEKGeneration()
	if oldGen >= activeGen {
		return activeGen, nil // nothing older than active to migrate
	}
	lp := c.lanes.Load()
	if lp == nil {
		return activeGen, nil // ready + zero lanes = vacuously clean
	}
	var firstErr error
	failed := 0
	for _, l := range *lp {
		if err := l.RewrapByGen(ctx, oldGen, activeGen); err != nil {
			failed++
			if firstErr == nil {
				firstErr = fmt.Errorf("rewrap: lane %s gen %d→%d: %w", l.Name(), oldGen, activeGen, err)
			}
		}
	}
	if failed > 0 {
		return activeGen, firstErr
	}
	return activeGen, nil
}

// RetiredGensBelow returns the sorted list of DEK generations the keeper still
// retains that are older than active. The producer passes the activeGen that
// Kick swept to (NOT a fresh keeper read) so the reported set is consistent
// with the sweep even if a rotation lands concurrently.
func (c *RewrapController) RetiredGensBelow(active uint32) []uint32 {
	if c.keeper == nil {
		return nil
	}
	versions := c.keeper.Versions()
	gens := make([]uint32, 0, len(versions))
	for g := range versions {
		if g < active {
			gens = append(gens, g)
		}
	}
	sort.Slice(gens, func(i, j int) bool { return gens[i] < gens[j] })
	return gens
}
