package encrypt

import (
	"context"
	"fmt"
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
	lanes  []RewrapLane
}

// NewRewrapController returns a controller bound to keeper with no lanes.
func NewRewrapController(keeper *DEKKeeper) *RewrapController {
	return &RewrapController{keeper: keeper}
}

// RegisterLane adds a lane. Lanes are registered at wiring time (S6b+); S6a
// registers none.
func (c *RewrapController) RegisterLane(l RewrapLane) {
	c.lanes = append(c.lanes, l)
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
	for _, l := range c.lanes {
		if err := l.RewrapByGen(ctx, oldGen, activeGen); err != nil {
			return fmt.Errorf("rewrap: lane %s gen %d→%d: %w", l.Name(), oldGen, activeGen, err)
		}
	}
	return nil
}
