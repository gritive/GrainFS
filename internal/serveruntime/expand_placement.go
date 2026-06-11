package serveruntime

import (
	"context"
	"fmt"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/server"
)

// placementPlanner plans a topology-generation growth (ClusterCoordinator).
type placementPlanner interface {
	PlanPlacementExpansion() (cluster.PlacementExpansionPlan, error)
}

// generationRecorder records a topology generation crash-safely (MetaRaft).
type generationRecorder interface {
	AddTopologyGeneration(ctx context.Context, base, expanded []string) error
}

// makeExpandPlacementFunc builds the server-injected closure that activates a
// running cluster's currently-formed shard groups as a new placement generation
// (S7-7). It holds the coordinator (the only authoritative source of the
// boot-frozen base placement set) and the meta-raft proposer. Returns nil when
// either dependency is absent (single-node / no cluster), which the handler maps
// to 503. The concrete-typed nil check here avoids the interface typed-nil trap;
// expandPlacementClosure takes interfaces so it is unit-testable with fakes.
func makeExpandPlacementFunc(coord *cluster.ClusterCoordinator, metaRaft *cluster.MetaRaft) server.ExpandPlacementFunc {
	if coord == nil || metaRaft == nil {
		return nil
	}
	return expandPlacementClosure(coord, metaRaft)
}

// expandPlacementClosure is the testable core: the coordinator plans the growth
// (base from the OpRouter, expanded from live candidates, no-op guard); on a
// non-no-op plan the recorder records the generation crash-safely (gen-0 capture
// before the expanded set). A no-op plan records nothing.
func expandPlacementClosure(planner placementPlanner, recorder generationRecorder) server.ExpandPlacementFunc {
	return func(ctx context.Context) (server.ExpandPlacementResult, error) {
		plan, err := planner.PlanPlacementExpansion()
		if err != nil {
			return server.ExpandPlacementResult{}, fmt.Errorf("plan placement expansion: %w", err)
		}
		res := server.ExpandPlacementResult{
			Base:     plan.Base,
			Expanded: plan.Expanded,
			Added:    plan.Added,
			Removed:  plan.Removed,
			NoOp:     plan.NoOp,
		}
		if plan.NoOp {
			return res, nil
		}
		if err := recorder.AddTopologyGeneration(ctx, plan.Base, plan.Expanded); err != nil {
			return res, fmt.Errorf("record placement generation: %w", err)
		}
		return res, nil
	}
}
