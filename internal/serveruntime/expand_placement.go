package serveruntime

import (
	"context"
	"fmt"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/server"
)

// makeExpandPlacementFunc builds the server-injected closure that activates a
// running cluster's currently-formed shard groups as a new placement generation
// (S7-7). It holds the coordinator (the only authoritative source of the
// boot-frozen base placement set) and the meta-raft proposer. The coordinator
// plans the growth (base from the OpRouter, expanded from the live candidate
// groups, no-op guard); the meta-raft records the generation crash-safely
// (gen-0 capture before the expanded set). Returns nil when either dependency
// is absent (single-node / no cluster), which the handler maps to 503.
func makeExpandPlacementFunc(coord *cluster.ClusterCoordinator, metaRaft *cluster.MetaRaft) server.ExpandPlacementFunc {
	if coord == nil || metaRaft == nil {
		return nil
	}
	return func(ctx context.Context) (server.ExpandPlacementResult, error) {
		plan, err := coord.PlanPlacementExpansion()
		if err != nil {
			return server.ExpandPlacementResult{}, fmt.Errorf("plan placement expansion: %w", err)
		}
		res := server.ExpandPlacementResult{
			Base:     plan.Base,
			Expanded: plan.Expanded,
			Added:    plan.Added,
			NoOp:     plan.NoOp,
		}
		if plan.NoOp {
			return res, nil
		}
		if err := metaRaft.AddTopologyGeneration(ctx, plan.Base, plan.Expanded); err != nil {
			return res, fmt.Errorf("record placement generation: %w", err)
		}
		return res, nil
	}
}
