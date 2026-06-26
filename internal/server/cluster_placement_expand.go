package server

import (
	"context"
	"encoding/json"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

// expandPlacementHandler exposes POST /v1/cluster/expand-placement on the admin
// Unix socket (S7-7). It records the cluster's current shard groups as a new
// topology placement generation, so object placement starts using groups formed
// since boot (e.g. via node joins) without remapping existing objects — the
// generation-probe read path serves old objects from the prior generation.
//
// Status code policy (mirrors transfer-leader):
//
//	mutation gate engaged                                  → 503
//	expand-placement not wired (single-node / no cluster)  → 503
//	internal error (not leader, propose failure, no groups)→ 500
//	happy path (including no-op)                            → 200 ExpandPlacementResult
func (s *Server) expandPlacementHandler(ctx context.Context, c *app.RequestContext) {
	if s.blockIfMutationDisabled(c, "cluster_expand_placement") {
		return
	}
	if s.expandPlacement == nil {
		c.JSON(consts.StatusServiceUnavailable, map[string]string{
			"error": "expand-placement not available (cluster mode not configured)",
		})
		return
	}
	result, err := s.expandPlacement(ctx)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{
			"error": err.Error(),
		})
		return
	}
	data, _ := json.Marshal(result)
	c.Data(consts.StatusOK, "application/json", data)
}

func (s *Server) retirePlacementGenerationHandler(ctx context.Context, c *app.RequestContext) {
	if s.blockIfMutationDisabled(c, "cluster_retire_placement_generation") {
		return
	}
	if s.retirePlacementGeneration == nil {
		c.JSON(consts.StatusServiceUnavailable, map[string]string{
			"error": "retire-placement-generation not available (cluster mode not configured)",
		})
		return
	}
	var req RetirePlacementGenerationRequest
	if err := json.Unmarshal(c.Request.Body(), &req); err != nil {
		c.JSON(consts.StatusBadRequest, map[string]string{
			"error": "invalid retire-placement-generation request",
		})
		return
	}
	result, err := s.retirePlacementGeneration(ctx, req.Epoch)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{
			"error": err.Error(),
		})
		return
	}
	data, _ := json.Marshal(result)
	c.Data(consts.StatusOK, "application/json", data)
}
