package server

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

// removePeerHandler handles POST /api/cluster/remove-peer. Removes the named
// voter from the Raft configuration via joint consensus.
func (s *Server) removePeerHandler(ctx context.Context, c *app.RequestContext) {
	if s.blockIfMutationDisabled(c, "cluster_remove_peer") {
		return
	}
	if !s.routeFeatureAvailable(routeFeatureClusterMembership) {
		c.JSON(consts.StatusServiceUnavailable, map[string]string{
			"error": "membership controller not configured",
		})
		return
	}
	if !s.routeFeatureAvailable(routeFeatureCluster) {
		c.JSON(consts.StatusServiceUnavailable, map[string]string{
			"error": "node is not in cluster mode",
		})
		return
	}

	var req removePeerRequest
	body, _ := c.Body()
	if err := json.Unmarshal(body, &req); err != nil {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "malformed JSON body"})
		return
	}
	if req.ID == "" {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "id is required"})
		return
	}

	if err := s.removeClusterPeer(ctx, req); err != nil {
		var notLeader clusterNotLeaderError
		var preflight removePeerPreflightError
		switch {
		case errors.As(err, &notLeader):
			if notLeader.retry {
				writeClusterNotLeaderRetry(c, notLeader.Error(), notLeader.leaderID)
				return
			}
			writeClusterNotLeader(c, notLeader.Error(), notLeader.leaderID)
			return
		case errors.Is(err, errRemovePeerSnapshotUnavailable):
			writeRemovePeerSnapshotUnavailable(c)
			return
		case errors.As(err, &preflight):
			writeRemovePeerPreflightFailure(c, preflight.result, preflight.id)
			return
		default:
			c.JSON(consts.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
	}

	c.JSON(consts.StatusOK, map[string]string{"status": "removed", "id": req.ID})
}
