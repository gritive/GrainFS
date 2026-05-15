package server

import (
	"context"
	"encoding/json"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/eventstore"
)

// joinClusterHandler handles POST /api/cluster/join for runtime local->cluster transition.
func (s *Server) joinClusterHandler(ctx context.Context, c *app.RequestContext) {
	if resp, blocked := s.mutationGate.BlockResponse("cluster_join"); blocked {
		body, _ := json.Marshal(resp)
		c.Data(resp.Status, "application/json", body)
		return
	}
	if !s.routeFeatureAvailable(routeFeatureClusterJoin) {
		writeXMLError(c, consts.StatusConflict, "InvalidRequest", "server is already in cluster mode or join not supported")
		return
	}

	var req struct {
		NodeID     string `json:"node_id"`
		RaftAddr   string `json:"raft_addr"`
		Peers      string `json:"peers"`
		ClusterKey string `json:"cluster_key"`
	}
	body, _ := c.Body()
	if err := json.Unmarshal(body, &req); err != nil {
		writeXMLError(c, consts.StatusBadRequest, "MalformedJSON", err.Error())
		return
	}

	if req.RaftAddr == "" || req.Peers == "" {
		writeXMLError(c, consts.StatusBadRequest, "InvalidArgument", "raft_addr and peers are required")
		return
	}

	if err := s.joinCluster(req.NodeID, req.RaftAddr, req.Peers, req.ClusterKey); err != nil {
		resp, _ := json.Marshal(map[string]string{"error": err.Error()})
		c.Data(consts.StatusInternalServerError, "application/json", resp)
		return
	}

	s.joinCluster = nil
	s.emitEvent(eventstore.Event{Type: eventstore.EventTypeSystem, Action: eventstore.EventActionClusterJoin})

	resp, _ := json.Marshal(map[string]string{"status": "joined", "mode": "cluster"})
	c.Data(consts.StatusOK, "application/json", resp)
}
