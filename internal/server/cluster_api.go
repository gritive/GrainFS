package server

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/gritive/GrainFS/internal/cluster"
)

func (s *Server) clusterStatus(ctx context.Context, c *app.RequestContext) {
	status := s.clusterStatusSnapshot(string(c.QueryArgs().Peek("bucket")))
	data, _ := json.Marshal(status)
	c.Data(consts.StatusOK, "application/json", data)
}

func (s *Server) clusterPlacement(ctx context.Context, c *app.RequestContext) {
	bucket := string(c.QueryArgs().Peek("bucket"))
	key := string(c.QueryArgs().Peek("key"))
	limit := 100
	if raw := string(c.QueryArgs().Peek("limit")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			c.JSON(consts.StatusBadRequest, map[string]string{"error": "limit must be a positive integer"})
			return
		}
		limit = n
	}
	if key != "" && bucket == "" {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "key filter requires bucket"})
		return
	}
	c.JSON(consts.StatusOK, s.clusterPlacementReport(bucket, key, limit))
}

func clusterStatusShardGroups(groups []cluster.ShardGroupEntry) []adminapi.ShardGroup {
	out := make([]adminapi.ShardGroup, 0, len(groups))
	for _, group := range groups {
		peers := append([]string(nil), group.PeerIDs...)
		out = append(out, adminapi.ShardGroup{ID: group.ID, PeerIDs: peers})
	}
	return out
}
