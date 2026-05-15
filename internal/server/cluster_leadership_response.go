package server

import (
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

func clusterNodeIsLeader(info ClusterInfo) (bool, string) {
	if info == nil {
		return false, ""
	}
	return info.State() == "Leader", info.LeaderID()
}

func clusterLeaderID(info ClusterInfo) string {
	if info == nil {
		return ""
	}
	return info.LeaderID()
}

func clusterNodeID(info ClusterInfo) string {
	if info == nil {
		return ""
	}
	return info.NodeID()
}

func clusterTerm(info ClusterInfo) uint64 {
	if info == nil {
		return 0
	}
	return info.Term()
}

func writeClusterNotLeader(c *app.RequestContext, msg, leaderID string) {
	c.JSON(consts.StatusConflict, map[string]any{
		"error":     msg,
		"leader_id": leaderID,
	})
}

func writeClusterNotLeaderRetry(c *app.RequestContext, msg, leaderID string) {
	c.JSON(consts.StatusConflict, map[string]any{
		"error":     msg,
		"leader_id": leaderID,
		"retry":     true,
	})
}
