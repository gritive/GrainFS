package server

func buildRaftHealthResponse(info ClusterInfo) map[string]any {
	if info == nil {
		return map[string]any{"available": false}
	}
	resp := map[string]any{
		"available": true,
		"node_id":   info.NodeID(),
		"state":     info.State(),
		"term":      info.Term(),
		"leader_id": info.LeaderID(),
		"peers":     info.Peers(),
	}
	if rsp, ok := info.(RaftStatsProvider); ok {
		resp["commit_index"] = rsp.CommitIndex()
		resp["applied_index"] = rsp.AppliedIndex()
		resp["last_log_index"] = rsp.LastLogIndex()
	}
	return resp
}
