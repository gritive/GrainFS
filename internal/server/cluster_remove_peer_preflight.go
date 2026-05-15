package server

import (
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/cluster"
)

func evaluateRemovePeerPreflight(info ClusterInfo, id string) (cluster.RemovePeerPreflightResult, bool) {
	if info == nil {
		return cluster.RemovePeerPreflightResult{}, false
	}
	snapshot := info.Snapshot().PeerSnapshot
	if len(snapshot) == 0 {
		return cluster.RemovePeerPreflightResult{}, false
	}
	return cluster.EvaluateRemovePeerPreflight(cluster.RemovePeerPreflightInput{
		TargetID: id,
		Voters:   removePeerPreflightVoters(info.Peers(), snapshot),
		Snapshot: snapshot,
	}), true
}

func removePeerPreflightVoters(voters []string, snapshot []cluster.PeerLivenessRow) []string {
	if len(voters) == 0 || len(snapshot) == 0 {
		return voters
	}
	byAddr := make(map[string]string, len(snapshot))
	byID := make(map[string]string, len(snapshot))
	for _, row := range snapshot {
		if row.IdentityState == cluster.PeerIdentitySelf || row.PeerID == "" {
			continue
		}
		byID[row.PeerID] = row.PeerID
		if row.RaftAddr != "" {
			byAddr[row.RaftAddr] = row.PeerID
		}
	}
	out := make([]string, len(voters))
	for i, voter := range voters {
		switch {
		case byID[voter] != "":
			out[i] = byID[voter]
		case byAddr[voter] != "":
			out[i] = byAddr[voter]
		default:
			out[i] = voter
		}
	}
	return out
}

func writeRemovePeerSnapshotUnavailable(c *app.RequestContext) {
	c.JSON(consts.StatusConflict, map[string]string{
		"error": "peer snapshot unavailable",
		"hint":  "cluster liveness snapshot is required before membership mutation",
	})
}

func writeRemovePeerPreflightFailure(c *app.RequestContext, result cluster.RemovePeerPreflightResult, id string) {
	switch result.Reason {
	case cluster.RemovePeerPreflightNotInCluster:
		c.JSON(consts.StatusNotFound, map[string]any{
			"error": "peer not in cluster",
			"id":    id,
		})
	case cluster.RemovePeerPreflightIdentityUnresolved:
		c.JSON(consts.StatusConflict, map[string]any{
			"error":          "membership identity unresolved",
			"blocking_peers": result.BlockingPeers,
			"hint":           "remove the unresolved legacy peer first or restore its node ID mapping",
		})
	default:
		c.JSON(consts.StatusConflict, map[string]any{
			"error":        "quorum would break",
			"voters_after": result.VotersAfter,
			"alive_after":  result.AliveAfter,
			"new_quorum":   result.NewQuorum,
			"hint":         "rerun with force=true to override",
		})
	}
}
