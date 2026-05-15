package server

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

// clusterTransferLeader is implemented by *cluster.MetaRaft to expose the
// underlying raft TransferLeadership operation. Optional — the handler
// returns 503 when s.cluster does not satisfy this interface.
type clusterTransferLeader interface {
	TransferLeadership() error
	IsLeader() bool
}

// TransferLeaderRequest is currently empty (no target — Raft auto-picks).
type TransferLeaderRequest struct{}

// TransferLeaderResult is the success response of POST /v1/cluster/transfer-leader.
type TransferLeaderResult struct {
	OldLeader  string `json:"old_leader"`
	Term       uint64 `json:"term"`
	TargetHint string `json:"target_hint,omitempty"`
}

// transferLeaderHandler exposes POST /v1/cluster/transfer-leader on the
// admin Unix socket. Status code policy (A1-a):
//
//	cluster mode not configured (cluster feature unavailable OR no transfer support) → 503
//	mutation gate engaged                                                 → 503
//	not currently the leader                                              → 409 + leader_id
//	raft.ErrNoPeers (single node)                                         → 503
//	raft.ErrNotLeader (race during transfer)                              → 409 + retry hint
//	other internal error                                                  → 500
//	happy path                                                            → 200 TransferLeaderResult
func (s *Server) transferLeaderHandler(_ context.Context, c *app.RequestContext) {
	if s.blockIfMutationDisabled(c, "cluster_transfer_leader") {
		return
	}
	result, err := s.transferClusterLeadership()
	if err == nil {
		data, _ := json.Marshal(result)
		c.Data(consts.StatusOK, "application/json", data)
		return
	}

	var notLeader clusterNotLeaderError
	switch {
	case errors.Is(err, errClusterModeNotConfigured):
		c.JSON(consts.StatusServiceUnavailable, map[string]string{
			"error": "cluster mode not configured",
		})
		return
	case errors.Is(err, errClusterTransferNotSupported):
		c.JSON(consts.StatusServiceUnavailable, map[string]string{
			"error": "cluster adapter does not support transfer-leader",
		})
		return
	case errors.As(err, &notLeader):
		if notLeader.retry {
			writeClusterNotLeaderRetry(c, notLeader.Error(), notLeader.leaderID)
			return
		}
		writeClusterNotLeader(c, notLeader.Error(), notLeader.leaderID)
		return
	case errors.Is(err, errClusterTransferSingleNode):
		c.JSON(consts.StatusServiceUnavailable, map[string]string{
			"error": "single-node mode: no peers to transfer to",
		})
		return
	default:
		c.JSON(consts.StatusInternalServerError, map[string]string{
			"error": err.Error(),
		})
		return
	}
}
