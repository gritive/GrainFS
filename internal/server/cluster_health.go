package server

import (
	"context"
	"encoding/json"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/gritive/GrainFS/internal/cluster"
)

// Health, QuorumInfo, PeerHealthRow are the wire types for GET /v1/cluster/health.
// Canonical definitions live in adminapi; aliased here so server's deriveHealth
// can keep returning literal `Health{...}` values without touching production code.
type (
	Health        = adminapi.Health
	QuorumInfo    = adminapi.QuorumInfo
	PeerHealthRow = adminapi.PeerHealthRow
)

// clusterHealth handles GET /v1/cluster/health. Server-side derivation of
// Issues so the dashboard and CLI share the same rules.
func (s *Server) clusterHealth(_ context.Context, c *app.RequestContext) {
	h := s.deriveHealth()
	data, _ := json.Marshal(h)
	c.Data(consts.StatusOK, "application/json", data)
}

// deriveHealth returns the current cluster health wire model.
func (s *Server) deriveHealth() Health {
	return s.clusterHealthSnapshot()
}

func buildClusterHealth(info ClusterInfo, degraded bool) Health {
	h := Health{
		Mode:     "local",
		Degraded: degraded,
	}
	hasConfiguredPeers := false
	if info != nil {
		h.Mode = "cluster"
		h.LeaderID = info.LeaderID()
		h.Term = info.Term()
		peers := info.Peers()
		hasConfiguredPeers = len(peers) > 0

		snap := info.Snapshot()

		// Prefer typed peer_snapshot evidence when available.
		rows := snap.PeerSnapshot
		if len(rows) > 0 {
			h.Peers = mapPeerHealthRows(rows)
		} else {
			// Fallback: peers list only. Default state to "configured".
			h.Peers = append(h.Peers, PeerHealthRow{PeerID: info.NodeID(), State: "self"})
			for _, p := range peers {
				if p == info.NodeID() {
					continue
				}
				h.Peers = append(h.Peers, PeerHealthRow{PeerID: p, State: "configured"})
			}
		}

		votersTotal := len(h.Peers)
		aliveCount := 0
		for _, p := range h.Peers {
			if p.State == "self" || p.State == "live" {
				aliveCount++
			}
		}
		required := (votersTotal / 2) + 1
		h.Quorum = QuorumInfo{
			VotersTotal: votersTotal,
			AliveCount:  aliveCount,
			Required:    required,
			Healthy:     aliveCount >= required,
		}
		h.DataGroups = mapDataGroupHealthRows(snap.DataGroupRaftHealth)
	}
	h.Issues = deriveIssues(h, hasConfiguredPeers)
	return h
}

func mapDataGroupHealthRows(rows []cluster.DataGroupRaftHealth) *adminapi.DataGroupHealthSummary {
	if len(rows) == 0 {
		return nil
	}
	out := &adminapi.DataGroupHealthSummary{
		Total:  len(rows),
		Groups: make([]adminapi.DataGroupHealthRow, 0, len(rows)),
	}
	for _, r := range rows {
		row := adminapi.DataGroupHealthRow{
			GroupID:        r.GroupID,
			PeerIDs:        append([]string(nil), r.PeerIDs...),
			RaftVoters:     append([]string(nil), r.RaftVoters...),
			LocalState:     r.LocalState,
			LeaderID:       r.LeaderID,
			Term:           r.Term,
			CommitIndex:    r.CommitIndex,
			LastLogIndex:   r.LastLogIndex,
			PeerMatchIndex: cloneUint64Map(r.PeerMatchIndex),
			MaxPeerLag:     r.MaxPeerLag,
			Issues:         append([]string(nil), r.Issues...),
		}
		if len(r.Issues) == 0 {
			out.Healthy++
		}
		for _, issue := range r.Issues {
			switch issue {
			case "leaderless", "unwired":
				out.Leaderless++
			case "peer_lag":
				out.Lagging++
			}
		}
		out.Groups = append(out.Groups, row)
	}
	return out
}

func cloneUint64Map(in map[string]uint64) map[string]uint64 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]uint64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
