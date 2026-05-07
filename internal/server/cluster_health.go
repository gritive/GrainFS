package server

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/cluster"
)

// Health is the response of GET /v1/cluster/health. Mirrors clusteradmin.Health.
type Health struct {
	Mode     string          `json:"mode"`
	Degraded bool            `json:"degraded"`
	LeaderID string          `json:"leader_id,omitempty"`
	Term     uint64          `json:"term,omitempty"`
	Quorum   QuorumInfo      `json:"quorum"`
	Peers    []PeerHealthRow `json:"peers,omitempty"`
	Issues   []string        `json:"issues,omitempty"`
}

// QuorumInfo summarises voter liveness vs the size required for majority.
type QuorumInfo struct {
	VotersTotal int  `json:"voters_total"`
	AliveCount  int  `json:"alive_count"`
	Required    int  `json:"required"`
	Healthy     bool `json:"healthy"`
}

// PeerHealthRow is one row in Health.Peers. State is one of:
// "self", "live", "cooldown", "down", or "configured".
type PeerHealthRow struct {
	PeerID   string `json:"peer_id"`
	State    string `json:"state"`
	RaftAddr string `json:"raft_addr,omitempty"`
}

// clusterHealth handles GET /v1/cluster/health. Server-side derivation of
// Issues so the dashboard and CLI share the same rules.
func (s *Server) clusterHealth(_ context.Context, c *app.RequestContext) {
	h := s.deriveHealth()
	data, _ := json.Marshal(h)
	c.Data(consts.StatusOK, "application/json", data)
}

// deriveHealth reads s.cluster + degradedFlag + peer_snapshot and returns
// a populated Health struct (with Issues derived).
func (s *Server) deriveHealth() Health {
	h := Health{
		Mode:     "local",
		Degraded: s.degradedFlag.Load(),
	}
	hasConfiguredPeers := false
	if s.cluster != nil {
		h.Mode = "cluster"
		h.LeaderID = s.cluster.LeaderID()
		h.Term = s.cluster.Term()
		peers := s.cluster.Peers()
		hasConfiguredPeers = len(peers) > 0

		// Prefer typed peer_snapshot evidence when available.
		var rows []cluster.PeerLivenessRow
		if snap, ok := s.cluster.(clusterPeerSnapshot); ok {
			rows = snap.PeerSnapshot()
		}
		if len(rows) > 0 {
			h.Peers = mapPeerHealthRows(rows)
		} else {
			// Fallback: peers list only. Default state to "configured".
			h.Peers = append(h.Peers, PeerHealthRow{PeerID: s.cluster.NodeID(), State: "self"})
			for _, p := range peers {
				if p == s.cluster.NodeID() {
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
	}
	h.Issues = deriveIssues(h, hasConfiguredPeers)
	return h
}

// mapPeerHealthRows translates internal PeerLivenessRow into the wire
// PeerHealthRow shape. State mapping:
//
//	PeerIdentitySelf                          → "self"
//	PeerLivenessLive                          → "live"
//	PeerLivenessHealthCooldown                → "cooldown"
//	PeerLivenessProbeFailed                   → "down"
//	other (Configured / UnresolvedLegacy etc) → "configured"
func mapPeerHealthRows(rows []cluster.PeerLivenessRow) []PeerHealthRow {
	out := make([]PeerHealthRow, 0, len(rows))
	for _, r := range rows {
		state := "configured"
		switch {
		case r.IdentityState == cluster.PeerIdentitySelf:
			state = "self"
		case r.LivenessState == cluster.PeerLivenessLive:
			state = "live"
		case r.LivenessState == cluster.PeerLivenessHealthCooldown:
			state = "cooldown"
		case r.LivenessState == cluster.PeerLivenessProbeFailed:
			state = "down"
		}
		out = append(out, PeerHealthRow{
			PeerID:   r.PeerID,
			State:    state,
			RaftAddr: r.RaftAddr,
		})
	}
	return out
}

// deriveIssues applies Phase-1 rules to produce human-readable issues.
// Pure function for table-driven testing.
//
// Rules:
//  1. Mode=local with configured peers → "single-node mode"
//  2. Cluster mode + quorum lost → "QUORUM LOST: A/V alive, need R"
//  3. Cluster mode + voters down/cooldown (quorum still healthy) → per-voter notice
//  4. Degraded EC flag → "EC degraded mode"
//  5. (extension point)
func deriveIssues(h Health, hasConfiguredPeers bool) []string {
	var issues []string
	if h.Mode == "local" && hasConfiguredPeers {
		issues = append(issues, "single-node mode")
	}
	if h.Mode == "cluster" {
		if !h.Quorum.Healthy {
			issues = append(issues, fmt.Sprintf(
				"QUORUM LOST: only %d/%d voters alive, need %d",
				h.Quorum.AliveCount, h.Quorum.VotersTotal, h.Quorum.Required))
		} else if h.Quorum.AliveCount < h.Quorum.VotersTotal {
			for _, p := range h.Peers {
				if p.State == "down" || p.State == "cooldown" {
					issues = append(issues, fmt.Sprintf("voter %s %s — investigate", p.PeerID, p.State))
				}
			}
		}
	}
	if h.Degraded {
		issues = append(issues, "EC degraded mode")
	}
	return issues
}
