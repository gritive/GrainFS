package server

import (
	"context"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

// BalancerInfo provides balancer state for the status endpoint.
type BalancerInfo interface {
	Status() BalancerStatusResult
}

// BalancerStatusResult is the data returned by BalancerInfo.Status().
type BalancerStatusResult struct {
	Active       bool
	ImbalancePct float64
	Nodes        []BalancerNodeInfo
}

// BalancerNodeInfo is per-node stats for the status response.
type BalancerNodeInfo struct {
	NodeID         string
	DiskUsedPct    float64
	DiskAvailBytes uint64
	RequestsPerSec float64
	JoinedAt       time.Time
	UpdatedAt      time.Time
}

func (s *Server) registerBalancerAPI(h *server.Hertz) {
	h.GET("/api/cluster/balancer/status", s.balancerStatusHandler)
}

type balancerNodeJSON struct {
	NodeID         string    `json:"node_id"`
	DiskUsedPct    float64   `json:"disk_used_pct"`
	DiskAvailBytes uint64    `json:"disk_avail_bytes"`
	RequestsPerSec float64   `json:"requests_per_sec"`
	JoinedAt       time.Time `json:"joined_at,omitempty"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type balancerStatusResponse struct {
	Available    bool               `json:"available"`
	Active       bool               `json:"active"`
	ImbalancePct float64            `json:"imbalance_pct"`
	Nodes        []balancerNodeJSON `json:"nodes"`
}

func (s *Server) balancerStatusHandler(_ context.Context, c *app.RequestContext) {
	if s.balancer == nil {
		c.JSON(consts.StatusOK, balancerStatusResponse{Available: false})
		return
	}
	st := s.balancer.Status()
	nodes := make([]balancerNodeJSON, len(st.Nodes))
	for i, n := range st.Nodes {
		nodes[i] = balancerNodeJSON(n)
	}
	c.JSON(consts.StatusOK, balancerStatusResponse{
		Available:    true,
		Active:       st.Active,
		ImbalancePct: st.ImbalancePct,
		Nodes:        nodes,
	})
}
