package server

import (
	"context"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/adminapi"
)

// BalancerInfo provides balancer state for the status endpoint.
type BalancerInfo interface {
	Status() BalancerStatusResult
}

// BalancerStatusResult is the domain shape returned by BalancerInfo.Status().
// Distinct from adminapi.BalancerStatus (the wire shape) because Nodes uses
// time.Time fields formatted to RFC3339 at the wire boundary.
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

func (s *Server) balancerStatusHandler(_ context.Context, c *app.RequestContext) {
	if s.balancer == nil {
		c.JSON(consts.StatusOK, adminapi.BalancerStatus{Available: false})
		return
	}
	st := s.balancer.Status()
	out := adminapi.BalancerStatus{
		Available:    true,
		Active:       st.Active,
		ImbalancePct: st.ImbalancePct,
		Nodes:        make([]adminapi.BalancerNodeStatus, len(st.Nodes)),
	}
	for i, n := range st.Nodes {
		out.Nodes[i] = adminapi.BalancerNodeStatus{
			NodeID:         n.NodeID,
			DiskUsedPct:    n.DiskUsedPct,
			DiskAvailBytes: n.DiskAvailBytes,
			RequestsPerSec: n.RequestsPerSec,
			JoinedAt:       formatRFC3339OrEmpty(n.JoinedAt),
			UpdatedAt:      formatRFC3339OrEmpty(n.UpdatedAt),
		}
	}
	c.JSON(consts.StatusOK, out)
}

func formatRFC3339OrEmpty(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}
