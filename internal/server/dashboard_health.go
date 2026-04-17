package server

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

// BadgerStatsProvider is implemented by ECBackend to expose BadgerDB internals.
type BadgerStatsProvider interface {
	BadgerStats() BadgerStatsResult
}

// BadgerStatsResult carries BadgerDB size and table statistics.
type BadgerStatsResult struct {
	LSMSizeBytes  int64 `json:"lsm_size_bytes"`
	VlogSizeBytes int64 `json:"vlog_size_bytes"`
	NumTables     int   `json:"num_tables"`
}

// RaftStatsProvider is implemented by the cluster node to expose Raft internals.
type RaftStatsProvider interface {
	CommitIndex() uint64
	AppliedIndex() uint64
	LastLogIndex() uint64
}

// ECPolicyProvider is implemented by ECBackend to expose per-bucket EC config.
type ECPolicyProvider interface {
	ListBuckets() ([]string, error)
	GetBucketECPolicy(bucket string) (bool, error)
}

func (s *Server) registerDashboardHealthAPI(h *server.Hertz) {
	h.GET("/admin/health/badger", localhostOnly(), s.badgerHealthHandler)
	h.GET("/admin/health/raft", localhostOnly(), s.raftHealthHandler)
	h.GET("/admin/buckets/ec", localhostOnly(), s.bucketsECHandler)
}

// badgerHealthHandler serves GET /admin/health/badger.
func (s *Server) badgerHealthHandler(_ context.Context, c *app.RequestContext) {
	p, ok := s.backend.(BadgerStatsProvider)
	if !ok {
		c.JSON(consts.StatusOK, map[string]any{"available": false})
		return
	}
	stats := p.BadgerStats()
	c.JSON(consts.StatusOK, map[string]any{
		"available":       true,
		"lsm_size_bytes":  stats.LSMSizeBytes,
		"vlog_size_bytes": stats.VlogSizeBytes,
		"num_tables":      stats.NumTables,
	})
}

// raftHealthHandler serves GET /admin/health/raft.
func (s *Server) raftHealthHandler(_ context.Context, c *app.RequestContext) {
	if s.cluster == nil {
		c.JSON(consts.StatusOK, map[string]any{"available": false})
		return
	}
	resp := map[string]any{
		"available":  true,
		"node_id":    s.cluster.NodeID(),
		"state":      s.cluster.State(),
		"term":       s.cluster.Term(),
		"leader_id":  s.cluster.LeaderID(),
		"peers":      s.cluster.Peers(),
	}
	if rsp, ok := s.cluster.(RaftStatsProvider); ok {
		resp["commit_index"] = rsp.CommitIndex()
		resp["applied_index"] = rsp.AppliedIndex()
		resp["last_log_index"] = rsp.LastLogIndex()
	}
	c.JSON(consts.StatusOK, resp)
}

// bucketsECHandler serves GET /admin/buckets/ec.
func (s *Server) bucketsECHandler(_ context.Context, c *app.RequestContext) {
	p, ok := s.backend.(ECPolicyProvider)
	if !ok {
		c.JSON(consts.StatusOK, map[string]any{"buckets": []any{}})
		return
	}
	buckets, err := p.ListBuckets()
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	type bucketEC struct {
		Name      string `json:"name"`
		ECEnabled bool   `json:"ec_enabled"`
	}
	result := make([]bucketEC, 0, len(buckets))
	for _, b := range buckets {
		enabled, _ := p.GetBucketECPolicy(b)
		result = append(result, bucketEC{Name: b, ECEnabled: enabled})
	}
	c.JSON(consts.StatusOK, map[string]any{"buckets": result})
}
