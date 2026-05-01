package server

import (
	"context"
	"errors"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/raft"
)

func (s *Server) registerRaftSnapshotAPI(h *server.Hertz) {
	admin := h.Group("/admin/raft/snapshot", localhostOnly())
	admin.GET("", s.raftSnapshotStatusHandler)
	admin.POST("", s.raftSnapshotTriggerHandler)
}

func (s *Server) raftSnapshotStatusHandler(_ context.Context, c *app.RequestContext) {
	if s.raftSnapshots == nil {
		c.JSON(consts.StatusServiceUnavailable, apiError("raft snapshot unavailable", "server is not running with a Raft snapshot manager"))
		return
	}
	status, err := s.raftSnapshots.RaftSnapshotStatus()
	if err != nil {
		c.JSON(consts.StatusInternalServerError, apiError("raft snapshot status failed", err.Error()))
		return
	}
	c.JSON(consts.StatusOK, map[string]any{
		"available":  status.Available,
		"index":      status.Index,
		"term":       status.Term,
		"size_bytes": status.SizeBytes,
	})
}

func (s *Server) raftSnapshotTriggerHandler(ctx context.Context, c *app.RequestContext) {
	if s.raftSnapshots == nil {
		c.JSON(consts.StatusServiceUnavailable, apiError("raft snapshot unavailable", "server is not running with a Raft snapshot manager"))
		return
	}
	result, err := s.raftSnapshots.TriggerRaftSnapshot(ctx)
	if err != nil {
		metrics.RaftSnapshotTriggerTotal.WithLabelValues("failed").Inc()
		if errors.Is(err, raft.ErrNotLeader) {
			c.JSON(consts.StatusConflict, apiError("raft snapshot requires leader", "send the request to the current Raft leader"))
			return
		}
		c.JSON(consts.StatusInternalServerError, apiError("raft snapshot trigger failed", err.Error()))
		return
	}
	metrics.RaftSnapshotTriggerTotal.WithLabelValues("success").Inc()
	metrics.RaftSnapshotLastIndex.Set(float64(result.Index))
	metrics.RaftSnapshotLastSizeBytes.Set(float64(result.SizeBytes))
	c.JSON(consts.StatusOK, map[string]any{
		"index":      result.Index,
		"term":       result.Term,
		"size_bytes": result.SizeBytes,
	})
}
