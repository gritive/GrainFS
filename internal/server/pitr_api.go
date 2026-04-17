package server

import (
	"context"
	"errors"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/snapshot"
	"github.com/gritive/GrainFS/internal/storage"
)

// registerPITRAPI registers the PITR endpoint.
//
// POST /admin/pitr  — restore to a point in time
func (s *Server) registerPITRAPI(h *server.Hertz) {
	h.POST("/admin/pitr", s.pitrRestoreHandler)
}

// POST /admin/pitr
// Body: {"to": "2026-04-17T10:00:00Z"}
// Response: {"restored_objects": N, "wal_entries_replayed": M, "stale_blobs": [...]}
func (s *Server) pitrRestoreHandler(ctx context.Context, c *app.RequestContext) {
	var req struct {
		To string `json:"to"`
	}
	if err := c.BindJSON(&req); err != nil || req.To == "" {
		c.JSON(consts.StatusBadRequest, apiError("missing 'to' field", "provide RFC3339 timestamp"))
		return
	}

	targetTime, err := time.Parse(time.RFC3339Nano, req.To)
	if err != nil {
		// Try RFC3339 without nanoseconds
		targetTime, err = time.Parse(time.RFC3339, req.To)
		if err != nil {
			c.JSON(consts.StatusBadRequest, apiError("invalid 'to' timestamp", "use RFC3339 format: 2006-01-02T15:04:05Z"))
			return
		}
	}

	mgr, err := s.snapshotManager()
	if err != nil {
		c.JSON(consts.StatusInternalServerError, apiError("snapshot unavailable", err.Error()))
		return
	}

	result, err := mgr.PITRRestore(targetTime)
	if err != nil {
		if errors.Is(err, snapshot.ErrNoSnapshotBefore) {
			c.JSON(consts.StatusNotFound, apiError(
				"no snapshot found before target time",
				"create a snapshot first with POST /admin/snapshots",
			))
			return
		}
		c.JSON(consts.StatusInternalServerError, apiError("PITR restore failed", err.Error()))
		return
	}

	stale := result.StaleBlobs
	if stale == nil {
		stale = []storage.StaleBlob{}
	}
	c.JSON(consts.StatusOK, map[string]interface{}{
		"restored_objects":     result.RestoredObjects,
		"wal_entries_replayed": result.WALEntriesReplayed,
		"stale_blobs":          stale,
		"base_snapshot_seq":    result.BaseSnapshotSeq,
	})
}
