package server

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/snapshot"
	"github.com/gritive/GrainFS/internal/storage"
)

// registerSnapshotAPI registers the snapshot management REST endpoints.
//
// POST   /admin/snapshots           — create snapshot
// GET    /admin/snapshots           — list snapshots
// POST   /admin/snapshots/:seq/restore — restore snapshot
// DELETE /admin/snapshots/:seq      — delete snapshot
func (s *Server) registerSnapshotAPI(h *server.Hertz) {
	admin := h.Group("/admin/snapshots")
	admin.POST("", s.createSnapshotHandler)
	admin.GET("", s.listSnapshotsHandler)
	admin.POST("/:seq/restore", s.restoreSnapshotHandler)
	admin.DELETE("/:seq", s.deleteSnapshotHandler)
}

func (s *Server) snapshotManager() (*snapshot.Manager, error) {
	snap, ok := s.backend.(storage.Snapshotable)
	if !ok {
		return nil, fmt.Errorf("backend does not support snapshots")
	}
	dir := filepath.Join(s.dataDir, "snapshots")
	walDir := filepath.Join(s.dataDir, "wal")
	return snapshot.NewManager(dir, snap, walDir)
}

// POST /admin/snapshots
func (s *Server) createSnapshotHandler(ctx context.Context, c *app.RequestContext) {
	mgr, err := s.snapshotManager()
	if err != nil {
		c.JSON(consts.StatusInternalServerError, apiError("snapshot unavailable", err.Error()))
		return
	}

	var req struct {
		Reason string `json:"reason"`
	}
	_ = c.BindJSON(&req) // reason is optional

	snap, err := mgr.Create(req.Reason)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, apiError("create snapshot failed", err.Error()))
		return
	}

	c.JSON(consts.StatusOK, map[string]interface{}{
		"seq":          snap.Seq,
		"timestamp":    snap.Timestamp,
		"object_count": snap.ObjectCount,
		"size_bytes":   snap.SizeBytes,
		"reason":       snap.Reason,
	})
}

// GET /admin/snapshots
func (s *Server) listSnapshotsHandler(ctx context.Context, c *app.RequestContext) {
	mgr, err := s.snapshotManager()
	if err != nil {
		c.JSON(consts.StatusInternalServerError, apiError("snapshot unavailable", err.Error()))
		return
	}

	snaps, err := mgr.List()
	if err != nil {
		c.JSON(consts.StatusInternalServerError, apiError("list snapshots failed", err.Error()))
		return
	}

	type snapInfo struct {
		Seq         uint64 `json:"seq"`
		Timestamp   string `json:"timestamp"`
		ObjectCount int    `json:"object_count"`
		SizeBytes   int64  `json:"size_bytes"`
		Reason      string `json:"reason,omitempty"`
	}
	items := make([]snapInfo, len(snaps))
	for i, sn := range snaps {
		items[i] = snapInfo{
			Seq:         sn.Seq,
			Timestamp:   sn.Timestamp.Format("2006-01-02T15:04:05Z"),
			ObjectCount: sn.ObjectCount,
			SizeBytes:   sn.SizeBytes,
			Reason:      sn.Reason,
		}
	}

	hint := ""
	if len(items) == 0 {
		hint = "no snapshots yet — POST /admin/snapshots to create one"
	}
	c.JSON(consts.StatusOK, map[string]interface{}{
		"snapshots": items,
		"hint":      hint,
	})
}

// POST /admin/snapshots/:seq/restore
func (s *Server) restoreSnapshotHandler(ctx context.Context, c *app.RequestContext) {
	seqStr := c.Param("seq")
	seq, err := strconv.ParseUint(seqStr, 10, 64)
	if err != nil {
		c.JSON(consts.StatusBadRequest, apiError("invalid seq", "seq must be a positive integer"))
		return
	}

	mgr, err := s.snapshotManager()
	if err != nil {
		c.JSON(consts.StatusInternalServerError, apiError("snapshot unavailable", err.Error()))
		return
	}

	count, stale, err := mgr.Restore(seq)
	if err != nil {
		if errors.Is(err, snapshot.ErrNotFound) {
			c.JSON(consts.StatusNotFound, apiError(
				fmt.Sprintf("snapshot %d not found", seq),
				"list available snapshots with GET /admin/snapshots",
			))
			return
		}
		c.JSON(consts.StatusInternalServerError, apiError("restore failed", err.Error()))
		return
	}

	if stale == nil {
		stale = []storage.StaleBlob{}
	}
	c.JSON(consts.StatusOK, map[string]interface{}{
		"restored_objects": count,
		"stale_blobs":      stale,
	})
}

// DELETE /admin/snapshots/:seq
func (s *Server) deleteSnapshotHandler(ctx context.Context, c *app.RequestContext) {
	seqStr := c.Param("seq")
	seq, err := strconv.ParseUint(seqStr, 10, 64)
	if err != nil {
		c.JSON(consts.StatusBadRequest, apiError("invalid seq", "seq must be a positive integer"))
		return
	}

	mgr, err := s.snapshotManager()
	if err != nil {
		c.JSON(consts.StatusInternalServerError, apiError("snapshot unavailable", err.Error()))
		return
	}

	if err := mgr.Delete(seq); err != nil {
		if errors.Is(err, snapshot.ErrNotFound) {
			c.JSON(consts.StatusNotFound, apiError(
				fmt.Sprintf("snapshot %d not found", seq),
				"list available snapshots with GET /admin/snapshots",
			))
			return
		}
		c.JSON(consts.StatusInternalServerError, apiError("delete failed", err.Error()))
		return
	}

	c.JSON(consts.StatusOK, map[string]bool{"deleted": true})
}

func apiError(msg, hint string) map[string]string {
	return map[string]string{"error": msg, "hint": hint}
}
