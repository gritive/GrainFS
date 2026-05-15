package server

import (
	"context"
	"errors"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

// registerSnapshotAPI registers the snapshot management REST endpoints.
//
// POST   /admin/snapshots           — create snapshot
// GET    /admin/snapshots           — list snapshots
// POST   /admin/snapshots/:seq/restore — restore snapshot
// DELETE /admin/snapshots/:seq      — delete snapshot
func (s *Server) registerSnapshotAPI(h *server.Hertz) {
	admin := h.Group(routePathAdminSnapshots, localhostOnly())
	admin.POST("", s.createSnapshotHandler)
	admin.GET("", s.listSnapshotsHandler)
	admin.POST(routePathSnapshotSeqRestore, s.restoreSnapshotHandler)
	admin.DELETE(routePathSnapshotSeq, s.deleteSnapshotHandler)
}

// POST /admin/snapshots
func (s *Server) createSnapshotHandler(ctx context.Context, c *app.RequestContext) {
	if s.blockIfMutationDisabled(c, "snapshot_create") {
		return
	}

	var req struct {
		Reason string `json:"reason"`
	}
	_ = c.BindJSON(&req) // reason is optional

	snap, err := s.createSnapshot(req.Reason)
	if err != nil {
		if errors.Is(err, errSnapshotUnavailable) {
			c.JSON(consts.StatusInternalServerError, apiError("snapshot unavailable", err.Error()))
			return
		}
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
func (s *Server) listSnapshotsHandler(_ context.Context, c *app.RequestContext) {
	snaps, err := s.listSnapshots()
	if err != nil {
		if errors.Is(err, errSnapshotUnavailable) {
			c.JSON(consts.StatusInternalServerError, apiError("snapshot unavailable", err.Error()))
			return
		}
		c.JSON(consts.StatusInternalServerError, apiError("list snapshots failed", err.Error()))
		return
	}

	c.JSON(consts.StatusOK, listSnapshotsResponse(snaps))
}

// POST /admin/snapshots/:seq/restore
func (s *Server) restoreSnapshotHandler(ctx context.Context, c *app.RequestContext) {
	if s.blockIfMutationDisabled(c, "snapshot_restore") {
		return
	}
	seq, ok := snapshotSeqParam(c)
	if !ok {
		return
	}

	count, stale, err := s.restoreSnapshot(seq)
	if err != nil {
		writeSnapshotCommandError(c, "restore", seq, err)
		return
	}

	c.JSON(consts.StatusOK, map[string]interface{}{
		"restored_objects": count,
		"stale_blobs":      stale,
	})
}

// DELETE /admin/snapshots/:seq
func (s *Server) deleteSnapshotHandler(ctx context.Context, c *app.RequestContext) {
	if s.blockIfMutationDisabled(c, "snapshot_delete") {
		return
	}
	seq, ok := snapshotSeqParam(c)
	if !ok {
		return
	}

	if err := s.deleteSnapshot(seq); err != nil {
		writeSnapshotCommandError(c, "delete", seq, err)
		return
	}

	c.JSON(consts.StatusOK, map[string]bool{"deleted": true})
}

func apiError(msg, hint string) map[string]string {
	return map[string]string{"error": msg, "hint": hint}
}
