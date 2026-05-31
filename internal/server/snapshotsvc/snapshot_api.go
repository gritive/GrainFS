package snapshotsvc

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
func (h *Handler) Register(hz *server.Hertz, adminPath, seqRestorePath, seqPath string) {
	admin := hz.Group(adminPath, h.deps.LocalhostOnly())
	admin.POST("", h.createSnapshotHandler)
	admin.GET("", h.listSnapshotsHandler)
	admin.POST(seqRestorePath, h.restoreSnapshotHandler)
	admin.DELETE(seqPath, h.deleteSnapshotHandler)
}

// POST /admin/snapshots
func (h *Handler) createSnapshotHandler(ctx context.Context, c *app.RequestContext) {
	if h.deps.MutationDisabled(c, "snapshot_create") {
		return
	}

	var req struct {
		Reason string `json:"reason"`
	}
	_ = c.BindJSON(&req) // reason is optional

	snap, err := h.createSnapshot(req.Reason)
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
func (h *Handler) listSnapshotsHandler(_ context.Context, c *app.RequestContext) {
	snaps, err := h.listSnapshots()
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
func (h *Handler) restoreSnapshotHandler(ctx context.Context, c *app.RequestContext) {
	if h.deps.MutationDisabled(c, "snapshot_restore") {
		return
	}
	seq, ok := snapshotSeqParam(c)
	if !ok {
		return
	}

	count, stale, err := h.restoreSnapshot(seq)
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
func (h *Handler) deleteSnapshotHandler(ctx context.Context, c *app.RequestContext) {
	if h.deps.MutationDisabled(c, "snapshot_delete") {
		return
	}
	seq, ok := snapshotSeqParam(c)
	if !ok {
		return
	}

	if err := h.deleteSnapshot(seq); err != nil {
		writeSnapshotCommandError(c, "delete", seq, err)
		return
	}

	c.JSON(consts.StatusOK, map[string]bool{"deleted": true})
}

func apiError(msg, hint string) map[string]string {
	return map[string]string{"error": msg, "hint": hint}
}
