package server

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/volume"
)

type volumeRequest struct {
	Size int64 `json:"size"`
}

type volumeResponse struct {
	Name            string `json:"name"`
	Size            int64  `json:"size"`
	BlockSize       int    `json:"block_size"`
	AllocatedBytes  int64  `json:"allocated_bytes"`
	AllocatedBlocks int64  `json:"allocated_blocks"`
}

func (s *Server) listVolumes(_ context.Context, c *app.RequestContext) {
	vols, err := s.volMgr.List()
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	result := make([]volumeResponse, 0, len(vols))
	for _, v := range vols {
		result = append(result, volumeResponse{Name: v.Name, Size: v.Size, BlockSize: v.BlockSize, AllocatedBytes: v.AllocatedBytes(), AllocatedBlocks: v.AllocatedBlocks})
	}
	c.JSON(consts.StatusOK, result)
}

func (s *Server) createVolume(_ context.Context, c *app.RequestContext) {
	name := c.Param("name")

	var req volumeRequest
	// Try JSON body first, fall back to query param
	body, _ := c.Body()
	if err := json.Unmarshal(body, &req); err != nil || req.Size == 0 {
		sizeStr := c.Query("size")
		if sizeStr != "" {
			size, err := strconv.ParseInt(sizeStr, 10, 64)
			if err == nil {
				req.Size = size
			}
		}
	}

	if req.Size <= 0 {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "size is required and must be positive"})
		return
	}

	vol, err := s.volMgr.Create(name, req.Size)
	if err != nil {
		c.JSON(consts.StatusConflict, map[string]string{"error": err.Error()})
		return
	}

	c.JSON(consts.StatusCreated, volumeResponse{Name: vol.Name, Size: vol.Size, BlockSize: vol.BlockSize, AllocatedBytes: vol.AllocatedBytes(), AllocatedBlocks: vol.AllocatedBlocks})
}

func (s *Server) getVolume(_ context.Context, c *app.RequestContext) {
	name := c.Param("name")

	vol, err := s.volMgr.Get(name)
	if err != nil {
		c.JSON(consts.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}

	c.JSON(consts.StatusOK, volumeResponse{Name: vol.Name, Size: vol.Size, BlockSize: vol.BlockSize, AllocatedBytes: vol.AllocatedBytes(), AllocatedBlocks: vol.AllocatedBlocks})
}

func (s *Server) deleteVolume(_ context.Context, c *app.RequestContext) {
	name := c.Param("name")

	if err := s.volMgr.Delete(name); err != nil {
		c.JSON(consts.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}

	c.SetStatusCode(consts.StatusNoContent)
}

func (s *Server) recalculateVolume(_ context.Context, c *app.RequestContext) {
	name := c.Param("name")

	before, after, err := s.volMgr.Recalculate(name)
	if err != nil {
		status := consts.StatusInternalServerError
		if errors.Is(err, volume.ErrNotFound) {
			status = consts.StatusNotFound
		}
		c.JSON(status, map[string]string{"error": err.Error()})
		return
	}

	c.JSON(consts.StatusOK, map[string]any{
		"volume": name,
		"before": before,
		"after":  after,
		"fixed":  before != after,
	})
}

// --- Snapshot handlers ---

type snapshotResponse struct {
	ID         string    `json:"id"`
	CreatedAt  time.Time `json:"created_at"`
	BlockCount int64     `json:"block_count"`
}

func (s *Server) createSnapshot(_ context.Context, c *app.RequestContext) {
	name := c.Param("name")

	snapID, err := s.volMgr.CreateSnapshot(name)
	if err != nil {
		status := consts.StatusInternalServerError
		if errors.Is(err, volume.ErrNotFound) {
			status = consts.StatusNotFound
		}
		c.JSON(status, map[string]string{"error": err.Error()})
		return
	}

	snaps, _ := s.volMgr.ListSnapshots(name)
	for _, snap := range snaps {
		if snap.ID == snapID {
			c.JSON(consts.StatusCreated, snapshotResponse{ID: snap.ID, CreatedAt: snap.CreatedAt, BlockCount: snap.BlockCount})
			return
		}
	}
	c.JSON(consts.StatusCreated, map[string]string{"id": snapID})
}

func (s *Server) listSnapshots(_ context.Context, c *app.RequestContext) {
	name := c.Param("name")

	snaps, err := s.volMgr.ListSnapshots(name)
	if err != nil {
		status := consts.StatusInternalServerError
		if errors.Is(err, volume.ErrNotFound) {
			status = consts.StatusNotFound
		}
		c.JSON(status, map[string]string{"error": err.Error()})
		return
	}

	result := make([]snapshotResponse, 0, len(snaps))
	for _, snap := range snaps {
		result = append(result, snapshotResponse{ID: snap.ID, CreatedAt: snap.CreatedAt, BlockCount: snap.BlockCount})
	}
	c.JSON(consts.StatusOK, result)
}

func (s *Server) deleteSnapshot(_ context.Context, c *app.RequestContext) {
	name := c.Param("name")
	snapID := c.Param("snap_id")

	if err := s.volMgr.DeleteSnapshot(name, snapID); err != nil {
		status := consts.StatusInternalServerError
		if errors.Is(err, volume.ErrNotFound) {
			status = consts.StatusNotFound
		}
		c.JSON(status, map[string]string{"error": err.Error()})
		return
	}
	c.SetStatusCode(consts.StatusNoContent)
}

func (s *Server) rollbackVolume(_ context.Context, c *app.RequestContext) {
	name := c.Param("name")
	snapID := c.Param("snap_id")

	if err := s.volMgr.Rollback(name, snapID); err != nil {
		status := consts.StatusInternalServerError
		if errors.Is(err, volume.ErrNotFound) {
			status = consts.StatusNotFound
		}
		c.JSON(status, map[string]string{"error": err.Error()})
		return
	}
	c.SetStatusCode(consts.StatusNoContent)
}

func (s *Server) cloneVolume(_ context.Context, c *app.RequestContext) {
	var req struct {
		Src string `json:"src"`
		Dst string `json:"dst"`
	}
	body, _ := c.Body()
	if err := json.Unmarshal(body, &req); err != nil || req.Src == "" || req.Dst == "" {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "src and dst are required"})
		return
	}

	if err := s.volMgr.Clone(req.Src, req.Dst); err != nil {
		status := consts.StatusInternalServerError
		if errors.Is(err, volume.ErrNotFound) {
			status = consts.StatusNotFound
		}
		c.JSON(status, map[string]string{"error": err.Error()})
		return
	}

	vol, err := s.volMgr.Get(req.Dst)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	c.JSON(consts.StatusCreated, volumeResponse{
		Name: vol.Name, Size: vol.Size, BlockSize: vol.BlockSize,
		AllocatedBytes: vol.AllocatedBytes(), AllocatedBlocks: vol.AllocatedBlocks,
	})
}
