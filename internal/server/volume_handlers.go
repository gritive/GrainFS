package server

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

type volumeRequest struct {
	Size int64 `json:"size"`
}

type volumeResponse struct {
	Name      string `json:"name"`
	Size      int64  `json:"size"`
	BlockSize int    `json:"block_size"`
}

func (s *Server) listVolumes(_ context.Context, c *app.RequestContext) {
	vols, err := s.volMgr.List()
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	result := make([]volumeResponse, 0, len(vols))
	for _, v := range vols {
		result = append(result, volumeResponse{Name: v.Name, Size: v.Size, BlockSize: v.BlockSize})
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

	c.JSON(consts.StatusCreated, volumeResponse{Name: vol.Name, Size: vol.Size, BlockSize: vol.BlockSize})
}

func (s *Server) getVolume(_ context.Context, c *app.RequestContext) {
	name := c.Param("name")

	vol, err := s.volMgr.Get(name)
	if err != nil {
		c.JSON(consts.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}

	c.JSON(consts.StatusOK, volumeResponse{Name: vol.Name, Size: vol.Size, BlockSize: vol.BlockSize})
}

func (s *Server) deleteVolume(_ context.Context, c *app.RequestContext) {
	name := c.Param("name")

	if err := s.volMgr.Delete(name); err != nil {
		c.JSON(consts.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}

	c.SetStatusCode(consts.StatusNoContent)
}
