package server

import (
	"context"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/vfs"
)

// VFSStatRequest is the request body for VFS stat operations.
type VFSStatRequest struct {
	VolumeName string `json:"volume_name"`
	FilePath   string `json:"file_path"`
}

// VFSStatResponse is the response from VFS stat operations.
type VFSStatResponse struct {
	Exists   bool   `json:"exists"`
	Name     string `json:"name,omitempty"`
	Size     int64  `json:"size,omitempty"`
	IsDir    bool   `json:"is_dir,omitempty"`
	ModTime   string `json:"mod_time,omitempty"`
	Error    string `json:"error,omitempty"`
}

// registerAdminAPI registers admin/debug endpoints for testing and operations.
// Admin endpoints require authentication or localhost access.
func (s *Server) registerAdminAPI(h *server.Hertz) {
	admin := h.Group("/admin/debug", localhostOnly())

	// VFS stat endpoint for testing cross-protocol cache coherency
	// Requires authentication unless accessed from localhost
	admin.POST("/vfs/stat", s.vfsStatHandler)
}

// vfsStatHandler performs VFS stat operations for testing.
// POST /admin/debug/vfs/stat
// Body: {"volume_name": "default", "file_path": "test.txt"}
// Response: {"exists": true, "name": "test.txt", "size": 12, ...}
func (s *Server) vfsStatHandler(ctx context.Context, c *app.RequestContext) {
	var req VFSStatRequest
	if err := c.BindJSON(&req); err != nil {
		c.JSON(consts.StatusBadRequest, VFSStatResponse{
			Error: fmt.Sprintf("invalid request: %v", err),
		})
		return
	}

	if req.VolumeName == "" || req.FilePath == "" {
		c.JSON(consts.StatusBadRequest, VFSStatResponse{
			Error: "volume_name and file_path are required",
		})
		return
	}

	// Create a temporary VFS instance that accesses the S3 bucket directly
	// For cross-protocol testing, we use the bucket name directly without prefix
	// This allows VFS to see objects uploaded via S3
	testVFS, err := vfs.NewDirect(s.backend, req.VolumeName,
		vfs.WithStatCacheTTL(1*time.Second),
		vfs.WithDirCacheTTL(1*time.Second),
	)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, VFSStatResponse{
			Error: fmt.Sprintf("create VFS: %v", err),
		})
		return
	}

	// Stat the file
	info, err := testVFS.Stat(req.FilePath)
	if err != nil {
		if os.IsNotExist(err) {
			c.JSON(consts.StatusOK, VFSStatResponse{
				Exists: false,
				Name:   path.Base(req.FilePath),
			})
			return
		}
		c.JSON(consts.StatusInternalServerError, VFSStatResponse{
			Error: fmt.Sprintf("stat error: %v", err),
		})
		return
	}

	c.JSON(consts.StatusOK, VFSStatResponse{
		Exists:  true,
		Name:    info.Name(),
		Size:    info.Size(),
		IsDir:   info.IsDir(),
		ModTime: info.ModTime().Format(time.RFC3339),
	})
}
