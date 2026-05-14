package p9server

import (
	"context"
	"fmt"
	"net"

	"github.com/hugelgupf/p9/p9"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/storage"
)

// Server is a 9P2000.L server backed by a storage.Backend.
// Read-only: Walk, Readdir, GetAttr, ReadAt only. All writes return EROFS.
type Server struct {
	backend storage.Backend
	p9srv   *p9.Server
}

// NewServer creates a new read-only 9P server backed by backend.
func NewServer(backend storage.Backend) *Server {
	s := &Server{backend: backend}
	s.p9srv = p9.NewServer(&attacher{backend: backend})
	return s
}

// ListenAndServe starts the TCP listener and serves 9P connections until ctx is cancelled.
func (s *Server) ListenAndServe(ctx context.Context, addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("9p listen %s: %w", addr, err)
	}
	log.Info().Str("addr", addr).Msg("9p server started")
	return s.p9srv.ServeContext(ctx, ln)
}

type attacher struct {
	backend storage.Backend
}

func (a *attacher) Attach() (p9.File, error) {
	return &rootFile{backend: a.backend}, nil
}
