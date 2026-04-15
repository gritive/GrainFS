// Package nfsserver provides an NFS v3 server backed by GrainVFS.
package nfsserver

import (
	"fmt"
	"log/slog"
	"net"
	"sync"

	nfs "github.com/willscott/go-nfs"
	nfshelper "github.com/willscott/go-nfs/helpers"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/vfs"
)

// Server wraps the go-nfs server with GrainVFS.
type Server struct {
	backend  storage.Backend
	volName  string
	mu       sync.Mutex
	listener net.Listener
}

// NewServer creates a new NFS server for the given volume.
func NewServer(backend storage.Backend, volName string) *Server {
	return &Server{backend: backend, volName: volName}
}

// ListenAndServe starts the NFS server on the given address.
func (s *Server) ListenAndServe(addr string) error {
	fs, err := vfs.New(s.backend, s.volName)
	if err != nil {
		return fmt.Errorf("create vfs: %w", err)
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("nfs listen: %w", err)
	}
	s.mu.Lock()
	s.listener = ln
	s.mu.Unlock()

	handler := nfshelper.NewNullAuthHandler(fs)
	cacheHandler := nfshelper.NewCachingHandler(handler, 1024)

	slog.Info("nfs server started", "component", "nfs", "addr", addr, "volume", s.volName)
	return nfs.Serve(ln, cacheHandler)
}

// Addr returns the listener address (useful when using port 0).
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return s.listener.Addr()
	}
	return nil
}

// Close stops the NFS server.
func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}
