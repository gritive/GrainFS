package nfs4server

import (
	"fmt"
	"log/slog"
	"net"
	"sync"

	"github.com/gritive/GrainFS/internal/storage"
)

// Server handles NFSv4.0 connections over TCP.
type Server struct {
	backend  storage.Backend
	mu       sync.Mutex
	listener net.Listener
	logger   *slog.Logger
}

// NewServer creates an NFSv4 server backed by the given storage backend.
func NewServer(backend storage.Backend) *Server {
	return &Server{
		backend: backend,
		logger:  slog.With("component", "nfs4"),
	}
}

// ListenAndServe starts the NFSv4 TCP server.
// By default binds to localhost for security (AUTH_SYS is spoofable).
func (s *Server) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("nfs4 listen: %w", err)
	}

	s.mu.Lock()
	s.listener = ln
	s.mu.Unlock()

	s.logger.Info("nfs4 server started", "addr", addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			return nil // listener closed
		}
		go s.handleConn(conn)
	}
}

// Close closes the listener.
func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

// Addr returns the listener address.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return s.listener.Addr()
	}
	return nil
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()

	for {
		frame, err := readRPCFrame(conn)
		if err != nil {
			return // connection closed or error
		}

		// Parse ONC RPC call header (minimal parsing)
		if len(frame) < 24 {
			continue // too short for valid RPC call
		}

		// For now: create a dispatcher per call (stateless v4.0 stubs)
		dispatcher := NewDispatcher(s.backend)

		// TODO: XDR decode the COMPOUND request from frame
		// For now, return a minimal COMPOUND reply
		_ = dispatcher

		// Write back a minimal reply frame
		reply := make([]byte, 24) // minimal RPC reply
		writeRPCFrame(conn, reply)
	}
}
