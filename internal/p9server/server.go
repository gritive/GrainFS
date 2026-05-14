package p9server

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/hugelgupf/p9/p9"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/storage"
)

// Server is a 9P2000.L server backed by a storage.Backend.
type Server struct {
	backend storage.Backend
	locks   *objectLocks
	p9srv   *p9.Server

	mu     sync.Mutex
	cancel context.CancelFunc
	ln     net.Listener
	conns  map[net.Conn]struct{}
}

// NewServer creates a 9P server backed by backend.
func NewServer(backend storage.Backend) *Server {
	locks := newObjectLocks()
	s := &Server{backend: backend, locks: locks, conns: make(map[net.Conn]struct{})}
	s.p9srv = p9.NewServer(&attacher{backend: backend, locks: locks})
	return s
}

// ListenAndServe starts the TCP listener and serves 9P connections until ctx is cancelled.
func (s *Server) ListenAndServe(ctx context.Context, addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("9p listen %s: %w", addr, err)
	}
	return s.Serve(ctx, ln)
}

// Serve accepts 9P connections from an already-open listener.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	ctx, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	s.cancel = cancel
	s.ln = ln
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		if s.ln == ln {
			s.cancel = nil
			s.ln = nil
		}
		s.mu.Unlock()
		cancel()
	}()
	log.Info().Str("addr", ln.Addr().String()).Msg("9p server started")
	return s.p9srv.ServeContext(ctx, &trackingListener{Listener: ln, server: s})
}

// Close stops the listener created by ListenAndServe.
func (s *Server) Close() error {
	s.mu.Lock()
	cancel := s.cancel
	ln := s.ln
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	for conn := range s.activeConns() {
		_ = conn.Close()
	}
	if ln != nil {
		return ln.Close()
	}
	return nil
}

func (s *Server) activeConns() map[net.Conn]struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	conns := make(map[net.Conn]struct{}, len(s.conns))
	for conn := range s.conns {
		conns[conn] = struct{}{}
	}
	return conns
}

func (s *Server) trackConn(conn net.Conn) net.Conn {
	wrapped := &trackedConn{Conn: conn, server: s}
	s.mu.Lock()
	s.conns[wrapped] = struct{}{}
	s.mu.Unlock()
	return wrapped
}

func (s *Server) untrackConn(conn net.Conn) {
	s.mu.Lock()
	delete(s.conns, conn)
	s.mu.Unlock()
}

type trackingListener struct {
	net.Listener
	server *Server
}

func (l *trackingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return l.server.trackConn(conn), nil
}

type trackedConn struct {
	net.Conn
	server *Server
	once   sync.Once
}

func (c *trackedConn) Close() error {
	c.once.Do(func() {
		c.server.untrackConn(c)
	})
	return c.Conn.Close()
}

type attacher struct {
	backend storage.Backend
	locks   *objectLocks
}

func (a *attacher) Attach() (p9.File, error) {
	return &rootFile{backend: a.backend, locks: a.locks}, nil
}
