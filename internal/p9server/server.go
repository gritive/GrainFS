package p9server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/hugelgupf/p9/p9"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/audit"
	"github.com/gritive/GrainFS/internal/iam/mountsastore"
	"github.com/gritive/GrainFS/internal/iam/policy"
	"github.com/gritive/GrainFS/internal/nfsexport"
	"github.com/gritive/GrainFS/internal/storage"
)

// exportGetter is the read-only view of the NFS export store used by the 9P
// server. The concrete *nfsexport.ExportService satisfies this interface
// (via its Get method); tests may inject stubs.
type exportGetter interface {
	Get(bucket string) (nfsexport.Config, bool)
}

// p9Authorizer is the slice of s3auth.Authorizer consumed by the 9P IAM gate.
// Defined here so tests can inject stubs without a full policy store.
type p9Authorizer interface {
	Authorize(ctx context.Context, saID, bucket string, ctxReq policy.RequestContext) policy.EvalResult
}

// ConfigReader is the small slice of the config store that the 9P server
// reads. The concrete *config.Store satisfies it; tests inject stubs.
// Used per-op to re-check iam.anon-enabled for anon-bound sessions
// (NFS§B T12, §9 T73 parity).
type ConfigReader interface {
	GetBool(key string) (bool, bool)
}

// Server is a 9P2000.L server backed by a storage.Backend.
type Server struct {
	backend  storage.Backend
	locks    *objectLocks
	p9srv    *p9.Server
	attacher *attacher

	mu     sync.Mutex
	cancel context.CancelFunc
	ln     net.Listener
	conns  map[net.Conn]struct{}
}

// ServerOption configures a Server.
type ServerOption func(*attacher)

// WithMountSAStore wires the mount-SA store into the 9P server for
// aname-based auth (NFS§B T9, spec D#6).
func WithMountSAStore(s *mountsastore.Store) ServerOption {
	return func(a *attacher) { a.mountSAStore = s }
}

// WithAuthorizer wires the IAM authorizer for grainfs:9PAttach evaluation.
// The concrete *s3auth.Authorizer satisfies p9Authorizer; tests may inject stubs.
func WithAuthorizer(authz p9Authorizer) ServerOption {
	return func(a *attacher) { a.authorizer = authz }
}

// WithExportStore wires the NFS export store so the 9P server can enforce
// per-bucket ReadOnly on all mutation operations (T11).
func WithExportStore(store exportGetter) ServerOption {
	return func(a *attacher) { a.exportStore = store }
}

// WithConfigReader wires the config store so the 9P server can re-check
// iam.anon-enabled on every fh-bearing op for anon-bound sessions
// (NFS§B T12, §9 T73 parity). nil keeps the previous behaviour
// (no per-op anon flip gate).
func WithConfigReader(c ConfigReader) ServerOption {
	return func(a *attacher) { a.cfg = c }
}

// WithAuditHook wires a hook that is called with a populated audit.S3Event
// after every grainfs:9PAttach allow/deny decision (T15 NFS§C). The hook is
// called synchronously on the Walk goroutine; implementations must not block.
// nil keeps the previous behaviour (no audit emit).
func WithAuditHook(hook func(audit.S3Event)) ServerOption {
	return func(a *attacher) { a.auditHook = hook }
}

// NewServer creates a 9P server backed by backend.
func NewServer(backend storage.Backend, opts ...ServerOption) *Server {
	locks := newObjectLocks()
	att := &attacher{backend: backend, locks: locks}
	for _, opt := range opts {
		opt(att)
	}
	s := &Server{backend: backend, locks: locks, conns: make(map[net.Conn]struct{}), attacher: att}
	s.p9srv = p9.NewServer(att)
	return s
}

// SetExportStore updates the export store on a running server. New connections
// and new Walk calls (per-session root fids) will see the updated store.
// Existing open fids retain the store they received at Walk time.
func (s *Server) SetExportStore(store exportGetter) {
	s.mu.Lock()
	s.attacher.exportStore = store
	s.mu.Unlock()
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
		if err := ln.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			return err
		}
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
	backend      storage.Backend
	locks        *objectLocks
	mountSAStore *mountsastore.Store
	authorizer   p9Authorizer
	exportStore  exportGetter
	cfg          ConfigReader
	auditHook    func(audit.S3Event)
}

func (a *attacher) Attach() (p9.File, error) {
	return &rootFile{
		backend:      a.backend,
		locks:        a.locks,
		mountSAStore: a.mountSAStore,
		authorizer:   a.authorizer,
		exportStore:  a.exportStore,
		cfg:          a.cfg,
		auditHook:    a.auditHook,
	}, nil
}
