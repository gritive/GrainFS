package admin

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/user"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"
	"github.com/rs/zerolog/log"
)

// Config configures the admin Unix-socket server.
type Config struct {
	SocketPath string // e.g. "<data>/admin.sock"
	Group      string // OS group name for chown; empty = caller's primary group
	Deps       *Deps
	// ExtraRoutes, if non-nil, is invoked after RegisterAdmin to wire
	// additional Hertz routes onto the admin UDS server. Used by the data
	// plane to expose cluster status/eventlog/remove-peer at /v1/cluster/*
	// without lifting *server.Server's deep dependencies into Deps.
	ExtraRoutes func(h *server.Hertz)
}

// Server owns the admin Hertz instance + Unix listener.
type Server struct {
	cfg    Config
	h      *server.Hertz
	socket net.Listener
	done   chan error
}

// Start brings up the admin server on a Unix socket. Boot order:
//  1. Stale-socket detection: probe path; if listener refuses → unlink, otherwise fatal.
//  2. net.Listen("unix", path).
//  3. chmod 0660 + optional chown to Config.Group.
//  4. Register handlers and Spin Hertz in a goroutine.
//
// Stop performs graceful shutdown: cancels Hertz, then removes the socket file.
func Start(cfg Config) (*Server, error) {
	if cfg.SocketPath == "" {
		return nil, errors.New("admin: SocketPath required")
	}
	if cfg.Deps == nil {
		return nil, errors.New("admin: Deps required")
	}
	if err := cleanupStaleSocket(cfg.SocketPath); err != nil {
		return nil, err
	}
	ln, err := net.Listen("unix", cfg.SocketPath)
	if err != nil {
		return nil, fmt.Errorf("listen unix %s: %w", cfg.SocketPath, err)
	}
	if err := os.Chmod(cfg.SocketPath, 0o660); err != nil {
		_ = ln.Close()
		_ = os.Remove(cfg.SocketPath)
		return nil, fmt.Errorf("chmod 0660 %s: %w", cfg.SocketPath, err)
	}
	if cfg.Group != "" {
		gr, err := user.LookupGroup(cfg.Group)
		if err != nil {
			_ = ln.Close()
			_ = os.Remove(cfg.SocketPath)
			return nil, fmt.Errorf("lookup group %q: %w", cfg.Group, err)
		}
		gid, err := strconv.Atoi(gr.Gid)
		if err != nil {
			_ = ln.Close()
			_ = os.Remove(cfg.SocketPath)
			return nil, fmt.Errorf("parse gid %q: %w", gr.Gid, err)
		}
		if err := os.Chown(cfg.SocketPath, -1, gid); err != nil {
			_ = ln.Close()
			_ = os.Remove(cfg.SocketPath)
			return nil, fmt.Errorf("chown %s to %s: %w", cfg.SocketPath, cfg.Group, err)
		}
	}

	// Wrap the raw UDS listener so every accepted conn carries the resolved
	// peer ucred in its RemoteAddr() as a *peerCredAddr. The peerCred Hertz
	// middleware (installed in RegisterAdmin) type-asserts on this to attach
	// PeerCredValue to the per-request context. Failure to resolve credentials
	// is non-fatal (audit-only, not authorization).
	pcl := newPeerCredListener(ln)
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		log.Warn().Str("event", "peercred_unsupported").Str("os", runtime.GOOS).
			Msg("admin UDS peercred unsupported on this OS; audit log actor_uid will be unresolved")
	}

	transporter := standard.NewTransporter
	h := server.New(
		server.WithListener(pcl),
		server.WithTransport(transporter),
		server.WithHostPorts(""),
	)
	RegisterAdmin(h, cfg.Deps)
	if cfg.ExtraRoutes != nil {
		cfg.ExtraRoutes(h)
	}

	s := &Server{cfg: cfg, h: h, socket: ln, done: make(chan error, 1)}
	go func() {
		s.done <- h.Run()
	}()
	return s, nil
}

// cleanupStaleSocket inspects path. If a socket file exists but no listener
// answers on it, removes the file. If a listener answers, returns an error
// indicating another grainfs is already running. Non-socket paths are also an
// error so we don't accidentally remove unrelated files.
func cleanupStaleSocket(path string) error {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("stat %s: %w", path, err)
	}
	if info.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("path %q exists and is not a socket; refusing to overwrite", path)
	}
	conn, err := net.DialTimeout("unix", path, 200*time.Millisecond)
	if err != nil {
		if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, os.ErrNotExist) {
			return os.Remove(path)
		}
		// Permission denied or similar — bail out to avoid clobbering.
		return fmt.Errorf("probe stale socket: %w", err)
	}
	_ = conn.Close()
	return fmt.Errorf("admin socket %q is already in use; another grainfs may be running", path)
}

// Stop gracefully shuts down the admin server and removes the socket file.
func (s *Server) Stop(ctx context.Context) error {
	if err := s.h.Shutdown(ctx); err != nil {
		_ = os.Remove(s.cfg.SocketPath)
		return err
	}
	_ = os.Remove(s.cfg.SocketPath)
	return nil
}

// SocketPath returns the path the admin server is listening on.
func (s *Server) SocketPath() string { return s.cfg.SocketPath }
