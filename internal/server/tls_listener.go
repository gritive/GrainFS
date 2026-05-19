// Package server: HotTLSListener — TLS hot-swap net.Listener (§5 T43).
//
// Always opens a plain TCP listener. On Accept(), if a cert+key pair is
// currently loaded it wraps the conn via tls.Server; otherwise it returns
// the raw plaintext conn. Reload() re-reads the cert/key from disk and
// atomically swaps the active tls.Config via a single atomic.Pointer so
// Accept() does one load with no torn reads.
//
// Boot-time posture matrix:
//
//	cert absent & key absent → plaintext (no error)
//	cert present & key present → TLS active
//	cert present & key absent (or vice versa) → Start/Reload returns error
//
// Already-accepted (and tls.Server-wrapped) connections are unaffected by
// subsequent Reload calls — only new Accepts observe the new posture. A
// cert-present → cert-absent reload makes future accepts plaintext; that
// matches the spec but is worth a code comment so future readers don't
// mistake it for a bug.
package server

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"sync/atomic"

	"github.com/gritive/GrainFS/internal/nodeconfig"
)

// tlsState is what we swap atomically. cfg is nil iff TLS is inactive.
type tlsState struct {
	cfg *tls.Config
}

// HotTLSListener wraps a plain TCP listener with an atomically-swappable
// TLS posture. Construct via NewHotTLSListener, then call Start() to bind.
type HotTLSListener struct {
	nc    *nodeconfig.NodeConfig
	addr  string
	ln    net.Listener
	state atomic.Pointer[tlsState]
}

// NewHotTLSListener returns a listener bound to addr (after Start). nc is
// used to resolve TLSCertPath/TLSKeyPath at every Reload, so env overrides
// and dataDir changes are honored.
func NewHotTLSListener(nc *nodeconfig.NodeConfig, addr string) *HotTLSListener {
	h := &HotTLSListener{nc: nc, addr: addr}
	h.state.Store(&tlsState{}) // plaintext by default
	return h
}

// Start opens the TCP listener and performs the initial Reload. On
// partial-cert (cert without key, or vice versa) Start returns the error
// from Reload AFTER the listener is bound; caller should Close().
func (h *HotTLSListener) Start() error {
	ln, err := net.Listen("tcp", h.addr)
	if err != nil {
		return err
	}
	h.ln = ln
	if err := h.Reload(); err != nil {
		_ = ln.Close()
		h.ln = nil
		return err
	}
	return nil
}

// Reload re-reads cert/key from disk. On both-absent it switches to
// plaintext; on both-present it loads the keypair into a new tls.Config
// and atomically swaps. On partial presence (or any other stat/load
// error) it returns the error and leaves the current state untouched.
func (h *HotTLSListener) Reload() error {
	certPath := h.nc.TLSCertPath()
	keyPath := h.nc.TLSKeyPath()
	_, errC := os.Stat(certPath)
	_, errK := os.Stat(keyPath)

	certMissing := errors.Is(errC, fs.ErrNotExist)
	keyMissing := errors.Is(errK, fs.ErrNotExist)

	switch {
	case certMissing && keyMissing:
		h.state.Store(&tlsState{}) // plaintext
		return nil
	case certMissing != keyMissing:
		// Partial: exactly one of cert/key exists. Refuse.
		return fmt.Errorf("tls: partial cert/key (cert=%v key=%v): need both or neither", !certMissing, !keyMissing)
	case errC != nil:
		return fmt.Errorf("tls: cert path inaccessible %q: %w", certPath, errC)
	case errK != nil:
		return fmt.Errorf("tls: key path inaccessible %q: %w", keyPath, errK)
	}

	c, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return fmt.Errorf("tls: load keypair: %w", err)
	}
	cfg := &tls.Config{
		Certificates: []tls.Certificate{c},
		MinVersion:   tls.VersionTLS12,
	}
	h.state.Store(&tlsState{cfg: cfg})
	return nil
}

// IsTLS reports whether TLS is currently active.
func (h *HotTLSListener) IsTLS() bool {
	st := h.state.Load()
	return st != nil && st.cfg != nil
}

// Accept implements net.Listener. Per-accept overhead: one atomic.Pointer
// load + (if TLS active) one tls.Server wrap; no allocation of tls.Config.
func (h *HotTLSListener) Accept() (net.Conn, error) {
	c, err := h.ln.Accept()
	if err != nil {
		return nil, err
	}
	st := h.state.Load()
	if st == nil || st.cfg == nil {
		return c, nil
	}
	return tls.Server(c, st.cfg), nil
}

// Close closes the underlying TCP listener. Idempotent on nil ln.
func (h *HotTLSListener) Close() error {
	if h.ln == nil {
		return nil
	}
	return h.ln.Close()
}

// Addr returns the underlying listener address. Panics if Start has not
// been called — callers must Start before using as a net.Listener.
func (h *HotTLSListener) Addr() net.Addr {
	return h.ln.Addr()
}
