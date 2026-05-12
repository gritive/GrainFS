package admin

import (
	"errors"
	"net"

	"github.com/rs/zerolog/log"
)

// peerCred holds the resolved (or unresolved) peer credentials of an admin UDS
// client. Used for forensic enrichment of audit logs only; never for
// authorization (authorization remains UDS group ownership: 0660 + chown :admin).
type peerCred struct {
	UID      uint32
	Resolved bool
}

// ucred is the platform-agnostic shape of SO_PEERCRED / LOCAL_PEERCRED.
// Per-OS readUcred populates this; on unsupported OSes it returns
// errPeerCredUnsupported.
type ucred struct {
	PID uint32
	UID uint32
	GID uint32
}

// errPeerCredUnsupported is returned by readUcred on OSes where SO_PEERCRED
// (or equivalent) is unavailable (anything but linux and darwin in this
// build). Audit logs on such hosts permanently carry actor_uid_resolved=false.
var errPeerCredUnsupported = errors.New("peercred: unsupported on this OS")

// peerCredAddr is a net.Addr that carries the connecting peer's ucred. The
// middleware type-asserts on this in c.GetConn().RemoteAddr() to attach
// credentials to the request context.
//
// We use a typed addr (not a stringly-encoded "@:uid=N" form) so credential
// propagation is type-safe — Go's type system catches drift if the audit
// reader and the listener disagree on the encoding.
type peerCredAddr struct {
	inner net.Addr // the underlying *net.UnixAddr from the accepted conn
	cred  peerCred
}

func (a *peerCredAddr) Network() string { return a.inner.Network() }

// String returns the inner address's string form. Audit log infrastructure
// that calls String() on RemoteAddr() sees an unmodified UDS path — the cred
// surfaces through Cred() only, accessible to typed consumers.
func (a *peerCredAddr) String() string { return a.inner.String() }

// Cred returns the peer credentials captured at Accept time.
func (a *peerCredAddr) Cred() peerCred { return a.cred }

// peerCredConn wraps a *net.UnixConn so its RemoteAddr() returns a typed
// *peerCredAddr. Close() and the other net.Conn methods delegate unmodified.
type peerCredConn struct {
	net.Conn
	addr *peerCredAddr
}

func (c *peerCredConn) RemoteAddr() net.Addr { return c.addr }

// peerCredListener wraps a Unix-socket net.Listener so every accepted conn is
// returned as a *peerCredConn whose RemoteAddr() carries the resolved ucred.
//
// Failure mode: if readUcred returns an error (kernel rejection, unsupported
// OS, conn already closed), the conn is still returned to Hertz — only the
// cred carries Resolved=false. SO_PEERCRED failure is forensic, not fatal.
type peerCredListener struct{ inner net.Listener }

func newPeerCredListener(inner net.Listener) *peerCredListener {
	return &peerCredListener{inner: inner}
}

func (l *peerCredListener) Accept() (net.Conn, error) {
	c, err := l.inner.Accept()
	if err != nil {
		return nil, err
	}
	addr := &peerCredAddr{inner: c.RemoteAddr()}
	if uc, ok := c.(*net.UnixConn); ok {
		if cred, rerr := readUcred(uc); rerr == nil {
			addr.cred = peerCred{UID: cred.UID, Resolved: true}
		} else if !errors.Is(rerr, errPeerCredUnsupported) {
			log.Warn().Err(rerr).Str("event", "peercred_read_failed").Msg("admin UDS peercred read failed; audit log uid will be unresolved for this conn")
		}
	}
	return &peerCredConn{Conn: c, addr: addr}, nil
}

func (l *peerCredListener) Close() error   { return l.inner.Close() }
func (l *peerCredListener) Addr() net.Addr { return l.inner.Addr() }
