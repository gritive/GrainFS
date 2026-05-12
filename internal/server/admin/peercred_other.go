//go:build !linux && !darwin

package admin

import "net"

// readUcred on unsupported OSes (e.g., FreeBSD, Windows) returns
// errPeerCredUnsupported. The listener still wraps each accepted conn —
// audit log entries on these hosts permanently carry actor_uid_resolved=false.
// Boot-time visibility is provided by admin.Start logging a single
// peercred_unsupported warn line.
func readUcred(_ *net.UnixConn) (ucred, error) {
	return ucred{}, errPeerCredUnsupported
}
