//go:build linux

package admin

import (
	"fmt"
	"net"
	"syscall"
)

// readUcred returns the peer ucred of a *net.UnixConn on Linux using
// SO_PEERCRED. The conn's raw fd is accessed via SyscallConn().Control,
// which is the supported path for socket-option introspection.
func readUcred(c *net.UnixConn) (ucred, error) {
	sc, err := c.SyscallConn()
	if err != nil {
		return ucred{}, fmt.Errorf("peercred_linux: SyscallConn: %w", err)
	}
	var u ucred
	var ctrlErr error
	cerr := sc.Control(func(fd uintptr) {
		cred, gerr := syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
		if gerr != nil {
			ctrlErr = gerr
			return
		}
		u.PID = uint32(cred.Pid)
		u.UID = cred.Uid
		u.GID = cred.Gid
	})
	if cerr != nil {
		return ucred{}, fmt.Errorf("peercred_linux: Control: %w", cerr)
	}
	if ctrlErr != nil {
		return ucred{}, fmt.Errorf("peercred_linux: GetsockoptUcred: %w", ctrlErr)
	}
	return u, nil
}
