//go:build darwin

package admin

import (
	"fmt"
	"net"

	"golang.org/x/sys/unix"
)

// readUcred returns the peer ucred of a *net.UnixConn on darwin using
// LOCAL_PEERCRED. The xucred struct exposes uid; pid is unavailable in this
// path (LOCAL_PEEREPID is a separate call we don't need for audit).
func readUcred(c *net.UnixConn) (ucred, error) {
	sc, err := c.SyscallConn()
	if err != nil {
		return ucred{}, fmt.Errorf("peercred_darwin: SyscallConn: %w", err)
	}
	var u ucred
	var ctrlErr error
	cerr := sc.Control(func(fd uintptr) {
		xu, gerr := unix.GetsockoptXucred(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
		if gerr != nil {
			ctrlErr = gerr
			return
		}
		u.UID = xu.Uid
		if len(xu.Groups) > 0 {
			u.GID = xu.Groups[0]
		}
	})
	if cerr != nil {
		return ucred{}, fmt.Errorf("peercred_darwin: Control: %w", cerr)
	}
	if ctrlErr != nil {
		return ucred{}, fmt.Errorf("peercred_darwin: GetsockoptXucred: %w", ctrlErr)
	}
	return u, nil
}
