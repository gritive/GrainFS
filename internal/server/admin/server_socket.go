package admin

import (
	"fmt"
	"net"
	"os"
	"os/user"
	"strconv"
)

func prepareAdminSocket(path, group string) (net.Listener, error) {
	if err := cleanupStaleSocket(path); err != nil {
		return nil, err
	}
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("listen unix %s: %w", path, err)
	}
	if err := chmodAdminSocket(path, ln); err != nil {
		return nil, err
	}
	if group != "" {
		if err := chownAdminSocket(path, group, ln); err != nil {
			return nil, err
		}
	}
	return ln, nil
}

func chmodAdminSocket(path string, ln net.Listener) error {
	if err := os.Chmod(path, 0o660); err != nil {
		closeAndRemoveSocket(ln, path)
		return fmt.Errorf("chmod 0660 %s: %w", path, err)
	}
	return nil
}

func chownAdminSocket(path, group string, ln net.Listener) error {
	gr, err := user.LookupGroup(group)
	if err != nil {
		closeAndRemoveSocket(ln, path)
		return fmt.Errorf("lookup group %q: %w", group, err)
	}
	gid, err := strconv.Atoi(gr.Gid)
	if err != nil {
		closeAndRemoveSocket(ln, path)
		return fmt.Errorf("parse gid %q: %w", gr.Gid, err)
	}
	if err := os.Chown(path, -1, gid); err != nil {
		closeAndRemoveSocket(ln, path)
		return fmt.Errorf("chown %s to %s: %w", path, group, err)
	}
	return nil
}

func closeAndRemoveSocket(ln net.Listener, path string) {
	_ = ln.Close()
	_ = os.Remove(path)
}
