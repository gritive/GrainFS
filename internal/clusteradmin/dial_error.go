package clusteradmin

import (
	"errors"
	"fmt"
	"net"
	"os"
	"syscall"
)

// wrapDialError converts low-level net.Dial errors into actionable
// admin-CLI messages. Returns the original error unchanged for non-dial
// errors so request-time errors (HTTP body parse, etc.) pass through.
//
// sockPath is included in the message; for HTTP transport callers it's
// empty and the original error passes through.
func wrapDialError(err error, sockPath string) error {
	if err == nil {
		return nil
	}
	if sockPath == "" {
		return err
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) || opErr.Op != "dial" {
		return err
	}
	switch {
	case errors.Is(err, syscall.ENOENT) || errors.Is(err, os.ErrNotExist):
		return fmt.Errorf("admin socket not found at %s\n"+
			"  Hint: confirm grainfs serve is running with --data <correct-dir>\n"+
			"        check the \"admin endpoint\" line in server startup logs", sockPath)
	case errors.Is(err, syscall.ECONNREFUSED):
		return fmt.Errorf("admin socket exists but no listener at %s\n"+
			"  Hint: grainfs serve may have crashed; check server logs", sockPath)
	case errors.Is(err, syscall.EACCES) || errors.Is(err, os.ErrPermission):
		return fmt.Errorf("permission denied on %s\n"+
			"  Hint: confirm group membership matches --admin-group on serve", sockPath)
	default:
		return err
	}
}
