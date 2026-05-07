package clusteradmin

import (
	"errors"
	"net"
	"os"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWrapDialError_FileNotExist(t *testing.T) {
	err := &net.OpError{
		Op:  "dial",
		Net: "unix",
		Err: &os.PathError{Op: "connect", Path: "/tmp/missing.sock", Err: syscall.ENOENT},
	}
	wrapped := wrapDialError(err, "/tmp/missing.sock")
	msg := wrapped.Error()
	require.Contains(t, msg, "admin socket not found")
	require.Contains(t, msg, "/tmp/missing.sock")
	require.Contains(t, msg, "grainfs serve is running")
}

func TestWrapDialError_ConnectionRefused(t *testing.T) {
	err := &net.OpError{
		Op:  "dial",
		Net: "unix",
		Err: &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED},
	}
	wrapped := wrapDialError(err, "/tmp/dead.sock")
	msg := wrapped.Error()
	require.Contains(t, msg, "no listener")
	require.Contains(t, msg, "may have crashed")
}

func TestWrapDialError_PermissionDenied(t *testing.T) {
	err := &net.OpError{
		Op:  "dial",
		Net: "unix",
		Err: &os.SyscallError{Syscall: "connect", Err: syscall.EACCES},
	}
	wrapped := wrapDialError(err, "/tmp/forbidden.sock")
	msg := wrapped.Error()
	require.Contains(t, msg, "permission denied")
	require.Contains(t, msg, "group membership")
	require.Contains(t, msg, "--admin-group")
}

func TestWrapDialError_OtherErrorPassThrough(t *testing.T) {
	err := errors.New("some other error")
	wrapped := wrapDialError(err, "/tmp/x.sock")
	require.Equal(t, err, wrapped)
}

func TestWrapDialError_NilPassThrough(t *testing.T) {
	wrapped := wrapDialError(nil, "/tmp/x.sock")
	require.NoError(t, wrapped)
}

func TestWrapDialError_EmptySockPathPassThrough(t *testing.T) {
	// HTTP transport callers pass empty sockPath. Don't add UDS hints.
	err := &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	wrapped := wrapDialError(err, "")
	require.Equal(t, err, wrapped)
}
