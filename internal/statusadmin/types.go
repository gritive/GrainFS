package statusadmin

import (
	"errors"
	"io"
	"time"
)

var (
	errEndpointNotConfigured = errors.New("admin endpoint not configured.\n" +
		"  Hint: set GRAINFS_ADMIN_SOCKET=<data-dir>/admin.sock or use --endpoint")
	errEndpointMustBeUDS = errors.New("admin endpoint must be a UDS socket path.\n" +
		"  Use the admin socket: --endpoint <data-dir>/admin.sock")
)

// BaseOptions holds the common options shared across all status admin ops.
type BaseOptions struct {
	Endpoint string
	JSONOut  bool
	Timeout  time.Duration
	Stdout   io.Writer
	Stderr   io.Writer
}

// GetOptions holds options for the status command.
type GetOptions struct {
	BaseOptions
}
