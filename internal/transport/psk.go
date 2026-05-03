package transport

import (
	"errors"
	"fmt"
)

// ErrEmptyClusterKey is returned when --cluster-key is empty in cluster mode.
// Treat as fatal — refuse to start.
var ErrEmptyClusterKey = errors.New("cluster key is empty")

// ErrShortClusterKey wraps an error when --cluster-key is shorter than the
// recommended length. Treat as warning — startup proceeds but the operator
// chose a weak key. Emit log.Warn at the call site.
var ErrShortClusterKey = errors.New("cluster key is shorter than recommended")

// ValidateClusterKey validates length only. It does NOT validate entropy:
// "aaaa…" of 64 chars passes. Operators should generate keys with
// `openssl rand -hex 32` (32 random bytes hex-encoded = 64 chars = 256-bit).
func ValidateClusterKey(s string) error {
	if s == "" {
		return ErrEmptyClusterKey
	}
	if len(s) < 64 {
		return fmt.Errorf("%w: %d chars; recommend 64 (32 random bytes hex-encoded, e.g. openssl rand -hex 32)", ErrShortClusterKey, len(s))
	}
	return nil
}
