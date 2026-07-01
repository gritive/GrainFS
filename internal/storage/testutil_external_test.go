package storage_test

import (
	"testing"

	"github.com/gritive/GrainFS/internal/cluster"
)

// newBackend returns the real single-node production backend for storage_test
// integration tests. External test packages may import cluster (no import
// cycle). Tests that need LocalBackend-internal behavior are removed with the
// backend, not re-homed here.
func newBackend(t *testing.T) *cluster.DistributedBackend {
	t.Helper()
	return cluster.NewSingletonBackendForTest(t)
}
