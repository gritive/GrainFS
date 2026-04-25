package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestMigrationExecutor_StopIdempotent verifies that calling Stop() twice
// does not panic (sync.Once prevents double-close of quit channel).
func TestMigrationExecutor_StopIdempotent(t *testing.T) {
	mover := &mockShardMover{}
	node := &mockMigrationRaft{nodeID: "node-a"}
	e := NewMigrationExecutor(mover, node, 1)

	require.NotPanics(t, func() {
		e.Stop()
		e.Stop()
	})
}
