package cluster

import (
	"context"
	"testing"
	"time"

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

// TestMigrationExecutor_SweepLoopExitsOnStop verifies that Start() launches the
// sweep goroutine and Stop() terminates it via the quit channel.
func TestMigrationExecutor_SweepLoopExitsOnStop(t *testing.T) {
	mover := &mockShardMover{}
	node := &mockMigrationRaft{nodeID: "node-a"}
	e := NewMigrationExecutorWithTTL(mover, node, 1, 100*time.Millisecond)

	ctx := context.Background()
	e.Start(ctx)

	done := make(chan struct{})
	go func() {
		e.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() did not return within 2s — sweepLoop likely goroutine-leaked")
	}
}
