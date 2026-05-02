package cluster

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/rs/zerolog"
)

// benchMigrationRaft immediately calls NotifyCommit for any proposed task,
// enabling Execute() to complete without blocking on phase 3.
type benchMigrationRaft struct {
	exec atomic.Pointer[MigrationExecutor]
}

func (m *benchMigrationRaft) Propose(data []byte) error {
	// Decode the outer command envelope, then extract bucket/key/versionID.
	cmd, err := DecodeCommand(data)
	if err != nil || cmd.Type != CmdMigrationDone {
		return nil
	}
	fsm, err := decodeMigrationDoneCmd(cmd.Data)
	if err != nil {
		return nil
	}
	exec := m.exec.Load()
	if exec != nil {
		exec.NotifyCommit(fsm.Bucket, fsm.Key, fsm.VersionID)
	}
	return nil
}

func (m *benchMigrationRaft) NodeID() string { return "bench-node" }

// BenchmarkMigrationExecutor_Execute measures Execute() throughput under contention.
func BenchmarkMigrationExecutor_Execute(b *testing.B) {
	mover := &mockShardMover{}
	node := &benchMigrationRaft{}
	e := NewMigrationExecutor(mover, node, 1)
	defer e.Stop()
	e.logger = zerolog.Nop()
	node.exec.Store(e)

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			task := MigrationTask{
				Bucket:    "bench",
				Key:       fmt.Sprintf("obj%d", i),
				VersionID: "v1",
				SrcNode:   "src",
				DstNode:   "dst",
			}
			_ = e.Execute(context.Background(), task)
			i++
		}
	})
}

// BenchmarkMigrationExecutor_CoordinationContention measures actor coordination
// overhead under concurrent Execute calls.
// Run with: go test -bench=BenchmarkMigrationExecutor_CoordinationContention
//
//	./internal/cluster/
func BenchmarkMigrationExecutor_CoordinationContention(b *testing.B) {
	mover := &mockShardMover{}
	node := &benchMigrationRaft{}
	e := NewMigrationExecutor(mover, node, 1)
	defer e.Stop()
	e.logger = zerolog.Nop()
	node.exec.Store(e)

	var wg sync.WaitGroup
	concurrency := 32
	perGoroutine := b.N / concurrency
	if perGoroutine == 0 {
		perGoroutine = 1
	}
	for g := range concurrency {
		wg.Add(1)
		go func(gIdx int) {
			defer wg.Done()
			for i := range perGoroutine {
				task := MigrationTask{
					Bucket:    "b",
					Key:       fmt.Sprintf("k%d_%d", gIdx, i),
					VersionID: "v1",
					SrcNode:   "s",
					DstNode:   "d",
				}
				_ = e.Execute(context.Background(), task)
			}
		}(g)
	}
	wg.Wait()
}
