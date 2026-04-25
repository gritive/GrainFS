package cluster

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
)

// benchMigrationRaft immediately calls NotifyCommit for any proposed task,
// enabling Execute() to complete without blocking on phase 3.
type benchMigrationRaft struct {
	mu   sync.Mutex
	exec *MigrationExecutor
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
	m.mu.Lock()
	exec := m.exec
	m.mu.Unlock()
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
	node.exec = e

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

// BenchmarkMigrationExecutor_MutexContention measures lock contention via mutex profile.
// Run with: go test -bench=BenchmarkMigrationExecutor_MutexContention
//
//	-mutexprofile=mutex.prof ./internal/cluster/
//
// Then: go tool pprof mutex.prof
func BenchmarkMigrationExecutor_MutexContention(b *testing.B) {
	runtime.SetMutexProfileFraction(1)
	defer runtime.SetMutexProfileFraction(0)

	mover := &mockShardMover{}
	node := &benchMigrationRaft{}
	e := NewMigrationExecutor(mover, node, 1)
	node.exec = e

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
