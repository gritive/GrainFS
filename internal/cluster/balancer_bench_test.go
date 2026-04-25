package cluster

import (
	"context"
	"runtime"
	"testing"
	"time"
)

func newTestBalancer(b *testing.B) *BalancerProposer {
	b.Helper()
	store := NewNodeStatsStore(1 * time.Minute)
	node := &mockRaftNode{state: 2, nodeID: "self", peerIDs: []string{}}
	return NewBalancerProposer("self", store, node, DefaultBalancerConfig())
}

// BenchmarkBalancerStatus measures Status() throughput under concurrent access.
// Status() sends a request to the actor loop and waits for the reply.
func BenchmarkBalancerStatus(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bp := newTestBalancer(b)
	go bp.Run(ctx)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = bp.Status()
		}
	})
}

// BenchmarkBalancerNotify measures NotifyMigrationDone throughput.
// It sends to the actor channel and is called from FSM goroutine.
func BenchmarkBalancerNotify(b *testing.B) {
	runtime.SetMutexProfileFraction(1)
	defer runtime.SetMutexProfileFraction(0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bp := newTestBalancer(b)
	go bp.Run(ctx)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bp.NotifyMigrationDone("bkt", "key", "v1")
		}
	})
}
