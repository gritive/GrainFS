package cluster

import (
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
// Status() acquires mu.Lock — this benchmark reveals contention.
func BenchmarkBalancerStatus(b *testing.B) {
	bp := newTestBalancer(b)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = bp.Status()
		}
	})
}

// BenchmarkBalancerNotify measures NotifyMigrationDone throughput.
// It acquires mu.Lock and is called from FSM goroutine.
func BenchmarkBalancerNotify(b *testing.B) {
	runtime.SetMutexProfileFraction(1)
	defer runtime.SetMutexProfileFraction(0)
	bp := newTestBalancer(b)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bp.NotifyMigrationDone("bkt", "key", "v1")
		}
	})
}
