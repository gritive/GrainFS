package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/gossip"
)

func newTestBalancer(b *testing.B) *BalancerProposer {
	b.Helper()
	store := gossip.NewNodeStatsStore(1 * time.Minute)
	node := &mockRaftNode{state: 2, nodeID: "self", peerIDs: []string{}}
	return NewBalancerProposer("self", store, node, defaultFakeBalancerCfg())
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
