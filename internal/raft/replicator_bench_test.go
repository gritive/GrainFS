package raft

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/transport"
)

func BenchmarkRaftReplicationWindow_InProcess(b *testing.B) {
	for _, window := range []int{1, 2, 4} {
		b.Run(fmt.Sprintf("window_%d", window), func(b *testing.B) {
			cluster := newTestCluster(b, 3)
			for _, n := range cluster.nodes {
				n.config.MaxAppendEntriesInflight = window
				n.config.MaxEntriesPerAE = 1
			}
			cluster.startAll()
			leader := cluster.waitForLeader(5 * time.Second)
			if leader == nil {
				b.Fatal("no leader")
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_, err := leader.ProposeWait(ctx, []byte("bench"))
				cancel()
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			cluster.stopAll()
		})
	}
}

func BenchmarkRaftReplicationWindow_QUIC(b *testing.B) {
	for _, window := range []int{1, 2, 4} {
		b.Run(fmt.Sprintf("window_%d", window), func(b *testing.B) {
			cluster := newQUICBenchCluster(b, 3, window)
			cluster.startAll()
			leader := cluster.waitForLeader(5 * time.Second)
			if leader == nil {
				b.Fatal("no leader")
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_, err := leader.ProposeWait(ctx, []byte("bench"))
				cancel()
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
		})
	}
}

func newQUICBenchCluster(b *testing.B, n int, window int) *quicCluster {
	b.Helper()
	ctx := context.Background()

	transports := make([]*transport.QUICTransport, n)
	for i := range transports {
		transports[i] = transport.NewQUICTransport()
		if err := transports[i].Listen(ctx, "127.0.0.1:0"); err != nil {
			b.Fatal(err)
		}
	}

	addrs := make([]string, n)
	for i, tr := range transports {
		addrs[i] = tr.LocalAddr()
	}

	nodes := make([]*Node, n)
	for i := 0; i < n; i++ {
		peers := make([]string, 0, n-1)
		for j := 0; j < n; j++ {
			if i != j {
				peers = append(peers, addrs[j])
			}
		}
		nodes[i] = NewNode(Config{
			ID:                       addrs[i],
			Peers:                    peers,
			ElectionTimeout:          200 * time.Millisecond,
			HeartbeatTimeout:         50 * time.Millisecond,
			MaxEntriesPerAE:          1,
			MaxAppendEntriesInflight: window,
			TrailingLogs:             1024,
		})
	}

	for i := range transports {
		for j := range transports {
			if i == j {
				continue
			}
			if err := transports[i].Connect(ctx, addrs[j]); err != nil {
				b.Fatal(err)
			}
		}
	}

	rpcs := make([]*QUICRPCTransport, n)
	for i := range nodes {
		rpcs[i] = NewQUICRPCTransport(transports[i], nodes[i])
		rpcs[i].SetTransport()
	}

	b.Cleanup(func() {
		for _, node := range nodes {
			node.Close()
		}
		for _, tr := range transports {
			tr.Close()
		}
	})

	return &quicCluster{nodes: nodes, transports: transports, rpcs: rpcs}
}
