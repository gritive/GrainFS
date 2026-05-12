package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/transport"
)

// v2MetaQUICCluster wires N raft v2 nodes connected via real QUIC through
// RaftV2MetaQUICTransport (StreamMetaRaft). It is the meta-stream counterpart
// of v2QUICCluster (raft_quic_rpc_test.go::newV2QUICCluster), which uses
// StreamControl for per-group raft.
//
// The 3-node election + replication run here is the proof that
// RaftV2MetaQUICTransport correctly routes RequestVote / AppendEntries over
// the meta stream using the shared v2 wire codec.
type v2MetaQUICCluster struct {
	nodes      []RaftNode
	transports []*transport.QUICTransport
	rpcs       []*RaftV2MetaQUICTransport
}

func startV2MetaQUICCluster(t *testing.T, n int) *v2MetaQUICCluster {
	t.Helper()

	ctx := context.Background()
	transports := make([]*transport.QUICTransport, n)
	for i := range transports {
		transports[i] = transport.MustNewQUICTransport("test-cluster-psk-meta-v2")
		require.NoError(t, transports[i].Listen(ctx, "127.0.0.1:0"))
	}

	addrs := make([]string, n)
	for i, tr := range transports {
		addrs[i] = tr.LocalAddr()
	}

	nodes := make([]RaftNode, n)
	for i := 0; i < n; i++ {
		peers := make([]string, 0, n-1)
		for j := 0; j < n; j++ {
			if i != j {
				peers = append(peers, addrs[j])
			}
		}
		rcfg := raft.Config{
			ID:               addrs[i],
			Peers:            peers,
			ElectionTimeout:  200 * time.Millisecond,
			HeartbeatTimeout: 50 * time.Millisecond,
		}
		node, _, err := newRaftNode(rcfg, nil, "")
		require.NoError(t, err)
		nodes[i] = node
	}

	// Full mesh QUIC connections.
	for i := range transports {
		for j := range transports {
			if i == j {
				continue
			}
			var lastErr error
			require.Eventually(t, func() bool {
				lastErr = transports[i].Connect(ctx, addrs[j])
				return lastErr == nil
			}, 5*time.Second, 50*time.Millisecond, "connect %d→%d: %v", i, j, lastErr)
		}
	}

	// Register the v2 meta-Raft bridge per node and wire SendRequestVote /
	// SendAppendEntries / SendInstallSnapshot back into each adapter — exactly
	// the wiring meta_raft.go::wireTransport does in production.
	rpcs := make([]*RaftV2MetaQUICTransport, n)
	for i := range nodes {
		rpcs[i] = NewRaftV2MetaQUICTransport(transports[i], nodes[i])
		nodes[i].SetTransport(rpcs[i].SendRequestVote, rpcs[i].SendAppendEntries)
		nodes[i].SetInstallSnapshotTransport(rpcs[i].SendInstallSnapshot)
	}

	t.Cleanup(func() {
		for _, node := range nodes {
			node.Close()
		}
		for _, tr := range transports {
			tr.Close()
		}
	})

	return &v2MetaQUICCluster{nodes: nodes, transports: transports, rpcs: rpcs}
}

func (c *v2MetaQUICCluster) startAll() {
	for _, n := range c.nodes {
		n.Start()
	}
}

func (c *v2MetaQUICCluster) waitForLeader(t *testing.T, timeout time.Duration) RaftNode {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case <-deadline:
			return nil
		default:
			for _, n := range c.nodes {
				if n.IsLeader() {
					return n
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// TestV2MetaQUIC_ThreeNode_ElectsLeader is the proof-of-life for M6.2.2:
// three raft v2 nodes connected via real QUIC StreamMetaRaft must elect
// exactly one leader. A wire-format mismatch in the meta-stream codec would
// hang election here.
func TestV2MetaQUIC_ThreeNode_ElectsLeader(t *testing.T) {
	cluster := startV2MetaQUICCluster(t, 3)
	cluster.startAll()

	leader := cluster.waitForLeader(t, 5*time.Second)
	require.NotNil(t, leader, "no leader elected in 3-node v2 meta-QUIC cluster")
}

// TestV2MetaQUIC_ThreeNode_Propose_Replicate verifies that AppendEntries
// flows correctly over StreamMetaRaft: a Propose on the leader must commit
// and propagate to all nodes' apply channels.
func TestV2MetaQUIC_ThreeNode_Propose_Replicate(t *testing.T) {
	cluster := startV2MetaQUICCluster(t, 3)
	cluster.startAll()

	leader := cluster.waitForLeader(t, 5*time.Second)
	require.NotNil(t, leader, "leader required for replication test")

	type applied struct {
		idx uint64
		cmd []byte
	}
	applyCh := make([]chan applied, len(cluster.nodes))
	for i, n := range cluster.nodes {
		ch := make(chan applied, 16)
		applyCh[i] = ch
		go func(src <-chan raft.LogEntry) {
			for e := range src {
				ch <- applied{idx: e.Index, cmd: e.Command}
			}
		}(n.ApplyCh())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	idx, err := leader.ProposeWait(ctx, []byte("v2-meta-quic-replicate"))
	require.NoError(t, err)
	require.Greater(t, idx, uint64(0))

	deadline := time.After(5 * time.Second)
	for i, ch := range applyCh {
		var seen bool
		for !seen {
			select {
			case a := <-ch:
				if a.idx == idx && string(a.cmd) == "v2-meta-quic-replicate" {
					seen = true
				}
			case <-deadline:
				t.Fatalf("node[%d] did not apply index %d before deadline", i, idx)
			}
		}
	}
}
