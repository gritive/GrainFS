// raftv2_group_mux_tcp_test.go — the dormant-TCP twin of the QUIC group-raft mux
// integration test. Unlike the meta-raft spec (which rides the data-plane Call
// path), per-group raft dispatches via raft.GroupRaftMux.ForGroup →
// GroupRaftSender, which routes RequestVote/AppendEntries over the per-peer mux
// carrier (StreamControl). With a TCP transport + EnableMux, that exercises the
// S2b-2 TCP mux CARRIER end-to-end — its only multi-node proof (unit/char before).
//
// Critically, GroupRaftSender falls back to tr.Call(StreamGroupRaft) on any mux
// dial/send failure, so election + replication ALONE would pass even if the carrier
// were broken/bypassed. The test therefore also asserts InboundMuxSessionCount > 0
// (a mux session was actually accepted) — making it discriminating FOR THE CARRIER.

package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/transport"
)

// buildTCPGroupMuxNodes wires N per-group v2 raft nodes over real TCP transports,
// each registered on its own GroupRaftMux under groupID. No explicit Connect
// (TCP dials lazily; the mux carrier dials on the first muxConnFor).
func buildTCPGroupMuxNodes(t *testing.T, n int, groupID string) ([]RaftNode, []*transport.TCPTransport, []*raft.GroupRaftMux) {
	t.Helper()
	ctx := context.Background()

	transports := make([]*transport.TCPTransport, n)
	for i := range transports {
		transports[i] = transport.MustNewTCPTransport("test-group-mux-tcp-psk")
		require.NoError(t, transports[i].Listen(ctx, "127.0.0.1:0"))
	}
	addrs := make([]string, n)
	for i, tr := range transports {
		addrs[i] = tr.LocalAddr()
	}
	muxes := make([]*raft.GroupRaftMux, n)
	for i := range muxes {
		muxes[i] = raft.NewGroupRaftMux(transports[i])
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
		node, _, err := newRaftNode(rcfg, "")
		require.NoError(t, err)
		sender := muxes[i].ForGroup(groupID)
		node.SetTransport(sender.RequestVote, sender.AppendEntries)
		muxes[i].Register(groupID, node)
		nodes[i] = node
	}

	t.Cleanup(func() {
		for _, node := range nodes {
			node.Close()
		}
		for _, tr := range transports {
			_ = tr.Close()
		}
	})
	return nodes, transports, muxes
}

// TestV2GroupMuxCluster_TCP_ThreeNode_MuxMode_Propose_Replicate proves the S2b-2
// TCP mux carrier carries real multi-node group-raft: three nodes elect a leader
// and replicate an entry, AND the traffic actually rode the mux carrier (inbound
// mux sessions established), not the legacy Call fallback.
func TestV2GroupMuxCluster_TCP_ThreeNode_MuxMode_Propose_Replicate(t *testing.T) {
	nodes, transports, muxes := buildTCPGroupMuxNodes(t, 3, "group-tcp-mux")
	for _, m := range muxes {
		m.EnableMux(2, 5*time.Millisecond)
	}
	for _, n := range nodes {
		n.Start()
	}

	verifyClusterReplicates(t, &v2GroupMuxCluster{nodes: nodes}, "v2-group-tcp-mux-replicate")

	// Discriminating FOR THE CARRIER: the legacy tr.Call(StreamGroupRaft) fallback
	// dials the data-plane ALPN and never populates muxInbound. A non-zero inbound
	// session count proves the raft traffic actually rode the TCP mux carrier.
	total := 0
	counts := make([]int, len(transports))
	for i, tr := range transports {
		counts[i] = tr.InboundMuxSessionCount()
		total += counts[i]
	}
	require.Greaterf(t, total, 0,
		"group-raft must ride the TCP mux carrier (inbound mux sessions), not the Call fallback; per-node=%v", counts)
}

// v2GroupMuxCluster groups the raft nodes under test so verifyClusterReplicates
// can drive election + replication transport-agnostically.
type v2GroupMuxCluster struct {
	nodes []RaftNode
}

func (c *v2GroupMuxCluster) waitForLeader(timeout time.Duration) RaftNode {
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

func verifyClusterReplicates(t *testing.T, cluster *v2GroupMuxCluster, payload string) {
	t.Helper()

	leader := cluster.waitForLeader(5 * time.Second)
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
	idx, err := leader.ProposeWait(ctx, []byte(payload))
	require.NoError(t, err)
	require.Greater(t, idx, uint64(0))

	deadline := time.After(5 * time.Second)
	for i, ch := range applyCh {
		var seen bool
		for !seen {
			select {
			case a := <-ch:
				if a.idx == idx && string(a.cmd) == payload {
					seen = true
				}
			case <-deadline:
				t.Fatalf("node[%d] did not apply index %d before deadline", i, idx)
			}
		}
	}
}
