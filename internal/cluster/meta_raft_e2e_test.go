package cluster

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/transport"
)

// TestMetaRaft_ThreeNodeBootstrap_E2E spins up a 3-node meta-Raft cluster
// in-process using MetaTransportFake (no port binding) and verifies:
//  1. node-0 bootstraps and becomes leader
//  2. node-1 joins → appears in node-0's FSM
//  3. node-2 joins → both node-1 and node-2 appear in node-0's FSM
//  4. exactly one leader among the three nodes
//  5. all nodes can be closed cleanly
func TestMetaRaft_ThreeNodeBootstrap_E2E(t *testing.T) {
	t.Parallel()

	tr := newMetaTransportFake()

	newNode := func(id string, peers []string) *MetaRaft {
		t.Helper()
		m, err := NewMetaRaft(MetaRaftConfig{
			NodeID:    id,
			Peers:     peers,
			DataDir:   t.TempDir(),
			Transport: tr,
		})
		require.NoError(t, err)
		tr.register(id, m)
		return m
	}

	m0 := newNode("node-0", nil)
	m1 := newNode("node-1", []string{"node-0"})
	m2 := newNode("node-2", []string{"node-0"})

	t.Cleanup(func() {
		_ = m0.Close()
		_ = m1.Close()
		_ = m2.Close()
	})

	// Bootstrap and start node-0 (singleton bootstrap).
	require.NoError(t, m0.Bootstrap())
	require.NoError(t, m0.Start(context.Background()))
	require.Eventually(t, func() bool {
		return m0.node.State() == raft.Leader
	}, 3*time.Second, 20*time.Millisecond, "node-0 must become leader")

	// Join node-1.
	require.NoError(t, m1.Bootstrap())
	require.NoError(t, m1.Start(context.Background()))

	ctx1, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel1()
	require.NoError(t, m0.Join(ctx1, "node-1", "node-1"))

	require.Eventually(t, func() bool {
		for _, n := range m0.fsm.Nodes() {
			if n.ID == "node-1" {
				return true
			}
		}
		return false
	}, 3*time.Second, 30*time.Millisecond, "node-1 must appear in FSM after join")

	// Join node-2.
	require.NoError(t, m2.Bootstrap())
	require.NoError(t, m2.Start(context.Background()))

	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	require.NoError(t, m0.Join(ctx2, "node-2", "node-2"))

	require.Eventually(t, func() bool {
		fsmNodes := m0.fsm.Nodes()
		has1, has2 := false, false
		for _, n := range fsmNodes {
			if n.ID == "node-1" {
				has1 = true
			}
			if n.ID == "node-2" {
				has2 = true
			}
		}
		return has1 && has2
	}, 3*time.Second, 30*time.Millisecond, "both node-1 and node-2 must appear in FSM")

	// Exactly one leader in the group.
	leaderCount := 0
	for _, m := range []*MetaRaft{m0, m1, m2} {
		if m.node.State() == raft.Leader {
			leaderCount++
		}
	}
	assert.Equal(t, 1, leaderCount, "exactly one leader must exist in the group")
}

func TestMetaRaft_ProposeShardGroup_E2E(t *testing.T) {
	m := newSingleMetaRaft(t)
	t.Cleanup(func() { _ = m.Close() })

	require.NoError(t, m.Bootstrap())
	require.NoError(t, m.Start(context.Background()))
	require.Eventually(t, func() bool {
		return m.node.State() == raft.Leader
	}, 2*time.Second, 20*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	sg := ShardGroupEntry{
		ID:      "group-0",
		PeerIDs: []string{"node-0"},
	}
	require.NoError(t, m.ProposeShardGroup(ctx, sg))

	groups := m.fsm.ShardGroups()
	require.Len(t, groups, 1)
	assert.Equal(t, "group-0", groups[0].ID)
	assert.Equal(t, []string{"node-0"}, groups[0].PeerIDs)
}

func TestMetaRaft_QUICStaticFiveNodeBootstrap_E2E(t *testing.T) {
	t.Parallel()

	const numNodes = 5

	addrs := make([]string, numNodes)
	for i := range addrs {
		addrs[i] = freeUDPAddr(t)
	}

	transports := make([]*transport.QUICTransport, numNodes)
	nodes := make([]*MetaRaft, numNodes)
	for i := range nodes {
		peers := make([]string, 0, numNodes-1)
		for j := range addrs {
			if i != j {
				peers = append(peers, addrs[j])
			}
		}

		m, err := NewMetaRaft(MetaRaftConfig{
			NodeID:  fmt.Sprintf("node-%d", i),
			Peers:   peers,
			DataDir: t.TempDir(),
		})
		require.NoError(t, err)
		tr := transport.MustNewQUICTransport("meta-raft-quic-test")
		require.NoError(t, tr.Listen(context.Background(), addrs[i]))
		metaTransport := NewMetaTransportQUIC(tr, m.Node())
		m.SetTransport(metaTransport)

		transports[i] = tr
		nodes[i] = m
	}

	t.Cleanup(func() {
		for _, m := range nodes {
			if m != nil {
				_ = m.Close()
			}
		}
		for _, tr := range transports {
			if tr != nil {
				tr.Close()
			}
		}
	})

	for _, m := range nodes {
		require.NoError(t, m.Bootstrap())
		require.NoError(t, m.Start(context.Background()))
	}

	var leader *MetaRaft
	require.Eventually(t, func() bool {
		leader = nil
		for _, m := range nodes {
			if m.IsLeader() {
				if leader != nil {
					return false
				}
				leader = m
			}
		}
		return leader != nil
	}, 10*time.Second, 50*time.Millisecond, "static five-node meta-Raft must elect exactly one QUIC leader")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, leader.ProposeBucketAssignment(ctx, "photos", "group-0"))

	require.Eventually(t, func() bool {
		for _, m := range nodes {
			if m.FSM().BucketAssignments()["photos"] != "group-0" {
				return false
			}
		}
		return true
	}, 5*time.Second, 50*time.Millisecond, "bucket assignment must replicate to every meta-Raft node")
}

func TestMetaRaft_QUICStaticFiveNodeSharedWithDataRaft_E2E(t *testing.T) {
	t.Parallel()

	const numNodes = 5

	addrs := make([]string, numNodes)
	for i := range addrs {
		addrs[i] = freeUDPAddr(t)
	}

	transports := make([]*transport.QUICTransport, numNodes)
	dataNodes := make([]*raft.Node, numNodes)
	metaNodes := make([]*MetaRaft, numNodes)
	for i := range metaNodes {
		peers := make([]string, 0, numNodes-1)
		for j := range addrs {
			if i != j {
				peers = append(peers, addrs[j])
			}
		}

		tr := transport.MustNewQUICTransport("shared-raft-quic-test")
		require.NoError(t, tr.Listen(context.Background(), addrs[i]))
		dataNode := raft.NewNode(raft.DefaultConfig(fmt.Sprintf("node-%d", i), peers))
		dataRPC := raft.NewQUICRPCTransport(tr, dataNode)
		dataRPC.SetTransport()
		metaNode, err := NewMetaRaft(MetaRaftConfig{
			NodeID:  fmt.Sprintf("node-%d", i),
			Peers:   peers,
			DataDir: t.TempDir(),
		})
		require.NoError(t, err)
		metaTransport := NewMetaTransportQUIC(tr, metaNode.Node())
		metaNode.SetTransport(metaTransport)

		transports[i] = tr
		dataNodes[i] = dataNode
		metaNodes[i] = metaNode
	}

	t.Cleanup(func() {
		for _, m := range metaNodes {
			if m != nil {
				_ = m.Close()
			}
		}
		for _, n := range dataNodes {
			if n != nil {
				n.Close()
			}
		}
		for _, tr := range transports {
			if tr != nil {
				tr.Close()
			}
		}
	})

	for _, n := range dataNodes {
		require.NoError(t, n.Bootstrap())
		n.Start()
	}
	for _, m := range metaNodes {
		require.NoError(t, m.Bootstrap())
		require.NoError(t, m.Start(context.Background()))
	}

	require.Eventually(t, func() bool {
		count := 0
		for _, n := range dataNodes {
			if n.IsLeader() {
				count++
			}
		}
		return count == 1
	}, 10*time.Second, 50*time.Millisecond, "data Raft must elect exactly one leader")

	var metaLeader *MetaRaft
	require.Eventually(t, func() bool {
		metaLeader = nil
		for _, m := range metaNodes {
			if m.IsLeader() {
				if metaLeader != nil {
					return false
				}
				metaLeader = m
			}
		}
		return metaLeader != nil
	}, 10*time.Second, 50*time.Millisecond, "meta Raft must elect exactly one leader while sharing QUIC with data Raft")
}

func freeUDPAddr(t *testing.T) string {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer pc.Close()
	return pc.LocalAddr().String()
}
