// raftv2_group_mux_test.go — multi-node election + replication test for the
// PR 28b v2 group-raft mux bridge.
//
// Mirrors the PR 27 TestV2QUICCluster_ThreeNode_* tests but exercises the
// per-group QUIC mux path (raft.GroupRaftQUICMux) rather than the meta-raft
// StreamControl path. The two paths share a wire codec but diverge in
// dispatch: meta-raft v2 goes through cluster.RaftV2QUICRPCTransport;
// per-group v2 has to dispatch via raft.GroupRaftQUICMux.RegisterV2 so the
// receiver routes inbound RPCs into the v2 adapter instead of nil-deref'ing
// a typed-nil *raft.Node.

package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/transport"
)

// v2GroupMuxCluster wires up N per-group v2 raft nodes, each registered on
// its own GroupRaftQUICMux under the same groupID, connected over real QUIC.
// The outbound side comes from mux.ForGroup; the inbound side from
// mux.RegisterV2 (the PR 28b new path).
type v2GroupMuxCluster struct {
	nodes      []RaftNode
	transports []*transport.QUICTransport
	muxes      []*raft.GroupRaftQUICMux
}

func newV2GroupMuxCluster(t *testing.T, n int, groupID string) *v2GroupMuxCluster {
	t.Helper()
	t.Setenv("GRAINFS_RAFT_V2", "cluster")
	resetRaftV2FlagForTest()
	t.Cleanup(resetRaftV2FlagForTest)

	ctx := context.Background()
	transports := make([]*transport.QUICTransport, n)
	for i := range transports {
		transports[i] = transport.MustNewQUICTransport("test-group-mux-psk-v2")
		require.NoError(t, transports[i].Listen(ctx, "127.0.0.1:0"))
	}

	addrs := make([]string, n)
	for i, tr := range transports {
		addrs[i] = tr.LocalAddr()
	}

	muxes := make([]*raft.GroupRaftQUICMux, n)
	for i := range muxes {
		muxes[i] = raft.NewGroupRaftQUICMux(transports[i])
	}

	// Full-mesh QUIC connect so every sender can reach every peer.
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
		sender := muxes[i].ForGroup(groupID)
		node.SetTransport(sender.RequestVote, sender.AppendEntries)
		// PR 28b: register via the v2-aware path so inbound RPCs dispatch
		// into the cluster.RaftNode adapter (not the underlying *raft.Node,
		// which is nil under v2).
		muxes[i].RegisterV2(groupID, node)
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

	return &v2GroupMuxCluster{nodes: nodes, transports: transports, muxes: muxes}
}

func (c *v2GroupMuxCluster) startAll() {
	for _, n := range c.nodes {
		n.Start()
	}
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

// TestV2GroupMuxCluster_ThreeNode_ElectsLeader is the proof-of-life for the
// M5 PR 28b per-group QUIC mux bridge: three v2 raft nodes registered on the
// mux under the same groupID must elect exactly one leader. Without RegisterV2
// (or with dispatch lookup still typed to *raft.Node), inbound RPCs would
// hit a typed-nil and the election would hang.
func TestV2GroupMuxCluster_ThreeNode_ElectsLeader(t *testing.T) {
	cluster := newV2GroupMuxCluster(t, 3, "group-pr28b")
	cluster.startAll()

	leader := cluster.waitForLeader(5 * time.Second)
	require.NotNil(t, leader, "no leader elected in 3-node v2 group-mux cluster")
}

// TestV2GroupMuxCluster_ThreeNode_Propose_Replicate verifies AppendEntries
// flows correctly over the per-group mux: a Propose on the leader commits and
// every follower observes the entry on its ApplyCh.
func TestV2GroupMuxCluster_ThreeNode_Propose_Replicate(t *testing.T) {
	cluster := newV2GroupMuxCluster(t, 3, "group-pr28b-replicate")
	cluster.startAll()
	verifyClusterReplicates(t, cluster, "v2-group-mux-replicate")
}

// TestV2GroupMuxCluster_ThreeNode_MuxMode_Propose_Replicate exercises the
// mux-mode dispatch sites (handleMuxRequest + dispatchToLocalGroup for
// coalesced heartbeats) in addition to the legacy handleRPC path. Without
// the RaftV2Handler-typed lookup in lookupNode, mux-mode coalesced
// heartbeats would silently drop on v2.
func TestV2GroupMuxCluster_ThreeNode_MuxMode_Propose_Replicate(t *testing.T) {
	cluster := newV2GroupMuxCluster(t, 3, "group-pr28b-muxmode")
	for _, m := range cluster.muxes {
		m.EnableMux(2, 5*time.Millisecond)
	}
	cluster.startAll()
	verifyClusterReplicates(t, cluster, "v2-group-mux-replicate-muxmode")
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
