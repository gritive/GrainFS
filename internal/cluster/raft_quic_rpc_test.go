package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/transport"
)

// v2QUICCluster wires up N raft v2 nodes connected via real QUIC transport
// through the new RaftQUICRPCTransport bridge. It is the v2 counterpart of
// internal/raft.quicCluster.
type v2QUICCluster struct {
	nodes      []RaftNode
	transports []*transport.QUICTransport
	rpcs       []*RaftQUICRPCTransport
}

type timeoutNowObservingNode struct {
	RaftNode
	handled chan<- string
}

func (n timeoutNowObservingNode) HandleTimeoutNow() {
	n.RaftNode.HandleTimeoutNow()
	select {
	case n.handled <- n.ID():
	default:
	}
}

// newV2QUICCluster creates an N-node cluster with optional election timeout
// override (default 200ms). Pass a custom duration as the third argument when
// the test needs to distinguish TimeoutNow from natural election (e.g. 5s).
func newV2QUICCluster(t *testing.T, n int, electionTimeout ...time.Duration) *v2QUICCluster {
	t.Helper()

	et := 200 * time.Millisecond
	if len(electionTimeout) > 0 {
		et = electionTimeout[0]
	}

	ctx := context.Background()
	transports := make([]*transport.QUICTransport, n)
	for i := range transports {
		transports[i] = transport.MustNewQUICTransport("test-cluster-psk-v2")
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
			ElectionTimeout:  et,
			HeartbeatTimeout: 50 * time.Millisecond,
		}
		node, _, err := newRaftNode(rcfg, "")
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
				attemptCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
				defer cancel()
				lastErr = transports[i].Connect(attemptCtx, addrs[j])
				return lastErr == nil
			}, 15*time.Second, 100*time.Millisecond, "connect %d→%d: %v", i, j, lastErr)
		}
	}

	// Register the v2 QUIC RPC bridge per node.
	rpcs := make([]*RaftQUICRPCTransport, n)
	for i := range nodes {
		rpcs[i] = NewRaftQUICRPCTransport(transports[i], nodes[i])
		rpcs[i].SetTransport()
		rpcs[i].SetTimeoutNowTransport()
	}

	t.Cleanup(func() {
		for _, node := range nodes {
			node.Close()
		}
		for _, tr := range transports {
			tr.Close()
		}
	})

	return &v2QUICCluster{nodes: nodes, transports: transports, rpcs: rpcs}
}

func (c *v2QUICCluster) startAll() {
	for _, n := range c.nodes {
		n.Start()
	}
}

func (c *v2QUICCluster) waitForLeader(timeout time.Duration) RaftNode {
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

// TestV2QUICCluster_ThreeNode_ElectsLeader is the proof-of-life for the M5 PR 27
// QUIC RPC bridge: three raft v2 nodes connected via real QUIC must elect
// exactly one leader. Without the bridge (or with a wire-format bug) this test
// hangs at election.
func TestV2QUICCluster_ThreeNode_ElectsLeader(t *testing.T) {
	cluster := newV2QUICCluster(t, 3)
	cluster.startAll()

	leader := cluster.waitForLeader(5 * time.Second)
	require.NotNil(t, leader, "no leader elected in 3-node v2 QUIC cluster")
}

// TestV2QUICCluster_ThreeNode_Propose_Replicate verifies that a Propose on the
// leader commits and propagates to all three nodes' apply channels — proving
// AppendEntries flows correctly over the v2 QUIC bridge.
func TestV2QUICCluster_ThreeNode_Propose_Replicate(t *testing.T) {
	cluster := newV2QUICCluster(t, 3)
	cluster.startAll()

	leader := cluster.waitForLeader(5 * time.Second)
	require.NotNil(t, leader, "leader required for replication test")

	// Drain apply channels on every node so the actor doesn't back-pressure.
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
	idx, err := leader.ProposeWait(ctx, []byte("v2-quic-replicate"))
	require.NoError(t, err)
	require.Greater(t, idx, uint64(0))

	deadline := time.After(5 * time.Second)
	for i, ch := range applyCh {
		var seen bool
		for !seen {
			select {
			case a := <-ch:
				if a.idx == idx && string(a.cmd) == "v2-quic-replicate" {
					seen = true
				}
			case <-deadline:
				t.Fatalf("node[%d] did not apply index %d before deadline", i, idx)
			}
		}
	}
}

// TestV2QUICCluster_ThreeNode_TransferLeadership verifies that TransferLeadership
// sends TimeoutNow over QUIC and a different node becomes leader within one
// election cycle — proving SendTimeoutNow is wired end-to-end.
//
// The test observes TimeoutNow at the receiver-side handler, then separately
// waits for the cluster to converge on a new leader.
func TestV2QUICCluster_ThreeNode_TransferLeadership(t *testing.T) {
	const transferDeadline = 450 * time.Millisecond

	cluster := newV2QUICCluster(t, 3, 600*time.Millisecond)
	timeoutNowHandled := make(chan string, 1)
	for _, rpc := range cluster.rpcs {
		rpc.SetNode(timeoutNowObservingNode{
			RaftNode: rpc.GetNode(),
			handled:  timeoutNowHandled,
		})
	}
	cluster.startAll()

	// Initial leader election: with ET=600ms the window is [600ms, 1.2s);
	// allow extra room for scheduler noise and QUIC setup.
	leader := cluster.waitForLeader(2 * time.Second)
	require.NotNil(t, leader, "initial leader required")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := leader.ProposeWait(ctx, []byte("v2-quic-transfer-barrier"))
	require.NoError(t, err)

	transferStarted := time.Now()
	err = leader.TransferLeadership()
	require.NoError(t, err)

	select {
	case nodeID := <-timeoutNowHandled:
		require.NotEmpty(t, nodeID, "TimeoutNow receiver")
	case <-time.After(transferDeadline):
		t.Fatal("TimeoutNow must be handled over QUIC during TransferLeadership")
	}

	remaining := transferDeadline - time.Since(transferStarted)
	require.Positive(t, remaining, "TimeoutNow must be handled before the transfer deadline")

	// TimeoutNow was already observed at the receiver. Requiring the new leader
	// within the original transfer window keeps natural election outside the pass
	// condition: ET=600ms, transferDeadline=450ms.
	require.Eventually(t, func() bool {
		for _, n := range cluster.nodes {
			if n != leader && n.IsLeader() {
				return true
			}
		}
		return false
	}, remaining, 20*time.Millisecond, "a new leader must emerge after TransferLeadership")
}
