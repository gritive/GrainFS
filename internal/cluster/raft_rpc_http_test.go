// raft_rpc_http_test.go — per-group raft over the Phase 8 HTTP transport.
// GroupRaftSender routes RequestVote/AppendEntries over the native
// /raft/group/rpc buffered route (Phase 8 N7-3) — an HTTP POST round-trip per
// RPC. This is the multi-node proof that the control plane works over the HTTP
// request/response path (the Phase 8 simplification thesis: HTTP's keep-alive
// pool + req/resp subsumes the hand-rolled mux).
//
// Discriminating-by-construction: the buffered route IS the carrier — there is
// no mux and no fallback, so election + replication is IMPOSSIBLE unless the
// native route carried the raft RPCs. The
// InboundNativeBuffered(RouteRaftGroupRPC) > 0 assertion is a positive
// confirmation on top of that.
package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/transport"
)

// buildHTTPGroupNodes wires N per-group v2 raft nodes over real HTTP transports,
// each registered on its own GroupRaftMux under groupID. Raft RPCs ride the
// native /raft/group/rpc buffered route = an HTTP POST per RPC.
func buildHTTPGroupNodes(t *testing.T, n int, groupID string) ([]RaftNode, []*transport.HTTPTransport, []*raft.GroupRaftMux) {
	t.Helper()
	ctx := context.Background()

	transports := make([]*transport.HTTPTransport, n)
	for i := range transports {
		transports[i] = transport.MustNewHTTPTransport("test-group-http-psk")
		require.NoError(t, transports[i].Listen(ctx, "127.0.0.1:0"))
	}
	addrs := make([]string, n)
	for i, tr := range transports {
		addrs[i] = tr.LocalAddr()
	}
	muxes := make([]*raft.GroupRaftMux, n)
	for i := range muxes {
		muxes[i] = raft.NewGroupRaftMux(transports[i]) // no EnableMux → legacy Call path
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
			ElectionTimeout:  300 * time.Millisecond,
			HeartbeatTimeout: 60 * time.Millisecond,
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

func TestHTTPGroupCluster_ThreeNode_NoMux_Propose_Replicate(t *testing.T) {
	nodes, transports, _ := buildHTTPGroupNodes(t, 3, "group-http")
	for _, n := range nodes {
		n.Start()
	}

	verifyClusterReplicates(t, &v2GroupMuxCluster{nodes: nodes}, "http-group-replicate")

	// Positive carrier proof (Phase 8 N7-3): GroupRaftSender dials the native
	// /raft/group/rpc buffered route with no fallback, so election + replication
	// is impossible unless the native route actually served the raft RPCs.
	// Assert per-route dispatches were counted (not vacuous).
	total := uint64(0)
	counts := make([]uint64, len(transports))
	for i, tr := range transports {
		counts[i] = tr.InboundNativeBuffered(transport.RouteRaftGroupRPC)
		total += counts[i]
	}
	require.Greaterf(t, total, uint64(0),
		"group raft must traverse the native /raft/group/rpc route; per-node=%v", counts)
}
