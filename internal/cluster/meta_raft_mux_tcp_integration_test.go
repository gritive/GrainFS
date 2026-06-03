package cluster

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/transport"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// Meta-Raft integration over TCP boots three in-process nodes and asserts they
// elect a leader and replicate a bucket assignment over the TCP transport.
//
// As of the meta-raft-over-carrier perf fix, meta-raft RIDES the shared per-peer
// mux carrier (the same one group-raft uses) instead of connection-per-RPC
// transport.Call. NewMetaTransportMux now stores groupMux + RegisterMetaNode, and
// RaftV2MetaTransport.Send{AppendEntries,RequestVote} try the mux first
// (RaftConn over groupID "__meta__") with a Call fallback. Before the fix every
// meta send did a fresh TLS handshake (tcp_call.go connection-per-RPC) — the
// cluster PUT throughput bottleneck. This spec asserts replication AND that the
// traffic actually rode the mux carrier (InboundMuxSessionCount > 0), which is
// the regression guard: reverting meta to Call-only drops mux sessions to 0.
var _ = Describe("Meta-Raft integration over TCP", func() {
	It("bootstraps three TCP nodes and replicates bucket assignment over the mux carrier", func() {
		t := GinkgoT()

		const numNodes = 3

		addrs := make([]string, numNodes)
		for i := range addrs {
			addrs[i] = freeTCPAddrForMuxSpec()
		}

		transports := make([]*transport.TCPTransport, numNodes)
		muxes := make([]*raft.GroupRaftMux, numNodes)
		metaNodes := make([]*MetaRaft, numNodes)
		for i := range metaNodes {
			peers := make([]string, 0, numNodes-1)
			for j := range addrs {
				if i != j {
					peers = append(peers, addrs[j])
				}
			}

			tr := transport.MustNewTCPTransport("meta-mux-tcp-e2e-psk")
			Expect(tr.Listen(context.Background(), addrs[i])).To(Succeed())

			// Mux on every node, before NewMetaTransportMux. The constructor
			// auto-registers the meta node so receiver-side __meta__ dispatch is
			// wired before any inbound call lands. NewGroupRaftMux takes the
			// muxDriverTransport interface, which TCPTransport satisfies (S2b-2).
			mux := raft.NewGroupRaftMux(tr)
			mux.EnableMux(2, 5*time.Millisecond)

			m, err := NewMetaRaft(fastMetaRaftConfig(MetaRaftConfig{
				NodeID:  fmt.Sprintf("node-%d", i),
				RaftID:  addrs[i],
				Peers:   peers,
				DataDir: t.TempDir(),
			}))
			Expect(err).NotTo(HaveOccurred())

			metaTransport := NewMetaTransportMux(tr, m.Node(), mux)
			m.SetTransport(metaTransport)

			transports[i] = tr
			muxes[i] = mux
			metaNodes[i] = m
		}

		DeferCleanup(func() {
			for _, m := range metaNodes {
				if m != nil {
					_ = m.Close()
				}
			}
			for _, tr := range transports {
				if tr != nil {
					_ = tr.Close()
				}
			}
		})

		for _, m := range metaNodes {
			Expect(m.Bootstrap()).To(Succeed())
			Expect(m.Start(context.Background(), nil)).To(Succeed())
		}

		var leader *MetaRaft
		Eventually(func() bool {
			leader = nil
			for _, m := range metaNodes {
				if m.IsLeader() {
					if leader != nil {
						return false
					}
					leader = m
				}
			}
			return leader != nil
		}, 10*time.Second, 50*time.Millisecond).Should(BeTrue(), "three-node meta-Raft on the TCP mux must elect exactly one leader")

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		DeferCleanup(cancel)
		Expect(leader.ProposeBucketAssignment(ctx, "photos", "group-0")).To(Succeed())

		Eventually(func() bool {
			for _, m := range metaNodes {
				if m.FSM().BucketAssignments()["photos"] != "group-0" {
					return false
				}
			}
			return true
		}, 5*time.Second, 50*time.Millisecond).Should(BeTrue(), "bucket assignment must replicate over the TCP mux to every meta-Raft node")

		// Regression guard: meta-raft must ride the mux CARRIER, not the legacy
		// connection-per-RPC Call fallback. The Call path (tcp_call.go) dials the
		// data-plane ALPN per RPC and never populates muxInbound, so a non-zero
		// inbound mux session count proves meta AppendEntries/heartbeats actually
		// rode the shared persistent carrier. This is a meta-only cluster (no group
		// raft), so any mux session here is meta traffic. Reverting meta to
		// Call-only (the pre-fix bottleneck) drops this to 0 and fails the test.
		total := 0
		counts := make([]int, len(transports))
		for i, tr := range transports {
			counts[i] = tr.InboundMuxSessionCount()
			total += counts[i]
		}
		Expect(total).To(BeNumerically(">", 0),
			"meta-raft must ride the TCP mux carrier (inbound mux sessions), not the Call fallback; per-node=%v", counts)
	})
})

func freeTCPAddrForMuxSpec() string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	Expect(err).NotTo(HaveOccurred())
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}
