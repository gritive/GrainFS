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

// Meta-Raft integration over TCP is the dormant-TCP twin of the QUIC meta-raft
// spec: it boots three in-process nodes and asserts they elect a leader and
// replicate a bucket assignment over the TCP transport. NOTE — meta-raft does NOT
// ride the per-group mux carrier: NewMetaTransportQUICMux discards groupMux
// ("StreamMetaRaft != StreamControl") and dispatches via RaftV2MetaQUICTransport,
// whose every send is transport.Call(StreamMetaRaft) — the data-plane
// connection-per-RPC path (tcp_call.go). So this proves meta-raft assembles over
// the TCP Call path; the S2b-2 mux CARRIER is proven separately by the group-raft
// spec (raftv2_group_mux_tcp_test.go, which asserts inbound mux sessions). The full
// serveruntime multi-node boot + parity bench stay S5(c). Construction is a
// near-mechanical swap of MustNewQUICTransport → MustNewTCPTransport.
var _ = Describe("Meta-Raft integration over TCP", func() {
	It("bootstraps three TCP nodes and replicates bucket assignment over the TCP Call path", func() {
		t := GinkgoT()

		const numNodes = 3

		addrs := make([]string, numNodes)
		for i := range addrs {
			addrs[i] = freeTCPAddrForMuxSpec()
		}

		transports := make([]*transport.TCPTransport, numNodes)
		muxes := make([]*raft.GroupRaftQUICMux, numNodes)
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

			// Mux on every node, before NewMetaTransportQUICMux. The constructor
			// auto-registers the meta node so receiver-side __meta__ dispatch is
			// wired before any inbound call lands. NewGroupRaftQUICMux takes the
			// muxDriverTransport interface, which TCPTransport satisfies (S2b-2).
			mux := raft.NewGroupRaftQUICMux(tr)
			mux.EnableMux(2, 5*time.Millisecond)

			m, err := NewMetaRaft(fastMetaRaftConfig(MetaRaftConfig{
				NodeID:  fmt.Sprintf("node-%d", i),
				RaftID:  addrs[i],
				Peers:   peers,
				DataDir: t.TempDir(),
			}))
			Expect(err).NotTo(HaveOccurred())

			metaTransport := NewMetaTransportQUICMux(tr, m.Node(), mux)
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
	})
})

func freeTCPAddrForMuxSpec() string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	Expect(err).NotTo(HaveOccurred())
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}
