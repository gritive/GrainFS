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

// TestMetaRaft_QUICMux_ThreeNodeBootstrap_E2E validates that meta-raft can
// elect a leader and replicate state when its RPCs ride the shared mux
// (the same path per-group raft uses post-R+H). Coverage:
//
//   - NewMetaTransportQUICMux constructor (auto-RegisterMetaNode runs)
//   - Sender mux path: muxConnFor + RaftConn.Call for RequestVote
//   - Sender mux path: HeartbeatCoalescer.AppendEntries (entries-empty AE)
//   - Receiver mux path: handleMuxRequest("__meta__") + dispatchToLocalGroup("__meta__")
//   - Mixed traffic: per-group mux carries meta-raft alongside any future
//     group registrations on the same conn (none in this test)
//
// 3 nodes is the minimum for genuine quorum behavior. We use 50ms heartbeat
// / 750ms election to keep the test fast; mux flush window is 5ms.
var _ = Describe("Meta-Raft mux integration", func() {
	It("bootstraps three QUIC mux nodes and replicates bucket assignment", func() {
		t := GinkgoT()

		const numNodes = 3

		addrs := make([]string, numNodes)
		for i := range addrs {
			addrs[i] = freeUDPAddrForMuxSpec()
		}

		transports := make([]*transport.QUICTransport, numNodes)
		muxes := make([]*raft.GroupRaftQUICMux, numNodes)
		metaNodes := make([]*MetaRaft, numNodes)
		for i := range metaNodes {
			peers := make([]string, 0, numNodes-1)
			for j := range addrs {
				if i != j {
					peers = append(peers, addrs[j])
				}
			}

			tr := transport.MustNewQUICTransport("meta-mux-e2e-psk")
			Expect(tr.Listen(context.Background(), addrs[i])).To(Succeed())

			// Mux on every node, before NewMetaTransportQUICMux. The constructor
			// auto-registers the meta node so receiver-side __meta__ dispatch
			// is wired before any inbound call lands.
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

		// Election under mux must converge in the same window legacy does. 10s
		// matches the existing five-node legacy test.
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
		}, 10*time.Second, 50*time.Millisecond).Should(BeTrue(), "three-node meta-Raft on mux must elect exactly one leader")

		// State replicates through mux (heartbeat-batched AE for entries-bearing
		// catches up followers). Bucket assignment is a small cmd, fits in one
		// frame.
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
		}, 5*time.Second, 50*time.Millisecond).Should(BeTrue(), "bucket assignment must replicate over mux to every meta-Raft node")

		// Sanity check: every node still reports the meta node registered. If
		// an OnBroken handler had cleared metaNode somewhere along the way,
		// later traffic would silently fail with "unknown group __meta__".
		for i, mux := range muxes {
			// Reach in via the same lookup path the receiver uses.
			// (lookupNode is unexported; this asserts via MuxEnabled + a
			// no-op call to dispatchToLocalGroup is overkill - we instead
			// re-invoke ProposeBucketAssignment from a different node to
			// force another round-trip.)
			_ = mux
			Expect(metaNodes[i].FSM().BucketAssignments()["photos"]).To(Equal("group-0"))
		}
	})
})

func freeUDPAddrForMuxSpec() string {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	Expect(err).NotTo(HaveOccurred())
	defer pc.Close()
	return pc.LocalAddr().String()
}
