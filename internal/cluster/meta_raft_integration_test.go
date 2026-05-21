package cluster

import (
	"context"
	"fmt"
	"net"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/transport"
)

const (
	testMetaRaftElectionTimeout  = 150 * time.Millisecond
	testMetaRaftHeartbeatTimeout = 40 * time.Millisecond
)

func fastMetaRaftConfig(cfg MetaRaftConfig) MetaRaftConfig {
	cfg.ElectionTimeout = testMetaRaftElectionTimeout
	cfg.HeartbeatTimeout = testMetaRaftHeartbeatTimeout
	return cfg
}

var _ = Describe("Meta-Raft integration", func() {
	It("bootstraps three nodes and joins followers", func() {
		t := GinkgoT()

		tr := newMetaTransportFake()

		newNode := func(id string, peers []string) *MetaRaft {
			m, err := NewMetaRaft(MetaRaftConfig{
				NodeID:    id,
				Peers:     peers,
				DataDir:   t.TempDir(),
				Transport: tr,
			})
			Expect(err).NotTo(HaveOccurred())
			tr.register(id, m)
			return m
		}

		m0 := newNode("node-0", nil)
		m1 := newNode("node-1", []string{"node-0"})
		m2 := newNode("node-2", []string{"node-0"})

		DeferCleanup(func() {
			_ = m0.Close()
			_ = m1.Close()
			_ = m2.Close()
		})

		Expect(m0.Bootstrap()).To(Succeed())
		Expect(m0.Start(context.Background(), nil)).To(Succeed())
		Eventually(func() bool {
			return m0.node.State() == raft.Leader
		}, 3*time.Second, 20*time.Millisecond).Should(BeTrue(), "node-0 must become leader")

		Expect(m1.Bootstrap()).To(Succeed())
		Expect(m1.Start(context.Background(), nil)).To(Succeed())

		ctx1, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
		DeferCleanup(cancel1)
		Expect(m0.Join(ctx1, "node-1", "node-1")).To(Succeed())

		Eventually(func() bool {
			for _, n := range m0.fsm.Nodes() {
				if n.ID == "node-1" {
					return true
				}
			}
			return false
		}, 3*time.Second, 30*time.Millisecond).Should(BeTrue(), "node-1 must appear in FSM after join")

		Expect(m2.Bootstrap()).To(Succeed())
		Expect(m2.Start(context.Background(), nil)).To(Succeed())

		ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
		DeferCleanup(cancel2)
		Expect(m0.Join(ctx2, "node-2", "node-2")).To(Succeed())

		Eventually(func() bool {
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
		}, 3*time.Second, 30*time.Millisecond).Should(BeTrue(), "both node-1 and node-2 must appear in FSM")

		leaderCount := 0
		for _, m := range []*MetaRaft{m0, m1, m2} {
			if m.node.State() == raft.Leader {
				leaderCount++
			}
		}
		Expect(leaderCount).To(Equal(1), "exactly one leader must exist in the group")
	})

	It("proposes shard groups", func() {
		t := GinkgoT()
		tr := newMetaTransportFake()
		m, err := NewMetaRaft(MetaRaftConfig{
			NodeID:    "node-0",
			Peers:     nil,
			DataDir:   t.TempDir(),
			Transport: tr,
		})
		Expect(err).NotTo(HaveOccurred())
		tr.register("node-0", m)
		DeferCleanup(func() { _ = m.Close() })

		Expect(m.Bootstrap()).To(Succeed())
		Expect(m.Start(context.Background(), nil)).To(Succeed())
		Eventually(func() bool {
			return m.node.State() == raft.Leader
		}, 2*time.Second, 20*time.Millisecond).Should(BeTrue())

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		DeferCleanup(cancel)

		sg := ShardGroupEntry{
			ID:      "group-0",
			PeerIDs: []string{"node-0"},
		}
		Expect(m.ProposeShardGroup(ctx, sg)).To(Succeed())

		groups := m.fsm.ShardGroups()
		Expect(groups).To(HaveLen(1))
		Expect(groups[0].ID).To(Equal("group-0"))
		Expect(groups[0].PeerIDs).To(Equal([]string{"node-0"}))
	})

	It("bootstraps five static QUIC nodes", func() {
		t := GinkgoT()

		const numNodes = 5

		addrs := make([]string, numNodes)
		for i := range addrs {
			addrs[i] = freeUDPAddrForSpec()
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

			m, err := NewMetaRaft(fastMetaRaftConfig(MetaRaftConfig{
				NodeID:  fmt.Sprintf("node-%d", i),
				RaftID:  addrs[i],
				Peers:   peers,
				DataDir: t.TempDir(),
			}))
			Expect(err).NotTo(HaveOccurred())
			tr := transport.MustNewQUICTransport("meta-raft-quic-test")
			Expect(tr.Listen(context.Background(), addrs[i])).To(Succeed())
			metaTransport := NewMetaTransportQUIC(tr, m.Node())
			m.SetTransport(metaTransport)

			transports[i] = tr
			nodes[i] = m
		}

		DeferCleanup(func() {
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
			Expect(m.Bootstrap()).To(Succeed())
			Expect(m.Start(context.Background(), nil)).To(Succeed())
		}

		var leader *MetaRaft
		Eventually(func() bool {
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
		}, 10*time.Second, 50*time.Millisecond).Should(BeTrue(), "static five-node meta-Raft must elect exactly one QUIC leader")

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		DeferCleanup(cancel)
		Expect(leader.ProposeBucketAssignment(ctx, "photos", "group-0")).To(Succeed())

		Eventually(func() bool {
			for _, m := range nodes {
				if m.FSM().BucketAssignments()["photos"] != "group-0" {
					return false
				}
			}
			return true
		}, 5*time.Second, 50*time.Millisecond).Should(BeTrue(), "bucket assignment must replicate to every meta-Raft node")
	})

	It("shares QUIC transport with data Raft", func() {
		t := GinkgoT()

		const numNodes = 5

		addrs := make([]string, numNodes)
		transports := make([]*transport.QUICTransport, numNodes)
		dataNodes := make([]RaftNode, numNodes)
		dataClosers := make([]func() error, numNodes)
		metaNodes := make([]*MetaRaft, numNodes)
		for i := range transports {
			tr := transport.MustNewQUICTransport("shared-raft-quic-test")
			Expect(tr.Listen(context.Background(), "127.0.0.1:0")).To(Succeed())
			transports[i] = tr
			addrs[i] = tr.LocalAddr()
		}
		for i := range metaNodes {
			peers := make([]string, 0, numNodes-1)
			for j := range addrs {
				if i != j {
					peers = append(peers, addrs[j])
				}
			}

			dataCfg := raft.DefaultConfig(addrs[i], peers)
			dataCfg.ElectionTimeout = testMetaRaftElectionTimeout
			dataCfg.HeartbeatTimeout = testMetaRaftHeartbeatTimeout
			dataNode, dataClose, err := newRaftNode(dataCfg, "")
			Expect(err).NotTo(HaveOccurred())
			dataRPC := NewRaftQUICRPCTransport(transports[i], dataNode)
			dataRPC.SetTransport()
			metaNode, err := NewMetaRaft(fastMetaRaftConfig(MetaRaftConfig{
				NodeID:  fmt.Sprintf("node-%d", i),
				RaftID:  addrs[i],
				Peers:   peers,
				DataDir: t.TempDir(),
			}))
			Expect(err).NotTo(HaveOccurred())
			metaTransport := NewMetaTransportQUIC(transports[i], metaNode.Node())
			metaNode.SetTransport(metaTransport)

			dataNodes[i] = dataNode
			dataClosers[i] = dataClose
			metaNodes[i] = metaNode
		}

		DeferCleanup(func() {
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
			for _, closeFn := range dataClosers {
				if closeFn != nil {
					_ = closeFn()
				}
			}
			for _, tr := range transports {
				if tr != nil {
					tr.Close()
				}
			}
		})

		for _, n := range dataNodes {
			Expect(n.Bootstrap()).To(Succeed())
			n.Start()
		}
		for _, m := range metaNodes {
			Expect(m.Bootstrap()).To(Succeed())
			Expect(m.Start(context.Background(), nil)).To(Succeed())
		}

		Eventually(func() bool {
			count := 0
			for _, n := range dataNodes {
				if n.IsLeader() {
					count++
				}
			}
			return count == 1
		}, 10*time.Second, 50*time.Millisecond).Should(BeTrue(), "data Raft must elect exactly one leader")

		var metaLeader *MetaRaft
		Eventually(func() bool {
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
		}, 10*time.Second, 50*time.Millisecond).Should(BeTrue(), "meta Raft must elect exactly one leader while sharing QUIC with data Raft")
	})
})

func freeUDPAddrForSpec() string {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	Expect(err).NotTo(HaveOccurred())
	defer pc.Close()
	return pc.LocalAddr().String()
}
