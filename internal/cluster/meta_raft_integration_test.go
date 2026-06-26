package cluster

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gritive/GrainFS/internal/raft"
)

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

})
