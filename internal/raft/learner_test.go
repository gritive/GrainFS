package raft

import (
	"context"
	"errors"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Learner membership", func() {
	ginkgo.It("commits AddLearner inline on a solo leader", func(ginkgo.SpecContext) {
		node, cleanup, err := startRaftIntegrationSingleVoter("leader")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer cleanup()

		gomega.Expect(node.AddLearner("learner-1", "addr-1")).To(gomega.Succeed())

		cfg := node.Configuration()
		var voters, learners []Server
		for _, server := range cfg.Servers {
			if server.Suffrage == Voter {
				voters = append(voters, server)
			} else {
				learners = append(learners, server)
			}
		}
		gomega.Expect(voters).To(gomega.HaveLen(1))
		gomega.Expect(voters[0].ID).To(gomega.Equal("leader"))
		gomega.Expect(learners).To(gomega.HaveLen(1))
		gomega.Expect(learners[0].ID).To(gomega.Equal("learner-1"))

		_, err = node.ProposeWait(context.Background(), []byte("hello"))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}, ginkgo.NodeTimeout(5*time.Second))

	ginkgo.It("rejects duplicate learners and voter self-add", func(ginkgo.SpecContext) {
		node, cleanup, err := startRaftIntegrationSingleVoter("leader")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer cleanup()

		gomega.Expect(node.AddLearner("x", "addr")).To(gomega.Succeed())
		gomega.Expect(errors.Is(node.AddLearner("x", "addr"), ErrAlreadyLearner)).To(gomega.BeTrue())
		gomega.Expect(errors.Is(node.AddLearner("leader", "addr"), ErrAlreadyLearner)).To(gomega.BeTrue())
	}, ginkgo.NodeTimeout(5*time.Second))

	ginkgo.It("prevents a learner-self node from campaigning", func(ginkgo.SpecContext) {
		node, err := NewNode(Config{
			ID:               "self",
			Peers:            []string{"other"},
			ElectionTimeout:  fastElectionTimeout,
			HeartbeatTimeout: testHeartbeat,
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer node.Stop()

		node.st.currentConfig = effectiveConfig{
			joint:    false,
			voters:   []string{"other"},
			learners: map[string]string{"self": "addr"},
		}
		node.st.invalidatePeerSet()
		node.publish()

		node.Start()
		go func() {
			for range node.ApplyCh() {
			}
		}()

		time.Sleep(5 * fastElectionTimeout)
		gomega.Expect(node.IsLeader()).To(gomega.BeFalse(), "learner-self must not become leader")
		gomega.Expect(node.State()).To(gomega.Equal(Follower), "learner-self must stay Follower")
	}, ginkgo.NodeTimeout(5*time.Second))

	ginkgo.It("promotes a caught-up learner to voter on leader and follower views", func(ginkgo.SpecContext) {
		fix, cleanup, err := startPromoteRaceCluster([]string{"n1"})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer cleanup()

		leader := fix.nodes[0]
		gomega.Expect(waitFor(2*time.Second, leader.IsLeader)).To(gomega.Succeed())

		n2, err := addPromoteRaceNode(fix, "n2", []string{"n1"}, slowElectionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(leader.AddLearner("n2", "n2-addr")).To(gomega.Succeed())
		gomega.Expect(waitFor(3*time.Second, func() bool {
			return leader.peerMatchIndexForTest("n2") >= leader.CommittedIndex()
		})).To(gomega.Succeed())

		gomega.Expect(leader.PromoteToVoter("n2")).To(gomega.Succeed())

		cfg := leader.Configuration()
		gomega.Expect(cfg.Servers).To(gomega.HaveLen(2))
		for _, server := range cfg.Servers {
			gomega.Expect(server.Suffrage).To(gomega.Equal(Voter), "leader: all servers must be voters after promote")
		}

		gomega.Expect(waitFor(3*time.Second, func() bool {
			got := n2.Configuration()
			if len(got.Servers) != 2 {
				return false
			}
			for _, server := range got.Servers {
				if server.Suffrage != Voter {
					return false
				}
			}
			return true
		})).To(gomega.Succeed(), "n2 must replay PromoteStage1 + joint AddVoter and see itself as Voter")
	}, ginkgo.NodeTimeout(10*time.Second))

	ginkgo.It("rejects PromoteToVoter for non-learners", func(ginkgo.SpecContext) {
		node, cleanup, err := startRaftIntegrationSingleVoter("leader")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer cleanup()

		gomega.Expect(errors.Is(node.PromoteToVoter("never-added"), ErrNotALearner)).To(gomega.BeTrue())
	}, ginkgo.NodeTimeout(5*time.Second))

	ginkgo.It("rejects PromoteToVoter when the learner is not caught up", func(ginkgo.SpecContext) {
		node, err := NewNode(Config{
			ID:                      "leader",
			Peers:                   nil,
			ElectionTimeout:         fastElectionTimeout,
			HeartbeatTimeout:        testHeartbeat,
			LearnerCatchupThreshold: 1,
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		node.Start()
		defer node.Stop()
		go func() {
			for range node.ApplyCh() {
			}
		}()

		gomega.Expect(waitFor(2*time.Second, node.IsLeader)).To(gomega.Succeed())
		gomega.Expect(node.AddLearner("L", "addr")).To(gomega.Succeed())
		for i := 0; i < 10; i++ {
			_, err := node.ProposeWait(context.Background(), []byte("x"))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		node.setPeerMatchIndexForTest("L", 0)
		gomega.Expect(errors.Is(node.PromoteToVoter("L"), ErrLearnerNotCaughtUp)).To(gomega.BeTrue())
	}, ginkgo.NodeTimeout(10*time.Second))
})
