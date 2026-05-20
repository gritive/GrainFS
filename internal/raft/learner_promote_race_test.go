package raft

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// TestLearnerPromote_LeaderStaysLeader_NoStepdown exercises the race the
// task report described: a solo-voter leader (n1) adds n2 as a learner,
// waits for it to catch up, then PromoteToVoter(n2). The hazard is that
// when n2 is promoted to voter (via the joint AddVoter phase 2 of v2's
// Path B), n2's election timer could fire before the leader's first
// heartbeat under the new voter epoch reaches it — causing n2 to campaign
// at a higher term and depose n1.
//
// Expected behaviour:
//   - PromoteToVoter returns nil
//   - n1 still IsLeader=true after promote completes
//   - n2 IsVoter in the resulting config
//
// Run with -count=100 to surface a statistical race.
var _ = ginkgo.Describe("Learner promote leader stability", func() {
	ginkgo.It("keeps a solo-voter leader stable while promoting a learner", func(ginkgo.SpecContext) {
		fix, cleanup, err := startPromoteRaceCluster([]string{"n1"})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer cleanup()

		leader := fix.nodes[0]
		gomega.Expect(waitFor(2*time.Second, func() bool { return leader.IsLeader() })).
			NotTo(gomega.HaveOccurred(), "n1 must bootstrap as leader")
		leaderTermBefore := leader.Term()

		_, err = addPromoteRaceNode(fix, "n2", []string{"n1"}, fastElectionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		gomega.Expect(leader.AddLearner("n2", "n2-addr")).To(gomega.Succeed())
		gomega.Expect(waitFor(3*time.Second, func() bool {
			return leader.peerMatchIndexForTest("n2") >= leader.CommittedIndex()
		})).NotTo(gomega.HaveOccurred(), "n2 must catch up to leader commit before promote")

		gomega.Expect(leader.PromoteToVoter("n2")).
			To(gomega.Succeed(), "PromoteToVoter must not fail (race regression)")
		gomega.Expect(leader.IsLeader()).To(gomega.BeTrue(), "n1 must remain leader after promote (stepdown race)")
		gomega.Expect(leader.Term()).
			To(gomega.Equal(leaderTermBefore), "leader term must not advance after stepdown+re-election")

		cfg := leader.Configuration()
		gomega.Expect(cfg.Servers).To(gomega.HaveLen(2))
		for _, server := range cfg.Servers {
			gomega.Expect(server.Suffrage).To(gomega.Equal(Voter), "server %s must be Voter post-promote", server.ID)
		}
	}, ginkgo.NodeTimeout(10*time.Second))

	ginkgo.It("keeps a three-voter leader stable while promoting a learner", func(ginkgo.SpecContext) {
		fix, cleanup, err := startPromoteRaceCluster([]string{"n1", "n2", "n3"})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer cleanup()

		leader := fix.nodes[0]
		gomega.Expect(waitFor(2*time.Second, func() bool { return leader.IsLeader() })).
			NotTo(gomega.HaveOccurred(), "n1 must win election in 3-voter cluster")
		leaderTermBefore := leader.Term()

		_, err = addPromoteRaceNode(fix, "n4", []string{"n1", "n2", "n3"}, fastElectionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		gomega.Expect(leader.AddLearner("n4", "n4-addr")).To(gomega.Succeed())
		gomega.Expect(waitFor(3*time.Second, func() bool {
			return leader.peerMatchIndexForTest("n4") >= leader.CommittedIndex()
		})).NotTo(gomega.HaveOccurred(), "n4 must catch up")

		gomega.Expect(leader.PromoteToVoter("n4")).To(gomega.Succeed())
		gomega.Expect(leader.IsLeader()).To(gomega.BeTrue(), "n1 must remain leader (3-voter promote race)")
		gomega.Expect(leader.Term()).
			To(gomega.Equal(leaderTermBefore), "leader term must not advance after 3-voter promote")

		cfg := leader.Configuration()
		gomega.Expect(cfg.Servers).To(gomega.HaveLen(4))
		for _, server := range cfg.Servers {
			gomega.Expect(server.Suffrage).To(gomega.Equal(Voter))
		}
	}, ginkgo.NodeTimeout(10*time.Second))
})
