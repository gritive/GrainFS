package raft

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("learner promotion race", func() {
	It("keeps a solo-voter leader stable while promoting a learner", func() {
		fix := startMembershipClusterGinkgo([]string{"n1"})
		leader := fix.nodes[0]
		Expect(waitFor(2*time.Second, func() bool { return leader.IsLeader() })).To(Succeed(),
			"n1 must bootstrap as leader")
		leaderTermBefore := leader.Term()

		fix.addNodeGinkgo("n2", []string{"n1"}, fastElectionTimeout)

		Expect(leader.AddLearner("n2", "n2-addr")).To(Succeed())
		Expect(waitFor(3*time.Second, func() bool {
			return leader.peerMatchIndexForTest("n2") >= leader.CommittedIndex()
		})).To(Succeed(), "n2 must catch up to leader commit before promote")

		Expect(leader.PromoteToVoter("n2")).To(Succeed(), "PromoteToVoter must not fail (race regression)")

		Expect(leader.IsLeader()).To(BeTrue(), "n1 must remain leader after promote (stepdown race)")
		Expect(leader.Term()).To(Equal(leaderTermBefore),
			"leader term must not advance — a stepdown+re-election would bump term")

		// Final config: 2 voters, no learners.
		cfg := leader.Configuration()
		Expect(cfg.Servers).To(HaveLen(2))
		for _, s := range cfg.Servers {
			Expect(s.Suffrage).To(Equal(Voter), "all servers must be Voter post-promote")
		}
	})

	It("keeps a 3-voter leader stable while promoting a learner", func() {
		fix := startMembershipClusterGinkgo([]string{"n1", "n2", "n3"})
		leader := fix.nodes[0]
		Expect(waitFor(2*time.Second, func() bool { return leader.IsLeader() })).To(Succeed(),
			"n1 must win election in 3-voter cluster")
		leaderTermBefore := leader.Term()

		fix.addNodeGinkgo("n4", []string{"n1", "n2", "n3"}, fastElectionTimeout)

		Expect(leader.AddLearner("n4", "n4-addr")).To(Succeed())
		Expect(waitFor(3*time.Second, func() bool {
			return leader.peerMatchIndexForTest("n4") >= leader.CommittedIndex()
		})).To(Succeed(), "n4 must catch up")

		Expect(leader.PromoteToVoter("n4")).To(Succeed())
		Expect(leader.IsLeader()).To(BeTrue(), "n1 must remain leader (3-voter promote race)")
		Expect(leader.Term()).To(Equal(leaderTermBefore),
			"leader term must not advance after 3-voter promote")

		cfg := leader.Configuration()
		Expect(cfg.Servers).To(HaveLen(4))
		for _, s := range cfg.Servers {
			Expect(s.Suffrage).To(Equal(Voter))
		}
	})
})
