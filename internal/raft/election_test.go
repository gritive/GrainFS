package raft

import (
	"testing"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/stretchr/testify/require"
)

// Election test timing knobs. Node 1 (the prospective leader) gets a much
// shorter election timeout than the others so the race is decisive: its
// randomized window [50,100)ms ends before the others' [300,600)ms even
// begins. Heartbeat interval is 30ms — well under any other node's election
// timeout, so followers do not time out while a leader is alive.
const (
	fastElectionTimeout = 50 * time.Millisecond
	slowElectionTimeout = 300 * time.Millisecond
	testHeartbeat       = 30 * time.Millisecond
)

// startCluster wires three Nodes through a memNetwork. n1 has a short election
// timeout; n2 and n3 have long ones. All three start as Follower; n1 wins the
// first election deterministically.
//
// The caller receives the three Nodes and a teardown closure registered via
// t.Cleanup. ApplyCh of each Node is drained in the background.
func startCluster(t *testing.T, ids ...string) (nodes []*Node, net *memNetwork) {
	t.Helper()
	require.Len(t, ids, 3, "startCluster expects exactly 3 ids")

	net = newMemNetwork()
	nodes = make([]*Node, 0, len(ids))

	for i, id := range ids {
		// Build peers list = ids except self.
		peers := make([]string, 0, len(ids)-1)
		for _, p := range ids {
			if p != id {
				peers = append(peers, p)
			}
		}

		electionTimeout := slowElectionTimeout
		if i == 0 {
			electionTimeout = fastElectionTimeout
		}
		n, err := NewNode(Config{
			ID:               id,
			Peers:            peers,
			ElectionTimeout:  electionTimeout,
			HeartbeatTimeout: testHeartbeat,
		})
		require.NoError(t, err)
		nodes = append(nodes, n)
	}

	// Register transports BEFORE starting actors so the first Candidate's
	// outbound RequestVote can route immediately.
	for _, n := range nodes {
		tr := net.Register(n.cfg.ID, n)
		n.SetTransport(tr)
	}
	for _, n := range nodes {
		n.Start()
		t.Cleanup(n.Stop)
		go func(n *Node) {
			for range n.ApplyCh() {
			}
		}(n)
	}
	return nodes, net
}

var _ = ginkgo.Describe("Election", func() {
	ginkgo.Context("three-voter cluster", func() {
		var nodes []*Node
		var n1, n2, n3 *Node

		ginkgo.BeforeEach(func() {
			var err error
			var cleanup func()
			nodes, _, cleanup, err = startRaftIntegrationCluster("n1", "n2", "n3")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.DeferCleanup(cleanup)
			n1, n2, n3 = nodes[0], nodes[1], nodes[2]
		})

		ginkgo.It("elects the short-timeout node as leader", func(ginkgo.SpecContext) {
			gomega.Expect(waitFor(2*time.Second, n1.IsLeader)).To(gomega.Succeed(), "n1 did not become leader")
			gomega.Expect(n1.State()).To(gomega.Equal(Leader))
			gomega.Expect(n1.LeaderID()).To(gomega.Equal("n1"))

			gomega.Expect(waitFor(2*time.Second, func() bool {
				return n2.LeaderID() == "n1" && n3.LeaderID() == "n1"
			})).To(gomega.Succeed(), "followers did not learn the leader")
			gomega.Expect(n2.State()).To(gomega.Equal(Follower))
			gomega.Expect(n3.State()).To(gomega.Equal(Follower))
		}, ginkgo.NodeTimeout(5*time.Second))

		ginkgo.It("prevents follower re-election with heartbeats", func(ginkgo.SpecContext) {
			gomega.Expect(waitFor(2*time.Second, func() bool {
				return n1.IsLeader() && n2.LeaderID() == "n1" && n3.LeaderID() == "n1"
			})).To(gomega.Succeed(), "cluster did not stabilise on n1")
			leaderTerm := n1.Term()

			deadline := time.Now().Add(500 * time.Millisecond)
			for time.Now().Before(deadline) {
				gomega.Expect(n2.Term()).To(gomega.Equal(leaderTerm), "n2 term must not advance under heartbeat")
				gomega.Expect(n3.Term()).To(gomega.Equal(leaderTerm), "n3 term must not advance under heartbeat")
				gomega.Expect(n2.State()).To(gomega.Equal(Follower), "n2 must remain Follower")
				gomega.Expect(n3.State()).To(gomega.Equal(Follower), "n3 must remain Follower")
				time.Sleep(20 * time.Millisecond)
			}
		}, ginkgo.NodeTimeout(5*time.Second))

		ginkgo.It("steps down a leader that observes a higher-term RequestVote", func(ginkgo.SpecContext) {
			gomega.Expect(waitFor(2*time.Second, n1.IsLeader)).To(gomega.Succeed())
			leaderTerm := n1.Term()

			reply := n1.HandleRequestVote(&RequestVoteArgs{
				Term:         leaderTerm + 5,
				CandidateID:  "n2",
				LastLogIndex: 1,
				LastLogTerm:  leaderTerm,
			})

			gomega.Expect(reply.VoteGranted).To(gomega.BeTrue(), "leader must grant a higher-term vote from an up-to-date candidate")
			gomega.Expect(reply.Term).To(gomega.Equal(leaderTerm + 5))
			gomega.Expect(n1.State()).To(gomega.Equal(Follower), "leader must step down")
			gomega.Expect(n1.IsLeader()).To(gomega.BeFalse())
			gomega.Expect(n1.Term()).To(gomega.Equal(leaderTerm + 5))
			gomega.Expect(n1.rs.Load().votedFor).To(gomega.Equal("n2"))
		}, ginkgo.NodeTimeout(5*time.Second))

		ginkgo.It("propagates the elected leader term across the cluster", func(ginkgo.SpecContext) {
			gomega.Expect(waitFor(2*time.Second, func() bool {
				return n1.IsLeader() && n2.LeaderID() == "n1" && n3.LeaderID() == "n1"
			})).To(gomega.Succeed())

			term := n1.Term()
			gomega.Expect(n2.Term()).To(gomega.Equal(term), "n2 term must match leader")
			gomega.Expect(n3.Term()).To(gomega.Equal(term), "n3 term must match leader")
			gomega.Expect(term).To(gomega.BeNumerically(">=", 1), "election advances term at least once")
		}, ginkgo.NodeTimeout(5*time.Second))
	})

	ginkgo.It("rejects higher-term RequestVote from a non-voter without stepping down", func(ginkgo.SpecContext) {
		node, err := NewNode(Config{ID: "n1", ElectionTimeout: time.Hour, HeartbeatTimeout: 10 * time.Millisecond})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		node.Start()
		defer node.Stop()

		gomega.Expect(waitFor(2*time.Second, node.IsLeader)).To(gomega.Succeed())
		leaderTerm := node.Term()

		reply := node.HandleRequestVote(&RequestVoteArgs{
			Term:         leaderTerm + 5,
			CandidateID:  "n2",
			LastLogIndex: 1,
			LastLogTerm:  leaderTerm,
		})

		gomega.Expect(reply.VoteGranted).To(gomega.BeFalse())
		gomega.Expect(reply.Term).To(gomega.Equal(leaderTerm))
		gomega.Expect(node.IsLeader()).To(gomega.BeTrue(), "non-voter RequestVote must not step down the leader")
		gomega.Expect(node.Term()).To(gomega.Equal(leaderTerm))
		gomega.Expect(node.rs.Load().votedFor).To(gomega.BeEmpty())
	}, ginkgo.NodeTimeout(5*time.Second))

})
