package raft

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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

var _ = Describe("election", func() {
	var n1, n2, n3 *Node

	BeforeEach(func() {
		nodes, _ := startClusterGinkgo("n1", "n2", "n3")
		n1, n2, n3 = nodes[0], nodes[1], nodes[2]
	})

	It("elects the single early candidate", func() {
		Expect(waitFor(2*time.Second, func() bool {
			return n1.IsLeader()
		})).To(Succeed(), "n1 did not become leader")

		Expect(n1.State()).To(Equal(Leader))
		Expect(n1.LeaderID()).To(Equal("n1"))

		// Followers should converge on the new leader (via heartbeat).
		Expect(waitFor(2*time.Second, func() bool {
			return n2.LeaderID() == "n1" && n3.LeaderID() == "n1"
		})).To(Succeed(), "followers did not learn the leader")

		Expect(n2.State()).To(Equal(Follower))
		Expect(n3.State()).To(Equal(Follower))
	})

	It("keeps followers from starting a new election while heartbeats arrive", func() {
		Expect(waitFor(2*time.Second, func() bool {
			return n1.IsLeader() && n2.LeaderID() == "n1" && n3.LeaderID() == "n1"
		})).To(Succeed(), "cluster did not stabilise on n1")

		leaderTerm := n1.Term()

		// Watch for 500ms — well beyond a follower's election timeout window.
		// Heartbeats every 30ms must keep the followers parked.
		deadline := time.Now().Add(500 * time.Millisecond)
		for time.Now().Before(deadline) {
			Expect(n2.Term()).To(Equal(leaderTerm), "n2 term must not advance under heartbeat")
			Expect(n3.Term()).To(Equal(leaderTerm), "n3 term must not advance under heartbeat")
			Expect(n2.State()).To(Equal(Follower), "n2 must remain Follower")
			Expect(n3.State()).To(Equal(Follower), "n3 must remain Follower")
			time.Sleep(20 * time.Millisecond)
		}
	})

	It("steps down when observing a higher-term RequestVote", func() {
		Expect(waitFor(2*time.Second, func() bool { return n1.IsLeader() })).To(Succeed())
		leaderTerm := n1.Term()

		// The leader has a no-op entry at index 1 (becomeLeader §5.4.2). The
		// intruder's log must be at least as up-to-date (same term, same index).
		reply := n1.HandleRequestVote(&RequestVoteArgs{
			Term:         leaderTerm + 5,
			CandidateID:  "n2",
			LastLogIndex: 1,
			LastLogTerm:  leaderTerm,
		})

		Expect(reply.VoteGranted).To(BeTrue(), "leader must grant a higher-term vote from an up-to-date candidate")
		Expect(reply.Term).To(Equal(leaderTerm + 5))
		Expect(n1.State()).To(Equal(Follower), "leader must step down")
		Expect(n1.IsLeader()).To(BeFalse())
		Expect(n1.Term()).To(Equal(leaderTerm + 5))
		Expect(n1.rs.Load().votedFor).To(Equal("n2"))
	})

	It("rejects a higher-term vote from a non-voter", func() {
		n, err := NewNode(Config{ID: "n1", ElectionTimeout: time.Hour, HeartbeatTimeout: 10 * time.Millisecond})
		Expect(err).NotTo(HaveOccurred())
		startGinkgoNode(n)

		Expect(waitFor(2*time.Second, func() bool { return n.IsLeader() })).To(Succeed())
		leaderTerm := n.Term()

		reply := n.HandleRequestVote(&RequestVoteArgs{
			Term:         leaderTerm + 5,
			CandidateID:  "n2",
			LastLogIndex: 1,
			LastLogTerm:  leaderTerm,
		})

		Expect(reply.VoteGranted).To(BeFalse())
		Expect(reply.Term).To(Equal(leaderTerm))
		Expect(n.IsLeader()).To(BeTrue(), "non-voter RequestVote must not step down the leader")
		Expect(n.Term()).To(Equal(leaderTerm))
		Expect(n.rs.Load().votedFor).To(BeEmpty())
	})

	It("agrees on the elected term across the cluster", func() {
		Expect(waitFor(2*time.Second, func() bool {
			return n1.IsLeader() && n2.LeaderID() == "n1" && n3.LeaderID() == "n1"
		})).To(Succeed())

		term := n1.Term()
		Expect(n2.Term()).To(Equal(term), "n2 term must match leader")
		Expect(n3.Term()).To(Equal(term), "n3 term must match leader")
		Expect(term).To(BeNumerically(">=", uint64(1)), "election advances term at least once")
	})
})
