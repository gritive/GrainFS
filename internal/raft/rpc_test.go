package raft

import (
	"testing"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// rpcTimeout bounds every reply wait so a misbehaving handler fails the test
// rather than hangs the test binary.
const rpcTimeout = 2 * time.Second

// awaitRequestVote synchronously dispatches a RequestVote and returns the
// reply, failing the test on timeout.
func awaitRequestVote(t *testing.T, n *Node, args *RequestVoteArgs) *RequestVoteReply {
	t.Helper()
	g := gomega.NewWithT(t)
	type result struct {
		reply *RequestVoteReply
	}
	ch := make(chan result, 1)
	go func() { ch <- result{n.HandleRequestVote(args)} }()
	select {
	case r := <-ch:
		g.Expect(r.reply).NotTo(gomega.BeNil(), "HandleRequestVote returned nil reply")
		return r.reply
	case <-time.After(rpcTimeout):
		t.Fatalf("HandleRequestVote timed out after %s", rpcTimeout)
		return nil
	}
}

// awaitAppendEntries is the AppendEntries counterpart to awaitRequestVote.
func awaitAppendEntries(t *testing.T, n *Node, args *AppendEntriesArgs) *AppendEntriesReply {
	t.Helper()
	g := gomega.NewWithT(t)
	type result struct {
		reply *AppendEntriesReply
	}
	ch := make(chan result, 1)
	go func() { ch <- result{n.HandleAppendEntries(args)} }()
	select {
	case r := <-ch:
		g.Expect(r.reply).NotTo(gomega.BeNil(), "HandleAppendEntries returned nil reply")
		return r.reply
	case <-time.After(rpcTimeout):
		t.Fatalf("HandleAppendEntries timed out after %s", rpcTimeout)
		return nil
	}
}

func startFollowerWithPeers(t *testing.T, id string, peers ...string) *Node {
	t.Helper()
	g := gomega.NewWithT(t)
	n, err := NewNode(Config{ID: id, Peers: peers, ElectionTimeout: time.Hour})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	n.Start()
	t.Cleanup(n.Stop)
	go func() {
		for range n.ApplyCh() {
		}
	}()
	return n
}

var _ = ginkgo.Describe("RPC integration", func() {
	awaitRequestVoteReply := func(node *Node, args *RequestVoteArgs) *RequestVoteReply {
		replyCh := make(chan *RequestVoteReply, 1)
		go func() { replyCh <- node.HandleRequestVote(args) }()
		var reply *RequestVoteReply
		gomega.Eventually(replyCh, rpcTimeout).Should(gomega.Receive(&reply))
		gomega.Expect(reply).NotTo(gomega.BeNil(), "HandleRequestVote returned nil reply")
		return reply
	}

	awaitAppendEntriesReply := func(node *Node, args *AppendEntriesArgs) *AppendEntriesReply {
		replyCh := make(chan *AppendEntriesReply, 1)
		go func() { replyCh <- node.HandleAppendEntries(args) }()
		var reply *AppendEntriesReply
		gomega.Eventually(replyCh, rpcTimeout).Should(gomega.Receive(&reply))
		gomega.Expect(reply).NotTo(gomega.BeNil(), "HandleAppendEntries returned nil reply")
		return reply
	}

	ginkgo.It("denies RequestVote from a candidate with a stale log", func(ginkgo.SpecContext) {
		nodes, _, cleanup, err := startRaftIntegrationCluster("n1", "shortlog", "n3")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(cleanup)
		node := nodes[0]

		gomega.Expect(waitFor(2*time.Second, node.IsLeader)).To(gomega.Succeed())
		gomega.Expect(node.Propose([]byte("seed"))).To(gomega.Succeed())
		gomega.Expect(waitFor(time.Second, func() bool {
			return node.CommittedIndex() >= 1
		})).To(gomega.Succeed())

		reply := awaitRequestVoteReply(node, &RequestVoteArgs{
			Term:         3,
			CandidateID:  "shortlog",
			LastLogIndex: 0,
			LastLogTerm:  0,
		})

		gomega.Expect(reply.VoteGranted).To(gomega.BeFalse(), "candidate's log is shorter")
		gomega.Expect(reply.Term).To(gomega.Equal(uint64(3)))
		gomega.Expect(node.State()).To(gomega.Equal(Follower))
		gomega.Expect(node.Term()).To(gomega.Equal(uint64(3)))
		gomega.Expect(node.rs.Load().votedFor).To(gomega.Equal(""), "votedFor cleared on step-down, not granted to stale-log candidate")
	}, ginkgo.NodeTimeout(5*time.Second))

	ginkgo.It("steps down on higher-term AppendEntries heartbeat", func(ginkgo.SpecContext) {
		node, cleanup, err := startRaftIntegrationSingleVoter("n1")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(cleanup)

		gomega.Expect(node.IsLeader()).To(gomega.BeTrue())
		gomega.Expect(node.Term()).To(gomega.Equal(uint64(1)))

		reply := awaitAppendEntriesReply(node, &AppendEntriesArgs{
			Term:     2,
			LeaderID: "newleader",
		})
		gomega.Expect(reply.Success).To(gomega.BeTrue(), "PR 5a accepts heartbeats")
		gomega.Expect(reply.Term).To(gomega.Equal(uint64(2)))
		gomega.Expect(node.State()).To(gomega.Equal(Follower), "must step down on higher-term AE")
		gomega.Expect(node.Term()).To(gomega.Equal(uint64(2)))
		gomega.Expect(node.rs.Load().votedFor).To(gomega.Equal(""), "votedFor cleared on step-down")
		gomega.Expect(node.LeaderID()).To(gomega.Equal("newleader"), "leader recognised on heartbeat")
		gomega.Expect(node.IsLeader()).To(gomega.BeFalse())

		reply = awaitAppendEntriesReply(node, &AppendEntriesArgs{
			Term:     2,
			LeaderID: "newleader",
		})
		gomega.Expect(reply.Success).To(gomega.BeTrue())
		gomega.Expect(reply.Term).To(gomega.Equal(uint64(2)))
		gomega.Expect(node.State()).To(gomega.Equal(Follower))
		gomega.Expect(node.LeaderID()).To(gomega.Equal("newleader"))

		reply = awaitAppendEntriesReply(node, &AppendEntriesArgs{
			Term:     1,
			LeaderID: "oldleader",
		})
		gomega.Expect(reply.Success).To(gomega.BeFalse(), "stale-term AE must be rejected")
		gomega.Expect(reply.Term).To(gomega.Equal(uint64(2)))
		gomega.Expect(node.LeaderID()).To(gomega.Equal("newleader"), "leaderID unchanged on stale AE")
	}, ginkgo.NodeTimeout(5*time.Second))

	ginkgo.Context("memTransport", func() {
		var network *memNetwork
		var n1 *Node
		var n2 *Node
		var transport Transport

		ginkgo.BeforeEach(func() {
			var err error
			network = newMemNetwork()
			n1, err = NewNode(Config{ID: "n1", Peers: []string{"n2"}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			n2, err = NewNode(Config{ID: "n2", Peers: []string{"n1"}, ElectionTimeout: time.Hour})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			transport = network.Register("n1", n1)
			network.Register("n2", n2)
			n1.SetTransport(transport)

			for _, node := range []*Node{n1, n2} {
				node.Start()
				ginkgo.DeferCleanup(node.Stop)
				go func(node *Node) {
					for range node.ApplyCh() {
					}
				}(node)
			}
		})

		ginkgo.It("routes RequestVote through the destination actor", func() {
			reply, err := transport.SendRequestVote("n2", &RequestVoteArgs{
				Term:        5,
				CandidateID: "n1",
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(reply.VoteGranted).To(gomega.BeTrue())
			gomega.Expect(reply.Term).To(gomega.Equal(uint64(5)))
			gomega.Expect(n2.Term()).To(gomega.Equal(uint64(5)))
			gomega.Expect(n2.State()).To(gomega.Equal(Follower))

			_, err = transport.SendRequestVote("ghost", &RequestVoteArgs{Term: 1})
			gomega.Expect(err).To(gomega.MatchError(ErrUnknownPeer))
		})
	})
})

// TestHandleRequestVote_GrantHappyPath: single-voter leader at term 1 receives
// a RequestVote at a higher term from an empty-log candidate. Per Raft §5.4
// the node must step down to Follower, advance to the higher term, and grant
// the vote (its own log is empty too, so the candidate is "as up-to-date").
func TestHandleRequestVote_GrantHappyPath(t *testing.T) {
	g := gomega.NewWithT(t)
	n := startFollowerWithPeers(t, "n1", "other")

	reply := awaitRequestVote(t, n, &RequestVoteArgs{
		Term:         2,
		CandidateID:  "other",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	g.Expect(reply.VoteGranted).To(gomega.BeTrue(), "expected VoteGranted=true")
	g.Expect(reply.Term).To(gomega.Equal(uint64(2)))

	g.Expect(n.State()).To(gomega.Equal(Follower))
	g.Expect(n.Term()).To(gomega.Equal(uint64(2)))
	g.Expect(n.rs.Load().votedFor).To(gomega.Equal("other"))
}

// TestHandleRequestVote_DenyStaleTerm: a candidate with a lower term than
// ours must be denied without state change.
func TestHandleRequestVote_DenyStaleTerm(t *testing.T) {
	g := gomega.NewWithT(t)
	n := startFollowerWithPeers(t, "n1", "bumper", "stale")

	// Force the actor to term 5 by sending a higher-term step-down RPC,
	// then test rejection of a term-3 RequestVote.
	_ = awaitRequestVote(t, n, &RequestVoteArgs{
		Term:        5,
		CandidateID: "bumper",
	})
	g.Expect(n.Term()).To(gomega.Equal(uint64(5)))

	beforeVotedFor := n.rs.Load().votedFor
	beforeState := n.State()

	reply := awaitRequestVote(t, n, &RequestVoteArgs{
		Term:        3,
		CandidateID: "stale",
	})

	g.Expect(reply.VoteGranted).To(gomega.BeFalse())
	g.Expect(reply.Term).To(gomega.Equal(uint64(5)))
	g.Expect(n.Term()).To(gomega.Equal(uint64(5)), "term must not regress")
	g.Expect(n.State()).To(gomega.Equal(beforeState))
	g.Expect(n.rs.Load().votedFor).To(gomega.Equal(beforeVotedFor), "votedFor must be unchanged on stale term")
}

// TestHandleRequestVote_DenyAlreadyVoted: at the same term, after voting for
// candidate A, a request from candidate B at the SAME term must be denied
// (split-vote prevention). We then verify that a request at a HIGHER term
// from B succeeds because the term advance clears votedFor.
func TestHandleRequestVote_DenyAlreadyVoted(t *testing.T) {
	g := gomega.NewWithT(t)
	n := startFollowerWithPeers(t, "n1", "alice", "bob")

	// Step 1: term advances to 2, vote granted to "alice".
	r1 := awaitRequestVote(t, n, &RequestVoteArgs{
		Term:        2,
		CandidateID: "alice",
	})
	g.Expect(r1.VoteGranted).To(gomega.BeTrue())
	g.Expect(n.rs.Load().votedFor).To(gomega.Equal("alice"))

	// Step 2: same term, different candidate → deny.
	r2 := awaitRequestVote(t, n, &RequestVoteArgs{
		Term:        2,
		CandidateID: "bob",
	})
	g.Expect(r2.VoteGranted).To(gomega.BeFalse(), "should deny: already voted for alice in term 2")
	g.Expect(r2.Term).To(gomega.Equal(uint64(2)))
	g.Expect(n.rs.Load().votedFor).To(gomega.Equal("alice"), "votedFor must remain alice")

	// Step 3: higher term from bob → step down clears votedFor, grant.
	r3 := awaitRequestVote(t, n, &RequestVoteArgs{
		Term:        3,
		CandidateID: "bob",
	})
	g.Expect(r3.VoteGranted).To(gomega.BeTrue())
	g.Expect(r3.Term).To(gomega.Equal(uint64(3)))
	g.Expect(n.rs.Load().votedFor).To(gomega.Equal("bob"))
}
