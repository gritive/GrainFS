package raft

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// timeoutNowRecorder wraps a Transport and records the peer that receives a
// SendTimeoutNow call. Used to verify target-selection logic.
type timeoutNowRecorder struct {
	inner  Transport
	target atomic.Pointer[string]
}

func (r *timeoutNowRecorder) SendRequestVote(peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	return r.inner.SendRequestVote(peer, args)
}
func (r *timeoutNowRecorder) SendAppendEntries(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	return r.inner.SendAppendEntries(peer, args)
}
func (r *timeoutNowRecorder) SendInstallSnapshot(peer string, args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	return r.inner.SendInstallSnapshot(peer, args)
}
func (r *timeoutNowRecorder) SendTimeoutNow(peer string, args *TimeoutNowArgs) (*TimeoutNowReply, error) {
	r.target.Store(&peer)
	return r.inner.SendTimeoutNow(peer, args)
}

var _ = ginkgo.Describe("Transfer leadership", func() {
	ginkgo.It("returns ErrNoPeers for a solo leader without stepping down", func(ginkgo.SpecContext) {
		node, cleanup, err := startRaftIntegrationSingleVoter("solo")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer cleanup()

		got := node.TransferLeadership()
		gomega.Expect(got).To(gomega.MatchError(gomega.MatchRegexp(ErrNoPeers.Error())))
		gomega.Expect(errors.Is(got, ErrNoPeers)).To(gomega.BeTrue(), "expected ErrNoPeers, got: %v", got)
		gomega.Expect(node.IsLeader()).To(gomega.BeTrue(), "solo node must stay leader after ErrNoPeers")
	}, ginkgo.NodeTimeout(5*time.Second))

	ginkgo.It("returns ErrNotLeader when called on a follower", func(ginkgo.SpecContext) {
		nodes, _, cleanup, err := startRaftIntegrationCluster("n1", "n2", "n3")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer cleanup()

		leader := nodes[0]
		gomega.Expect(waitFor(2*time.Second, leader.IsLeader)).To(gomega.Succeed(), "n1 must be leader")

		follower := nodes[1]
		gomega.Expect(follower.IsLeader()).To(gomega.BeFalse())
		got := follower.TransferLeadership()
		gomega.Expect(errors.Is(got, ErrNotLeader)).To(gomega.BeTrue(), "expected ErrNotLeader, got: %v", got)
	}, ginkgo.NodeTimeout(5*time.Second))

	ginkgo.It("steps down the leader and allows a new leader to emerge", func(ginkgo.SpecContext) {
		nodes, _, cleanup, err := startRaftIntegrationCluster("n1", "n2", "n3")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer cleanup()

		n1, n2, n3 := nodes[0], nodes[1], nodes[2]
		gomega.Expect(waitFor(2*time.Second, n1.IsLeader)).To(gomega.Succeed(), "n1 must be leader")

		gomega.Expect(n1.TransferLeadership()).To(gomega.Succeed(), "TransferLeadership must return nil on a multi-voter leader")
		gomega.Expect(n1.IsLeader()).To(gomega.BeFalse(), "n1 must not be leader after TransferLeadership")
		gomega.Expect(waitFor(5*time.Second, func() bool {
			return n2.IsLeader() || n3.IsLeader()
		})).To(gomega.Succeed(), "a new leader must emerge after transfer")
	}, ginkgo.NodeTimeout(10*time.Second))

	ginkgo.It("lets a follower accept TimeoutNow", func(ginkgo.SpecContext) {
		nodes, _, cleanup, err := startRaftIntegrationCluster("n1", "n2", "n3")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer cleanup()

		n1, n2 := nodes[0], nodes[1]
		gomega.Expect(waitFor(2*time.Second, n1.IsLeader)).To(gomega.Succeed(), "n1 must be leader")

		reply := n2.HandleTimeoutNow(&TimeoutNowArgs{
			Term:   n1.Term(),
			Leader: "n1",
		})
		gomega.Expect(reply.Success).To(gomega.BeTrue(), "n2 must accept TimeoutNow when it is a Follower")
	}, ginkgo.NodeTimeout(5*time.Second))

	ginkgo.It("rejects stale-term TimeoutNow without making the node leader", func(ginkgo.SpecContext) {
		nodes, _, cleanup, err := startRaftIntegrationCluster("n1", "n2", "n3")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer cleanup()

		n1, n2 := nodes[0], nodes[1]
		gomega.Expect(waitFor(2*time.Second, n1.IsLeader)).To(gomega.Succeed(), "n1 must be leader")
		gomega.Expect(waitFor(2*time.Second, func() bool {
			return n2.Term() > 0
		})).To(gomega.Succeed(), "n2 must observe an election term")

		currentTerm := n2.Term()
		gomega.Expect(currentTerm).To(gomega.BeNumerically(">", 0), "follower must have a term after election")

		staleTerm := currentTerm - 1
		if staleTerm == 0 {
			n2.HandleRequestVote(&RequestVoteArgs{
				Term:         currentTerm + 5,
				CandidateID:  "intruder",
				LastLogIndex: n2.CommittedIndex(),
				LastLogTerm:  currentTerm + 5,
			})
			currentTerm = n2.Term()
			staleTerm = currentTerm - 1
		}

		reply := n2.HandleTimeoutNow(&TimeoutNowArgs{
			Term:   staleTerm,
			Leader: "stale-leader",
		})
		gomega.Expect(reply.Success).To(gomega.BeFalse(), "stale-term TimeoutNow must be rejected")
		gomega.Expect(reply.Term).To(gomega.BeNumerically(">=", currentTerm), "reply must carry n2's currentTerm")
		gomega.Expect(n2.IsLeader()).To(gomega.BeFalse(), "n2 must not become leader from a stale TimeoutNow")
	}, ginkgo.NodeTimeout(5*time.Second))

	ginkgo.It("has leaders ignore TimeoutNow", func(ginkgo.SpecContext) {
		nodes, _, cleanup, err := startRaftIntegrationCluster("n1", "n2", "n3")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer cleanup()

		leader := nodes[0]
		gomega.Expect(waitFor(2*time.Second, leader.IsLeader)).To(gomega.Succeed(), "n1 must be leader")

		reply := leader.HandleTimeoutNow(&TimeoutNowArgs{
			Term:   leader.Term(),
			Leader: "n2",
		})
		gomega.Expect(reply.Success).To(gomega.BeFalse(), "leader must ignore TimeoutNow")
	}, ginkgo.NodeTimeout(5*time.Second))

	ginkgo.It("targets the peer with the highest match index", func(ginkgo.SpecContext) {
		nodes, net, cleanup, err := startRaftIntegrationCluster("n1", "n2", "n3")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer cleanup()

		n1, n3 := nodes[0], nodes[2]
		gomega.Expect(waitFor(2*time.Second, n1.IsLeader)).To(gomega.Succeed(), "n1 must be leader")

		net.mu.Lock()
		delete(net.nodes, "n3")
		net.mu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		for i := 0; i < 5; i++ {
			_, err := n1.ProposeWait(ctx, []byte("entry"))
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "proposal %d must succeed with n1+n2 quorum", i)
		}

		net.mu.Lock()
		net.nodes["n3"] = n3
		net.mu.Unlock()

		rec := &timeoutNowRecorder{inner: net.Register("n1", n1)}
		n1.SetTransport(rec)

		gomega.Expect(n1.TransferLeadership()).To(gomega.Succeed(), "TransferLeadership must succeed")
		gomega.Expect(n1.IsLeader()).To(gomega.BeFalse(), "n1 must have stepped down")
		gomega.Expect(waitFor(2*time.Second, func() bool {
			return rec.target.Load() != nil
		})).To(gomega.Succeed(), "TimeoutNow must have been sent to some peer")
		gomega.Expect(*rec.target.Load()).To(gomega.Equal("n2"), "n2 must receive TimeoutNow")
	}, ginkgo.NodeTimeout(10*time.Second))

	ginkgo.It("returns ErrNodeStopped for stopped nodes", func(ginkgo.SpecContext) {
		node, cleanup, err := startRaftIntegrationSingleVoter("solo")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		cleanup()

		got := node.TransferLeadership()
		gomega.Expect(errors.Is(got, ErrNodeStopped)).To(gomega.BeTrue(), "expected ErrNodeStopped, got: %v", got)
	}, ginkgo.NodeTimeout(5*time.Second))
})
