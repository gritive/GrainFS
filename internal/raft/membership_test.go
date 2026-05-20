package raft

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/stretchr/testify/require"
)

// startMembershipCluster builds a cluster with the given IDs all wired through
// a shared memNetwork. The first ID gets the fast election timeout so it
// wins term 1 deterministically. Unlike startCapturingCluster this helper
// supports any number of nodes, allows pre-Start configuration overrides
// (peers list per node), and drains ApplyCh in the background like
// startCluster.
//
// The function returns the nodes (in the same order as ids) and the shared
// memNetwork so tests can register additional nodes (the AddVoter happy
// path needs a node to exist on the network at the time the leader's
// joint AE arrives).
type membershipFixture struct {
	nodes []*Node
	net   *memNetwork
	wg    sync.WaitGroup
}

func startMembershipCluster(t *testing.T, ids []string) *membershipFixture {
	t.Helper()
	require.NotEmpty(t, ids)
	fix := &membershipFixture{net: newMemNetwork()}

	for i, id := range ids {
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
		fix.nodes = append(fix.nodes, n)
	}

	for _, n := range fix.nodes {
		n.SetTransport(fix.net.Register(n.cfg.ID, n))
	}
	for _, n := range fix.nodes {
		n.Start()
		t.Cleanup(n.Stop)
		fix.wg.Add(1)
		go func(n *Node) {
			defer fix.wg.Done()
			for range n.ApplyCh() {
			}
		}(n)
	}
	return fix
}

// addNode brings a fresh Node up on the network with the given ID. The
// Node's seed peers are the current member set (so if the leader installs
// a snapshot to it later, the seed is harmless). The Node is started and
// registered with the shared memNetwork.
func (f *membershipFixture) addNode(t *testing.T, id string, seedPeers []string, electionTimeout time.Duration) *Node {
	t.Helper()
	if electionTimeout == 0 {
		electionTimeout = slowElectionTimeout
	}
	n, err := NewNode(Config{
		ID:               id,
		Peers:            seedPeers,
		ElectionTimeout:  electionTimeout,
		HeartbeatTimeout: testHeartbeat,
	})
	require.NoError(t, err)
	n.SetTransport(f.net.Register(id, n))
	n.Start()
	t.Cleanup(n.Stop)
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		for range n.ApplyCh() {
		}
	}()
	f.nodes = append(f.nodes, n)
	return n
}

func sortedVoterIDs(c Configuration) []string {
	out := make([]string, 0, len(c.Servers))
	for _, s := range c.Servers {
		out = append(out, s.ID)
	}
	sort.Strings(out)
	return out
}

var _ = ginkgo.Describe("Membership changes", func() {
	var fix *membershipFixture
	var leader *Node

	ginkgo.BeforeEach(func() {
		var err error
		var cleanup func()
		fix, cleanup, err = startPromoteRaceCluster([]string{"n1", "n2", "n3"})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(cleanup)
		leader = fix.nodes[0]
		gomega.Expect(waitFor(2*time.Second, leader.IsLeader)).To(gomega.Succeed(), "n1 did not become leader")
	})

	ginkgo.It("adds a voter and converges configuration on all members", func(ginkgo.SpecContext) {
		_, err := addPromoteRaceNode(fix, "n4", []string{"n1", "n2", "n3"}, slowElectionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		gomega.Expect(leader.AddVoterCtx(ctx, "n4", "n4-addr")).To(gomega.Succeed())

		want := []string{"n1", "n2", "n3", "n4"}
		gomega.Expect(waitFor(3*time.Second, func() bool {
			for _, node := range fix.nodes {
				got := sortedVoterIDs(node.Configuration())
				if len(got) != len(want) {
					return false
				}
				for i, id := range want {
					if got[i] != id {
						return false
					}
				}
			}
			return true
		})).To(gomega.Succeed(), "configuration did not converge to {n1..n4}")
	}, ginkgo.NodeTimeout(10*time.Second))

	ginkgo.It("rejects adding an existing voter", func(ginkgo.SpecContext) {
		gomega.Expect(leader.AddVoter("n2", "addr")).To(gomega.HaveOccurred())
	}, ginkgo.NodeTimeout(5*time.Second))

	ginkgo.It("removes a voter and converges remaining members", func(ginkgo.SpecContext) {
		_, err := addPromoteRaceNode(fix, "n4", []string{"n1", "n2", "n3"}, slowElectionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		gomega.Expect(leader.AddVoterCtx(ctx, "n4", "n4-addr")).To(gomega.Succeed())
		gomega.Expect(leader.RemoveVoter("n3")).To(gomega.Succeed())

		want := []string{"n1", "n2", "n4"}
		gomega.Expect(waitFor(3*time.Second, func() bool {
			for _, node := range fix.nodes {
				if node.cfg.ID == "n3" {
					continue
				}
				got := sortedVoterIDs(node.Configuration())
				if len(got) != len(want) {
					return false
				}
				for i, id := range want {
					if got[i] != id {
						return false
					}
				}
			}
			return true
		})).To(gomega.Succeed(), "configuration did not converge to {n1, n2, n4}")
	}, ginkgo.NodeTimeout(10*time.Second))

	ginkgo.It("rejects concurrent AddVoter while one conf change is in flight", func(ginkgo.SpecContext) {
		_, err := addPromoteRaceNode(fix, "n4", []string{"n1", "n2", "n3"}, slowElectionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_, err = addPromoteRaceNode(fix, "n5", []string{"n1", "n2", "n3"}, slowElectionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		gomega.Eventually(func() bool {
			errCh := make(chan error, 2)
			go func() { errCh <- leader.AddVoter("n4", "addr") }()
			go func() { errCh <- leader.AddVoter("n5", "addr") }()
			var sawInFlight bool
			for i := 0; i < 2; i++ {
				err := <-errCh
				if err == ErrConfChangeInFlight {
					sawInFlight = true
				}
			}
			_ = leader.RemoveVoter("n4")
			_ = leader.RemoveVoter("n5")
			return sawInFlight
		}, 5*time.Second, 50*time.Millisecond).Should(gomega.BeTrue(),
			"expected at least one concurrent AddVoter to see ErrConfChangeInFlight")
	}, ginkgo.NodeTimeout(10*time.Second))

	ginkgo.It("steps down a leader after removing itself", func(ginkgo.SpecContext) {
		gomega.Expect(leader.RemoveVoter("n1")).To(gomega.Succeed())

		gomega.Expect(waitFor(3*time.Second, func() bool {
			return !leader.IsLeader() && leader.State() == Follower
		})).To(gomega.Succeed(), "self-removed leader did not step down")

		gomega.Expect(waitFor(5*time.Second, func() bool {
			return fix.nodes[1].IsLeader() || fix.nodes[2].IsLeader()
		})).To(gomega.Succeed(), "no successor leader after self-removal")
	}, ginkgo.NodeTimeout(10*time.Second))
})

// TestMembership_TruncateAfterRevertsConfig: a follower with a config entry
// in its log gets that suffix truncated by a conflicting AE; effective
// config must revert to whatever was active just before the dropped
// config entry. Direct unit test on truncateAndRevertConfig.
func TestMembership_TruncateAfterRevertsConfig(t *testing.T) {
	n, err := NewNode(Config{ID: "n1", Peers: []string{"n2", "n3"}})
	require.NoError(t, err)
	// Seed the log with a normal entry, then a joint entry, then a final
	// entry. currentConfig should track to the final state.
	require.NoError(t, n.st.log.Append([]LogEntry{{Term: 1, Index: 1, Type: LogEntryNoOp}}))
	n.appendAndTrackConfig([]LogEntry{{
		Term: 1, Index: 2, Type: LogEntryJointConfChange,
		Command: encodeJointConfChange([]string{"n1", "n2", "n3"}, []string{"n1", "n2", "n3", "n4"}),
	}})
	require.True(t, n.st.currentConfig.joint)
	n.appendAndTrackConfig([]LogEntry{{
		Term: 1, Index: 3, Type: LogEntryConfChange,
		Command: encodeJointExitConfChange([]string{"n1", "n2", "n3", "n4"}, nil),
	}})
	require.False(t, n.st.currentConfig.joint)
	require.Equal(t, []string{"n1", "n2", "n3", "n4"}, n.st.currentConfig.voters)
	require.Equal(t, uint64(3), n.st.appendedConfigIndex)
	require.Len(t, n.st.configHistory, 2)

	// Truncate past index 1 — drops both config entries; revert to the
	// pre-history config (the original {n1,n2,n3}).
	n.truncateAndRevertConfig(1)
	require.False(t, n.st.currentConfig.joint)
	require.Equal(t, []string{"n1", "n2", "n3"}, n.st.currentConfig.voters)
	require.Equal(t, uint64(0), n.st.appendedConfigIndex)
	require.Empty(t, n.st.configHistory)
	require.Equal(t, uint64(1), n.st.log.LastIndex())
}

// TestMembership_TruncateRevertsToJoint: truncate past only the FINAL entry
// (idx 2) so the joint entry survives. Effective config must end up in
// joint state.
func TestMembership_TruncateRevertsToJoint(t *testing.T) {
	n, err := NewNode(Config{ID: "n1", Peers: []string{"n2", "n3"}})
	require.NoError(t, err)
	require.NoError(t, n.st.log.Append([]LogEntry{{Term: 1, Index: 1, Type: LogEntryNoOp}}))
	n.appendAndTrackConfig([]LogEntry{{
		Term: 1, Index: 2, Type: LogEntryJointConfChange,
		Command: encodeJointConfChange([]string{"n1", "n2", "n3"}, []string{"n1", "n2", "n3", "n4"}),
	}})
	n.appendAndTrackConfig([]LogEntry{{
		Term: 1, Index: 3, Type: LogEntryConfChange,
		Command: encodeJointExitConfChange([]string{"n1", "n2", "n3", "n4"}, nil),
	}})

	n.truncateAndRevertConfig(2) // drops idx 3 only
	require.True(t, n.st.currentConfig.joint, "after truncating final, must be back in joint state")
	require.Equal(t, []string{"n1", "n2", "n3", "n4"}, n.st.currentConfig.voters)
	require.Equal(t, []string{"n1", "n2", "n3"}, n.st.currentConfig.oldVoters)
	require.Equal(t, uint64(2), n.st.appendedConfigIndex)
	require.Len(t, n.st.configHistory, 1)
}

// TestMembership_InstallSnapshotResetsConfig: a follower whose live config
// is in the joint state receives an InstallSnapshot RPC carrying a
// post-joint Cnew. Its effective config must reset to the snapshot's
// voter set (non-joint).
func TestMembership_InstallSnapshotResetsConfig(t *testing.T) {
	n, err := NewNode(Config{ID: "follower", Peers: []string{"leader"}})
	require.NoError(t, err)

	// Force the follower into a joint state via direct state manipulation
	// (pre-Start so the actor goroutine is not running). Append a joint
	// entry to the log and update tracking.
	require.NoError(t, n.st.log.Append([]LogEntry{{Term: 1, Index: 1, Type: LogEntryNoOp}}))
	n.appendAndTrackConfig([]LogEntry{{
		Term: 1, Index: 2, Type: LogEntryJointConfChange,
		Command: encodeJointConfChange([]string{"leader", "follower"}, []string{"leader", "follower", "newcomer"}),
	}})
	require.True(t, n.st.currentConfig.joint)
	require.Equal(t, uint64(2), n.st.appendedConfigIndex)

	n.Start()
	t.Cleanup(n.Stop)
	go func() {
		for range n.ApplyCh() {
		}
	}()

	// Inject a snapshot whose Configuration is post-joint Cnew.
	reply := n.HandleInstallSnapshot(&InstallSnapshotArgs{
		Term:              1,
		LeaderID:          "leader",
		LastIncludedIndex: 5,
		LastIncludedTerm:  1,
		Configuration:     []string{"leader", "follower", "newcomer"},
		Data:              []byte("snap"),
	})
	require.NotNil(t, reply)

	// Configuration must reset to the snapshot's voter set.
	require.NoError(t, waitFor(time.Second, func() bool {
		got := sortedVoterIDs(n.Configuration())
		return len(got) == 3 && got[0] == "follower" && got[1] == "leader" && got[2] == "newcomer"
	}), "Configuration did not reset to snapshot voters")

	rs := n.rs.Load()
	require.False(t, rs.config.joint, "must exit joint state after snapshot install")
}

// TestMembership_ColdOnlyVoterCanElect: per Raft §4.3, a server in Cold but
// not in Cnew is still a legitimate voter during the joint period — "any
// server from either configuration may serve as leader." Concrete scenario:
// 2→1 shrink (Cold={a,b}, Cnew={b}). If b is partitioned mid-joint, a must
// be allowed to call an election to drive the joint forward; the
// onElectionTimeout guard must consult Cold ∪ Cnew, not Cnew alone.
func TestMembership_ColdOnlyVoterCanElect(t *testing.T) {
	n, err := NewNode(Config{ID: "a", Peers: []string{"b"}})
	require.NoError(t, err)
	// Drive the node into a joint state with Cold={a,b} and Cnew={b}.
	// Pre-Start state manipulation, mirroring TestMembership_TruncateAfterRevertsConfig.
	require.NoError(t, n.st.log.Append([]LogEntry{{Term: 1, Index: 1, Type: LogEntryNoOp}}))
	n.appendAndTrackConfig([]LogEntry{{
		Term: 1, Index: 2, Type: LogEntryJointConfChange,
		Command: encodeJointConfChange([]string{"a", "b"}, []string{"b"}),
	}})
	require.True(t, n.st.currentConfig.joint)
	require.False(t, n.st.currentConfig.containsVoter("a"), "a is in Cold but not Cnew")

	startTerm := n.st.currentTerm
	require.Equal(t, Follower, n.st.state)

	// onElectionTimeout must NOT short-circuit a Cold-only voter — it must
	// transition to Candidate and bump the term so the joint can make
	// progress. Pre-Start: transport is nil so RV broadcasts no-op safely;
	// stable store is in-memory so persistHardState is non-blocking.
	n.onElectionTimeout()

	require.Equal(t, Candidate, n.st.state, "Cold-only voter must be allowed to become Candidate during joint state")
	require.Equal(t, startTerm+1, n.st.currentTerm, "becomeCandidate must bump term")
}
