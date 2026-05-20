package raft

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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

var _ = Describe("membership", func() {
	It("grows a 3-voter cluster to 4 voters", func() {
		fix := startMembershipClusterGinkgo([]string{"n1", "n2", "n3"})
		leader := fix.nodes[0]

		Expect(waitFor(2*time.Second, func() bool { return leader.IsLeader() })).To(Succeed(),
			"n1 did not become leader")

		// Bring n4 up before AddVoter so it can receive the joint entry.
		// Its seed Peers must reference the current cluster (so the followers
		// lookup table sees the same network handle), but currentConfig will
		// be overwritten by the joint entry the leader sends.
		fix.addNodeGinkgo("n4", []string{"n1", "n2", "n3"}, slowElectionTimeout)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		DeferCleanup(cancel)
		Expect(leader.AddVoterCtx(ctx, "n4", "n4-addr")).To(Succeed())

		// Configuration on every member must include n4 — and only n4 — added.
		want := []string{"n1", "n2", "n3", "n4"}
		Expect(waitFor(3*time.Second, func() bool {
			for _, n := range fix.nodes {
				got := sortedVoterIDs(n.Configuration())
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
		})).To(Succeed(), "configuration did not converge to {n1..n4}")
	})

	It("rejects adding a duplicate voter", func() {
		fix := startMembershipClusterGinkgo([]string{"n1", "n2", "n3"})
		leader := fix.nodes[0]
		Expect(waitFor(2*time.Second, func() bool { return leader.IsLeader() })).To(Succeed())

		err := leader.AddVoter("n2", "addr")
		Expect(err).To(HaveOccurred())
	})

	It("shrinks a 4-voter cluster to 3 voters", func() {
		fix := startMembershipClusterGinkgo([]string{"n1", "n2", "n3"})
		leader := fix.nodes[0]
		Expect(waitFor(2*time.Second, func() bool { return leader.IsLeader() })).To(Succeed())
		fix.addNodeGinkgo("n4", []string{"n1", "n2", "n3"}, slowElectionTimeout)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		DeferCleanup(cancel)
		Expect(leader.AddVoterCtx(ctx, "n4", "n4-addr")).To(Succeed())

		// Now remove n3.
		Expect(leader.RemoveVoter("n3")).To(Succeed())

		want := []string{"n1", "n2", "n4"}
		Expect(waitFor(3*time.Second, func() bool {
			// Skip n3 — it's been ejected and may eventually time out / step
			// back to follower; the remaining live members must converge.
			for _, n := range fix.nodes {
				if n.cfg.ID == "n3" {
					continue
				}
				got := sortedVoterIDs(n.Configuration())
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
		})).To(Succeed(), "configuration did not converge to {n1, n2, n4}")
	})

	It("rejects concurrent AddVoter calls", func() {
		fix := startMembershipClusterGinkgo([]string{"n1", "n2", "n3"})
		leader := fix.nodes[0]
		Expect(waitFor(2*time.Second, func() bool { return leader.IsLeader() })).To(Succeed())
		fix.addNodeGinkgo("n4", []string{"n1", "n2", "n3"}, slowElectionTimeout)
		fix.addNodeGinkgo("n5", []string{"n1", "n2", "n3"}, slowElectionTimeout)

		// Spin up a goroutine for the first change; the second should see
		// ErrConfChangeInFlight if it lands while the first is still mid-flight.
		// Because the joint commit is fast (memNetwork), we issue them as fast
		// as possible and accept that occasionally the second succeeds — but
		// at least ONE of the next-voter slots must produce ErrConfChangeInFlight
		// for this assertion to be meaningful. We retry until we observe it.
		Eventually(func() bool {
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
			// If the AddVoters are now both committed, reset for the next try.
			// (Idempotent — RemoveVoter for n4/n5 if they made it in, ignore
			// errors.)
			_ = leader.RemoveVoter("n4")
			_ = leader.RemoveVoter("n5")
			return sawInFlight
		}).WithTimeout(5*time.Second).WithPolling(50*time.Millisecond).Should(BeTrue(),
			"expected at least one concurrent AddVoter to see ErrConfChangeInFlight")
	})

	It("reverts config when truncating after config entries", func() {
		n, err := NewNode(Config{ID: "n1", Peers: []string{"n2", "n3"}})
		Expect(err).NotTo(HaveOccurred())
		// Seed the log with a normal entry, then a joint entry, then a final
		// entry. currentConfig should track to the final state.
		Expect(n.st.log.Append([]LogEntry{{Term: 1, Index: 1, Type: LogEntryNoOp}})).To(Succeed())
		n.appendAndTrackConfig([]LogEntry{{
			Term: 1, Index: 2, Type: LogEntryJointConfChange,
			Command: encodeJointConfChange([]string{"n1", "n2", "n3"}, []string{"n1", "n2", "n3", "n4"}),
		}})
		Expect(n.st.currentConfig.joint).To(BeTrue())
		n.appendAndTrackConfig([]LogEntry{{
			Term: 1, Index: 3, Type: LogEntryConfChange,
			Command: encodeJointExitConfChange([]string{"n1", "n2", "n3", "n4"}, nil),
		}})
		Expect(n.st.currentConfig.joint).To(BeFalse())
		Expect(n.st.currentConfig.voters).To(Equal([]string{"n1", "n2", "n3", "n4"}))
		Expect(n.st.appendedConfigIndex).To(Equal(uint64(3)))
		Expect(n.st.configHistory).To(HaveLen(2))

		// Truncate past index 1 — drops both config entries; revert to the
		// pre-history config (the original {n1,n2,n3}).
		n.truncateAndRevertConfig(1)
		Expect(n.st.currentConfig.joint).To(BeFalse())
		Expect(n.st.currentConfig.voters).To(Equal([]string{"n1", "n2", "n3"}))
		Expect(n.st.appendedConfigIndex).To(Equal(uint64(0)))
		Expect(n.st.configHistory).To(BeEmpty())
		Expect(n.st.log.LastIndex()).To(Equal(uint64(1)))
	})

	It("reverts to joint config when truncating only the final entry", func() {
		n, err := NewNode(Config{ID: "n1", Peers: []string{"n2", "n3"}})
		Expect(err).NotTo(HaveOccurred())
		Expect(n.st.log.Append([]LogEntry{{Term: 1, Index: 1, Type: LogEntryNoOp}})).To(Succeed())
		n.appendAndTrackConfig([]LogEntry{{
			Term: 1, Index: 2, Type: LogEntryJointConfChange,
			Command: encodeJointConfChange([]string{"n1", "n2", "n3"}, []string{"n1", "n2", "n3", "n4"}),
		}})
		n.appendAndTrackConfig([]LogEntry{{
			Term: 1, Index: 3, Type: LogEntryConfChange,
			Command: encodeJointExitConfChange([]string{"n1", "n2", "n3", "n4"}, nil),
		}})

		n.truncateAndRevertConfig(2) // drops idx 3 only
		Expect(n.st.currentConfig.joint).To(BeTrue(), "after truncating final, must be back in joint state")
		Expect(n.st.currentConfig.voters).To(Equal([]string{"n1", "n2", "n3", "n4"}))
		Expect(n.st.currentConfig.oldVoters).To(Equal([]string{"n1", "n2", "n3"}))
		Expect(n.st.appendedConfigIndex).To(Equal(uint64(2)))
		Expect(n.st.configHistory).To(HaveLen(1))
	})

	It("resets config from an installed snapshot", func() {
		n, err := NewNode(Config{ID: "follower", Peers: []string{"leader"}})
		Expect(err).NotTo(HaveOccurred())

		// Force the follower into a joint state via direct state manipulation
		// (pre-Start so the actor goroutine is not running). Append a joint
		// entry to the log and update tracking.
		Expect(n.st.log.Append([]LogEntry{{Term: 1, Index: 1, Type: LogEntryNoOp}})).To(Succeed())
		n.appendAndTrackConfig([]LogEntry{{
			Term: 1, Index: 2, Type: LogEntryJointConfChange,
			Command: encodeJointConfChange([]string{"leader", "follower"}, []string{"leader", "follower", "newcomer"}),
		}})
		Expect(n.st.currentConfig.joint).To(BeTrue())
		Expect(n.st.appendedConfigIndex).To(Equal(uint64(2)))

		n.Start()
		DeferCleanup(n.Stop)
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
		Expect(reply).NotTo(BeNil())

		// Configuration must reset to the snapshot's voter set.
		Expect(waitFor(time.Second, func() bool {
			got := sortedVoterIDs(n.Configuration())
			return len(got) == 3 && got[0] == "follower" && got[1] == "leader" && got[2] == "newcomer"
		})).To(Succeed(), "Configuration did not reset to snapshot voters")

		rs := n.rs.Load()
		Expect(rs.config.joint).To(BeFalse(), "must exit joint state after snapshot install")
	})

	It("steps down when the leader removes itself", func() {
		fix := startMembershipClusterGinkgo([]string{"n1", "n2", "n3"})
		leader := fix.nodes[0]
		Expect(waitFor(2*time.Second, func() bool { return leader.IsLeader() })).To(Succeed())

		Expect(leader.RemoveVoter("n1")).To(Succeed())

		// Leader steps down once Cnew (excluding n1) commits.
		Expect(waitFor(3*time.Second, func() bool {
			return !leader.IsLeader() && leader.State() == Follower
		})).To(Succeed(), "self-removed leader did not step down")

		// One of the remaining voters must take over (election may take a
		// few hundred milliseconds because n2 and n3 use slowElectionTimeout).
		Expect(waitFor(5*time.Second, func() bool {
			return fix.nodes[1].IsLeader() || fix.nodes[2].IsLeader()
		})).To(Succeed(), "no successor leader after self-removal")
	})

	It("allows a cold-only voter to call an election during joint config", func() {
		n, err := NewNode(Config{ID: "a", Peers: []string{"b"}})
		Expect(err).NotTo(HaveOccurred())
		// Drive the node into a joint state with Cold={a,b} and Cnew={b}.
		// Pre-Start state manipulation, mirroring TestMembership_TruncateAfterRevertsConfig.
		Expect(n.st.log.Append([]LogEntry{{Term: 1, Index: 1, Type: LogEntryNoOp}})).To(Succeed())
		n.appendAndTrackConfig([]LogEntry{{
			Term: 1, Index: 2, Type: LogEntryJointConfChange,
			Command: encodeJointConfChange([]string{"a", "b"}, []string{"b"}),
		}})
		Expect(n.st.currentConfig.joint).To(BeTrue())
		Expect(n.st.currentConfig.containsVoter("a")).To(BeFalse(), "a is in Cold but not Cnew")

		startTerm := n.st.currentTerm
		Expect(n.st.state).To(Equal(Follower))

		// onElectionTimeout must NOT short-circuit a Cold-only voter — it must
		// transition to Candidate and bump the term so the joint can make
		// progress. Pre-Start: transport is nil so RV broadcasts no-op safely;
		// stable store is in-memory so persistHardState is non-blocking.
		n.onElectionTimeout()

		Expect(n.st.state).To(Equal(Candidate), "Cold-only voter must be allowed to become Candidate during joint state")
		Expect(n.st.currentTerm).To(Equal(startTerm+1), "becomeCandidate must bump term")
	})
})
