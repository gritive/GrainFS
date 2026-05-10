package raftv2

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

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

// TestMembership_AddVoterHappyPath grows a 3-voter cluster to 4 voters and
// verifies that all members observe the new configuration after AddVoter
// returns. The added node is brought up on the network BEFORE AddVoter
// fires so the joint entry can replicate to it on the leader's first
// dispatch (the leader's nextIndex for n4 starts at lastLogIndex+1, but
// any conflict hint will pull it back to 1 — n4 is allowed to start
// empty).
func TestMembership_AddVoterHappyPath(t *testing.T) {
	fix := startMembershipCluster(t, []string{"n1", "n2", "n3"})
	leader := fix.nodes[0]

	require.NoError(t, waitFor(2*time.Second, func() bool { return leader.IsLeader() }),
		"n1 did not become leader")

	// Bring n4 up before AddVoter so it can receive the joint entry.
	// Its seed Peers must reference the current cluster (so the followers
	// lookup table sees the same network handle), but currentConfig will
	// be overwritten by the joint entry the leader sends.
	fix.addNode(t, "n4", []string{"n1", "n2", "n3"}, slowElectionTimeout)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, leader.AddVoterCtx(ctx, "n4", "n4-addr"))

	// Configuration on every member must include n4 — and only n4 — added.
	want := []string{"n1", "n2", "n3", "n4"}
	require.NoError(t, waitFor(3*time.Second, func() bool {
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
	}), "configuration did not converge to {n1..n4}")
}

// TestMembership_AddVoterRejectsDuplicate verifies the leader rejects
// adding an existing voter with a clean error rather than appending a
// no-op joint entry.
func TestMembership_AddVoterRejectsDuplicate(t *testing.T) {
	fix := startMembershipCluster(t, []string{"n1", "n2", "n3"})
	leader := fix.nodes[0]
	require.NoError(t, waitFor(2*time.Second, func() bool { return leader.IsLeader() }))

	err := leader.AddVoter("n2", "addr")
	require.Error(t, err)
}

// TestMembership_RemoveVoterHappyPath shrinks a 4-voter cluster to 3 voters.
// After RemoveVoter returns, every remaining member's Configuration drops
// the removed ID.
func TestMembership_RemoveVoterHappyPath(t *testing.T) {
	fix := startMembershipCluster(t, []string{"n1", "n2", "n3"})
	leader := fix.nodes[0]
	require.NoError(t, waitFor(2*time.Second, func() bool { return leader.IsLeader() }))
	fix.addNode(t, "n4", []string{"n1", "n2", "n3"}, slowElectionTimeout)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, leader.AddVoterCtx(ctx, "n4", "n4-addr"))

	// Now remove n3.
	require.NoError(t, leader.RemoveVoter("n3"))

	want := []string{"n1", "n2", "n4"}
	require.NoError(t, waitFor(3*time.Second, func() bool {
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
	}), "configuration did not converge to {n1, n2, n4}")
}

// TestMembership_AddVoterRejectsConcurrent: two AddVoter calls at the same
// time — only one is allowed to be in flight; the second sees
// ErrConfChangeInFlight.
func TestMembership_AddVoterRejectsConcurrent(t *testing.T) {
	fix := startMembershipCluster(t, []string{"n1", "n2", "n3"})
	leader := fix.nodes[0]
	require.NoError(t, waitFor(2*time.Second, func() bool { return leader.IsLeader() }))
	fix.addNode(t, "n4", []string{"n1", "n2", "n3"}, slowElectionTimeout)
	fix.addNode(t, "n5", []string{"n1", "n2", "n3"}, slowElectionTimeout)

	// Spin up a goroutine for the first change; the second should see
	// ErrConfChangeInFlight if it lands while the first is still mid-flight.
	// Because the joint commit is fast (memNetwork), we issue them as fast
	// as possible and accept that occasionally the second succeeds — but
	// at least ONE of the next-voter slots must produce ErrConfChangeInFlight
	// for this assertion to be meaningful. We retry until we observe it.
	require.Eventually(t, func() bool {
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
	}, 5*time.Second, 50*time.Millisecond, "expected at least one concurrent AddVoter to see ErrConfChangeInFlight")
}
