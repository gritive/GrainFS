package raft

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// TestPromoteToVoter_BroadcastsHeartbeatOnCnewCommit pins the M6.0
// follow-up race: a learner elevated to voter via Path B's two-entry
// sequence (stage-1 drop-from-learners, then Cjoint AddVoter) becomes a
// voter the moment it appends Cjoint. The follower-side fix
// (membership.go appendAndTrackConfig — resetElectionTimer on self
// learner→voter transition) gives the new voter a fresh randomized
// election window starting at the suffrage flip, so the leader's next
// heartbeat arrives before the timer fires.
//
// Delay regime: aeDelay is the per-RPC one-way latency injected on
// every transport call. The fix closes the race within the protocol's
// design envelope (aeDelay roughly bounded by ET — heartbeat << ET).
// Beyond that envelope the race is structural: with peerInFlight
// single-flight gating, an in-flight Cjoint AE blocks the next
// heartbeat until reply, so the next AE arrives at n2 ~2·aeDelay after
// the Cjoint arrival. When 2·aeDelay > 2·ET (== max election timer
// draw), no plain timer reset can close it without extending the
// post-promotion grace window — a protocol change beyond this fix.
//
// The chosen matrix exercises the envelope this fix targets. Production
// e2e (election 750–1500ms, RTT 50–200ms) sits comfortably in this
// regime.
func TestPromoteToVoter_BroadcastsHeartbeatOnCnewCommit(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "Raft learner promotion integration race")
}

const (
	promoteRaceIterations = 50
	promoteRaceWorkers    = 8
)

var _ = ginkgo.Describe("TestPromoteToVoter_BroadcastsHeartbeatOnCnewCommit", func() {
	delays := []time.Duration{
		10 * time.Millisecond,
		20 * time.Millisecond,
		30 * time.Millisecond,
	}

	for _, delay := range delays {
		delay := delay
		ginkgo.It(fmt.Sprintf("keeps leader stable with delay=%s", delay), func(ginkgo.SpecContext) {
			err := runPromoteRaceIterations(delay)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}, ginkgo.NodeTimeout(90*time.Second))
	}
})

type promoteRaceResult struct {
	delay time.Duration
	iter  int
	err   error
}

func runPromoteRaceIterations(delay time.Duration) error {
	jobs := make(chan int)
	results := make(chan promoteRaceResult, promoteRaceIterations)

	var wg sync.WaitGroup
	for worker := 0; worker < promoteRaceWorkers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for iter := range jobs {
				results <- promoteRaceResult{
					delay: delay,
					iter:  iter,
					err:   checkPromoteRaceWithDelay(delay),
				}
			}
		}()
	}

	for iter := 0; iter < promoteRaceIterations; iter++ {
		jobs <- iter
	}
	close(jobs)
	wg.Wait()
	close(results)

	var failures []string
	for result := range results {
		if result.err != nil {
			failures = append(failures, fmt.Sprintf("delay=%s iter=%d: %v", result.delay, result.iter, result.err))
		}
	}
	if len(failures) > 0 {
		return errors.New(strings.Join(failures, "\n"))
	}
	return nil
}

func checkPromoteRaceWithDelay(aeDelay time.Duration) error {
	fix, cleanup, err := startPromoteRaceCluster([]string{"n1"})
	if err != nil {
		return err
	}
	defer cleanup()

	leader := fix.nodes[0]
	if err := waitFor(2*time.Second, func() bool { return leader.IsLeader() }); err != nil {
		return fmt.Errorf("n1 must bootstrap as leader: %w", err)
	}
	leaderTermBefore := leader.Term()

	n2, err := addPromoteRaceNode(fix, "n2", []string{"n1"}, fastElectionTimeout)
	if err != nil {
		return err
	}

	// Wrap transports so we can flip latency on AFTER the AddLearner
	// catchup phase, isolating the delay to the PromoteToVoter dance.
	leaderDelay := newDelayTransport(leader.loadTransport(), aeDelay)
	n2Delay := newDelayTransport(n2.loadTransport(), aeDelay)
	leader.SetTransport(leaderDelay)
	n2.SetTransport(n2Delay)

	if err := leader.AddLearner("n2", "n2-addr"); err != nil {
		return fmt.Errorf("AddLearner n2: %w", err)
	}
	if err := waitFor(3*time.Second, func() bool {
		return leader.peerMatchIndexForTest("n2") >= leader.CommittedIndex()
	}); err != nil {
		return fmt.Errorf("n2 must catch up to leader commit before promote: %w", err)
	}

	leaderDelay.enable()
	n2Delay.enable()
	defer func() {
		leaderDelay.disable()
		n2Delay.disable()
	}()

	if err := leader.PromoteToVoter("n2"); err != nil {
		return fmt.Errorf("PromoteToVoter must not fail (Cnew commit election race): %w", err)
	}
	if !leader.IsLeader() {
		return fmt.Errorf("n1 must remain leader after promote; state=%v term=%d", leader.State(), leader.Term())
	}
	if got := leader.Term(); got != leaderTermBefore {
		return fmt.Errorf("leader term advanced after promote: before=%d after=%d", leaderTermBefore, got)
	}

	cfg := leader.Configuration()
	if len(cfg.Servers) != 2 {
		return fmt.Errorf("post-promote server count: got=%d want=2", len(cfg.Servers))
	}
	for _, server := range cfg.Servers {
		if server.Suffrage != Voter {
			return fmt.Errorf("server %s suffrage after promote: got=%v want=%v", server.ID, server.Suffrage, Voter)
		}
	}
	return nil
}

func startPromoteRaceCluster(ids []string) (*membershipFixture, func(), error) {
	if len(ids) == 0 {
		return nil, nil, errors.New("startPromoteRaceCluster: ids must not be empty")
	}

	fix := &membershipFixture{net: newMemNetwork()}
	for i, id := range ids {
		peers := make([]string, 0, len(ids)-1)
		for _, peer := range ids {
			if peer != id {
				peers = append(peers, peer)
			}
		}

		electionTimeout := slowElectionTimeout
		if i == 0 {
			electionTimeout = fastElectionTimeout
		}
		node, err := NewNode(Config{
			ID:               id,
			Peers:            peers,
			ElectionTimeout:  electionTimeout,
			HeartbeatTimeout: testHeartbeat,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("NewNode %s: %w", id, err)
		}
		fix.nodes = append(fix.nodes, node)
	}

	for _, node := range fix.nodes {
		node.SetTransport(fix.net.Register(node.cfg.ID, node))
	}
	for _, node := range fix.nodes {
		startPromoteRaceNode(fix, node)
	}

	cleanup := func() {
		for i := len(fix.nodes) - 1; i >= 0; i-- {
			fix.nodes[i].Stop()
		}
		fix.wg.Wait()
	}
	return fix, cleanup, nil
}

func addPromoteRaceNode(fix *membershipFixture, id string, seedPeers []string, electionTimeout time.Duration) (*Node, error) {
	if electionTimeout == 0 {
		electionTimeout = slowElectionTimeout
	}
	node, err := NewNode(Config{
		ID:               id,
		Peers:            seedPeers,
		ElectionTimeout:  electionTimeout,
		HeartbeatTimeout: testHeartbeat,
	})
	if err != nil {
		return nil, fmt.Errorf("NewNode %s: %w", id, err)
	}

	node.SetTransport(fix.net.Register(id, node))
	fix.nodes = append(fix.nodes, node)
	startPromoteRaceNode(fix, node)
	return node, nil
}

func startPromoteRaceNode(fix *membershipFixture, node *Node) {
	node.Start()
	fix.wg.Add(1)
	go func() {
		defer fix.wg.Done()
		for range node.ApplyCh() {
		}
	}()
}

// delayTransport wraps a Transport and (when armed) sleeps for delay
// before forwarding each RPC. The arm/disarm flag is atomic so test
// code can flip it between phases without restarting the cluster.
type delayTransport struct {
	inner Transport
	delay time.Duration
	armed atomic.Bool
}

func newDelayTransport(inner Transport, delay time.Duration) *delayTransport {
	return &delayTransport{inner: inner, delay: delay}
}

func (d *delayTransport) enable()  { d.armed.Store(true) }
func (d *delayTransport) disable() { d.armed.Store(false) }

func (d *delayTransport) sleep() {
	if d.armed.Load() && d.delay > 0 {
		time.Sleep(d.delay)
	}
}

func (d *delayTransport) SendRequestVote(peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	d.sleep()
	return d.inner.SendRequestVote(peer, args)
}

func (d *delayTransport) SendAppendEntries(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	d.sleep()
	return d.inner.SendAppendEntries(peer, args)
}

func (d *delayTransport) SendInstallSnapshot(peer string, args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	d.sleep()
	return d.inner.SendInstallSnapshot(peer, args)
}

func (d *delayTransport) SendTimeoutNow(peer string, args *TimeoutNowArgs) (*TimeoutNowReply, error) {
	d.sleep()
	return d.inner.SendTimeoutNow(peer, args)
}
