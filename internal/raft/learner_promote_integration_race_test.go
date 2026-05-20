package raft

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
	const iterations = 50
	delays := []time.Duration{
		10 * time.Millisecond,
		20 * time.Millisecond,
		30 * time.Millisecond,
	}
	for _, d := range delays {
		d := d
		t.Run(fmt.Sprintf("delay=%s", d), func(t *testing.T) {
			for i := 0; i < iterations; i++ {
				i := i
				t.Run(fmt.Sprintf("iter%d", i), func(t *testing.T) {
					t.Parallel()
					runPromoteRaceWithDelay(t, d)
				})
			}
		})
	}
}

func runPromoteRaceWithDelay(t *testing.T, aeDelay time.Duration) {
	t.Helper()

	fix := startMembershipCluster(t, []string{"n1"})
	leader := fix.nodes[0]
	require.NoError(t, waitFor(2*time.Second, func() bool { return leader.IsLeader() }),
		"n1 must bootstrap as leader")
	leaderTermBefore := leader.Term()

	n2 := fix.addNode(t, "n2", []string{"n1"}, fastElectionTimeout)

	// Wrap transports so we can flip latency on AFTER the AddLearner
	// catchup phase, isolating the delay to the PromoteToVoter dance.
	leaderDelay := newDelayTransport(leader.loadTransport(), aeDelay)
	n2Delay := newDelayTransport(n2.loadTransport(), aeDelay)
	leader.SetTransport(leaderDelay)
	n2.SetTransport(n2Delay)

	require.NoError(t, leader.AddLearner("n2", "n2-addr"))
	require.NoError(t, waitFor(3*time.Second, func() bool {
		return leader.peerMatchIndexForTest("n2") >= leader.CommittedIndex()
	}), "n2 must catch up to leader commit before promote")

	leaderDelay.enable()
	n2Delay.enable()
	defer func() {
		leaderDelay.disable()
		n2Delay.disable()
	}()

	err := leader.PromoteToVoter("n2")
	require.NoError(t, err,
		"PromoteToVoter must not fail (Cnew commit election race)")
	require.True(t, leader.IsLeader(),
		"n1 must remain leader after promote — stepdown indicates the race fired")
	require.Equal(t, leaderTermBefore, leader.Term(),
		"leader term must not advance — stepdown+re-election would bump it")

	cfg := leader.Configuration()
	require.Len(t, cfg.Servers, 2)
	for _, s := range cfg.Servers {
		require.Equal(t, Voter, s.Suffrage, "all servers must be Voter post-promote")
	}
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
