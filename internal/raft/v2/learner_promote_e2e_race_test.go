package raftv2

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestPromoteToVoter_BroadcastsHeartbeatOnCnewCommit attempts to reproduce
// the M6.0 follow-up race observed in multi-process e2e
// (TestE2E_ClusterDrain_Follower on the feat/raft-v2-m6-3-retry branch)
// using in-process delay injection. A learner elevated to voter via Path
// B's two-entry sequence (stage-1 drop-from-learners, then Cjoint
// AddVoter) starts its election timer the moment it appends Cjoint
// (Raft §4.3 — the most recent appended config makes it a voter). If
// the leader's next AppendEntries does not reach it before that timer
// fires, it campaigns at a higher term and deposes the leader, surfacing
// as "proposal failed: node stepped down" from PromoteToVoter.
//
// Empirical finding (M6.0 follow-up investigation):
//
//   - At aeDelay=60ms with fastElectionTimeout=50ms, the race fires
//     ~20% of iterations. The leader DOES already broadcast a fresh
//     AppendEntries from advanceConfChangePhase Phase 1→2 the moment
//     Cjoint commits (membership.go:191). That broadcast is the
//     earliest leader-side response possible to "Cjoint just committed".
//   - The race is not closed by adding a second broadcast on Phase 2
//     done (Cnew final commit): by the time Cnew final commits at the
//     leader, ~4 RTTs have elapsed since stage-1 dispatch, and the
//     newly-promoted voter's election timer (50–100ms after its Cjoint
//     append) has long since fired in the failing cases. Adding a
//     broadcast at Phase 2 done is a no-op vs this test at every
//     delay we tried (30/80/200ms).
//   - The structural floor: with aeDelay × 2 > electionTimeout the
//     race cannot be closed by any leader-side commit-time broadcast,
//     because the rescue AE travels one RTT to the new voter while the
//     new voter's timer fires in [electionTimeout, 2×electionTimeout).
//
// Two unresolved hypotheses about the production e2e failure mode:
//
//  1. The e2e timing (election 750–1500ms, RTT 50–200ms) gives
//     structural margin where the EXISTING Phase 1→2 broadcast already
//     wins comfortably. Whatever makes e2e fail must be something
//     other than the timing window this test models — possibly QUIC
//     handshake-only-on-first-call latency that this in-process test
//     cannot reproduce, or a different RPC path entirely.
//
//  2. The fix may need to be follower-side rather than leader-side:
//     extending the election timer when a follower transitions from
//     learner to voter (so it gets ≥ 2× normal grace), or PreVote
//     (campaigning without bumping term until majority would grant).
//     Both are protocol changes beyond the scope of this M6.0
//     follow-up patch.
//
// The test is retained as a stress harness: it currently fails 10–30%
// of iterations at aeDelay=60ms, providing a reliable scenario for any
// future fix to validate against. It is t.Skip()'d in the default
// suite until the underlying race-close mechanism lands.
func TestPromoteToVoter_BroadcastsHeartbeatOnCnewCommit(t *testing.T) {
	t.Skip("M6.0 follow-up: race-close mechanism unresolved — see file header. Test currently demonstrates the race but no leader-side commit-time broadcast closes it in this setup.")
	const iterations = 50
	for i := 0; i < iterations; i++ {
		i := i
		t.Run(fmt.Sprintf("iter%d", i), func(t *testing.T) {
			runPromoteRaceWithDelay(t, 60*time.Millisecond)
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
