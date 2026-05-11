package raftv2

// Property tests for raft/v2 using pgregory.net/rapid.
//
// Design: rapid.Check drives a raftStateMachine that implements rapid.StateMachine.
// Each call to Check generates a sequence of commands (Propose, StepDownLeader,
// Partition, Heal); after every command rapid calls Check() which asserts all
// registered invariants. Failed sequences are shrunk automatically by rapid.
//
// Invariants (PR 17 + PR 18):
//   1. Election Safety   — at most one leader per term.
//   2. Leader Append-Only — per-node ApplyCh index sequence is strictly monotone.
//   3. Log Matching      — if two logs agree at (index, term), they agree on all prior entries.
//
// Invariants planned for later PR 18 steps (not yet present):
//   4. Leader Completeness — if entry committed in term T, all sampled leaders in T'>T have it.
//   5. State Machine Safety — all FSMs apply the same entry at each index.
//   6. Liveness          — under stable leadership a proposed entry eventually commits.
//
// Count knob: TestProperty_* uses rapid.Check default (100 sequences) for CI.
// For the full 100k-sequence hardening run, pass -rapid.checks=100000 on the CLI:
//
//	go test -race -count=1 ./internal/raft/v2/... -run TestProperty -rapid.checks=100000
//
// This matches rapid's idiomatic flag-driven scaling documented in
// https://pkg.go.dev/pgregory.net/rapid#hdr-Flags.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"pgregory.net/rapid"
)

// raftStateMachine implements rapid.StateMachine over a live 3-voter raft cluster.
// The cluster is started in newRaftStateMachine() and torn down via t.Cleanup.
// The invObserver accumulates leader and apply observations; Check() drains
// pending observations from ObsCh and then asserts both invariants.
type raftStateMachine struct {
	cluster *propertyCluster
	obs     *invObserver
}

// newRaftStateMachine creates a fresh 3-voter cluster for one rapid property run.
// t is the outer *testing.T so require.* helpers work correctly; rt is the
// per-sequence rapid.T whose Cleanup fires between sequences, ensuring every
// cluster is stopped before the next sequence begins. This bounds the goroutine
// count to one cluster at a time even under -rapid.checks=100000.
func newRaftStateMachine(t *testing.T, rt *rapid.T) *raftStateMachine {
	ids := [3]string{"a", "b", "c"}
	sm := &raftStateMachine{
		cluster: newPropertyCluster(t, ids),
		obs:     newInvObserver(),
	}
	// Use rt.Cleanup (per-sequence) not t.Cleanup (per-whole-test) so the
	// cluster is stopped between sequences rather than accumulating 100k clusters.
	rt.Cleanup(sm.cluster.Stop)
	// Wait for the initial leader. If none emerges, that is acceptable — rapid
	// will still exercise the cluster with Propose failures, which are valid.
	sm.cluster.waitForLeader(2 * time.Second)
	return sm
}

// observeCurrentLeaders samples each node and records a (term, id) pair only
// for nodes that believe themselves to be the current leader (rs.isLeader=true).
// Recording only self-claimed leadership is the precise definition of Election
// Safety: "at most one node can be Leader in any given term." Recording follower
// beliefs of who is leader would introduce false-positive violations during term
// transitions where a follower's leaderID lags behind the new term.
//
// Loads readState exactly once per node (rs.Load()) and reads isLeader+term
// from the same snapshot. Calling n.IsLeader() then n.Term() separately would
// span two Load calls; per state.go contract that is a TOCTOU and can record
// a spurious (newTerm, oldLeaderID) tuple after a step-down+term-bump race —
// directly producing false-positive Election Safety violations under sustained
// StepDownLeader actions.
func (sm *raftStateMachine) observeCurrentLeaders() {
	for _, n := range sm.cluster.Nodes {
		rs := n.rs.Load()
		if rs.isLeader {
			sm.obs.recordLeader(rs.term, rs.leaderID)
		}
	}
}

// Check is called by rapid after every action. It drains pending observations
// from ObsCh and asserts all invariants.
func (sm *raftStateMachine) Check(t *rapid.T) {
	sm.observeCurrentLeaders()
	sm.obs.drainObsCh(sm.cluster.ObsCh)

	if err := checkElectionSafety(sm.obs.leaderObs); err != nil {
		t.Fatal(err)
	}
	if err := checkLeaderAppendOnly(sm.obs.nodeApplied); err != nil {
		t.Fatal(err)
	}
	if err := checkLogMatching(sm.obs.nodeApplied); err != nil {
		t.Fatal(err)
	}
}

// Propose generates a random command and submits it to the current leader.
// If there is no leader, or if ProposeWait fails (valid during partitions /
// step-downs), the error is silently ignored — it is not a violation.
func (sm *raftStateMachine) Propose(t *rapid.T) {
	leader := sm.cluster.leader()
	if leader == nil {
		return
	}
	sm.obs.proposeCounter++
	cmd := []byte(fmt.Sprintf("cmd-%d", sm.obs.proposeCounter))
	// 500ms ≈ 1.25× propSlowElectionTimeout (400ms): tolerate one full leadership
	// transition without false-positive timeout. Errors are swallowed below.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	// Tolerate error: leadership may change between leader() check and ProposeWait.
	leader.ProposeWait(ctx, cmd) //nolint:errcheck
}

// StepDownLeader injects a higher-term RequestVote into the current leader,
// forcing it to step down. If there is no leader, the action is a no-op.
func (sm *raftStateMachine) StepDownLeader(t *rapid.T) {
	leader := sm.cluster.leader()
	if leader == nil {
		return
	}
	// +10: ensure term-jump survives a concurrent election tick that could also
	// be advancing currentTerm; +1 would race the natural election timer.
	higherTerm := leader.Term() + 10
	// Use the leader's own commit index to make the intruder's log at least
	// as up-to-date, satisfying the Raft election restriction (§5.4.1).
	// CommittedIndex is a lower bound on LastLogIndex (the leader may have
	// uncommitted entries past commit); the intruder's claimed LastLogTerm
	// (higherTerm) exceeds anything in the leader's log so the up-to-date
	// check still passes via the term-dominance branch.
	leader.HandleRequestVote(&RequestVoteArgs{
		Term:         higherTerm,
		CandidateID:  "intruder",
		LastLogIndex: leader.CommittedIndex(),
		LastLogTerm:  higherTerm,
	})
}

// Partition isolates one randomly chosen node from the rest of the cluster.
// The partition is applied bidirectionally via partitionNet.
func (sm *raftStateMachine) Partition(t *rapid.T) {
	// Pick one node index to isolate (0, 1, or 2).
	idx := rapid.IntRange(0, 2).Draw(t, "partitioned_node_idx")
	peer := sm.cluster.Nodes[idx].ID()
	sm.cluster.Net.Partition(peer)
}

// Heal removes all partitions, restoring full connectivity.
func (sm *raftStateMachine) Heal(t *rapid.T) {
	sm.cluster.Net.Heal()
}

// TestProperty_ElectionSafetyAndLeaderAppendOnly exercises the 3-voter cluster
// with random op sequences and asserts both invariants after every action.
//
// Default CI run: rapid.Check default count (100 sequences, each up to ~100 actions).
// Full hardening (100k sequences): -rapid.checks=100000 flag:
//
//	go test -race -count=1 ./internal/raft/v2/... -run TestProperty -rapid.checks=100000
func TestProperty_ElectionSafetyAndLeaderAppendOnly(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		sm := newRaftStateMachine(t, rt)
		rt.Repeat(rapid.StateMachineActions(sm))
	})
}
