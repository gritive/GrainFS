// Package raftv2_test holds the v1↔v2 equivalence harness.
//
// Goal: drive the v1 (internal/raft) and v2 (internal/raft/v2) implementations
// through the same scripted scenario and assert that the observable outputs —
// committed log entries plus final (state, term, leaderID, isLeader,
// committedIndex) snapshot — match exactly. As feature PRs land in v2, each
// adds its scenario here so divergence is caught at landing time.
//
// PR 2 ships the skeleton plus a single scenario (single-node propose).
// Multi-voter scenarios are deferred until election lands (PR 4-5) and will be
// reported via t.Skipf until then.
//
// Black-box on purpose: imports both v1 and v2 only through their public APIs.
// The harness defines its own LogEntry / NodeState equivalents so the
// equivalentRaft interface does not leak either package's types.
package raftv2_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v1 "github.com/gritive/GrainFS/internal/raft"
	v2 "github.com/gritive/GrainFS/internal/raft/v2"
)

// harnessEntry is the harness-local equivalent of v1.LogEntry / v2.LogEntry.
// Both impls have identical field shape; we transcode at adapter boundaries so
// the shared interface does not depend on either package.
type harnessEntry struct {
	Term    uint64
	Index   uint64
	Command []byte
	Type    int8 // matches both v1.LogEntryType and v2.LogEntryType underlying type
}

// harnessState is the harness-local equivalent of NodeState. Values mirror
// both impls (Follower=0, Candidate=1, Leader=2).
type harnessState int

const (
	hFollower harnessState = iota
	hCandidate
	hLeader
)

func (s harnessState) String() string {
	switch s {
	case hFollower:
		return "Follower"
	case hCandidate:
		return "Candidate"
	case hLeader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// equivalentRaft is the minimal surface both v1 and v2 expose to the harness.
// The interface deliberately does NOT mention v1 or v2 types — adapters
// transcode to harnessEntry / harnessState.
type equivalentRaft interface {
	Start(t *testing.T)
	Stop()
	Propose(cmd []byte) error
	// DrainApply pulls up to n entries from the impl's ApplyCh, transcoding to
	// harnessEntry. Returns early on timeout — that is itself a signal of
	// divergence (one side delivered fewer entries than the scenario expected).
	DrainApply(n int, timeout time.Duration) []harnessEntry
	State() harnessState
	Term() uint64
	LeaderID() string
	IsLeader() bool
	CommittedIndex() uint64
}

// v1Adapter wraps internal/raft.Node behind equivalentRaft.
//
// v1 does not expose CommittedIndex publicly; the harness approximates it by
// remembering the highest index seen on ApplyCh (= last applied entry, which
// for committed-then-applied semantics equals committedIndex up to that
// point). This is sufficient for equivalence: both sides converge to the same
// "last delivered index" after DrainApply returns. If a future scenario needs
// strict committedIndex semantics that diverge from lastApplied (e.g. commit
// without apply), this proxy must be revisited.
type v1Adapter struct {
	n                *v1.Node
	lastDrainedIndex uint64
}

func newV1Adapter(id string, peers []string) *v1Adapter {
	cfg := v1.DefaultConfig(id, peers)
	// Tighten election timeout so single-voter promotion is fast in tests.
	// Asymmetric vs v2 (which auto-promotes inside run()) — see decision note
	// at top of file: "same Config" applies at observable-behavior level, not
	// at config-construction level.
	cfg.ElectionTimeout = 50 * time.Millisecond
	n := v1.NewNode(cfg)
	// Stub transports: never invoked for single-voter (PreVote returns true
	// immediately when len(peers)==0), but v1 internals dereference the func
	// pointers on multi-voter paths. Setting them up-front keeps the adapter
	// usable as scenarios grow.
	n.SetTransport(
		func(peer string, args *v1.RequestVoteArgs) (*v1.RequestVoteReply, error) {
			return &v1.RequestVoteReply{}, nil
		},
		func(peer string, args *v1.AppendEntriesArgs) (*v1.AppendEntriesReply, error) {
			return &v1.AppendEntriesReply{}, nil
		},
	)
	return &v1Adapter{n: n}
}

func (a *v1Adapter) Start(t *testing.T) {
	t.Helper()
	// Bootstrap is a no-op for in-memory nodes (store == nil) but the harness
	// follows the documented v1 startup sequence so future PRs adding a store
	// stay correct without harness changes.
	if err := a.n.Bootstrap(); err != nil {
		t.Fatalf("v1 Bootstrap: %v", err)
	}
	a.n.Start()
	// Wait for leadership so callers don't see the post-start election delay.
	if err := waitFor(2*time.Second, a.n.IsLeader); err != nil {
		t.Fatalf("v1 did not become leader: %v", err)
	}
}

func (a *v1Adapter) Stop() { a.n.Stop() }

func (a *v1Adapter) Propose(cmd []byte) error { return a.n.Propose(cmd) }

func (a *v1Adapter) DrainApply(n int, timeout time.Duration) []harnessEntry {
	out := make([]harnessEntry, 0, n)
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for len(out) < n {
		select {
		case e, ok := <-a.n.ApplyCh():
			if !ok {
				return out
			}
			entry := harnessEntry{
				Term:    e.Term,
				Index:   e.Index,
				Command: append([]byte(nil), e.Command...),
				Type:    int8(e.Type),
			}
			a.lastDrainedIndex = entry.Index
			out = append(out, entry)
		case <-deadline.C:
			return out
		}
	}
	return out
}

func (a *v1Adapter) State() harnessState    { return harnessState(a.n.State()) }
func (a *v1Adapter) Term() uint64           { return a.n.Term() }
func (a *v1Adapter) LeaderID() string       { return a.n.LeaderID() }
func (a *v1Adapter) IsLeader() bool         { return a.n.IsLeader() }
func (a *v1Adapter) CommittedIndex() uint64 { return a.lastDrainedIndex }

// v2Adapter wraps internal/raft/v2.Node behind equivalentRaft.
type v2Adapter struct {
	n *v2.Node
}

func newV2Adapter(id string, peers []string) *v2Adapter {
	return &v2Adapter{n: v2.NewNode(v2.Config{ID: id, Peers: peers})}
}

func (a *v2Adapter) Start(t *testing.T) {
	t.Helper()
	a.n.Start()
	if err := waitFor(2*time.Second, a.n.IsLeader); err != nil {
		t.Fatalf("v2 did not become leader: %v", err)
	}
}

func (a *v2Adapter) Stop() { a.n.Stop() }

func (a *v2Adapter) Propose(cmd []byte) error { return a.n.Propose(cmd) }

func (a *v2Adapter) DrainApply(n int, timeout time.Duration) []harnessEntry {
	out := make([]harnessEntry, 0, n)
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for len(out) < n {
		select {
		case e, ok := <-a.n.ApplyCh():
			if !ok {
				return out
			}
			out = append(out, harnessEntry{
				Term:    e.Term,
				Index:   e.Index,
				Command: append([]byte(nil), e.Command...),
				Type:    int8(e.Type),
			})
		case <-deadline.C:
			return out
		}
	}
	return out
}

func (a *v2Adapter) State() harnessState    { return harnessState(a.n.State()) }
func (a *v2Adapter) Term() uint64           { return a.n.Term() }
func (a *v2Adapter) LeaderID() string       { return a.n.LeaderID() }
func (a *v2Adapter) IsLeader() bool         { return a.n.IsLeader() }
func (a *v2Adapter) CommittedIndex() uint64 { return a.n.CommittedIndex() }

// ScenarioStep is a single action driven against the impl, plus how many
// committed entries the scenario expects on ApplyCh as a result.
type ScenarioStep struct {
	Description string
	Action      func(r equivalentRaft) error
	ExpectApply int
}

// Scenario is a scripted sequence to drive against both impls. The runner
// collects the transcript (drained entries + final snapshot) from each side
// and compares them.
type Scenario struct {
	Name      string
	NumVoters int
	Steps     []ScenarioStep
}

// snapshot of the impl's read-side state after the scenario, used for the
// post-run cross-impl comparison.
type finalState struct {
	State          harnessState
	Term           uint64
	LeaderID       string
	IsLeader       bool
	CommittedIndex uint64
}

// runScenario drives sc through both v1 and v2 and asserts equivalence. Each
// impl is built with the same logical config (single voter "n1" + empty Peers
// for NumVoters==1). On any divergence, both sides are logged via t.Errorf so
// the failure message localises the disagreement.
func runScenario(t *testing.T, sc Scenario) {
	t.Helper()
	if sc.NumVoters > 1 {
		t.Skipf("multi-voter scenarios deferred until election lands (PR 4-5)")
	}

	const drainTimeout = 2 * time.Second

	run := func(label string, impl equivalentRaft) ([]harnessEntry, finalState) {
		impl.Start(t)
		defer impl.Stop()

		var transcript []harnessEntry
		for i, step := range sc.Steps {
			if step.Action != nil {
				if err := step.Action(impl); err != nil {
					t.Fatalf("%s: step %d (%s): action failed: %v", label, i, step.Description, err)
				}
			}
			if step.ExpectApply > 0 {
				drained := impl.DrainApply(step.ExpectApply, drainTimeout)
				if len(drained) != step.ExpectApply {
					t.Fatalf("%s: step %d (%s): expected %d apply entries, got %d (%+v)",
						label, i, step.Description, step.ExpectApply, len(drained), drained)
				}
				transcript = append(transcript, drained...)
			}
		}

		// Snapshot AFTER all steps (including DrainApply) so the read-side
		// values reflect post-apply state on both impls. v1 and v2 publish
		// commit/apply through different paths; reading mid-step risks a
		// false divergence.
		snap := finalState{
			State:          impl.State(),
			Term:           impl.Term(),
			LeaderID:       impl.LeaderID(),
			IsLeader:       impl.IsLeader(),
			CommittedIndex: impl.CommittedIndex(),
		}
		return transcript, snap
	}

	v1Trans, v1Snap := run("v1", newV1Adapter("n1", nil))
	v2Trans, v2Snap := run("v2", newV2Adapter("n1", nil))

	compareOutputs(t, sc.Name, v1Trans, v2Trans, v1Snap, v2Snap)
}

// compareOutputs asserts that v1 and v2 produced identical transcripts and
// final snapshots. On mismatch, both sides are logged so the disagreement is
// obvious from the failure output alone.
func compareOutputs(t *testing.T, name string, v1Trans, v2Trans []harnessEntry, v1Snap, v2Snap finalState) {
	t.Helper()
	require.Equalf(t, len(v1Trans), len(v2Trans), "%s: applied entry count diverges (v1=%d v2=%d)", name, len(v1Trans), len(v2Trans))
	for i := range v1Trans {
		require.Equalf(t, v1Trans[i], v2Trans[i], "%s: transcript[%d] diverges (v1=%+v v2=%+v)", name, i, v1Trans[i], v2Trans[i])
	}
	require.Equalf(t, v1Snap, v2Snap, "%s: final snapshot diverges (v1=%+v v2=%+v)", name, v1Snap, v2Snap)
}

// waitFor polls cond until true or the deadline elapses. Self-contained — the
// harness deliberately avoids depending on v2's package-private waitFor.
func waitFor(d time.Duration, cond func() bool) error {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return nil
		}
		time.Sleep(time.Millisecond)
	}
	if cond() {
		return nil
	}
	return context.DeadlineExceeded
}

// TestEquivalence_SingleNodePropose drives both impls through a Propose("hello")
// on a single-voter cluster and asserts they produce the same entry on
// ApplyCh and the same final read-state snapshot.
func TestEquivalence_SingleNodePropose(t *testing.T) {
	runScenario(t, Scenario{
		Name:      "single-node propose 'hello'",
		NumVoters: 1,
		Steps: []ScenarioStep{
			{
				Description: "Propose 'hello' and drain 1 apply",
				Action: func(r equivalentRaft) error {
					return r.Propose([]byte("hello"))
				},
				ExpectApply: 1,
			},
		},
	})
}
