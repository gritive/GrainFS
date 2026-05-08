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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v1 "github.com/gritive/GrainFS/internal/raft"
	chaosraft "github.com/gritive/GrainFS/internal/raft/chaos"
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
	// ProposeWait submits a command and blocks until it is committed, returning
	// the commit index. Both adapters delegate to their underlying node's
	// ProposeWait method.
	ProposeWait(ctx context.Context, cmd []byte) (uint64, error)
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
type v1Adapter struct {
	n *v1.Node
	t *testing.T // set in Start; transport stubs dereference this
}

func newV1Adapter(id string, peers []string) *v1Adapter {
	cfg := v1.DefaultConfig(id, peers)
	// Keep election timeout short so the single-voter node promotes quickly.
	// A very long timeout would block the initial Follower→Leader transition.
	// Re-election after winning is not possible for a stable single-voter
	// leader (hasQuorum returns true immediately; no step-down path).
	cfg.ElectionTimeout = 50 * time.Millisecond
	n := v1.NewNode(cfg)
	return &v1Adapter{n: n}
}

func (a *v1Adapter) Start(t *testing.T) {
	t.Helper()
	a.t = t
	// Install transport stubs now that t is available. Fatal on any RPC: if a
	// future scenario reaches multi-voter code paths without proper transport
	// wiring, silent zero-value returns would mask election failures.
	a.n.SetTransport(
		func(peer string, args *v1.RequestVoteArgs) (*v1.RequestVoteReply, error) {
			a.t.Fatalf("v1 RequestVote transport called unexpectedly (peer=%s)", peer)
			return nil, nil
		},
		func(peer string, args *v1.AppendEntriesArgs) (*v1.AppendEntriesReply, error) {
			a.t.Fatalf("v1 AppendEntries transport called unexpectedly (peer=%s)", peer)
			return nil, nil
		},
	)
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

func (a *v1Adapter) ProposeWait(ctx context.Context, cmd []byte) (uint64, error) {
	return a.n.ProposeWait(ctx, cmd)
}

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

func (a *v1Adapter) State() harnessState    { return harnessState(a.n.State()) }
func (a *v1Adapter) Term() uint64           { return a.n.Term() }
func (a *v1Adapter) LeaderID() string       { return a.n.LeaderID() }
func (a *v1Adapter) IsLeader() bool         { return a.n.IsLeader() }
func (a *v1Adapter) CommittedIndex() uint64 { return a.n.CommittedIndex() }

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

func (a *v2Adapter) ProposeWait(ctx context.Context, cmd []byte) (uint64, error) {
	return a.n.ProposeWait(ctx, cmd)
}

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

// TestEquivalence_MultipleProposes drives both impls through 5 sequential
// Proposes and asserts that both emit entries in submission order with
// monotonically increasing indices.
func TestEquivalence_MultipleProposes(t *testing.T) {
	var steps []ScenarioStep
	for i := 1; i <= 5; i++ {
		cmd := []byte(fmt.Sprintf("cmd-%d", i))
		steps = append(steps, ScenarioStep{
			Action:      func(r equivalentRaft) error { return r.Propose(cmd) },
			ExpectApply: 1,
			Description: fmt.Sprintf("Propose cmd-%d", i),
		})
	}
	runScenario(t, Scenario{
		Name:      "5 sequential proposes",
		NumVoters: 1,
		Steps:     steps,
	})
}

// TestEquivalence_ProposeWaitReturnsIndex verifies that ProposeWait blocks
// until commit and returns sequential indices for sequential calls. Tests both
// impls independently (not via runScenario) since Action only returns error
// and we need the returned index value.
func TestEquivalence_ProposeWaitReturnsIndex(t *testing.T) {
	runProposeWaitTest(t, "v1", func() equivalentRaft { return newV1Adapter("n1", nil) })
	runProposeWaitTest(t, "v2", func() equivalentRaft { return newV2Adapter("n1", nil) })
}

func runProposeWaitTest(t *testing.T, label string, makeImpl func() equivalentRaft) {
	t.Helper()
	t.Run(label, func(t *testing.T) {
		impl := makeImpl()
		impl.Start(t)
		defer impl.Stop()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		idx1, err := impl.ProposeWait(ctx, []byte("first"))
		require.NoError(t, err)
		require.Equal(t, uint64(1), idx1, "%s: first ProposeWait should return index 1", label)

		idx2, err := impl.ProposeWait(ctx, []byte("second"))
		require.NoError(t, err)
		require.Equal(t, uint64(2), idx2, "%s: second ProposeWait should return index 2", label)

		// Drain to keep ApplyCh consumer happy (otherwise actor blocks on send during Stop).
		entries := impl.DrainApply(2, 1*time.Second)
		require.Len(t, entries, 2)
	})
}

// otherIDs returns all entries of all except self, preserving order.
func otherIDs(all []string, self string) []string {
	out := make([]string, 0, len(all)-1)
	for _, id := range all {
		if id != self {
			out = append(out, id)
		}
	}
	return out
}

// buildV1Cluster wires N v1 Nodes through a ChaosTransport with asymmetric
// election timeouts: fastID gets a short timeout (deterministic winner), all
// other voters get a long timeout. Cleanup is registered via t.Cleanup.
func buildV1Cluster(t *testing.T, ids []string, fastID string) []*v1.Node {
	t.Helper()
	transport := chaosraft.NewChaosTransport()
	nodes := make([]*v1.Node, len(ids))
	for i, id := range ids {
		cfg := v1.DefaultConfig(id, otherIDs(ids, id))
		// Asymmetric timeouts: v1 randomises within [base, 2*base). With
		// fast=50ms and slow=500ms there is no overlap, so fastID always
		// fires its candidate timer first and wins term 1 uncontested.
		if id == fastID {
			cfg.ElectionTimeout = 50 * time.Millisecond
		} else {
			cfg.ElectionTimeout = 500 * time.Millisecond
		}
		n := v1.NewNode(cfg)
		nodes[i] = n
		transport.Register(n)
		transport.Wire(n)
	}
	for _, n := range nodes {
		if err := n.Bootstrap(); err != nil {
			t.Fatalf("v1 Bootstrap %s: %v", n.ID(), err)
		}
		n.Start()
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			n.Stop()
		}
	})
	return nodes
}

// v2HarnessNetwork is a minimal in-process Transport substrate for the
// external (raftv2_test) test package. It mirrors v2's package-private
// memNetwork — kept here because memNetwork is unexported and equivalence_test
// lives in raftv2_test by design (black-box adapter coverage for v1 + v2).
type v2HarnessNetwork struct {
	nodes map[string]*v2.Node
}

func newV2HarnessNetwork() *v2HarnessNetwork {
	return &v2HarnessNetwork{nodes: make(map[string]*v2.Node)}
}

// register installs node and returns a Transport that routes through the
// shared registry. The registry is only mutated before any node starts, so no
// lock is required.
func (n *v2HarnessNetwork) register(self string, node *v2.Node) v2.Transport {
	n.nodes[self] = node
	return &v2HarnessTransport{self: self, net: n}
}

type v2HarnessTransport struct {
	self string
	net  *v2HarnessNetwork
}

func (t *v2HarnessTransport) SendRequestVote(peer string, args *v2.RequestVoteArgs) (*v2.RequestVoteReply, error) {
	dst, ok := t.net.nodes[peer]
	if !ok {
		return nil, v2.ErrUnknownPeer
	}
	return dst.HandleRequestVote(args), nil
}

func (t *v2HarnessTransport) SendAppendEntries(peer string, args *v2.AppendEntriesArgs) (*v2.AppendEntriesReply, error) {
	dst, ok := t.net.nodes[peer]
	if !ok {
		return nil, v2.ErrUnknownPeer
	}
	return dst.HandleAppendEntries(args), nil
}

// buildV2Cluster wires N v2 Nodes through v2HarnessNetwork with the same
// asymmetric election-timeout setup as buildV1Cluster.
func buildV2Cluster(t *testing.T, ids []string, fastID string) []*v2.Node {
	t.Helper()
	net := newV2HarnessNetwork()
	nodes := make([]*v2.Node, len(ids))
	for i, id := range ids {
		cfg := v2.Config{
			ID:               id,
			Peers:            otherIDs(ids, id),
			HeartbeatTimeout: 50 * time.Millisecond,
		}
		if id == fastID {
			cfg.ElectionTimeout = 50 * time.Millisecond
		} else {
			cfg.ElectionTimeout = 500 * time.Millisecond
		}
		nodes[i] = v2.NewNode(cfg)
	}
	// Register all nodes with the network BEFORE any actor starts so the
	// first Candidate's RequestVote can route immediately.
	for _, n := range nodes {
		n.SetTransport(net.register(n.ID(), n))
	}
	for _, n := range nodes {
		n.Start()
		// Drain ApplyCh so actor goroutines never block on apply send.
		go func(n *v2.Node) {
			for range n.ApplyCh() {
			}
		}(n)
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			n.Stop()
		}
	})
	return nodes
}

// waitForV1Leader returns (leaderID, term) for the first leader observed
// across nodes, or fails the test on timeout.
func waitForV1Leader(t *testing.T, nodes []*v1.Node, timeout time.Duration) (string, uint64) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.IsLeader() {
				return n.ID(), n.Term()
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("waitForV1Leader: no leader within %v", timeout)
	return "", 0
}

// waitForV2Leader is the v2 counterpart of waitForV1Leader.
func waitForV2Leader(t *testing.T, nodes []*v2.Node, timeout time.Duration) (string, uint64) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.IsLeader() {
				return n.ID(), n.Term()
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("waitForV2Leader: no leader within %v", timeout)
	return "", 0
}

// TestEquivalence_ThreeVoterElection drives a 3-voter election independently
// against v1 and v2 with asymmetric election timeouts (n1 fast, n2/n3 slow).
// Both impls must elect n1 in term 1 — divergence on either leader or term
// would surface a v2 election regression vs v1.
//
// Bypasses runScenario: that harness is single-voter only and compares
// committed-log transcripts. Election outcome is the only observable that
// matters here, and we want to drive both clusters' transports independently
// (chaos for v1, memNetwork for v2).
func TestEquivalence_ThreeVoterElection(t *testing.T) {
	ids := []string{"n1", "n2", "n3"}
	const fast = "n1"

	v1Nodes := buildV1Cluster(t, ids, fast)
	v2Nodes := buildV2Cluster(t, ids, fast)

	v1Leader, v1Term := waitForV1Leader(t, v1Nodes, 2*time.Second)
	v2Leader, v2Term := waitForV2Leader(t, v2Nodes, 2*time.Second)

	// Asymmetric timeouts (fast 50ms vs slow 500ms, no overlap after
	// randomisation) make n1 the deterministic winner in both clusters.
	require.Equal(t, fast, v1Leader, "v1 leader (asymmetric timeouts should pick n1)")
	require.Equal(t, fast, v2Leader, "v2 leader (asymmetric timeouts should pick n1)")

	// Equivalence: same leader, same term.
	require.Equal(t, v1Leader, v2Leader, "v1 and v2 should elect the same leader")
	require.Equal(t, v1Term, v2Term, "v1 and v2 should agree on the term")
	require.Equal(t, uint64(1), v1Term, "first uncontested election should produce term 1")
}

// TestEquivalence_ThreeVoterPropose drives a single ProposeWait("hello") on
// the n1 leader of a 3-voter cluster against both v1 and v2 implementations
// and asserts equivalent outcomes:
//   - leader == n1, term == 1 (asymmetric election timeouts make this
//     deterministic; also verified in TestEquivalence_ThreeVoterElection),
//   - ProposeWait returns index 1 on both,
//   - all three nodes' CommittedIndex reaches 1 within 2s.
//
// We do not compare apply transcripts here because both buildV1Cluster and
// buildV2Cluster drain ApplyCh in background goroutines (see
// equivalence_test.go:441 and :517). CommittedIndex is the observable
// convergence signal — once it hits the proposed index on every node, the
// committed-log replication path has completed identically on both impls.
func TestEquivalence_ThreeVoterPropose(t *testing.T) {
	ids := []string{"n1", "n2", "n3"}
	const fast = "n1"

	v1Nodes := buildV1Cluster(t, ids, fast)
	v2Nodes := buildV2Cluster(t, ids, fast)

	// Wait for leadership in both clusters.
	v1Leader, v1Term := waitForV1Leader(t, v1Nodes, 2*time.Second)
	v2Leader, v2Term := waitForV2Leader(t, v2Nodes, 2*time.Second)
	require.Equal(t, fast, v1Leader)
	require.Equal(t, fast, v2Leader)
	require.Equal(t, v1Term, v2Term)

	// Locate the leader nodes (n1 by id) for ProposeWait.
	var v1Lead *v1.Node
	for _, n := range v1Nodes {
		if n.ID() == v1Leader {
			v1Lead = n
			break
		}
	}
	var v2Lead *v2.Node
	for _, n := range v2Nodes {
		if n.ID() == v2Leader {
			v2Lead = n
			break
		}
	}
	require.NotNil(t, v1Lead)
	require.NotNil(t, v2Lead)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	v1Idx, err := v1Lead.ProposeWait(ctx, []byte("hello"))
	require.NoError(t, err, "v1 ProposeWait")
	v2Idx, err := v2Lead.ProposeWait(ctx, []byte("hello"))
	require.NoError(t, err, "v2 ProposeWait")
	require.Equal(t, v1Idx, v2Idx, "v1 and v2 should return the same commit index")
	require.Equal(t, uint64(1), v1Idx)

	// Equivalence: every voter's CommittedIndex eventually reaches the
	// proposed index. The buildXCluster background drains keep ApplyCh
	// flowing so CommittedIndex can advance on followers.
	require.NoError(t, waitFor(2*time.Second, func() bool {
		for _, n := range v1Nodes {
			if n.CommittedIndex() < v1Idx {
				return false
			}
		}
		for _, n := range v2Nodes {
			if n.CommittedIndex() < v2Idx {
				return false
			}
		}
		return true
	}), "not all nodes reached the proposed commit index")
}
