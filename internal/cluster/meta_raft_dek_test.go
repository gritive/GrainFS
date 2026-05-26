package cluster

import (
	"context"
	"crypto/rand"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/compat"
	"github.com/gritive/GrainFS/internal/encrypt"
)

// fakeDEKNode is a minimal RaftNode for ProposeDEKRotate leadership tests. It
// embeds the RaftNode interface (nil) so any un-overridden method panics — the
// DEK propose path only touches IsLeader() and ProposeWait(). Leadership is
// backed by an atomic.Bool so the step-down test can flip it mid-flight.
//
// ProposeWait synchronously drives the wired MetaRaft's FSM apply (mirroring the
// real wait-applied semantics + the KEK leader tests' fake submitter) so the
// keeper's active gen actually advances and waitAppliedResult returns
// immediately instead of blocking on the 60s epoch timeout.
type fakeDEKNode struct {
	RaftNode
	m          *MetaRaft
	leader     atomic.Bool
	proposeErr error
	// flipAfter, when > 0, makes IsLeader() return true for the first N calls
	// then false thereafter (drives the step-down-before-propose race).
	flipAfter   int64
	isLeaderCnt atomic.Int64
	idx         atomic.Uint64
	// onPropose runs (under no lock) at the start of ProposeWait; nil = no-op.
	onPropose func()
}

func newFakeDEKNode(leader bool) *fakeDEKNode {
	n := &fakeDEKNode{}
	n.leader.Store(leader)
	return n
}

// markAppliedForTest publishes idx as applied + wakes waitApplied callers,
// mirroring the real apply-loop notify (meta_raft.go ~950-955).
func (m *MetaRaft) markAppliedForTest(idx uint64) {
	m.lastApplied.Store(idx)
	m.applyNotifyMu.Lock()
	old := m.applyNotify
	m.applyNotify = make(chan struct{})
	m.applyNotifyMu.Unlock()
	close(old)
}

func (n *fakeDEKNode) IsLeader() bool {
	c := n.isLeaderCnt.Add(1)
	if n.flipAfter > 0 && c > n.flipAfter {
		return false
	}
	return n.leader.Load()
}

func (n *fakeDEKNode) ProposeWait(_ context.Context, data []byte) (uint64, error) {
	if n.onPropose != nil {
		n.onPropose()
	}
	if n.proposeErr != nil {
		return 0, n.proposeErr
	}
	idx := n.idx.Add(1)
	if n.m != nil {
		// Drive the FSM apply synchronously then publish the applied index so
		// the proposer's waitAppliedResult(idx) returns immediately.
		_ = n.m.fsm.applyCmdAtIndex(data, idx)
		n.m.markAppliedForTest(idx)
	}
	return idx, nil
}

// newTestMetaRaftForDEK builds a MetaRaft wired for DEK propose tests: a fresh
// FSM with a gen-0 DEK keeper + clusterID, a permissive capability gate
// (no SetMetaRaftSnapshot → empty config.Servers → plan.Allowed() == true), and
// the apply-result plumbing waitAppliedResult needs.
func newTestMetaRaftForDEK(t *testing.T, node RaftNode) (*MetaRaft, *encrypt.DEKKeeper) {
	t.Helper()
	kek := make([]byte, encrypt.KEKSize)
	if _, err := rand.Read(kek); err != nil {
		t.Fatalf("rand kek: %v", err)
	}
	keeper, err := encrypt.NewDEKKeeper(kek, dekTestClusterID())
	if err != nil {
		t.Fatalf("NewDEKKeeper: %v", err)
	}
	fsm := NewMetaFSM()
	fsm.SetClusterID(dekTestClusterID())
	fsm.SetDEKKeeper(keeper)
	fsm.SetKEKStore(newTestKEKStore(t, kek))

	m := &MetaRaft{
		node:           node,
		fsm:            fsm,
		capabilityGate: NewCapabilityGate(compat.DefaultRegistry, time.Minute),
		done:           make(chan struct{}),
		applyNotify:    make(chan struct{}),
		applyErrs:      make(map[uint64]error),
	}
	if fn, ok := node.(*fakeDEKNode); ok {
		fn.m = m
	}
	return m, keeper
}

func TestProposeDEKRotate_NotLeaderRejected(t *testing.T) {
	m, _ := newTestMetaRaftForDEK(t, newFakeDEKNode(false))
	if err := m.ProposeDEKRotate(context.Background()); err == nil {
		t.Fatal("non-leader ProposeDEKRotate must reject")
	}
}

func TestProposeDEKRotate_LeaderWithoutPublishedEpochSelfAcquires(t *testing.T) {
	// HIGH 2 / Pass 3: leader-just-won, watcher hasn't published the epoch yet
	// (dekEpoch nil). ProposeDEKRotate must NOT drop the request — it self-
	// ensures the epoch and proceeds (here it reaches the fake propose).
	m, _ := newTestMetaRaftForDEK(t, newFakeDEKNode(true))
	if m.dekEpoch.Load() != nil {
		t.Fatal("precondition: epoch must be nil to exercise the window")
	}
	err := m.ProposeDEKRotate(context.Background())
	if errors.Is(err, ErrDEKRotateInProgress) || (err != nil && strings.Contains(err.Error(), "not leader")) {
		t.Fatalf("leader must self-acquire epoch, not drop the rotation: %v", err)
	}
	if m.dekEpoch.Load() == nil {
		t.Fatal("ProposeDEKRotate must have published a DEK epoch")
	}
}

func TestProposeDEKRotate_ConcurrentReturnsInProgress(t *testing.T) {
	m, _ := newTestMetaRaftForDEK(t, newFakeDEKNode(true))
	if _, err := m.ensureDEKLeadership(); err != nil {
		t.Fatalf("ensureDEKLeadership on leader: %v", err)
	}
	m.dekRotateMu.Lock() // simulate an in-flight rotation
	defer m.dekRotateMu.Unlock()
	if err := m.ProposeDEKRotate(context.Background()); !errors.Is(err, ErrDEKRotateInProgress) {
		t.Fatalf("concurrent rotation must return ErrDEKRotateInProgress, got %v", err)
	}
}

func TestProposeDEKRotate_StepDownBeforeProposeRejected(t *testing.T) {
	// Leader self-ensures, then m.node flips to follower before propose; the
	// pre-propose IsLeader() re-check must reject so a stepped-down former leader
	// can't forward a self-initiated rotation under a stale epoch.
	node := newFakeDEKNode(true)
	// flipAfter is coupled to ProposeDEKRotate's IsLeader() call count: in the
	// success path it calls IsLeader() exactly 4 times — (1) authority check,
	// (2) ensureDEKLeadership top-of-loop, (3) ensureDEKLeadership post-CAS,
	// (4) pre-propose re-check. flipAfter=3 returns leader for the first three
	// (so the epoch publishes) then follower for the fourth (so the pre-propose
	// re-check rejects). If that call count changes, update this value.
	node.flipAfter = 3
	m, _ := newTestMetaRaftForDEK(t, node)
	err := m.ProposeDEKRotate(context.Background())
	if err == nil || !strings.Contains(err.Error(), "not leader") {
		t.Fatalf("step-down before propose must reject with not-leader, got %v", err)
	}
}

func TestEnsureDEKLeadership_DoesNotCancelLiveEpoch(t *testing.T) {
	// HIGH / Pass 4 (a): a watcher ensure AFTER a self-ensure must NOT cancel the
	// in-flight epoch.
	m, _ := newTestMetaRaftForDEK(t, newFakeDEKNode(true))
	epA, err := m.ensureDEKLeadership() // hook self-ensure
	if err != nil {
		t.Fatalf("ensureDEKLeadership: %v", err)
	}
	epB, err := m.ensureDEKLeadership() // watcher leader-edge ensure
	if err != nil {
		t.Fatalf("ensureDEKLeadership re-ensure: %v", err)
	}
	if epA != epB {
		t.Fatal("ensureDEKLeadership must return the SAME live epoch, not a new one")
	}
	if epA.ctx.Err() != nil {
		t.Fatal("re-ensure must NOT cancel the live epoch")
	}
	// Only loseDEKLeadership cancels.
	m.loseDEKLeadership()
	if epA.ctx.Err() == nil {
		t.Fatal("loseDEKLeadership must cancel the epoch")
	}
}

func TestEnsureDEKLeadership_FollowerDoesNotPublish(t *testing.T) {
	// HIGH / Pass 5 (ABA guard): a stale ensure on a non-leader must NOT leave a
	// published epoch.
	m, _ := newTestMetaRaftForDEK(t, newFakeDEKNode(false)) // step-down already happened
	m.loseDEKLeadership()                                   // epoch already nil
	ep, err := m.ensureDEKLeadership()
	if err == nil {
		t.Fatal("ensureDEKLeadership on a follower must return not-leader error")
	}
	if ep != nil {
		t.Fatal("ensureDEKLeadership on a follower must not return an epoch")
	}
	if m.dekEpoch.Load() != nil {
		t.Fatal("ensureDEKLeadership on a follower must NOT leave a published epoch (ABA hole)")
	}
}

func TestProposeDEKRotate_TwoConcurrentNilEpochCallers_ExactlyOneProceeds(t *testing.T) {
	// HIGH / Pass 4 (b): two concurrent nil-epoch ProposeDEKRotate callers — the
	// TryLock-before-ensure ordering means exactly one proceeds, the other gets
	// ErrDEKRotateInProgress, no epoch churn, the committed rotation is not
	// dropped.
	node := newFakeDEKNode(true)
	// Block the first propose inside ProposeWait until both goroutines have
	// entered ProposeDEKRotate, so the loser is guaranteed to hit the held
	// TryLock rather than racing past it.
	var entered sync.WaitGroup
	entered.Add(2)
	release := make(chan struct{})
	node.onPropose = func() {
		<-release
	}
	m, keeper := newTestMetaRaftForDEK(t, node)

	results := make([]error, 2)
	var run sync.WaitGroup
	run.Add(2)
	for i := 0; i < 2; i++ {
		go func(idx int) {
			defer run.Done()
			entered.Done()
			results[idx] = m.ProposeDEKRotate(context.Background())
		}(i)
	}
	// Give both goroutines time to enter and contend on TryLock, then release
	// the blocked propose.
	entered.Wait()
	time.Sleep(20 * time.Millisecond)
	close(release)
	run.Wait()

	var success, inProgress int
	for _, err := range results {
		switch {
		case err == nil:
			success++
		case errors.Is(err, ErrDEKRotateInProgress):
			inProgress++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if success != 1 || inProgress != 1 {
		t.Fatalf("want exactly 1 success + 1 in-progress, got success=%d inProgress=%d", success, inProgress)
	}
	if m.dekEpoch.Load() == nil {
		t.Fatal("a published epoch must survive concurrent callers")
	}
	if m.dekEpoch.Load().ctx.Err() != nil {
		t.Fatal("the published epoch must not be cancelled by concurrent callers")
	}
	if _, a := keeper.VersionsAndActive(); a != 1 {
		t.Fatalf("the winning rotation must have advanced active to 1, got %d", a)
	}
}

func TestProposeDEKBootstrap_RejectsNonZeroGen(t *testing.T) {
	m, _ := newTestMetaRaftForDEK(t, newFakeDEKNode(true))
	if err := m.ProposeDEKBootstrap(context.Background(), 5, []byte("wrapped"), 0); err == nil {
		t.Fatal("ProposeDEKBootstrap must reject gen != 0")
	}
}
