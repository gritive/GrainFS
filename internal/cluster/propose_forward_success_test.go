package cluster

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// forwardSuccessTransport models a leader that accepts the forwarded propose and
// replies with a committed log index (no error). It records the peer it was
// asked to forward to so the test can assert the forward actually happened. The
// embedded shardTransport is nil — only CallBuffered is reached on the
// propose-forward path (native /forward/propose/legacy buffered route).
type forwardSuccessTransport struct {
	shardTransport
	committedIdx uint64
	applyErr     error        // encoded into the reply (leader-side FSM apply error)
	calledPeer   atomic.Value // string: the peer addr forwardPropose targeted
}

func (f *forwardSuccessTransport) CallBuffered(_ context.Context, addr, _ string, _ []byte) ([]byte, error) {
	f.calledPeer.Store(addr)
	return encodeProposeForwardReply(f.committedIdx, f.applyErr), nil
}

// newForwardSuccessBackend builds a follower DistributedBackend whose leader
// hint points at "peer-A" (so forwardPeersForPropose returns exactly that peer
// and forwardPropose is actually invoked) and whose transport replies success.
func newForwardSuccessBackend(t *testing.T, committedIdx uint64, applyErr error) (*DistributedBackend, *forwardSuccessTransport) {
	t.Helper()
	tr := &forwardSuccessTransport{committedIdx: committedIdx, applyErr: applyErr}
	b := &DistributedBackend{node: &proposeTimeoutNode{leader: false, leaderID: "peer-A"}}
	keeper, clusterID := testDEKKeeper(t)
	b.shardSvc = NewShardService(t.TempDir(), tr,
		WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))
	return b, tr
}

// TestPropose_FollowerForwardSuccessReadYourWrites is the previously-uncovered
// path: a follower forwards a propose to the leader, gets back a committed index
// with NO error, then blocks on the read-your-writes apply-wait until its OWN
// local FSM catches up to that index — and only then returns nil.
//
// Existing cluster unit coverage never hit this: newTestDistributedBackend spins
// a solo leader (always the leader fast-path), and propose_timeout_test.go only
// drives forward TIMEOUT / transport-ERROR returns. Forwarding to a leader and
// succeeding is its own branch (the post-forward waitLocalApplied on a committed
// idx). Shipping the propose decomposition — which MOVES that code — without
// exercising it would be unacceptable for raft consensus code, so this closes
// the unit-level gap. (True multi-node real-raft coverage remains the job of the
// e2e/integration suites; this is the fake-seam mitigation.)
func TestPropose_FollowerForwardSuccessReadYourWrites(t *testing.T) {
	const committedIdx uint64 = 42
	b, tr := newForwardSuccessBackend(t, committedIdx, nil)

	// lastApplied starts at 0 (< committedIdx), so the read-your-writes wait must
	// block. Bump it to >= committedIdx after a short delay; propose must observe
	// the catch-up and return nil — NOT return before the apply lands.
	const catchUpDelay = 25 * time.Millisecond
	go func() {
		time.Sleep(catchUpDelay)
		b.lastApplied.Store(committedIdx)
	}()

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := b.propose(ctx, CmdFSMValueResealDone, FSMValueResealDoneCmd{Gen: 1})
	elapsed := time.Since(start)

	require.NoError(t, err, "forward-success + apply catch-up must return nil")
	require.Equal(t, "peer-A", tr.calledPeer.Load(),
		"the configured leader hint must be the forward target")
	require.GreaterOrEqual(t, elapsed, catchUpDelay,
		"read-your-writes wait must block until lastApplied catches up to the committed idx")
}

// leaderCommitNode is a leader whose ProposeWait commits instantly at a fixed
// index with no error, so propose() enters the leader apply-wait loop (the local
// FSM has not yet applied that index). It lets a test drive the apply-wait branch
// without any real raft.
type leaderCommitNode struct {
	RaftNode
	committedIdx uint64
}

func (n *leaderCommitNode) IsLeader() bool { return true }
func (n *leaderCommitNode) ProposeWait(context.Context, []byte) (uint64, error) {
	return n.committedIdx, nil
}

// TestPropose_LeaderApplyWaitClientCancelSurfacesCanceled locks in landmine #1:
// when the caller cancels its context while propose() is blocked in the
// apply-wait loop (commit succeeded, local apply hasn't caught up), the BARE
// context.Canceled must surface — NOT a nil success, and NOT ErrProposeTimeout.
//
// This is the regression no other test catches: every timeout test produces
// context.DeadlineExceeded (→ non-nil sentinel), so a buggy waitLocalApplied
// that collapsed `return proposeCtx.Err()` to `return sentinel` would still pass
// every timeout/forward test while turning a client cancellation into a false
// nil success. The project has a documented P0 history of exactly this class
// (tests green while broken behavior hides), so the cancel path needs an
// explicit guard, not just the deadline path.
func TestPropose_LeaderApplyWaitClientCancelSurfacesCanceled(t *testing.T) {
	// committedIdx is never reached by lastApplied (stays 0), so the apply-wait
	// loop blocks until the context is canceled.
	b := &DistributedBackend{node: &leaderCommitNode{committedIdx: 99}}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	defer cancel()

	err := b.propose(ctx, CmdFSMValueResealDone, FSMValueResealDoneCmd{Gen: 1})
	require.Error(t, err, "client cancel during apply-wait must NOT return a false nil success")
	require.ErrorIs(t, err, context.Canceled,
		"the bare context.Canceled must surface (sentinel-first-then-bare-ctx-err)")
	require.NotErrorIs(t, err, ErrProposeTimeout,
		"a cancel is not a deadline — it must not be masked by the retryable timeout sentinel")
}

// TestPropose_FollowerForwardSuccessSurfacesApplyError covers the same forward-
// success branch but with an FSM apply error recorded locally at the committed
// index: once lastApplied >= idx, waitLocalApplied must surface that ApplyError
// instead of nil.
func TestPropose_FollowerForwardSuccessSurfacesApplyError(t *testing.T) {
	const committedIdx uint64 = 7
	b, _ := newForwardSuccessBackend(t, committedIdx, nil)

	sentinel := errors.New("fsm-apply-failed-at-7")
	// Apply-loop ordering: recordApplyResult before lastApplied.Store, so by the
	// time the propose loop observes lastApplied >= idx the error is already set.
	b.recordApplyResult(committedIdx, sentinel)
	b.lastApplied.Store(committedIdx)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := b.propose(ctx, CmdFSMValueResealDone, FSMValueResealDoneCmd{Gen: 1})
	require.ErrorIs(t, err, sentinel,
		"a local FSM apply error at the committed idx must surface after a successful forward")
}
