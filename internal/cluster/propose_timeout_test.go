package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/transport"
	"github.com/stretchr/testify/require"
)

// proposeTimeoutNode drives propose() down its deadline branches without any
// real raft/transport. The embedded RaftNode is nil — only the four methods
// propose() reaches before timing out are overridden.
type proposeTimeoutNode struct {
	RaftNode
	leader   bool
	leaderID string
}

func (n *proposeTimeoutNode) IsLeader() bool   { return n.leader }
func (n *proposeTimeoutNode) LeaderID() string { return n.leaderID }
func (n *proposeTimeoutNode) Peers() []string  { return nil }

// blockingShardTransport.CallPooled blocks until the forward context expires,
// then returns the bare context error — modelling a forward SendRequest whose
// proposeCtx fired mid-flight. The embedded shardTransport is nil; SendRequest
// only reaches CallPooled. The wrapped error is non-ErrNotLeader, so it drives
// propose()'s `!allNotLeader` early-return — the path the Done() branch test
// can't reach.
type blockingShardTransport struct {
	shardTransport
}

func (b *blockingShardTransport) CallPooled(ctx context.Context, _ string, _ *transport.Message) (*transport.Message, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// ProposeWait blocks until the caller's context expires, then returns the bare
// context error — exactly what a raft node does when the commit can't complete
// inside the propose deadline.
func (n *proposeTimeoutNode) ProposeWait(ctx context.Context, _ []byte) (uint64, error) {
	<-ctx.Done()
	return 0, ctx.Err()
}

// TestPropose_LeaderTimeoutSurfacesRetryableSentinel: when the leader-side raft
// commit can't finish inside the propose deadline, propose() must surface
// ErrProposeTimeout (mapped to a retryable 503), NOT the bare context error
// that falls through to a fatal 500.
func TestPropose_LeaderTimeoutSurfacesRetryableSentinel(t *testing.T) {
	b := &DistributedBackend{node: &proposeTimeoutNode{leader: true}}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	err := b.propose(ctx, CmdFSMValueResealDone, FSMValueResealDoneCmd{Gen: 1})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrProposeTimeout,
		"leader propose deadline must surface the retryable sentinel")
}

// TestPropose_FollowerTimeoutMasksLastErrWithSentinel is the load-bearing case
// the advisor flagged: the follower forward loop sets lastErr=ErrNotLeader on
// every failed attempt and returns it on deadline. Pattern-matching bare
// context.DeadlineExceeded would MISS this (the returned error is ErrNotLeader,
// not a context error) and leak a 500. The sentinel must mask lastErr.
func TestPropose_FollowerTimeoutMasksLastErrWithSentinel(t *testing.T) {
	// IsLeader=false + no peers => forwardPeersForPropose() is empty =>
	// lastErr=raft.ErrNotLeader, loop retries until the deadline fires.
	b := &DistributedBackend{node: &proposeTimeoutNode{leader: false}}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()

	err := b.propose(ctx, CmdFSMValueResealDone, FSMValueResealDoneCmd{Gen: 1})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrProposeTimeout,
		"follower forward deadline must surface the retryable sentinel")
	require.False(t, errors.Is(err, raft.ErrNotLeader),
		"sentinel must mask lastErr=ErrNotLeader that bare-DeadlineExceeded matching would leak as a 500")
}

// TestPropose_FollowerForwardTransportTimeoutSurfacesSentinel covers the second
// lastErr return the advisor flagged: when a forward SendRequest itself times
// out (proposeCtx expiring mid-flight), forwardPropose returns a NON-ErrNotLeader
// wrapped error → allNotLeader=false → propose() takes the `if !allNotLeader`
// early-return. Without the sentinel guard there it leaks the raw timeout as a
// 500, bypassing the Done() branch entirely.
func TestPropose_FollowerForwardTransportTimeoutSurfacesSentinel(t *testing.T) {
	// LeaderID set => forwardPeersForPropose returns [leader] => forwardPropose
	// is actually called (vs the no-peers test which never reaches it).
	b := &DistributedBackend{node: &proposeTimeoutNode{leader: false, leaderID: "peer-A"}}
	keeper, clusterID := testDEKKeeper(t)
	b.shardSvc = NewShardService(t.TempDir(), &blockingShardTransport{},
		WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()

	err := b.propose(ctx, CmdFSMValueResealDone, FSMValueResealDoneCmd{Gen: 1})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrProposeTimeout,
		"a forward whose transport call times out must surface the retryable sentinel, not a 500")
}
