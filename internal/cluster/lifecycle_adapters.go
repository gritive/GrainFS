package cluster

import (
	"context"
	"time"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/lifecycle"
	"github.com/gritive/GrainFS/internal/raft"
)

// LifecycleProposer satisfies lifecycle.Proposer over the cluster meta-FSM.
// Mirrors iam.MetaProposer.
type LifecycleProposer struct {
	Propose func(ctx context.Context, cmdType clusterpb.MetaCmdType, payload []byte) error
}

func (p *LifecycleProposer) ProposeLifecyclePut(ctx context.Context, bucket string, raw []byte) error {
	return p.Propose(ctx, clusterpb.MetaCmdTypeBucketLifecyclePut, lifecycle.EncodePutPayload(bucket, raw))
}

func (p *LifecycleProposer) ProposeLifecycleDelete(ctx context.Context, bucket string) error {
	return p.Propose(ctx, clusterpb.MetaCmdTypeBucketLifecycleDelete, lifecycle.EncodeDeletePayload(bucket))
}

// raftNodeAccess is the minimum surface RaftLeadership needs. Defined as an
// interface so tests can supply a fake without bringing up a real raft.Node.
//
// As of M5 PR 28 RaftLeadership polls State() rather than driving off the
// observer pattern — v2 has no observer surface, and reconcile latency of
// ≤raftLeadershipPollInterval is acceptable for lifecycle/scrubber consumers.
type raftNodeAccess interface {
	State() raft.NodeState
}

// raftLeadershipPollInterval is the cadence at which RaftLeadership.Subscribe
// samples State() to detect leader transitions. Reconcile latency is bounded
// by this interval; chosen to be small enough that lifecycle gains leader
// promptly after election while keeping wakeup cost negligible.
const raftLeadershipPollInterval = 500 * time.Millisecond

// RaftLeadership satisfies lifecycle.LeadershipSignal over a RaftNode. Works
// for both v1 (*raft.Node) and v2 (raftV2Node) — both expose State().
type RaftLeadership struct {
	Node raftNodeAccess
	// PollInterval overrides raftLeadershipPollInterval; zero uses the default.
	// Test-only knob — production callers leave this unset.
	PollInterval time.Duration
}

func (r *RaftLeadership) IsLeader() bool { return r.Node.State() == raft.Leader }

func (r *RaftLeadership) Subscribe() (<-chan struct{}, func()) {
	out := make(chan struct{}, 4)
	done := make(chan struct{})
	interval := r.PollInterval
	if interval <= 0 {
		interval = raftLeadershipPollInterval
	}
	// Sample the current state synchronously so callers that mutate the node
	// immediately after Subscribe still observe the transition: if the
	// goroutine sampled later, it could race with the caller's mutation and
	// miss the edge.
	last := r.IsLeader()
	go func() {
		defer close(out)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				cur := r.IsLeader()
				if cur != last {
					last = cur
					select {
					case out <- struct{}{}:
					default:
					}
				}
			}
		}
	}()
	cancel := func() { close(done) }
	return out, cancel
}
