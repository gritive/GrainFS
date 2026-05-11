package cluster

import (
	"context"

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
type raftNodeAccess interface {
	State() raft.NodeState
	RegisterObserver(ch chan<- raft.Event)
	DeregisterObserver(ch chan<- raft.Event)
}

// RaftLeadership satisfies lifecycle.LeadershipSignal over a *raft.Node.
type RaftLeadership struct {
	Node raftNodeAccess
}

func (r *RaftLeadership) IsLeader() bool { return r.Node.State() == raft.Leader }

func (r *RaftLeadership) Subscribe() (<-chan struct{}, func()) {
	raw := make(chan raft.Event, 4)
	r.Node.RegisterObserver(raw)
	out := make(chan struct{}, 4)
	done := make(chan struct{})
	go func() {
		defer close(out)
		for {
			select {
			case e, ok := <-raw:
				if !ok {
					return
				}
				if e.Type == raft.EventLeaderChange {
					select {
					case out <- struct{}{}:
					default:
					}
				}
			case <-done:
				return
			}
		}
	}()
	cancel := func() {
		r.Node.DeregisterObserver(raw)
		close(done)
	}
	return out, cancel
}
