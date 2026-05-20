package cluster

import (
	"context"
	"errors"
	"time"
)

// localBackendLookup retrieves the local GroupBackend instance for a
// placement group. Returns nil when the group is not currently running on
// this node.
type localBackendLookup interface {
	Backend(groupID string) *GroupBackend
}

// LocalExecution decides whether the local node can answer a routed op
// without forwarding. Owns the ctx-bound waits that gate local reads
// (follower ReadIndex+WaitApplied) and writes (self-only-voter leader-wait).
//
// Returning (nil, nil) means "forward instead" — a non-error signal so
// callers don't need to translate errors back into routing decisions.
//
// F3: ResolveWrite re-checks Node().IsLeader() at entry to close the
// route→execute leadership flip race.
type LocalExecution struct {
	groups localBackendLookup
}

func NewLocalExecution(groups localBackendLookup) *LocalExecution {
	return &LocalExecution{groups: groups}
}

const (
	localExecFollowerReadDeadline = 5 * time.Second
	localExecSelfOnlyLeaderWait   = 5 * time.Second
)

// ResolveRead returns the local *GroupBackend if the read can be answered
// locally, or nil to signal forward. Errors reserved for ctx propagation.
func (e *LocalExecution) ResolveRead(ctx context.Context, target RouteTarget) (*GroupBackend, error) {
	gb := e.groups.Backend(target.GroupID)
	if gb == nil {
		return nil, nil
	}
	if target.SelfIsOnlyVoter {
		return gb, nil
	}
	if gb.DistributedBackend == nil {
		if target.CanReadLocal() {
			probe := gb.testLeaderProbe
			if probe == nil || probe.IsLeader() {
				return gb, nil
			}
		}
		return nil, nil
	}
	if !target.SelfIsLeader {
		return nil, nil
	}
	if gb.DistributedBackend == nil || gb.Node() == nil {
		return nil, nil
	}
	probe := gb.leaderProbe()
	if probe != nil && !probe.IsLeader() {
		return nil, nil
	}
	readCtx, cancel := context.WithTimeout(ctx, localExecFollowerReadDeadline)
	defer cancel()
	idx, err := gb.ReadIndex(readCtx)
	if err != nil {
		return nil, nil
	}
	if err := gb.WaitApplied(readCtx, idx); err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			if ctx.Err() == nil {
				return nil, nil
			}
		}
		return nil, err
	}
	return gb, nil
}

// ResolveWrite returns the local *GroupBackend if the write can be answered
// locally (self is currently leader, OR self is the only voter and acquires
// leadership within localExecSelfOnlyLeaderWait). Returns nil to signal
// forward when self is non-leader at execute time.
//
// F3: re-checks leaderProbe().IsLeader() at entry to close the route→execute
// leadership flip race. The legacy code trusted RouteTarget.SelfIsLeader,
// which could be stale when leadership changed between route resolution
// and write execution.
func (e *LocalExecution) ResolveWrite(ctx context.Context, target RouteTarget) (*GroupBackend, error) {
	gb := e.groups.Backend(target.GroupID)
	if gb == nil {
		return nil, nil
	}
	probe := gb.leaderProbe()
	if probe == nil {
		return nil, nil
	}
	if target.SelfIsLeader && probe.IsLeader() {
		return gb, nil
	}
	if !target.SelfIsOnlyVoter {
		return nil, nil
	}
	if probe.IsLeader() {
		return gb, nil
	}
	waitCtx, cancel := context.WithTimeout(ctx, localExecSelfOnlyLeaderWait)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-waitCtx.Done():
			return nil, waitCtx.Err()
		case <-ticker.C:
			if probe.IsLeader() {
				return gb, nil
			}
		}
	}
}

// ResolveObjectWrite returns the local voter backend for object PUTs even when
// this node is not the current data-group leader. Object data shards can be
// written by any local voter; the group metadata commit is still serialized
// through DistributedBackend.propose, which forwards the small metadata command
// to the leader when needed.
func (e *LocalExecution) ResolveObjectWrite(ctx context.Context, target RouteTarget) (*GroupBackend, error) {
	gb := e.groups.Backend(target.GroupID)
	if gb == nil {
		return nil, nil
	}
	if target.SelfIsVoter && !target.SelfIsOnlyVoter {
		return gb, nil
	}
	return e.ResolveWrite(ctx, target)
}

// ResolveObjectPlacementRead returns the local voter backend for immutable
// object-index reads whose metadata already names the object version and EC
// placement. This lets surviving voters reconstruct k-of-n EC objects even
// when the data-group leader is down; callers must only use this for
// version-pinned EC reads and should fall back to ResolveRead/forward when the
// local backend has not applied the version metadata yet.
func (e *LocalExecution) ResolveObjectPlacementRead(_ context.Context, target RouteTarget) (*GroupBackend, error) {
	gb := e.groups.Backend(target.GroupID)
	if gb == nil || !target.SelfIsVoter {
		return nil, nil
	}
	return gb, nil
}
