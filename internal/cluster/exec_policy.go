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
// F3: ResolveWrite re-checks RaftNode().IsLeader() at entry to close the
// route→execute leadership flip race.
type LocalExecution struct {
	groups localBackendLookup
}

func NewLocalExecution(groups localBackendLookup) *LocalExecution {
	return &LocalExecution{groups: groups}
}

const (
	localExecFollowerReadDeadline = 250 * time.Millisecond
	localExecSelfOnlyLeaderWait   = 5 * time.Second
)

// ResolveRead returns the local *GroupBackend if the read can be answered
// locally, or nil to signal forward. Errors reserved for ctx propagation.
func (e *LocalExecution) ResolveRead(ctx context.Context, target RouteTarget) (*GroupBackend, error) {
	gb := e.groups.Backend(target.GroupID)
	if gb == nil {
		return nil, nil
	}
	if target.CanReadLocal() {
		return gb, nil
	}
	if !target.SelfIsVoter {
		return nil, nil
	}
	if gb.RaftNode() == nil {
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
