package cluster

import (
	"context"
	"fmt"
	"time"
)

// MetaForwardFunc forwards an encoded MetaCmd to the meta-Raft leader.
type MetaForwardFunc func(ctx context.Context, command []byte) error

// ForwardingBucketAssigner persists bucket placement through the local
// meta-Raft leader, or forwards the encoded meta command when this node is a
// follower.
type ForwardingBucketAssigner struct {
	local   *MetaRaft
	forward MetaForwardFunc
}

var bucketAssignmentLocalApplyTimeout = 10 * time.Second

func NewForwardingBucketAssigner(local *MetaRaft, forward MetaForwardFunc) *ForwardingBucketAssigner {
	return &ForwardingBucketAssigner{local: local, forward: forward}
}

func (a *ForwardingBucketAssigner) ProposeBucketAssignment(ctx context.Context, bucket, groupID string) error {
	if a.local == nil {
		return fmt.Errorf("meta bucket assigner: local meta raft not configured")
	}
	if a.local.IsLeader() || a.forward == nil {
		return a.local.ProposeBucketAssignment(ctx, bucket, groupID)
	}
	payload, err := encodeMetaPutBucketAssignmentCmd(bucket, groupID)
	if err != nil {
		return fmt.Errorf("meta bucket assigner: encode PutBucketAssignment: %w", err)
	}
	data, err := encodeMetaCmd(MetaCmdTypePutBucketAssignment, payload)
	if err != nil {
		return fmt.Errorf("meta bucket assigner: encode MetaCmd: %w", err)
	}
	if err := a.forward(ctx, data); err != nil {
		return err
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, bucketAssignmentLocalApplyTimeout)
		defer cancel()
	}
	return waitForLocalBucketAssignment(ctx, a.local, bucket, groupID)
}

func waitForLocalBucketAssignment(ctx context.Context, local *MetaRaft, bucket, groupID string) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if local.FSM().BucketAssignments()[bucket] == groupID {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
