package cluster

import (
	"context"
	"fmt"
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
	return a.forward(ctx, data)
}
