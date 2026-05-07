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
var objectIndexLocalApplyTimeout = 10 * time.Second

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

// ForwardingObjectIndexProposer persists global object-index mutations through
// the meta-Raft leader, or forwards encoded meta commands when this node is a
// follower.
type ForwardingObjectIndexProposer struct {
	local   *MetaRaft
	forward MetaForwardFunc
}

func NewForwardingObjectIndexProposer(local *MetaRaft, forward MetaForwardFunc) *ForwardingObjectIndexProposer {
	return &ForwardingObjectIndexProposer{local: local, forward: forward}
}

func (p *ForwardingObjectIndexProposer) ProposeObjectIndex(ctx context.Context, entry ObjectIndexEntry, preserveLatest bool) error {
	if p.local == nil {
		return fmt.Errorf("meta object index proposer: local meta raft not configured")
	}
	if p.local.IsLeader() || p.forward == nil {
		return p.local.ProposeObjectIndex(ctx, entry, preserveLatest)
	}
	payload, err := encodeMetaPutObjectIndexCmd(entry, preserveLatest)
	if err != nil {
		return fmt.Errorf("meta object index proposer: encode PutObjectIndex: %w", err)
	}
	data, err := encodeMetaCmd(MetaCmdTypePutObjectIndex, payload)
	if err != nil {
		return fmt.Errorf("meta object index proposer: encode MetaCmd: %w", err)
	}
	if err := p.forward(ctx, data); err != nil {
		return err
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, objectIndexLocalApplyTimeout)
		defer cancel()
	}
	return waitForLocalObjectIndex(ctx, p.local, entry)
}

func (p *ForwardingObjectIndexProposer) ProposeDeleteObjectIndex(ctx context.Context, bucket, key, versionID string) error {
	if p.local == nil {
		return fmt.Errorf("meta object index proposer: local meta raft not configured")
	}
	if p.local.IsLeader() || p.forward == nil {
		return p.local.ProposeDeleteObjectIndex(ctx, bucket, key, versionID)
	}
	payload, err := encodeMetaDeleteObjectIndexCmd(bucket, key, versionID)
	if err != nil {
		return fmt.Errorf("meta object index proposer: encode DeleteObjectIndex: %w", err)
	}
	data, err := encodeMetaCmd(MetaCmdTypeDeleteObjectIndex, payload)
	if err != nil {
		return fmt.Errorf("meta object index proposer: encode MetaCmd: %w", err)
	}
	if err := p.forward(ctx, data); err != nil {
		return err
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, objectIndexLocalApplyTimeout)
		defer cancel()
	}
	return waitForLocalObjectIndexDelete(ctx, p.local, bucket, key, versionID)
}

func waitForLocalObjectIndex(ctx context.Context, local *MetaRaft, want ObjectIndexEntry) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if got, ok := local.FSM().ObjectIndexVersion(want.Bucket, want.Key, want.VersionID); ok &&
			got.PlacementGroupID == want.PlacementGroupID {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func waitForLocalObjectIndexDelete(ctx context.Context, local *MetaRaft, bucket, key, versionID string) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, ok := local.FSM().ObjectIndexVersion(bucket, key, versionID); !ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
