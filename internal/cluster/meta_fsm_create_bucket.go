package cluster

import (
	"context"
	"fmt"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/reservedname"
	"github.com/gritive/GrainFS/internal/storage"
)

// encodeMetaCreateBucketCmd encodes a MetaCreateBucketCmd FlatBuffers payload.
func encodeMetaCreateBucketCmd(bucket, groupID string, bypassReserved bool) ([]byte, error) {
	b := clusterBuilderPool.Get()
	bucketOff := b.CreateString(bucket)
	groupIDOff := b.CreateString(groupID)
	clusterpb.MetaCreateBucketCmdStart(b)
	clusterpb.MetaCreateBucketCmdAddBucket(b, bucketOff)
	clusterpb.MetaCreateBucketCmdAddGroupId(b, groupIDOff)
	clusterpb.MetaCreateBucketCmdAddBypassReserved(b, bypassReserved)
	return fbFinish(b, clusterpb.MetaCreateBucketCmdEnd(b)), nil
}

// decodeMetaCreateBucketCmd decodes a MetaCreateBucketCmd FlatBuffers payload
// and returns (bucket, groupID, bypassReserved, error).
func decodeMetaCreateBucketCmd(data []byte) (bucket, groupID string, bypassReserved bool, err error) {
	if len(data) == 0 {
		return "", "", false, fmt.Errorf("meta_fsm: CreateBucket: empty payload")
	}
	var (
		c      *clusterpb.MetaCreateBucketCmd
		decErr error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				decErr = fmt.Errorf("meta_fsm: invalid MetaCreateBucketCmd flatbuffer: %v", r)
			}
		}()
		c = clusterpb.GetRootAsMetaCreateBucketCmd(data, 0)
	}()
	if decErr != nil {
		return "", "", false, decErr
	}
	return string(c.Bucket()), string(c.GroupId()), c.BypassReserved(), nil
}

// applyCreateBucket atomically creates a bucket record (existence + group assignment)
// in the MetaFSM. Mirrors apply.go's applyCreateBucket reserved-name guard but
// operates on the meta-raft in-memory state (not the local BadgerDB FSM).
func (f *MetaFSM) applyCreateBucket(data []byte) error {
	bucket, groupID, bypassReserved, err := decodeMetaCreateBucketCmd(data)
	if err != nil {
		return err
	}
	if bucket == "" {
		return fmt.Errorf("meta_fsm: CreateBucket: empty bucket name")
	}
	if groupID == "" {
		return fmt.Errorf("meta_fsm: CreateBucket: empty group ID")
	}

	// Reserved-name guard: same predicate as apply.go:214.
	if !bypassReserved && reservedname.IsReservedBucketName(bucket) {
		return fmt.Errorf("bucket name %q is reserved and cannot be created via public API", bucket)
	}

	f.mu.Lock()
	if _, exists := f.bucketRecords[bucket]; exists {
		f.mu.Unlock()
		return storage.ErrBucketAlreadyExists
	}
	f.bucketRecords[bucket] = BucketRecord{GroupID: groupID}
	cb := f.onBucketAssigned
	f.mu.Unlock()

	if cb != nil {
		cb(bucket, groupID)
	}
	return nil
}

// ProposeCreateBucket encodes a CreateBucket command and proposes it to the
// cluster, blocking until the entry is applied to the local FSM.
func (m *MetaRaft) ProposeCreateBucket(ctx context.Context, bucket, groupID string, bypassReserved bool) error {
	payload, err := encodeMetaCreateBucketCmd(bucket, groupID, bypassReserved)
	if err != nil {
		return fmt.Errorf("meta_raft: encode CreateBucket: %w", err)
	}
	data, err := encodeMetaCmd(MetaCmdTypeCreateBucket, payload)
	if err != nil {
		return fmt.Errorf("meta_raft: encode MetaCmd: %w", err)
	}
	idx, err := m.node.ProposeWait(ctx, data)
	if err != nil {
		return fmt.Errorf("meta_raft: ProposeWait: %w", err)
	}
	return m.waitAppliedResult(ctx, idx)
}
