package cluster

import (
	"context"
	"fmt"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// encodeMetaDeleteBucketCmd encodes a MetaDeleteBucketCmd FlatBuffers payload.
func encodeMetaDeleteBucketCmd(bucket string) ([]byte, error) {
	b := clusterBuilderPool.Get()
	bucketOff := b.CreateString(bucket)
	clusterpb.MetaDeleteBucketCmdStart(b)
	clusterpb.MetaDeleteBucketCmdAddBucket(b, bucketOff)
	return fbFinish(b, clusterpb.MetaDeleteBucketCmdEnd(b)), nil
}

// decodeMetaDeleteBucketCmd decodes a MetaDeleteBucketCmd FlatBuffers payload
// and returns (bucket, error).
func decodeMetaDeleteBucketCmd(data []byte) (bucket string, err error) {
	if len(data) == 0 {
		return "", fmt.Errorf("meta_fsm: DeleteBucket: empty payload")
	}
	var (
		c      *clusterpb.MetaDeleteBucketCmd
		decErr error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				decErr = fmt.Errorf("meta_fsm: invalid MetaDeleteBucketCmd flatbuffer: %v", r)
			}
		}()
		c = clusterpb.GetRootAsMetaDeleteBucketCmd(data, 0)
	}()
	if decErr != nil {
		return "", decErr
	}
	return string(c.Bucket()), nil
}

// applyDeleteBucket idempotently removes a bucket record from the MetaFSM and
// fires the onBucketUnassigned callback (if registered) after releasing the lock.
// A delete for a bucket that does not exist is a no-op (idempotent).
func (f *MetaFSM) applyDeleteBucket(data []byte) error {
	bucket, err := decodeMetaDeleteBucketCmd(data)
	if err != nil {
		return err
	}
	if bucket == "" {
		return fmt.Errorf("meta_fsm: DeleteBucket: empty bucket name")
	}

	f.mu.Lock()
	_, existed := f.bucketRecords[bucket]
	if existed {
		delete(f.bucketRecords, bucket)
	}
	cb := f.onBucketUnassigned
	f.mu.Unlock()

	// Fire callback outside lock, mirroring applyCreateBucket/applyPutBucketAssignment.
	// Only fire when the record actually existed to avoid spurious unassign callbacks
	// on idempotent re-deletes (which could confuse callers that track state).
	if existed && cb != nil {
		cb(bucket)
	}
	return nil
}

// SetOnBucketUnassigned registers a callback fired (outside f.mu) after a
// DeleteBucket is applied and the record was present. Must not block.
// Set before MetaRaft.Start() to avoid a data race with the apply loop.
func (f *MetaFSM) SetOnBucketUnassigned(fn func(bucket string)) {
	f.mu.Lock()
	f.onBucketUnassigned = fn
	f.mu.Unlock()
}

// ProposeDeleteBucket encodes a DeleteBucket command and proposes it to the
// cluster, blocking until the entry is applied to the local FSM.
func (m *MetaRaft) ProposeDeleteBucket(ctx context.Context, bucket string) error {
	payload, err := encodeMetaDeleteBucketCmd(bucket)
	if err != nil {
		return fmt.Errorf("meta_raft: encode DeleteBucket: %w", err)
	}
	data, err := encodeMetaCmd(MetaCmdTypeDeleteBucket, payload)
	if err != nil {
		return fmt.Errorf("meta_raft: encode MetaCmd: %w", err)
	}
	idx, err := m.node.ProposeWait(ctx, data)
	if err != nil {
		return fmt.Errorf("meta_raft: ProposeWait: %w", err)
	}
	return m.waitAppliedResult(ctx, idx)
}
