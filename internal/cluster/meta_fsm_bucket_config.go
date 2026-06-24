package cluster

import (
	"context"
	"fmt"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/storage"
)

// --- SetBucketVersioning ---

// encodeMetaSetBucketVersioningCmd encodes a SetBucketVersioningCmd FlatBuffers payload.
func encodeMetaSetBucketVersioningCmd(bucket, state string) ([]byte, error) {
	b := clusterBuilderPool.Get()
	bucketOff := b.CreateString(bucket)
	stateOff := b.CreateString(state)
	clusterpb.SetBucketVersioningCmdStart(b)
	clusterpb.SetBucketVersioningCmdAddBucket(b, bucketOff)
	clusterpb.SetBucketVersioningCmdAddState(b, stateOff)
	return fbFinish(b, clusterpb.SetBucketVersioningCmdEnd(b)), nil
}

// decodeMetaSetBucketVersioningCmd decodes a SetBucketVersioningCmd FlatBuffers payload
// and returns (bucket, state, error).
func decodeMetaSetBucketVersioningCmd(data []byte) (bucket, state string, err error) {
	if len(data) == 0 {
		return "", "", fmt.Errorf("meta_fsm: SetBucketVersioning: empty payload")
	}
	var (
		c      *clusterpb.SetBucketVersioningCmd
		decErr error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				decErr = fmt.Errorf("meta_fsm: invalid SetBucketVersioningCmd flatbuffer: %v", r)
			}
		}()
		c = clusterpb.GetRootAsSetBucketVersioningCmd(data, 0)
	}()
	if decErr != nil {
		return "", "", decErr
	}
	return string(c.Bucket()), string(c.State()), nil
}

// applySetBucketVersioning RMW-updates the Versioning field on an existing
// bucket record. Returns storage.ErrBucketNotFound if the bucket does not exist.
func (f *MetaFSM) applySetBucketVersioning(data []byte) error {
	bucket, state, err := decodeMetaSetBucketVersioningCmd(data)
	if err != nil {
		return err
	}
	if bucket == "" {
		return fmt.Errorf("meta_fsm: SetBucketVersioning: empty bucket name")
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	rec, exists := f.bucketRecords[bucket]
	if !exists {
		return storage.ErrBucketNotFound
	}
	rec.Versioning = state
	f.bucketRecords[bucket] = rec
	return nil
}

// ProposeSetBucketVersioning encodes a SetBucketVersioning command and proposes
// it to the cluster, blocking until the entry is applied to the local FSM.
func (m *MetaRaft) ProposeSetBucketVersioning(ctx context.Context, bucket, state string) error {
	payload, err := encodeMetaSetBucketVersioningCmd(bucket, state)
	if err != nil {
		return fmt.Errorf("meta_raft: encode SetBucketVersioning: %w", err)
	}
	data, err := encodeMetaCmd(MetaCmdTypeSetBucketVersioning, payload)
	if err != nil {
		return fmt.Errorf("meta_raft: encode MetaCmd: %w", err)
	}
	_, err = m.proposeOrForwardWithIndex(ctx, m.node, data)
	return err
}

// --- SetBucketPolicy ---

// encodeMetaSetBucketPolicyCmd encodes a SetBucketPolicyCmd FlatBuffers payload.
func encodeMetaSetBucketPolicyCmd(bucket string, policy []byte) ([]byte, error) {
	b := clusterBuilderPool.Get()
	bucketOff := b.CreateString(bucket)
	policyOff := b.CreateByteVector(policy)
	clusterpb.SetBucketPolicyCmdStart(b)
	clusterpb.SetBucketPolicyCmdAddBucket(b, bucketOff)
	clusterpb.SetBucketPolicyCmdAddPolicyJson(b, policyOff)
	return fbFinish(b, clusterpb.SetBucketPolicyCmdEnd(b)), nil
}

// decodeMetaSetBucketPolicyCmd decodes a SetBucketPolicyCmd FlatBuffers payload
// and returns (bucket, policy, error). policy is a slice into the FB buffer —
// callers that store it must deep-copy.
func decodeMetaSetBucketPolicyCmd(data []byte) (bucket string, policy []byte, err error) {
	if len(data) == 0 {
		return "", nil, fmt.Errorf("meta_fsm: SetBucketPolicy: empty payload")
	}
	var (
		c      *clusterpb.SetBucketPolicyCmd
		decErr error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				decErr = fmt.Errorf("meta_fsm: invalid SetBucketPolicyCmd flatbuffer: %v", r)
			}
		}()
		c = clusterpb.GetRootAsSetBucketPolicyCmd(data, 0)
	}()
	if decErr != nil {
		return "", nil, decErr
	}
	return string(c.Bucket()), c.PolicyJsonBytes(), nil
}

// applySetBucketPolicy RMW-updates the Policy field on an existing bucket record.
// Deep-copies the incoming bytes so the FSM state does not alias the decoded FB buffer.
// Returns storage.ErrBucketNotFound if the bucket does not exist.
func (f *MetaFSM) applySetBucketPolicy(data []byte) error {
	bucket, policy, err := decodeMetaSetBucketPolicyCmd(data)
	if err != nil {
		return err
	}
	if bucket == "" {
		return fmt.Errorf("meta_fsm: SetBucketPolicy: empty bucket name")
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	rec, exists := f.bucketRecords[bucket]
	if !exists {
		return storage.ErrBucketNotFound
	}
	// Deep-copy the incoming bytes — the policy slice points into the FB buffer
	// which may be recycled by clusterBuilderPool after this call returns.
	rec.Policy = append([]byte(nil), policy...)
	f.bucketRecords[bucket] = rec
	return nil
}

// ProposeSetBucketPolicy encodes a SetBucketPolicy command and proposes it to
// the cluster, blocking until the entry is applied to the local FSM.
func (m *MetaRaft) ProposeSetBucketPolicy(ctx context.Context, bucket string, policy []byte) error {
	payload, err := encodeMetaSetBucketPolicyCmd(bucket, policy)
	if err != nil {
		return fmt.Errorf("meta_raft: encode SetBucketPolicy: %w", err)
	}
	data, err := encodeMetaCmd(MetaCmdTypeSetBucketPolicy, payload)
	if err != nil {
		return fmt.Errorf("meta_raft: encode MetaCmd: %w", err)
	}
	_, err = m.proposeOrForwardWithIndex(ctx, m.node, data)
	return err
}

// --- DeleteBucketPolicy ---

// encodeMetaDeleteBucketPolicyCmd encodes a DeleteBucketPolicyCmd FlatBuffers payload.
func encodeMetaDeleteBucketPolicyCmd(bucket string) ([]byte, error) {
	b := clusterBuilderPool.Get()
	bucketOff := b.CreateString(bucket)
	clusterpb.DeleteBucketPolicyCmdStart(b)
	clusterpb.DeleteBucketPolicyCmdAddBucket(b, bucketOff)
	return fbFinish(b, clusterpb.DeleteBucketPolicyCmdEnd(b)), nil
}

// decodeMetaDeleteBucketPolicyCmd decodes a DeleteBucketPolicyCmd FlatBuffers payload
// and returns (bucket, error).
func decodeMetaDeleteBucketPolicyCmd(data []byte) (bucket string, err error) {
	if len(data) == 0 {
		return "", fmt.Errorf("meta_fsm: DeleteBucketPolicy: empty payload")
	}
	var (
		c      *clusterpb.DeleteBucketPolicyCmd
		decErr error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				decErr = fmt.Errorf("meta_fsm: invalid DeleteBucketPolicyCmd flatbuffer: %v", r)
			}
		}()
		c = clusterpb.GetRootAsDeleteBucketPolicyCmd(data, 0)
	}()
	if decErr != nil {
		return "", decErr
	}
	return string(c.Bucket()), nil
}

// applyDeleteBucketPolicy idempotently clears the Policy field on an existing
// bucket record. Idempotent: if Policy is already nil, this is a no-op (no error).
// Returns storage.ErrBucketNotFound only if the bucket record itself does not exist.
func (f *MetaFSM) applyDeleteBucketPolicy(data []byte) error {
	bucket, err := decodeMetaDeleteBucketPolicyCmd(data)
	if err != nil {
		return err
	}
	if bucket == "" {
		return fmt.Errorf("meta_fsm: DeleteBucketPolicy: empty bucket name")
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	rec, exists := f.bucketRecords[bucket]
	if !exists {
		return storage.ErrBucketNotFound
	}
	rec.Policy = nil
	f.bucketRecords[bucket] = rec
	return nil
}

// ProposeDeleteBucketPolicy encodes a DeleteBucketPolicy command and proposes it
// to the cluster, blocking until the entry is applied to the local FSM.
func (m *MetaRaft) ProposeDeleteBucketPolicy(ctx context.Context, bucket string) error {
	payload, err := encodeMetaDeleteBucketPolicyCmd(bucket)
	if err != nil {
		return fmt.Errorf("meta_raft: encode DeleteBucketPolicy: %w", err)
	}
	data, err := encodeMetaCmd(MetaCmdTypeDeleteBucketPolicy, payload)
	if err != nil {
		return fmt.Errorf("meta_raft: encode MetaCmd: %w", err)
	}
	_, err = m.proposeOrForwardWithIndex(ctx, m.node, data)
	return err
}
