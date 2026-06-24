package cluster

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// fakeMetaBucketStore is a test double for MetaBucketStore that records all calls.
type fakeMetaBucketStore struct {
	// recorded calls
	createCalls []fakeCreateCall
	deleteCalls []string
	setCalls    []fakeSetVersioningCall
	setPolicies []fakeSetPolicyCall
	delPolicies []string

	// configurable errors
	deleteErr error

	// call order tracking for delete ordering test
	callOrder []string

	// knownBuckets: populated by CreateBucket; Record() returns true for these.
	knownBuckets map[string]struct{}
}

type fakeCreateCall struct {
	bucket         string
	groupID        string
	bypassReserved bool
}

type fakeSetVersioningCall struct {
	bucket string
	state  string
}

type fakeSetPolicyCall struct {
	bucket string
	policy []byte
}

func (f *fakeMetaBucketStore) CreateBucket(_ context.Context, bucket, groupID string, bypassReserved bool) error {
	f.createCalls = append(f.createCalls, fakeCreateCall{bucket: bucket, groupID: groupID, bypassReserved: bypassReserved})
	if f.knownBuckets == nil {
		f.knownBuckets = make(map[string]struct{})
	}
	f.knownBuckets[bucket] = struct{}{}
	return nil
}

func (f *fakeMetaBucketStore) DeleteBucket(_ context.Context, bucket string) error {
	f.deleteCalls = append(f.deleteCalls, bucket)
	f.callOrder = append(f.callOrder, "meta-delete")
	return f.deleteErr
}

func (f *fakeMetaBucketStore) SetVersioning(_ context.Context, bucket, state string) error {
	f.setCalls = append(f.setCalls, fakeSetVersioningCall{bucket: bucket, state: state})
	return nil
}

func (f *fakeMetaBucketStore) SetPolicy(_ context.Context, bucket string, policy []byte) error {
	f.setPolicies = append(f.setPolicies, fakeSetPolicyCall{bucket: bucket, policy: append([]byte(nil), policy...)})
	return nil
}

func (f *fakeMetaBucketStore) DeletePolicy(_ context.Context, bucket string) error {
	f.delPolicies = append(f.delPolicies, bucket)
	return nil
}

func (f *fakeMetaBucketStore) Record(bucket string) (BucketRecord, bool) {
	if f.knownBuckets != nil {
		_, ok := f.knownBuckets[bucket]
		return BucketRecord{}, ok
	}
	return BucketRecord{}, false
}

func (f *fakeMetaBucketStore) RecordLinearized(_ context.Context, bucket string) (BucketRecord, bool, error) {
	return BucketRecord{}, false, nil
}

func (f *fakeMetaBucketStore) AllRecords() map[string]BucketRecord {
	return nil
}

// --- Create tests ---

// TestBucketWrite_Create_GoesToMetaBucketStore verifies that createBucketInternal
// routes exactly ONE CreateBucket call to MetaBucketStore (no group-0 CmdCreateBucket),
// with bypassReserved=false for the normal entrypoint.
func TestBucketWrite_Create_GoesToMetaBucketStore(t *testing.T) {
	b := newTestDistributedBackend(t)
	fake := &fakeMetaBucketStore{}
	b.SetMetaBucketStore(fake)

	ctx := context.Background()
	err := b.CreateBucket(ctx, "my-bucket")
	require.NoError(t, err)

	require.Len(t, fake.createCalls, 1)
	assert.Equal(t, "my-bucket", fake.createCalls[0].bucket)
	assert.False(t, fake.createCalls[0].bypassReserved, "normal CreateBucket must pass bypassReserved=false")
}

// TestBucketWrite_CreateBypassReserved_PassesTrueFlag verifies that
// CreateBucketBypassReserved threads bypassReserved=true through to the MetaBucketStore.
func TestBucketWrite_CreateBypassReserved_PassesTrueFlag(t *testing.T) {
	b := newTestDistributedBackend(t)
	fake := &fakeMetaBucketStore{}
	b.SetMetaBucketStore(fake)

	ctx := context.Background()
	err := b.CreateBucketBypassReserved(ctx, "reserved-bucket")
	require.NoError(t, err)

	require.Len(t, fake.createCalls, 1)
	assert.True(t, fake.createCalls[0].bypassReserved, "CreateBucketBypassReserved must pass bypassReserved=true")
}

// --- Delete tests ---

// seedBucketForDelete seeds the bucket so that DeleteBucket's HeadBucket check
// passes. Task 12: CmdCreateBucket is a retired no-op; existence lives in
// MetaBucketStore (the sole authority), so seed via the wired MBS.
func seedBucketForDelete(t *testing.T, b *DistributedBackend, bucket string) {
	t.Helper()
	require.NoError(t, b.MetaBucketStore().CreateBucket(context.Background(), bucket, "local", false))
}

// TestBucketWrite_Delete_ConsensusBeforeRemoveAll verifies the ordering contract:
// MetaBucketStore.DeleteBucket (consensus) MUST be called BEFORE removeAll.
func TestBucketWrite_Delete_ConsensusBeforeRemoveAll(t *testing.T) {
	b := newTestDistributedBackend(t)
	fake := &fakeMetaBucketStore{}
	b.SetMetaBucketStore(fake)

	ctx := context.Background()
	// Seed the bucket in the FSM without going through MetaBucketStore.
	seedBucketForDelete(t, b, "del-bucket")

	b.removeAll = func(path string) error {
		fake.callOrder = append(fake.callOrder, "remove")
		return nil
	}

	require.NoError(t, b.DeleteBucket(ctx, "del-bucket"))

	// Verify ordering: meta-delete must precede remove
	require.Len(t, fake.callOrder, 2)
	assert.Equal(t, "meta-delete", fake.callOrder[0], "consensus delete must precede physical remove")
	assert.Equal(t, "remove", fake.callOrder[1], "physical remove must follow consensus delete")
}

// TestBucketWrite_Delete_MetaErrorAbortsRemoveAll verifies that when
// MetaBucketStore.DeleteBucket returns an error, removeAll is NOT called.
func TestBucketWrite_Delete_MetaErrorAbortsRemoveAll(t *testing.T) {
	b := newTestDistributedBackend(t)
	fake := &fakeMetaBucketStore{
		deleteErr: errors.New("meta-raft unavailable"),
	}
	b.SetMetaBucketStore(fake)

	ctx := context.Background()
	// Seed the bucket in the FSM without going through MetaBucketStore.
	seedBucketForDelete(t, b, "del-bucket")

	removeAllCalled := false
	b.removeAll = func(path string) error {
		removeAllCalled = true
		return nil
	}

	err := b.DeleteBucket(ctx, "del-bucket")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "meta-raft unavailable")
	assert.False(t, removeAllCalled, "removeAll must NOT be called when meta DeleteBucket fails")
}

// --- SetBucketVersioningPropose tests ---

// TestBucketWrite_SetVersioning_GoesToMetaBucketStore verifies that
// SetBucketVersioningPropose calls MetaBucketStore.SetVersioning.
func TestBucketWrite_SetVersioning_GoesToMetaBucketStore(t *testing.T) {
	b := newTestDistributedBackend(t)
	fake := &fakeMetaBucketStore{}
	b.SetMetaBucketStore(fake)

	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "ver-bucket"))

	err := b.SetBucketVersioningPropose("ver-bucket", "Enabled")
	require.NoError(t, err)

	require.Len(t, fake.setCalls, 1)
	assert.Equal(t, "ver-bucket", fake.setCalls[0].bucket)
	assert.Equal(t, "Enabled", fake.setCalls[0].state)
}

// --- SetBucketPolicyPropose tests ---

// TestBucketWrite_SetPolicy_GoesToMetaBucketStore verifies that
// SetBucketPolicyPropose calls MetaBucketStore.SetPolicy.
func TestBucketWrite_SetPolicy_GoesToMetaBucketStore(t *testing.T) {
	b := newTestDistributedBackend(t)
	fake := &fakeMetaBucketStore{}
	b.SetMetaBucketStore(fake)

	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "pol-bucket"))

	policy := []byte(`{"Version":"2012-10-17","Statement":[]}`)
	err := b.SetBucketPolicyPropose("pol-bucket", policy)
	require.NoError(t, err)

	require.Len(t, fake.setPolicies, 1)
	assert.Equal(t, "pol-bucket", fake.setPolicies[0].bucket)
	assert.Equal(t, policy, fake.setPolicies[0].policy)
}

// --- DeleteBucketPolicyPropose tests ---

// TestBucketWrite_DeletePolicy_GoesToMetaBucketStore verifies that
// DeleteBucketPolicyPropose calls MetaBucketStore.DeletePolicy.
func TestBucketWrite_DeletePolicy_GoesToMetaBucketStore(t *testing.T) {
	b := newTestDistributedBackend(t)
	fake := &fakeMetaBucketStore{}
	b.SetMetaBucketStore(fake)

	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "pol-bucket"))

	err := b.DeleteBucketPolicyPropose("pol-bucket")
	require.NoError(t, err)

	require.Len(t, fake.delPolicies, 1)
	assert.Equal(t, "pol-bucket", fake.delPolicies[0])
}

// TestBucketWrite_Delete_NotFound returns ErrBucketNotFound for a missing bucket.
func TestBucketWrite_Delete_NotFound(t *testing.T) {
	b := newTestDistributedBackend(t)
	fake := &fakeMetaBucketStore{}
	b.SetMetaBucketStore(fake)

	ctx := context.Background()
	err := b.DeleteBucket(ctx, "no-such-bucket")
	require.ErrorIs(t, err, storage.ErrBucketNotFound)
	assert.Empty(t, fake.deleteCalls, "meta DeleteBucket must not be called for non-existent bucket")
}
