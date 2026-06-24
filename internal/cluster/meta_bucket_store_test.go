package cluster

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
)

// fakeMetaForBucketStore applies Propose* commands directly to an in-memory
// MetaFSM without starting a real raft cluster. It satisfies metaRaftBucketBackend.
type fakeMetaForBucketStore struct {
	fsm *MetaFSM
	// readIndexErr controls ReadIndex behaviour:
	//   nil             → return (1, nil)  [leader]
	//   raft.ErrNotLeader → leaderless degrade path
	//   other           → genuine error propagation
	readIndexErr error
}

func newFakeMetaForBucketStore() *fakeMetaForBucketStore {
	return &fakeMetaForBucketStore{fsm: NewMetaFSM()}
}

func encodedCreateBucket(bucket, groupID string, bypassReserved bool) ([]byte, error) {
	payload, err := encodeMetaCreateBucketCmd(bucket, groupID, bypassReserved)
	if err != nil {
		return nil, fmt.Errorf("encode CreateBucket payload: %w", err)
	}
	return encodeMetaCmd(MetaCmdTypeCreateBucket, payload)
}

func encodedDeleteBucket(bucket string) ([]byte, error) {
	payload, err := encodeMetaDeleteBucketCmd(bucket)
	if err != nil {
		return nil, fmt.Errorf("encode DeleteBucket payload: %w", err)
	}
	return encodeMetaCmd(MetaCmdTypeDeleteBucket, payload)
}

func encodedSetVersioning(bucket, state string) ([]byte, error) {
	payload, err := encodeMetaSetBucketVersioningCmd(bucket, state)
	if err != nil {
		return nil, fmt.Errorf("encode SetBucketVersioning payload: %w", err)
	}
	return encodeMetaCmd(MetaCmdTypeSetBucketVersioning, payload)
}

func encodedSetPolicy(bucket string, policy []byte) ([]byte, error) {
	payload, err := encodeMetaSetBucketPolicyCmd(bucket, policy)
	if err != nil {
		return nil, fmt.Errorf("encode SetBucketPolicy payload: %w", err)
	}
	return encodeMetaCmd(MetaCmdTypeSetBucketPolicy, payload)
}

func encodedDeletePolicy(bucket string) ([]byte, error) {
	payload, err := encodeMetaDeleteBucketPolicyCmd(bucket)
	if err != nil {
		return nil, fmt.Errorf("encode DeleteBucketPolicy payload: %w", err)
	}
	return encodeMetaCmd(MetaCmdTypeDeleteBucketPolicy, payload)
}

func (f *fakeMetaForBucketStore) ProposeCreateBucket(_ context.Context, bucket, groupID string, bypassReserved bool) error {
	data, err := encodedCreateBucket(bucket, groupID, bypassReserved)
	if err != nil {
		return err
	}
	return f.fsm.applyCmd(data)
}

func (f *fakeMetaForBucketStore) ProposeDeleteBucket(_ context.Context, bucket string) error {
	data, err := encodedDeleteBucket(bucket)
	if err != nil {
		return err
	}
	return f.fsm.applyCmd(data)
}

func (f *fakeMetaForBucketStore) ProposeSetBucketVersioning(_ context.Context, bucket, state string) error {
	data, err := encodedSetVersioning(bucket, state)
	if err != nil {
		return err
	}
	return f.fsm.applyCmd(data)
}

func (f *fakeMetaForBucketStore) ProposeSetBucketPolicy(_ context.Context, bucket string, policy []byte) error {
	data, err := encodedSetPolicy(bucket, policy)
	if err != nil {
		return err
	}
	return f.fsm.applyCmd(data)
}

func (f *fakeMetaForBucketStore) ProposeDeleteBucketPolicy(_ context.Context, bucket string) error {
	data, err := encodedDeletePolicy(bucket)
	if err != nil {
		return err
	}
	return f.fsm.applyCmd(data)
}

func (f *fakeMetaForBucketStore) FSM() *MetaFSM { return f.fsm }

func (f *fakeMetaForBucketStore) ReadIndex(_ context.Context) (uint64, error) {
	if f.readIndexErr != nil {
		return 0, f.readIndexErr
	}
	return 1, nil
}

func (f *fakeMetaForBucketStore) WaitApplied(_ context.Context, _ uint64) error { return nil }

// ----- tests -----

// TestMetaBucketStore_SetVersioning verifies that SetVersioning("b1", "Enabled")
// is recorded so that Record("b1").Versioning == "Enabled".
func TestMetaBucketStore_SetVersioning(t *testing.T) {
	fake := newFakeMetaForBucketStore()
	store := newMetaBucketStoreFromIface(fake)
	ctx := t.Context()

	require.NoError(t, store.CreateBucket(ctx, "b1", "g-0", false))
	require.NoError(t, store.SetVersioning(ctx, "b1", "Enabled"))

	rec, ok := store.Record("b1")
	require.True(t, ok, "Record must exist after CreateBucket")
	assert.Equal(t, "Enabled", rec.Versioning, "Versioning must be Enabled after SetVersioning")
}

// TestMetaBucketStore_RecordLinearized_Leader verifies RecordLinearized on a
// leader (ReadIndex returns (1, nil)) returns the record with nil error.
func TestMetaBucketStore_RecordLinearized_Leader(t *testing.T) {
	fake := newFakeMetaForBucketStore()
	store := newMetaBucketStoreFromIface(fake)
	ctx := t.Context()

	require.NoError(t, store.CreateBucket(ctx, "b1", "g-0", false))

	rec, ok, err := store.RecordLinearized(ctx, "b1")
	require.NoError(t, err)
	require.True(t, ok, "RecordLinearized must find the bucket on a leader")
	assert.Equal(t, "g-0", rec.GroupID)
}

// TestMetaBucketStore_RecordLinearized_DegradesToLocalOnLeaderless verifies that
// when ReadIndex returns raft.ErrNotLeader, RecordLinearized degrades to a local
// read and returns nil error (availability over strict consistency).
func TestMetaBucketStore_RecordLinearized_DegradesToLocalOnLeaderless(t *testing.T) {
	fake := newFakeMetaForBucketStore()
	fake.readIndexErr = raft.ErrNotLeader
	store := newMetaBucketStoreFromIface(fake)
	ctx := t.Context()

	require.NoError(t, store.CreateBucket(ctx, "b1", "g-0", false))

	rec, ok, err := store.RecordLinearized(ctx, "b1")
	require.NoError(t, err, "leaderless degradation must not return error")
	require.True(t, ok)
	assert.Equal(t, "g-0", rec.GroupID)
}

// TestMetaBucketStore_RecordLinearized_DegradesToLocalOnTransportError verifies
// that a non-ErrNotLeader error from ReadIndex (e.g. a transport failure or a
// stale-leader-hint wrap) also degrades to the local snapshot and returns nil
// error. The linearizing barrier is best-effort — it must never fail a write
// because the barrier is temporarily unavailable (P1b fix).
func TestMetaBucketStore_RecordLinearized_DegradesToLocalOnTransportError(t *testing.T) {
	fake := newFakeMetaForBucketStore()
	fake.readIndexErr = errors.New("meta read-index forward: i/o timeout")
	store := newMetaBucketStoreFromIface(fake)
	ctx := t.Context()

	require.NoError(t, store.CreateBucket(ctx, "b1", "g-0", false))

	// Deliberately bypass the fake's Propose methods to pre-seed FSM directly,
	// so RecordLinearized has a local record to return when it degrades.
	rec, ok, err := store.RecordLinearized(ctx, "b1")
	require.NoError(t, err, "transport-error ReadIndex must degrade to local (nil error)")
	require.True(t, ok)
	assert.Equal(t, "g-0", rec.GroupID)
}
