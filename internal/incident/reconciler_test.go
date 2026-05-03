package incident

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeEventReader struct{ facts map[string][]Fact }

func (r fakeEventReader) Facts(_ context.Context, id string, _ time.Time) ([]Fact, error) {
	return r.facts[id], nil
}

type fakeReceiptLookup struct{ receipts map[string]string }

func (r fakeReceiptLookup) ReceiptForCorrelation(_ context.Context, id string) (string, bool, error) {
	v, ok := r.receipts[id]
	return v, ok, nil
}

func TestReconciler_FixesDriftToFixed(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore()
	now := time.Unix(100, 0).UTC()
	require.NoError(t, store.Put(ctx, IncidentState{ID: "cid", State: StateActing, UpdatedAt: now}))

	rec := NewReconciler(store, fakeEventReader{facts: map[string][]Fact{
		"cid": {
			{CorrelationID: "cid", Type: FactObserved, Cause: CauseMissingShard, Scope: Scope{Kind: ScopeShard, Bucket: "b", Key: "k", ShardID: 0}, At: now},
			{CorrelationID: "cid", Type: FactVerified, At: now.Add(time.Millisecond)},
		},
	}}, fakeReceiptLookup{receipts: map[string]string{"cid": "rcpt"}}, NewReducer(), ReconcilerOptions{Limit: 10, Since: time.Hour})

	result, err := rec.RunOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, result.DriftFixed)
	got, ok, err := store.Get(ctx, "cid")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, StateFixed, got.State)
	assert.Equal(t, "rcpt", got.Proof.ReceiptID)
}
