package incident

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type memoryStore struct{ states map[string]IncidentState }

func newMemoryStore() *memoryStore { return &memoryStore{states: map[string]IncidentState{}} }
func (s *memoryStore) Put(_ context.Context, st IncidentState) error {
	s.states[st.ID] = st
	return nil
}
func (s *memoryStore) Get(_ context.Context, id string) (IncidentState, bool, error) {
	st, ok := s.states[id]
	return st, ok, nil
}
func (s *memoryStore) List(_ context.Context, _ int) ([]IncidentState, error) {
	out := make([]IncidentState, 0, len(s.states))
	for _, st := range s.states {
		out = append(out, st)
	}
	return out, nil
}

func TestRecorder_RecordsReducedState(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore()
	rec := NewRecorder(store, NewReducer())
	now := time.Unix(100, 0).UTC()

	err := rec.Record(ctx, []Fact{
		{CorrelationID: "cid", Type: FactObserved, Cause: CauseMissingShard, Scope: Scope{Kind: ScopeShard, Bucket: "b", Key: "k", ShardID: 0}, At: now},
		{CorrelationID: "cid", Type: FactActionFailed, Action: ActionReconstructShard, ErrorCode: "insufficient_survivors", At: now.Add(time.Millisecond)},
	})
	require.NoError(t, err)
	got, ok, err := store.Get(ctx, "cid")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, StateBlocked, got.State)
	assert.Contains(t, got.NextAction, "Restore a peer")
}
