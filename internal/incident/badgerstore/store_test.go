package badgerstore

import (
	"context"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/incident"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithLogger(nil))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return New(db)
}

func TestStore_PutGet(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	now := time.Unix(100, 0).UTC()
	state := incident.IncidentState{ID: "cid-1", State: incident.StateFixed, Cause: incident.CauseMissingShard, UpdatedAt: now}
	require.NoError(t, s.Put(ctx, state))

	got, ok, err := s.Get(ctx, "cid-1")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, incident.StateFixed, got.State)
	assert.Equal(t, incident.CauseMissingShard, got.Cause)
}

func TestStore_ListNewestFirstAndLimit(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	require.NoError(t, s.Put(ctx, incident.IncidentState{ID: "old", State: incident.StateBlocked, UpdatedAt: time.Unix(100, 0).UTC()}))
	require.NoError(t, s.Put(ctx, incident.IncidentState{ID: "new", State: incident.StateFixed, UpdatedAt: time.Unix(200, 0).UTC()}))

	got, err := s.List(ctx, 1)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "new", got[0].ID)
}

func TestStore_UpdateRemovesOldListIndex(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	require.NoError(t, s.Put(ctx, incident.IncidentState{ID: "cid", State: incident.StateObserved, UpdatedAt: time.Unix(100, 0).UTC()}))
	require.NoError(t, s.Put(ctx, incident.IncidentState{ID: "cid", State: incident.StateFixed, UpdatedAt: time.Unix(200, 0).UTC()}))

	got, err := s.List(ctx, 10)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, incident.StateFixed, got[0].State)
}
