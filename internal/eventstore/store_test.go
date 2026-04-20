package eventstore_test

import (
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/eventstore"
)

func newTestDB(t *testing.T) *badger.DB {
	t.Helper()
	opts := badger.DefaultOptions(t.TempDir())
	opts.Logger = nil
	db, err := badger.Open(opts)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func TestStore_AppendAndQuery(t *testing.T) {
	db := newTestDB(t)
	s := eventstore.New(db)

	now := time.Now()
	events := []eventstore.Event{
		{Type: eventstore.EventTypeS3, Action: eventstore.EventActionPut, Bucket: "photos", Key: "img.jpg", User: "admin", Size: 1024},
		{Type: eventstore.EventTypeS3, Action: eventstore.EventActionGet, Bucket: "photos", Key: "img.jpg", User: "admin"},
		{Type: eventstore.EventTypeSystem, Action: eventstore.EventActionSnapshotCreate},
	}
	for _, e := range events {
		require.NoError(t, s.Append(e))
	}

	got, err := s.Query(now.Add(-time.Second), now.Add(time.Minute), 200, nil)
	require.NoError(t, err)
	assert.Len(t, got, 3)
}

func TestStore_QueryEmpty(t *testing.T) {
	db := newTestDB(t)
	s := eventstore.New(db)

	got, err := s.Query(time.Now().Add(-time.Hour), time.Now(), 200, nil)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestStore_QueryLimit(t *testing.T) {
	db := newTestDB(t)
	s := eventstore.New(db)

	now := time.Now()
	for i := range 10 {
		require.NoError(t, s.Append(eventstore.Event{
			Type:   eventstore.EventTypeS3,
			Action: eventstore.EventActionPut,
			Key:    string(rune('a' + i)),
		}))
	}

	got, err := s.Query(now.Add(-time.Second), now.Add(time.Minute), 3, nil)
	require.NoError(t, err)
	assert.Len(t, got, 3)

	// limit 0은 0개
	got, err = s.Query(now.Add(-time.Second), now.Add(time.Minute), 0, nil)
	require.NoError(t, err)
	assert.Empty(t, got)

	// limit 1
	got, err = s.Query(now.Add(-time.Second), now.Add(time.Minute), 1, nil)
	require.NoError(t, err)
	assert.Len(t, got, 1)

	// limit 200 이상 (10개 있으므로 10개)
	got, err = s.Query(now.Add(-time.Second), now.Add(time.Minute), 201, nil)
	require.NoError(t, err)
	assert.Len(t, got, 10)
}

func TestStore_QueryTypeFilter(t *testing.T) {
	db := newTestDB(t)
	s := eventstore.New(db)

	now := time.Now()
	require.NoError(t, s.Append(eventstore.Event{Type: eventstore.EventTypeS3, Action: eventstore.EventActionPut}))
	require.NoError(t, s.Append(eventstore.Event{Type: eventstore.EventTypeSystem, Action: eventstore.EventActionSnapshotCreate}))
	require.NoError(t, s.Append(eventstore.Event{Type: eventstore.EventTypeS3, Action: eventstore.EventActionGet}))

	since := now.Add(-time.Second)
	until := now.Add(time.Minute)

	// s3만
	got, err := s.Query(since, until, 200, []string{eventstore.EventTypeS3})
	require.NoError(t, err)
	assert.Len(t, got, 2)
	for _, e := range got {
		assert.Equal(t, eventstore.EventTypeS3, e.Type)
	}

	// system만
	got, err = s.Query(since, until, 200, []string{eventstore.EventTypeSystem})
	require.NoError(t, err)
	assert.Len(t, got, 1)
	assert.Equal(t, eventstore.EventTypeSystem, got[0].Type)

	// 두 가지 (= nil과 동일)
	got, err = s.Query(since, until, 200, []string{eventstore.EventTypeS3, eventstore.EventTypeSystem})
	require.NoError(t, err)
	assert.Len(t, got, 3)

	// nil = all
	got, err = s.Query(since, until, 200, nil)
	require.NoError(t, err)
	assert.Len(t, got, 3)
}

func TestStore_DBNotNil(t *testing.T) {
	db := newTestDB(t)
	s := eventstore.New(db)
	assert.NotNil(t, s)
}

func TestStore_MetadataRoundTrip(t *testing.T) {
	db := newTestDB(t)
	s := eventstore.New(db)

	now := time.Now()
	in := eventstore.Event{
		Type:   "heal",
		Action: "reconstruct",
		Bucket: "photos",
		Key:    "img.jpg",
		Metadata: map[string]any{
			"phase":          "reconstruct",
			"shard_id":       float64(2), // JSON numbers decode as float64
			"correlation_id": "abc-123",
			"duration_ms":    float64(140),
		},
	}
	require.NoError(t, s.Append(in))

	got, err := s.Query(now.Add(-time.Second), now.Add(time.Minute), 1, []string{"heal"})
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "reconstruct", got[0].Metadata["phase"])
	assert.EqualValues(t, 2, got[0].Metadata["shard_id"])
	assert.Equal(t, "abc-123", got[0].Metadata["correlation_id"])
	assert.EqualValues(t, 140, got[0].Metadata["duration_ms"])
}

func TestStore_NoMetadataOmitsField(t *testing.T) {
	// Events without Metadata must round-trip cleanly (existing call sites
	// must not start emitting empty metadata maps).
	db := newTestDB(t)
	s := eventstore.New(db)

	now := time.Now()
	require.NoError(t, s.Append(eventstore.Event{Type: eventstore.EventTypeS3, Action: eventstore.EventActionPut}))

	got, err := s.Query(now.Add(-time.Second), now.Add(time.Minute), 1, nil)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Nil(t, got[0].Metadata)
}
