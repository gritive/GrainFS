package migration

import (
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestDB(t *testing.T) *badger.DB {
	t.Helper()
	opts := badger.DefaultOptions(t.TempDir()).WithLogger(nil)
	db, err := badger.Open(opts)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func TestJobStore_GetCursor_NotFound_ReturnsEmpty(t *testing.T) {
	s := NewJobStore(newTestDB(t))
	cur, err := s.GetCursor("nope")
	require.NoError(t, err)
	assert.Empty(t, cur)
}

func TestJobStore_SaveCursor_RoundTrip(t *testing.T) {
	s := NewJobStore(newTestDB(t))
	require.NoError(t, s.SaveCursor("b", "token-abc"))
	cur, err := s.GetCursor("b")
	require.NoError(t, err)
	assert.Equal(t, "token-abc", cur)
}

func TestJobStore_GetJob_NotFound_ReturnsNil(t *testing.T) {
	s := NewJobStore(newTestDB(t))
	job, err := s.GetJob("nope")
	require.NoError(t, err)
	assert.Nil(t, job)
}

func TestJobStore_SaveJob_RoundTrip(t *testing.T) {
	s := NewJobStore(newTestDB(t))
	state := &JobState{
		Bucket:    "b",
		Status:    StatusRunning,
		Copied:    42,
		StartedAt: time.Now().Truncate(time.Millisecond),
		UpdatedAt: time.Now().Truncate(time.Millisecond),
	}
	require.NoError(t, s.SaveJob(state))
	got, err := s.GetJob("b")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "b", got.Bucket)
	assert.Equal(t, StatusRunning, got.Status)
	assert.Equal(t, int64(42), got.Copied)
}

func TestJobStore_ListJobs_ByStatus(t *testing.T) {
	s := NewJobStore(newTestDB(t))
	require.NoError(t, s.SaveJob(&JobState{Bucket: "a", Status: StatusRunning}))
	require.NoError(t, s.SaveJob(&JobState{Bucket: "b", Status: StatusComplete}))
	require.NoError(t, s.SaveJob(&JobState{Bucket: "c", Status: StatusRunning}))

	running, err := s.ListJobs(StatusRunning)
	require.NoError(t, err)
	require.Len(t, running, 2)
	buckets := []string{running[0].Bucket, running[1].Bucket}
	assert.ElementsMatch(t, []string{"a", "c"}, buckets)
}

func TestJobStore_ListJobs_Empty(t *testing.T) {
	s := NewJobStore(newTestDB(t))
	got, err := s.ListJobs(StatusRunning)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestJobStore_DeleteJob_ClearsJobAndCursor(t *testing.T) {
	s := NewJobStore(newTestDB(t))
	require.NoError(t, s.SaveJob(&JobState{Bucket: "b", Status: StatusRunning}))
	require.NoError(t, s.SaveCursor("b", "tok"))
	require.NoError(t, s.DeleteJob("b"))

	job, err := s.GetJob("b")
	require.NoError(t, err)
	assert.Nil(t, job)

	cur, err := s.GetCursor("b")
	require.NoError(t, err)
	assert.Empty(t, cur)
}

func TestJobStore_DeleteJob_NonExistent_NoError(t *testing.T) {
	s := NewJobStore(newTestDB(t))
	assert.NoError(t, s.DeleteJob("no-such-bucket"))
}
