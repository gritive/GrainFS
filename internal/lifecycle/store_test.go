package lifecycle

import (
	"testing"

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

func TestStore_GetReturnsNilWhenNotSet(t *testing.T) {
	s := NewStore(newTestDB(t))
	cfg, err := s.Get("no-such-bucket")
	require.NoError(t, err)
	assert.Nil(t, cfg)
}

func TestStore_PutAndGet(t *testing.T) {
	s := NewStore(newTestDB(t))
	cfg := &LifecycleConfiguration{
		Rules: []Rule{{ID: "r1", Status: "Enabled", Expiration: &Expiration{Days: 30}}},
	}
	require.NoError(t, s.Put("my-bucket", cfg))

	got, err := s.Get("my-bucket")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Len(t, got.Rules, 1)
	assert.Equal(t, "r1", got.Rules[0].ID)
	assert.Equal(t, 30, got.Rules[0].Expiration.Days)
}

func TestStore_Delete(t *testing.T) {
	s := NewStore(newTestDB(t))
	cfg := &LifecycleConfiguration{Rules: []Rule{{ID: "r", Status: "Enabled"}}}
	require.NoError(t, s.Put("b", cfg))

	require.NoError(t, s.Delete("b"))

	got, err := s.Get("b")
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestStore_DeleteNonExistent(t *testing.T) {
	s := NewStore(newTestDB(t))
	assert.NoError(t, s.Delete("no-bucket"))
}

func TestStore_PutOverwrites(t *testing.T) {
	s := NewStore(newTestDB(t))
	cfg1 := &LifecycleConfiguration{Rules: []Rule{{ID: "old", Status: "Enabled"}}}
	cfg2 := &LifecycleConfiguration{Rules: []Rule{{ID: "new", Status: "Disabled"}}}

	require.NoError(t, s.Put("b", cfg1))
	require.NoError(t, s.Put("b", cfg2))

	got, err := s.Get("b")
	require.NoError(t, err)
	assert.Equal(t, "new", got.Rules[0].ID)
}
