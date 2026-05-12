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
	require.NoError(t, s.put("my-bucket", cfg))

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
	require.NoError(t, s.put("b", cfg))

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

	require.NoError(t, s.put("b", cfg1))
	require.NoError(t, s.put("b", cfg2))

	got, err := s.Get("b")
	require.NoError(t, err)
	assert.Equal(t, "new", got.Rules[0].ID)
}

func TestStore_PutRaw_RoundTrip(t *testing.T) {
	db := newTestDB(t)
	s := NewStore(db)
	raw := []byte(`<LifecycleConfiguration><Rule><ID>r1</ID><Status>Enabled</Status></Rule></LifecycleConfiguration>`)
	require.NoError(t, s.PutRaw("b", raw))
	got, err := s.Get("b")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Len(t, got.Rules, 1)
	require.Equal(t, "r1", got.Rules[0].ID)
}

func TestStore_GetRaw_ReturnsByteForByte(t *testing.T) {
	db := newTestDB(t)
	s := NewStore(db)
	raw := []byte(`<LifecycleConfiguration><Rule><ID>r1</ID><Status>Enabled</Status></Rule></LifecycleConfiguration>`)
	require.NoError(t, s.PutRaw("b", raw))
	got, err := s.GetRaw("b")
	require.NoError(t, err)
	assert.Equal(t, raw, got)
}

func TestStore_GetRaw_NotFound(t *testing.T) {
	s := NewStore(newTestDB(t))
	got, err := s.GetRaw("nope")
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestStore_ListBuckets(t *testing.T) {
	st := NewStore(newTestDB(t))
	require.NoError(t, st.PutRaw("bucket-a", []byte("<x/>")))
	require.NoError(t, st.PutRaw("bucket-c", []byte("<x/>")))
	require.NoError(t, st.PutRaw("bucket-b", []byte("<x/>")))
	got, err := st.ListBuckets()
	require.NoError(t, err)
	assert.Equal(t, []string{"bucket-a", "bucket-b", "bucket-c"}, got) // Badger iterates keys in lexical order
}

func TestStore_ListBuckets_Empty(t *testing.T) {
	st := NewStore(newTestDB(t))
	got, err := st.ListBuckets()
	require.NoError(t, err)
	assert.Empty(t, got)
}
