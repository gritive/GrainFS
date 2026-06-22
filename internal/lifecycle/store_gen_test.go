package lifecycle

import (
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true).WithLogger(nil))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return NewStore(db)
}

func TestStore_PutRawBumpGen_MonotonicFromZero(t *testing.T) {
	s := newTestStore(t)
	g0, err := s.GetGen("b")
	require.NoError(t, err)
	require.Equal(t, uint64(0), g0, "absent record has generation 0")

	require.NoError(t, s.PutRawBumpGen("b", []byte("<x/>")))
	g1, _ := s.GetGen("b")
	require.Equal(t, uint64(1), g1, "first put → gen 1")

	require.NoError(t, s.PutRawBumpGen("b", []byte("<y/>")))
	g2, _ := s.GetGen("b")
	require.Equal(t, uint64(2), g2, "second put → gen 2")

	raw, err := s.GetRaw("b")
	require.NoError(t, err)
	require.Equal(t, []byte("<y/>"), raw, "XML still byte-for-byte after gen bumps")
}

func TestStore_DeleteIfGen_CAS(t *testing.T) {
	s := newTestStore(t)
	require.NoError(t, s.PutRawBumpGen("b", []byte("<x/>"))) // gen 1

	// Stale observed gen (0) must NOT delete.
	require.NoError(t, s.DeleteIfGen("b", 0))
	raw, _ := s.GetRaw("b")
	require.Equal(t, []byte("<x/>"), raw, "CAS mismatch must keep the record")

	// Matching observed gen deletes both XML and gen.
	require.NoError(t, s.DeleteIfGen("b", 1))
	raw, _ = s.GetRaw("b")
	require.Nil(t, raw, "matching CAS deletes the record")
	g, _ := s.GetGen("b")
	require.Equal(t, uint64(0), g, "generation removed with the record")
}

func TestStore_DeleteIfGen_Unconditional(t *testing.T) {
	s := newTestStore(t)
	require.NoError(t, s.PutRawBumpGen("b", []byte("<x/>"))) // gen 1
	require.NoError(t, s.DeleteIfGen("b", UnconditionalDeleteGen))
	raw, _ := s.GetRaw("b")
	require.Nil(t, raw, "unconditional sentinel deletes regardless of generation")
}
