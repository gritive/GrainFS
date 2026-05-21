package mountsastore_test

import (
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/iam/mountsastore"
)

func newTestStore(t *testing.T) *mountsastore.Store {
	t.Helper()
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true).WithLogger(nil))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	s, err := mountsastore.NewStore(db)
	require.NoError(t, err)
	return s
}

func TestStore_CreateAndGet(t *testing.T) {
	s := newTestStore(t)

	sa := mountsastore.MountSA{Name: "alice", NumericUID: 1001, CreatedAt: 1700000000, CreatedBy: "admin"}
	require.NoError(t, s.ApplyCreate(sa))

	got, ok := s.Get("alice")
	require.True(t, ok)
	require.Equal(t, sa, got)
}

func TestStore_CreateDuplicateIdempotent(t *testing.T) {
	s := newTestStore(t)

	sa := mountsastore.MountSA{Name: "bob", NumericUID: 1002, CreatedAt: 1700000000}
	require.NoError(t, s.ApplyCreate(sa))
	require.NoError(t, s.ApplyCreate(sa))

	got, ok := s.Get("bob")
	require.True(t, ok)
	require.Equal(t, sa, got)
}

func TestStore_DeleteMissingError(t *testing.T) {
	s := newTestStore(t)

	err := s.ApplyDelete("nonexistent")
	require.Error(t, err)
}

func TestStore_ListAll(t *testing.T) {
	s := newTestStore(t)

	sa1 := mountsastore.MountSA{Name: "alice", NumericUID: 1001, CreatedAt: 1700000000}
	sa2 := mountsastore.MountSA{Name: "bob", NumericUID: 1002, CreatedAt: 1700000001}
	require.NoError(t, s.ApplyCreate(sa1))
	require.NoError(t, s.ApplyCreate(sa2))

	all := s.ListAll()
	require.Len(t, all, 2)
}
