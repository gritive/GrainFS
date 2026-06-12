package badgermeta_test

import (
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/gritive/GrainFS/internal/badgermeta"
	"github.com/gritive/GrainFS/internal/badgerutil"
	"github.com/gritive/GrainFS/internal/metastore"
	"github.com/gritive/GrainFS/internal/metastore/storetest"
	"github.com/stretchr/testify/require"
)

func openBadgerStore(t *testing.T) *badgermeta.Store {
	t.Helper()
	db, err := badger.Open(badgerutil.SmallOptions(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return badgermeta.Wrap(db)
}

func TestBadgerStoreConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) metastore.Store {
		return openBadgerStore(t)
	})
}
